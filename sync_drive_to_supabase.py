"""
sync_drive_to_supabase.py

Job autonome (lancé par systemd timer) qui :
1. Récupère les nouveaux fichiers CSV depuis Drive (réutilise la sync existante)
2. Parse les fichiers GPS et match via parsing_utils.py (partagé avec l'app)
3. Upsert les données normalisées dans Supabase

Identifiants Supabase attendus dans un fichier .env à côté de ce script :
    SUPABASE_URL=https://xxxxx.supabase.co
    SUPABASE_SERVICE_KEY=eyJ...        <- clé service_role (jamais la clé anon ici)

Installation :
    pip install supabase python-dotenv
"""

import logging
import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

from parsing_utils import (
    read_csv_auto,
    is_gf1_export_format,
    standardize_gps_gf1_export,
    is_gps_match_file,
    parse_match_info_from_filename,
    is_tactical_file,
    parse_tactical_filename,
    nettoyer_nom_joueuse,
    normalize_str,
    row_to_gps_metrics,
    row_to_zones_vitesse,
    row_to_evenement_and_tags,
    deduce_categorie_from_filename,
)
from drive_utils import fetch_new_csv_files

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sync_drive_to_supabase")

CAPTEUR_GF1_NOM = "GF1"  # à ajuster/étendre le jour où un 2e capteur arrive

# Labels de ligne qui ne désignent pas une joueuse (marqueurs d'équipe/début
# de période) — mêmes valeurs que celles exclues par _adv_rows côté
# paris_football_club.py:5365. L'adversaire du match s'ajoute dynamiquement
# (nom variable d'un match à l'autre).
_ROW_LABELS_EQUIPE = {"pfc", "start", ""}


def _extract_row_brut(row_val) -> str:
    """Convertit la valeur brute de la colonne Row (peut être NaN pandas sur
    une cellule vide) en chaîne. str(NaN) vaudrait "nan" et créerait une
    fausse joueuse "NAN" côté get_or_create_joueuse — d'où le pd.isna."""
    return "" if pd.isna(row_val) else str(row_val).strip()


def _is_row_joueuse(row_brut: str, adversaire: str) -> bool:
    """True si row_brut désigne une joueuse PFC (pas un marqueur d'équipe
    ni l'adversaire)."""
    norm = normalize_str(row_brut or "")
    if norm in _ROW_LABELS_EQUIPE:
        return False
    if norm == normalize_str(adversaire or ""):
        return False
    return True


def get_client() -> Client:
    load_dotenv()
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


# ------------------------------------------------------------
# Résolution des référentiels (joueuses, capteurs, matchs, entraînements)
# Chaque fonction fait un get-or-create pour éviter les doublons
# si le script tourne plusieurs fois sur les mêmes fichiers.
# ------------------------------------------------------------

def get_or_create_joueuse(sb: Client, nom_complet: str, categorie: str) -> str:
    nom_complet = nettoyer_nom_joueuse(nom_complet)
    existing = (
        sb.table("joueuses").select("id").eq("nom_complet", nom_complet).execute()
    )
    if existing.data:
        return existing.data[0]["id"]
    inserted = (
        sb.table("joueuses")
        .insert({"nom_complet": nom_complet, "categorie": categorie})
        .execute()
    )
    return inserted.data[0]["id"]


def get_or_create_capteur(sb: Client, nom: str) -> str:
    existing = sb.table("capteurs_gps").select("id").eq("nom", nom).execute()
    if existing.data:
        return existing.data[0]["id"]
    inserted = sb.table("capteurs_gps").insert({"nom": nom, "format_source": "gf1"}).execute()
    return inserted.data[0]["id"]


def get_or_create_match(sb: Client, match_date, adversaire: str, categorie: str,
                         journee: str = "") -> str:
    date_iso = pd.Timestamp(match_date).date().isoformat()
    existing = (
        sb.table("matchs")
        .select("id")
        .eq("date", date_iso)
        .eq("adversaire", adversaire)
        .eq("categorie", categorie)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]
    inserted = (
        sb.table("matchs")
        .insert({
            "date": date_iso,
            "adversaire": adversaire,
            "categorie": categorie,
            "journee": journee,
        })
        .execute()
    )
    return inserted.data[0]["id"]


def get_or_create_entrainement(sb: Client, session_date, categorie: str) -> str:
    date_iso = pd.Timestamp(session_date).date().isoformat()
    existing = (
        sb.table("entrainements")
        .select("id")
        .eq("date", date_iso)
        .eq("categorie", categorie)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]
    inserted = (
        sb.table("entrainements")
        .insert({"date": date_iso, "categorie": categorie})
        .execute()
    )
    return inserted.data[0]["id"]


# ------------------------------------------------------------
# Upsert des données GPS
# ------------------------------------------------------------

def upsert_session_gps(sb: Client, joueuse_id: str, capteur_id: str,
                        match_id: Optional[str], entrainement_id: Optional[str],
                        metrics: dict, zones_vitesse: list[dict]) -> str:
    payload = {
        "joueuse_id": joueuse_id,
        "capteur_id": capteur_id,
        "match_id": match_id,
        "entrainement_id": entrainement_id,
        **metrics,
    }
    result = (
        sb.table("sessions_gps")
        .upsert(payload, on_conflict="joueuse_id,match_id,entrainement_id,capteur_id")
        .execute()
    )
    session_id = result.data[0]["id"]

    if zones_vitesse:
        sb.table("zones_vitesse_gps").delete().eq("session_gps_id", session_id).execute()
        rows = [{**z, "session_gps_id": session_id} for z in zones_vitesse]
        sb.table("zones_vitesse_gps").insert(rows).execute()

    return session_id


# ------------------------------------------------------------
# Upsert des données de match (tagging vidéo)
# ------------------------------------------------------------

def upsert_evenement_match(sb: Client, match_id: str, evenement: dict, tags: dict) -> str:
    payload = {"match_id": match_id, "tags": tags, **evenement}
    result = sb.table("evenements_match").insert(payload).execute()
    evenement_id = result.data[0]["id"]

    if tags:  # inchangé : écriture EAV conservée en parallèle de la colonne tags
        rows = [
            {"evenement_id": evenement_id, "cle": cle, "valeur": str(valeur)}
            for cle, valeur in tags.items()
        ]
        sb.table("evenement_tags").insert(rows).execute()

    return evenement_id


# ------------------------------------------------------------
# Orchestration — un fichier GPS
# ------------------------------------------------------------

def sync_fichier_gps(sb: Client, filepath: str, categorie: str) -> None:
    filename = os.path.basename(filepath)
    logger.info("Traitement GPS : %s", filename)

    df = read_csv_auto(filepath)
    if not is_gf1_export_format(df):
        logger.warning("Format GPS non reconnu, ignoré : %s", filename)
        return

    df = standardize_gps_gf1_export(df, filename)
    if "NOM" not in df.columns:
        logger.warning("Colonne NOM absente après standardisation : %s", filename)
        return

    capteur_id = get_or_create_capteur(sb, CAPTEUR_GF1_NOM)

    est_match = is_gps_match_file(filename)
    match_id = None
    entrainement_id = None
    if est_match:
        minfo = parse_match_info_from_filename(filename)
        match_id = get_or_create_match(
            sb,
            match_date=minfo["date"] or df["DATE"].iloc[0],
            adversaire=minfo["adversaire"] or "Inconnu",
            categorie=categorie,
            journee=minfo["journee"],
        )
    else:
        entrainement_id = get_or_create_entrainement(
            sb, session_date=df["DATE"].iloc[0], categorie=categorie
        )

    for _, row in df.iterrows():
        nom = row.get("NOM")
        if not nom or str(nom).strip() == "":
            continue
        joueuse_id = get_or_create_joueuse(sb, nom, categorie)
        metrics = row_to_gps_metrics(row)
        zones = row_to_zones_vitesse(row)
        upsert_session_gps(sb, joueuse_id, capteur_id, match_id, entrainement_id, metrics, zones)

    logger.info("GPS OK : %s (%d lignes)", filename, len(df))


# ------------------------------------------------------------
# Orchestration — un fichier match (tagging vidéo)
# ------------------------------------------------------------

def sync_fichier_match(sb: Client, filepath: str, categorie: str) -> None:
    filename = os.path.basename(filepath)
    logger.info("Traitement match : %s", filename)

    if not is_tactical_file(filename):
        logger.warning("Fichier non reconnu comme tactique, ignoré : %s", filename)
        return

    df = read_csv_auto(filepath)
    minfo = parse_tactical_filename(filename)
    if not minfo["date"]:
        logger.warning("Date introuvable dans le nom de fichier, ignoré : %s", filename)
        return

    match_id = get_or_create_match(
        sb,
        match_date=minfo["date"],
        adversaire=minfo["adversaire"] or "Inconnu",
        categorie=categorie,
        journee=minfo["journee"],
    )
    adversaire = minfo["adversaire"] or "Inconnu"

    count = 0
    for _, row in df.iterrows():
        evenement, tags = row_to_evenement_and_tags(row)
        if not evenement.get("action"):
            continue  # ligne vide / sans action exploitable
        row_brut = _extract_row_brut(row.get("Row"))
        evenement["row_brut"] = row_brut
        if _is_row_joueuse(row_brut, adversaire):
            evenement["joueuse_id"] = get_or_create_joueuse(sb, row_brut, categorie)
        upsert_evenement_match(sb, match_id, evenement, tags)
        count += 1

    logger.info("Match OK : %s (%d événements)", filename, count)


# ------------------------------------------------------------
# Point d'entrée
# ------------------------------------------------------------

def main():
    sb = get_client()

    logger.info("Recherche des nouveaux fichiers sur Drive...")
    nouveaux_fichiers = fetch_new_csv_files()
    logger.info("%d nouveau(x) fichier(s) trouvé(s) sur Drive.", len(nouveaux_fichiers))

    for f in nouveaux_fichiers:
        filename = os.path.basename(f)
        categorie = deduce_categorie_from_filename(filename)
        try:
            if is_tactical_file(filename):
                sync_fichier_match(sb, f, categorie)
            else:
                sync_fichier_gps(sb, f, categorie)
        except Exception:
            logger.exception("Échec sync pour %s", f)

    logger.info("Sync terminée.")


if __name__ == "__main__":
    main()
