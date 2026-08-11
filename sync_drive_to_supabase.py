"""
sync_drive_to_supabase.py

Job autonome (lancé par systemd timer) qui :
1. Récupère les nouveaux fichiers CSV depuis Drive (réutilise la sync existante)
2. Parse les fichiers GPS et match (réutilise le parsing existant de l'app,
   ex. extract_any_date_from_string, normalisation des noms de joueuses)
3. Upsert les données normalisées dans Supabase

Identifiants Supabase attendus dans un fichier .env à côté de ce script :
    SUPABASE_URL=https://pzguyqhwcofmjdqvjnmj.supabase.co
    SUPABASE_SERVICE_KEY=sb_secret_v-965dY30pFXE61o2....

Installation :
    pip install supabase python-dotenv
"""

import logging
import os
from datetime import datetime, date
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# ------------------------------------------------------------
# TODO : réutiliser les modules existants de l'app plutôt que
# les stubs ci-dessous. Exemple attendu :
#
# from parsing_utils import (
#     extract_any_date_from_string,
#     normalize_player_name,
#     parse_gps_export,     # -> liste de dicts par joueuse/activité
#     parse_match_export,   # -> liste de dicts par événement
# )
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sync_drive_to_supabase")


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

def get_or_create_joueuse(sb: Client, nom_complet: str, categorie: str,
                           poste: Optional[str] = None) -> str:
    nom_complet = nom_complet.strip()  # supposé déjà normalisé en amont
    existing = (
        sb.table("joueuses")
        .select("id")
        .eq("nom_complet", nom_complet)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    inserted = (
        sb.table("joueuses")
        .insert({
            "nom_complet": nom_complet,
            "categorie": categorie,
            "poste": poste,
        })
        .execute()
    )
    return inserted.data[0]["id"]


def get_or_create_capteur(sb: Client, nom: str, format_source: str = "") -> str:
    existing = sb.table("capteurs_gps").select("id").eq("nom", nom).execute()
    if existing.data:
        return existing.data[0]["id"]
    inserted = (
        sb.table("capteurs_gps")
        .insert({"nom": nom, "format_source": format_source})
        .execute()
    )
    return inserted.data[0]["id"]


def get_or_create_match(sb: Client, match_date: date, adversaire: str,
                         categorie: str, **extra) -> str:
    existing = (
        sb.table("matchs")
        .select("id")
        .eq("date", match_date.isoformat())
        .eq("adversaire", adversaire)
        .eq("categorie", categorie)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    payload = {
        "date": match_date.isoformat(),
        "adversaire": adversaire,
        "categorie": categorie,
        **extra,
    }
    inserted = sb.table("matchs").insert(payload).execute()
    return inserted.data[0]["id"]


def get_or_create_entrainement(sb: Client, session_date: date, categorie: str,
                                type_seance: Optional[str] = None) -> str:
    existing = (
        sb.table("entrainements")
        .select("id")
        .eq("date", session_date.isoformat())
        .eq("categorie", categorie)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    inserted = (
        sb.table("entrainements")
        .insert({
            "date": session_date.isoformat(),
            "categorie": categorie,
            "type_seance": type_seance,
        })
        .execute()
    )
    return inserted.data[0]["id"]


# ------------------------------------------------------------
# Upsert des données GPS
# ------------------------------------------------------------

def upsert_session_gps(sb: Client, joueuse_id: str, capteur_id: str,
                        match_id: Optional[str], entrainement_id: Optional[str],
                        metrics: dict, zones_vitesse: list[dict]) -> str:
    """
    metrics attend les clés : date_activite, temps_joue, distance_totale,
    distance_hid_13, distance_hid_19, vitesse_max, acceleration_max,
    nb_sprints_23, nb_sprints_25, nb_acc_2/3/4, nb_dec_2/3/4, donnees_brutes
    zones_vitesse : liste de {"vitesse_min":, "vitesse_max":, "distance":}
    """
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
        # on repart de zéro pour cette session à chaque sync, plus simple
        # qu'un upsert ligne à ligne sur des tranches de vitesse
        sb.table("zones_vitesse_gps").delete().eq("session_gps_id", session_id).execute()
        rows = [{**z, "session_gps_id": session_id} for z in zones_vitesse]
        sb.table("zones_vitesse_gps").insert(rows).execute()

    return session_id


# ------------------------------------------------------------
# Upsert des données de match (tagging vidéo)
# ------------------------------------------------------------

def upsert_evenement_match(sb: Client, match_id: str, joueuse_id: Optional[str],
                            evenement: dict, tags: dict) -> str:
    """
    evenement attend les clés : timeline, start_time, duration, instance_number,
    action, poste, poste_receveuse, x_localisation, y_localisation, issue_action
    tags : dict des colonnes qualitatives du tagging (clé -> valeur)
    """
    payload = {"match_id": match_id, "joueuse_id": joueuse_id, **evenement}
    result = sb.table("evenements_match").insert(payload).execute()
    evenement_id = result.data[0]["id"]

    if tags:
        rows = [
            {"evenement_id": evenement_id, "cle": cle, "valeur": str(valeur)}
            for cle, valeur in tags.items()
            if valeur not in (None, "")
        ]
        if rows:
            sb.table("evenement_tags").insert(rows).execute()

    return evenement_id


# ------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------

def sync_fichier_gps(sb: Client, filepath: str, categorie: str) -> None:
    logger.info("Traitement GPS : %s", filepath)
    # TODO : remplacer par parse_gps_export(filepath) qui retourne une liste
    # de dicts {joueuse, match_date/entrainement_date, capteur, metrics, zones}
    raise NotImplementedError("Brancher ici le parsing GPS existant de l'app")


def sync_fichier_match(sb: Client, filepath: str, categorie: str) -> None:
    logger.info("Traitement match : %s", filepath)
    # TODO : remplacer par parse_match_export(filepath) qui retourne
    # (match_info, liste d'événements avec leurs tags)
    raise NotImplementedError("Brancher ici le parsing match existant de l'app")


def main():
    sb = get_client()

    # TODO : remplacer par la logique de détection des nouveaux fichiers
    # Drive déjà en place dans l'app (incrémental, cache local, etc.)
    nouveaux_fichiers_gps: list[str] = []
    nouveaux_fichiers_match: list[str] = []

    for f in nouveaux_fichiers_gps:
        try:
            sync_fichier_gps(sb, f, categorie="U19F")  # catégorie à déduire du chemin/nom
        except Exception:
            logger.exception("Échec sync GPS pour %s", f)

    for f in nouveaux_fichiers_match:
        try:
            sync_fichier_match(sb, f, categorie="U19F")
        except Exception:
            logger.exception("Échec sync match pour %s", f)

    logger.info("Sync terminée.")


if __name__ == "__main__":
    main()
