"""
kine_utils.py

Séances de kiné / réathlétisation (table Supabase `seances_kine`) : listes de la saisie
(reprises de l'onglet « Saisie Séances » du classeur « Suivi kiné 26-27 U19N.xlsx »), lecture
de l'historique Excel pour l'import, empreinte anti-doublon, indicateurs de suivi.
Sans dépendance Streamlit (tests : tests/test_kine_utils.py).
"""

from __future__ import annotations

import hashlib
import io
import unicodedata
from typing import Optional

import pandas as pd

TYPES_SEANCE = [
    "Renforcement musculaire", "Massage / Récupération", "Travail cardio (vélo)", "Bilan / Évaluation",
    "Récupération / Drainage", "Retour terrain", "Strapping / Tape", "Proprioception / Stabilité",
    "Travail de mobilité", "Réathlétisation", "Travail préventif", "Cryothérapie",
    "Rééducation fonctionnelle", "Repos général",
]
# Colonne « Compte comme rééducation ? » de l'onglet _listes du classeur
COMPTE_REEDUCATION = {
    "Renforcement musculaire": True, "Massage / Récupération": False, "Travail cardio (vélo)": True,
    "Bilan / Évaluation": True, "Récupération / Drainage": False, "Retour terrain": True,
    "Strapping / Tape": False, "Proprioception / Stabilité": True, "Travail de mobilité": True,
    "Réathlétisation": True, "Travail préventif": False, "Cryothérapie": False,
    "Rééducation fonctionnelle": True, "Repos général": False,
}
_ZONES_COTE = ["Cheville", "Pied", "Mollet", "Tibia", "Genou", "Cuisse antérieure (Quadriceps)",
               "Cuisse postérieure (Ischios)", "Adducteurs", "Hanche", "Fessier", "Épaule", "Bras",
               "Coude", "Avant-bras", "Poignet/Main"]
ZONES = [f"{z} {c}" for z in _ZONES_COTE for c in ("D", "G", "Bilatéral")] + [
    "Bassin", "Lombaires", "Dos", "Thorax", "Cervicales", "Tête", "Abdominaux", "Général / Corps entier"]
TYPES_BLESSURE = ["Surcharge", "Aiguë", "Chronique"]
DISPONIBILITES = ["Disponible", "Indisponible", "À réévaluer"]
KINES = ["Jordan", "Baptiste"]

# Colonnes de la table ↔ en-têtes du classeur
COLONNES_EXCEL = {
    "date": "Date", "joueuse": "Joueuse", "type_seance": "Type de Séance", "zone": "Zone / Région",
    "type_blessure": "Type de blessure", "disponibilite": "Disponibilité lendemain",
    "observations": "Observations / Notes Kiné", "kine": "Kiné",
}


def _norm(s) -> str:
    s = "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.lower().split())


def _texte(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    v = str(v).strip()
    return v or None


def empreinte(ligne: dict) -> str:
    """Identifiant d'une séance (date, joueuse, type, zone, kiné, observations) : un même
    fichier importé deux fois — ou une séance déjà saisie dans l'app — n'est pas doublé."""
    d = pd.Timestamp(ligne.get("date")).date().isoformat() if ligne.get("date") is not None else ""
    cle = "|".join([d] + [_norm(ligne.get(k)) for k in ("joueuse", "type_seance", "zone", "kine", "observations")])
    return hashlib.sha1(cle.encode("utf-8")).hexdigest()


def lire_classeur(contenu: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Onglet « Saisie Séances » du classeur Excel → (séances prêtes à importer, lignes rejetées).
    La ligne d'en-tête (Date / Joueuse / …) est recherchée dans les premières lignes.
    Rejet : date ou joueuse manquante (motif dans la colonne « motif »)."""
    xls = pd.ExcelFile(io.BytesIO(contenu))
    feuille = next((s for s in xls.sheet_names if "saisie" in _norm(s)), None)
    if feuille is None:
        raise ValueError("onglet « Saisie Séances » introuvable dans le classeur")
    brut = pd.read_excel(xls, sheet_name=feuille, header=None)
    entete = None
    for i in range(min(15, len(brut))):
        vals = {_norm(v) for v in brut.iloc[i].tolist()}
        if "date" in vals and "joueuse" in vals:
            entete = i
            break
    if entete is None:
        raise ValueError("ligne d'en-tête (Date, Joueuse…) introuvable")
    df = pd.read_excel(xls, sheet_name=feuille, header=entete)
    par_nom = {_norm(c): c for c in df.columns}
    out = pd.DataFrame()
    for col, titre in COLONNES_EXCEL.items():
        src = par_nom.get(_norm(titre))
        out[col] = df[src] if src is not None else None
    out = out.dropna(how="all")
    out["date"] = pd.to_datetime(out["date"], errors="coerce", dayfirst=True)
    for col in COLONNES_EXCEL:
        if col != "date":
            out[col] = out[col].map(_texte)
    out["ligne_excel"] = out.index + entete + 2  # numéro de ligne tel qu'affiché dans Excel
    motif = pd.Series("", index=out.index)
    motif[out["joueuse"].isna()] = "joueuse manquante"
    motif[out["date"].isna()] = (motif[out["date"].isna()] + " · date manquante").str.strip(" ·")
    rejets = out[motif != ""].assign(motif=motif[motif != ""])
    ok = out[motif == ""].copy()
    ok["empreinte"] = [empreinte(r) for r in ok.to_dict("records")]
    ok = ok.drop_duplicates("empreinte")
    return ok.reset_index(drop=True), rejets.reset_index(drop=True)


def lignes_supabase(df: pd.DataFrame, saisi_par: str = "") -> list[dict]:
    """Séances (lire_classeur ou saisie) → lignes à insérer dans seances_kine."""
    rows = []
    for r in df.to_dict("records"):
        row = {c: r.get(c) for c in COLONNES_EXCEL}
        row["date"] = pd.Timestamp(r["date"]).date().isoformat()
        row["empreinte"] = r.get("empreinte") or empreinte(r)
        row["saisi_par"] = saisi_par or None
        rows.append({k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row.items()})
    return rows


# ── Suivi ──────────────────────────────────────────────────────────────────

def statut_equipe(df: pd.DataFrame, aujourd_hui=None) -> pd.DataFrame:
    """Une ligne par joueuse suivie : dernière séance (date, zone, blessure, disponibilité,
    kiné), séances sur 7 et 30 jours, total. Indisponibles d'abord, puis plus récentes."""
    cols = ["Joueuse", "Dernière séance", "Disponibilité", "Zone", "Blessure", "Kiné",
            "7 j", "30 j", "Total"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    ref = pd.Timestamp(aujourd_hui or pd.Timestamp.today()).normalize()
    d = df.sort_values(["date", "id"] if "id" in df.columns else ["date"])
    out = []
    for j, g in d.groupby("joueuse"):
        der = g.iloc[-1]
        out.append({
            "Joueuse": j, "Dernière séance": der["date"].date(), "Disponibilité": der.get("disponibilite") or "",
            "Zone": der.get("zone") or "", "Blessure": der.get("type_blessure") or "", "Kiné": der.get("kine") or "",
            "7 j": int((g["date"] > ref - pd.Timedelta(days=7)).sum()),
            "30 j": int((g["date"] > ref - pd.Timedelta(days=30)).sum()),
            "Total": int(len(g)),
        })
    res = pd.DataFrame(out, columns=cols)
    ordre = {"Indisponible": 0, "À réévaluer": 1, "Disponible": 2}
    res["_o"] = res["Disponibilité"].map(ordre).fillna(3)
    return res.sort_values(["_o", "Dernière séance"], ascending=[True, False]).drop(columns="_o").reset_index(drop=True)


def resume_joueuse(df: pd.DataFrame) -> dict:
    """Séances d'une joueuse → nb, nb de rééducation, première / dernière date, dernière
    disponibilité, zones (Counter trié), types (Counter trié)."""
    if df is None or df.empty:
        return {"n": 0, "n_reeduc": 0, "premiere": None, "derniere": None, "dispo": "", "zones": [], "types": []}
    d = df.sort_values("date")
    return {
        "n": int(len(d)),
        "n_reeduc": int(d["type_seance"].map(lambda t: COMPTE_REEDUCATION.get(t, False)).sum()),
        "premiere": d["date"].iloc[0].date(), "derniere": d["date"].iloc[-1].date(),
        "dispo": d["disponibilite"].dropna().iloc[-1] if d["disponibilite"].notna().any() else "",
        "zones": list(d["zone"].dropna().value_counts().items()),
        "types": list(d["type_seance"].dropna().value_counts().items()),
    }


def seances_par_semaine(df: pd.DataFrame) -> pd.DataFrame:
    """Nombre de séances par semaine (lundi) et type de séance — pour un graphique empilé."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["semaine", "type_seance", "n"])
    d = df.assign(semaine=df["date"].dt.to_period("W-SUN").dt.start_time,
                  type_seance=df["type_seance"].fillna("Non précisé"))
    return d.groupby(["semaine", "type_seance"]).size().reset_index(name="n")


# Zones détaillées du classeur (côté compris) → zones du schéma corporel du bilan médical
# (MEDICAL_ZONES_CORPS de l'app). None = pas de zone sur le schéma (Général / Corps entier).
_ZONE_CORPS = [
    (("cheville", "pied"), "Cheville/Pied"), (("mollet", "tibia"), "Mollet/Tibia"), (("genou",), "Genou"),
    (("cuisse",), "Cuisse"), (("adducteur", "hanche", "fessier", "bassin"), "Hanche/Aine"),
    (("epaule",), "Épaule"), (("avant-bras", "bras", "coude"), "Bras/Coude"), (("poignet", "main"), "Poignet/Main"),
    (("lombaire", "dos"), "Dos"), (("thorax", "abdo"), "Thorax/Abdomen"), (("cervical",), "Cou"), (("tete",), "Tête"),
]


def zone_corps(zone) -> Optional[str]:
    """« Cheville G » → « Cheville/Pied », « Adducteurs D » → « Hanche/Aine »…"""
    z = _norm(zone)
    if not z:
        return None
    for mots, cible in _ZONE_CORPS:
        if any(z.startswith(m) or f" {m}" in f" {z}" for m in mots):
            return cible
    return None
