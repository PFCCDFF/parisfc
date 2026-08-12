"""
parsing_utils.py

Fonctions de parsing PURES (sans dépendance à Streamlit, à st.session_state,
ou aux variables globales de l'app) extraites de paris_football_club.py.

Importé à la fois par :
- paris_football_club.py (l'app Streamlit)
- sync_drive_to_supabase.py (le job de sync, hors Streamlit)

Objectif : une seule source de vérité pour le parsing des noms, dates et
fichiers GPS/tactiques, pour ne jamais avoir deux logiques qui divergent.
"""

import os
import re
import unicodedata
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

# ------------------------------------------------------------
# Constantes
# ------------------------------------------------------------

# Colonnes attendues dans un export GF1 (balise GPS actuelle).
# Si demain un autre capteur est utilisé, is_gf1_export_format()
# retournera False pour ce nouveau format — prévoir alors un
# is_xxx_export_format() équivalent.
GPS_GF1_REQUIRED = {
    "Activity Date",
    "Capteur",
    "Numéro de joueur",
    "Nom de joueur",
    "Temps joué",
    "Distance (m)",
    "Distance par plage de vitesse (13-15 km/h)",
    "Distance par plage de vitesse (15-19 km/h)",
    "Distance par plage de vitesse (19-23 km/h)",
    "Distance par plage de vitesse (23-25 km/h)",
    "Distance par plage de vitesse (>25 km/h)",
}


# ------------------------------------------------------------
# Normalisation de texte / noms
# ------------------------------------------------------------

def normalize_str(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.split()).lower()
    return s


def nettoyer_nom_joueuse(nom):
    if not isinstance(nom, str):
        nom = str(nom) if nom is not None else ""
    s = nom.strip().upper()
    s = (
        s.replace("É", "E").replace("È", "E").replace("Ê", "E")
        .replace("À", "A").replace("Ù", "U").replace("Î", "I")
        .replace("Ï", "I").replace("Ô", "O").replace("Ö", "O")
        .replace("Â", "A").replace("Ä", "A").replace("Ç", "C")
    )
    s = " ".join(s.split())

    # Déduplication : "NOM PRENOM NOM PRENOM" → "NOM PRENOM"
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        if len(parts) > 1 and parts[0] == parts[1]:
            s = parts[0]
    else:
        words = s.split()
        n = len(words)
        for split_at in range(1, n):
            left = " ".join(words[:split_at])
            right = " ".join(words[split_at:])
            if left == right:
                s = left
                break

    return s


def nom_tokens(nom: str) -> frozenset:
    """Ensemble des tokens d'un nom, pour comparaison ordre-indépendante."""
    return frozenset(nettoyer_nom_joueuse(nom).split())


# ------------------------------------------------------------
# Extraction de dates depuis des noms de fichiers
# ------------------------------------------------------------

def extract_any_date_from_string(s: str):
    """Extrait une date depuis un nom de fichier / label, plusieurs formats.

    Supporte : 27-01-2026, 27/01/2026, 27.01.2026, 27-01-26, 2026-01-27,
    20260127. Retourne un pandas.Timestamp (naïf) ou None.
    """
    if not s:
        return None
    txt = str(s)

    patterns = [
        # dd-mm-yyyy / dd.mm.yyyy / dd/mm/yyyy
        # NB : (?<!\d)/(?!\d) remplacent \b — \b échoue quand la date est
        # précédée d'un "_" (underscore = caractère de mot en regex, donc
        # pas de frontière entre "_" et un chiffre). Très fréquent dans
        # nos noms de fichiers tactiques.
        r'(?<!\d)(?P<d>\d{1,2})[\-\./](?P<m>\d{1,2})[\-\./](?P<y>\d{4})(?!\d)',
        # yyyy-mm-dd / yyyy.mm.dd / yyyy/mm/dd
        r'(?<!\d)(?P<y>\d{4})[\-\./](?P<m>\d{1,2})[\-\./](?P<d>\d{1,2})(?!\d)',
        # dd-mm-yy / dd.mm.yy / dd/mm/yy
        r'(?<!\d)(?P<d>\d{1,2})[\-\./](?P<m>\d{1,2})[\-\./](?P<y>\d{2})(?!\d)',
        # yyyymmdd
        r'(?<!\d)(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})(?!\d)',
    ]

    for pat in patterns:
        m = re.search(pat, txt)
        if not m:
            continue
        gd = m.groupdict()
        try:
            y = int(gd['y'])
            mth = int(gd['m'])
            d = int(gd['d'])
            if y < 100:
                y = 2000 + y if y <= 69 else 1900 + y
            return pd.Timestamp(year=y, month=mth, day=d)
        except Exception:
            continue

    return None


def parse_date_from_gf1_filename(fn: str) -> Optional[datetime]:
    base = os.path.basename(fn)
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{2,4})", base)
    if not m:
        return None
    d, mo, y = m.group(1), m.group(2), m.group(3)
    if len(y) == 2:
        y = "20" + y
    try:
        return datetime(int(y), int(mo), int(d))
    except Exception:
        return None


def parse_week_from_gf1_filename(fn: str) -> Optional[int]:
    if not fn:
        return None
    base = os.path.basename(str(fn))
    m = re.search(r"\bS(\d{1,2})\b", base, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        w = int(m.group(1))
        if 1 <= w <= 53:
            return w
    except Exception:
        return None
    return None


# ------------------------------------------------------------
# Lecture CSV robuste (encodage / séparateur variables)
# ------------------------------------------------------------

def read_csv_auto(path: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "latin1"]
    seps = [",", ";", "\t"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            for bad_lines in ("error", "skip"):
                try:
                    df = pd.read_csv(path, encoding=enc, sep=sep, on_bad_lines=bad_lines)
                    if df.shape[1] == 1 and sep != "\t":
                        break
                    return df
                except Exception as e:
                    last_err = e
                    if bad_lines == "error":
                        continue
                    break
    raise last_err if last_err else ValueError(f"Impossible de lire le CSV: {path}")


# ------------------------------------------------------------
# GPS — détection et standardisation du format GF1
# ------------------------------------------------------------

def is_gf1_export_format(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    cols = set(map(str, df.columns))
    return len(GPS_GF1_REQUIRED.intersection(cols)) >= 8


def standardize_gps_gf1_export(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Normalise un export GF1 brut vers un schéma de colonnes commun,
    quel que soit le contexte (séance ou match)."""
    if df is None or df.empty:
        return df
    d = df.copy()

    rename_map = {
        "Activity Date": "DATE",
        "Nom de joueur": "NOM",
        "Temps joué": "Durée_min",
        "Distance (m)": "Distance (m)",
        "Distance HID (>13 km/h)": "Distance HID (>13 km/h)",
        "Distance HID (>19 km/h)": "Distance HID (>19 km/h)",
        "# of Sprints (>23 km/h)": "Sprints_23",
        "# of Sprints (>25 km/h)": "Sprints_25",
        "Vitesse max (km/h)": "Vitesse max (km/h)",
        "Accélération maximale (m/s²)": "Accélération maximale (m/s²)",
        "#accel/decel": "#accel/decel",
    }
    for k, v in list(rename_map.items()):
        if k in d.columns:
            d = d.rename(columns={k: v})

    if "Durée_min" in d.columns:
        def _parse_hmmss(val):
            s = str(val).strip()
            if not s or s.lower() in ("nan", "none", ""):
                return np.nan
            try:
                return float(s)
            except ValueError:
                pass
            parts = s.split(":")
            try:
                if len(parts) == 3:
                    h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
                    return round(h * 60 + m + sec / 60, 1)
                elif len(parts) == 2:
                    m, sec = int(parts[0]), int(parts[1])
                    return round(m + sec / 60, 1)
            except Exception:
                pass
            return np.nan
        d["Durée_min"] = d["Durée_min"].apply(_parse_hmmss)

    if "DATE" in d.columns:
        _dates = pd.to_datetime(d["DATE"], errors="coerce", utc=True)
        d["DATE"] = _dates.dt.tz_localize(None).dt.normalize()
    else:
        dt = parse_date_from_gf1_filename(filename)
        d["DATE"] = pd.Timestamp(dt.date()) if dt else pd.NaT

    d["SEMAINE"] = d["DATE"].dt.isocalendar().week.astype("Int64")
    w_file = parse_week_from_gf1_filename(filename)
    if w_file is not None:
        d["SEMAINE"] = pd.Series([w_file] * len(d), index=d.index, dtype="Int64")

    for c in ["Distance (m)", "Sprints_23", "Sprints_25", "Vitesse max (km/h)",
              "Accélération maximale (m/s²)", "#accel/decel"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    def _num(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    d["V_0_7"] = _num("Distance par plage de vitesse (0-7 km/h)")
    d["V_7_13"] = _num("Distance par plage de vitesse (7-13 km/h)")
    d["V_13_15"] = _num("Distance par plage de vitesse (13-15 km/h)")
    d["V_15_19"] = _num("Distance par plage de vitesse (15-19 km/h)")
    d["V_19_23"] = _num("Distance par plage de vitesse (19-23 km/h)")
    d["V_23_25"] = _num("Distance par plage de vitesse (23-25 km/h)")
    d["V_sup25"] = _num("Distance par plage de vitesse (>25 km/h)")

    for col_orig, col_std in [
        ("# of Accelerations (>2 m/s²)", "Acc_2"),
        ("# of Accelerations (>3 m/s²)", "Acc_3"),
        ("# of Accelerations (>4 m/s²)", "Acc_4"),
        ("# of Decélerations (>2 m/s²)", "Dec_2"),
        ("# of Decélerations (>3 m/s²)", "Dec_3"),
        ("# of Decélerations (>4 m/s²)", "Dec_4"),
    ]:
        if col_orig in df.columns:
            d[col_std] = pd.to_numeric(df[col_orig], errors="coerce")

    d["__source_file"] = os.path.basename(filename)
    return d


# ------------------------------------------------------------
# GPS match — détection et infos depuis le nom de fichier
# ------------------------------------------------------------

def is_gps_match_file(filename: str) -> bool:
    """Détecte un fichier GPS de match (par opposition à une séance)."""
    fn = normalize_str(filename)
    match_patterns = ["u19", "u17", "u16", "u15", "_j0", "_j1", "_j2", " vs ", "match", "contre"]
    seance_patterns = ["seance", "séance", "entrainement", "entraînement"]
    has_match = any(p in fn for p in match_patterns)
    has_seance = any(p in fn for p in seance_patterns)
    return has_match and not has_seance


def parse_match_info_from_filename(filename: str) -> dict:
    """Extrait adversaire, date et journée depuis le nom de fichier GPS match."""
    name = os.path.splitext(filename)[0]
    info = {"adversaire": "", "date": None, "journee": "", "label": name}

    date_match = re.search(r'(\d{2})[_\-](\d{2})[_\-](\d{2,4})', name)
    if date_match:
        d, m, y = date_match.groups()
        y = "20" + y if len(y) == 2 else y
        try:
            info["date"] = pd.Timestamp(f"{y}-{m}-{d}")
        except Exception:
            pass

    j_match = re.search(r'[_\-](J\d+)[_\-]', name, re.IGNORECASE)
    if j_match:
        info["journee"] = j_match.group(1).upper()

    adv_match = re.search(
        r'Paris[_\s]FC[_\s\-]+([^_\-\d]+)|([^_\-\d]+)[_\s\-]+Paris[_\s]FC',
        name, re.IGNORECASE)
    if adv_match:
        adv = (adv_match.group(1) or adv_match.group(2) or "").strip().strip("_- ")
        adv = re.sub(r'\b(U19|U17|U16|U15|J\d+)\b', '', adv, flags=re.IGNORECASE).strip().strip("_- ")
        info["adversaire"] = adv[:40] if adv else ""

    date_str = info["date"].strftime("%d/%m/%Y") if info["date"] else ""
    parts = [p for p in [info["journee"], info["adversaire"], date_str] if p]
    info["label"] = " · ".join(parts) if parts else name

    return info


# ------------------------------------------------------------
# Tactique (tagging vidéo Sportscode) — détection et infos fichier
# ------------------------------------------------------------

def is_tactical_file(filename: str) -> bool:
    """Détecte un fichier tactique : PFC_VS__ dans le nom."""
    fn = normalize_str(filename)
    return fn.startswith(normalize_str("PFC_VS")) or "pfc_vs" in fn


def parse_tactical_filename(filename: str) -> dict:
    """Extrait date, adversaire, journée et saison depuis le nom d'un fichier tactique."""
    name = os.path.splitext(os.path.basename(filename))[0]
    info = {"date": None, "journee": "", "adversaire": "", "adv_norm": "", "saison": "", "label": name}

    sais_m = re.search(r'(?<![0-9])(\d{2})(\d{2})(?![0-9])', name)
    if sais_m:
        a1, a2 = int(sais_m.group(1)), int(sais_m.group(2))
        if 20 <= a1 <= 30 and a2 == a1 + 1:
            info["saison"] = f"{a1}/{a2}"

    dt = extract_any_date_from_string(name)
    if dt is not None and pd.notna(dt):
        try:
            info["date"] = pd.Timestamp(dt).normalize()
        except Exception:
            pass

    pj_m = re.search(r'[_\s]P\d+J(\d{1,2})[_\s]', name, re.IGNORECASE)
    j_m = re.search(r'[_\s\-]J0*(\d{1,2})[_\s\-]', name, re.IGNORECASE)
    if pj_m:
        info["journee"] = pj_m.group(1).zfill(2)
    elif j_m:
        info["journee"] = j_m.group(1).zfill(2)

    adv = None
    adv_m = re.search(
        r'PFC_VS_[\s_]+\d{4}[\s_]+\w+[\s_]+([A-Za-z\u00C0-\u024F][\w\s]+?)[\s_]+(?:P\d+)?J\d+',
        name, re.IGNORECASE)
    if adv_m:
        adv = adv_m.group(1).replace("_", " ").strip()
    if not adv:
        m2 = re.search(r'Paris[_\s]+FC[_\s]*-[_\s]*([A-Za-z\u00C0-\u024F][A-Za-z\u00C0-\u024F_\s]+)', name, re.IGNORECASE)
        if m2:
            adv = m2.group(1).replace("_", " ").strip()
            adv = re.sub(r'\s*\d+\s*', ' ', adv).strip()
    if not adv:
        m3 = re.search(r'([A-Za-z\u00C0-\u024F][A-Za-z\u00C0-\u024F_\s]+?)[_\s]*-[_\s]*Paris', name, re.IGNORECASE)
        if m3:
            adv = m3.group(1).replace("_", " ").strip()
            adv = re.sub(r'\s*\d+\s*', ' ', adv).strip()
    if adv:
        adv = re.sub(r'\b(U19F?|U17|U16|U15|NAT|CSV)\b', '', adv, flags=re.IGNORECASE)
        adv = re.sub(r'\s{2,}', ' ', adv).strip(" _-")
        info["adversaire"] = adv
        info["adv_norm"] = normalize_str(adv)

    return info


# ------------------------------------------------------------
# GPS — construction des métriques + zones de vitesse pour Supabase
# ------------------------------------------------------------

# Colonnes standardisées -> clés attendues par sessions_gps (voir schema_supabase_parisfc.sql)
_GPS_METRIC_COLS = {
    "Durée_min": "temps_joue_min",
    "Distance (m)": "distance_totale",
    "Distance HID (>13 km/h)": "distance_hid_13",
    "Distance HID (>19 km/h)": "distance_hid_19",
    "Vitesse max (km/h)": "vitesse_max",
    "Accélération maximale (m/s²)": "acceleration_max",
    "Sprints_23": "nb_sprints_23",
    "Sprints_25": "nb_sprints_25",
    "Acc_2": "nb_acc_2",
    "Acc_3": "nb_acc_3",
    "Acc_4": "nb_acc_4",
    "Dec_2": "nb_dec_2",
    "Dec_3": "nb_dec_3",
    "Dec_4": "nb_dec_4",
}

# Bornes des plages de vitesse du format GF1 (km/h)
_GPS_ZONES_GF1 = [
    ("V_0_7", 0, 7),
    ("V_7_13", 7, 13),
    ("V_13_15", 13, 15),
    ("V_15_19", 15, 19),
    ("V_19_23", 19, 23),
    ("V_23_25", 23, 25),
    ("V_sup25", 25, None),
]


def row_to_gps_metrics(row: pd.Series) -> dict:
    """Convertit une ligne standardisée (post standardize_gps_gf1_export)
    en dict prêt pour sessions_gps. Les champs bruts non couverts par le
    schéma partent dans donnees_brutes (jsonb)."""
    metrics = {}
    for src_col, dest_key in _GPS_METRIC_COLS.items():
        if src_col in row.index and pd.notna(row[src_col]):
            metrics[dest_key] = float(row[src_col])

    date_val = row.get("DATE")
    metrics["date_activite"] = (
        pd.Timestamp(date_val).isoformat() if pd.notna(date_val) else None
    )

    # Champs bruts non modélisés explicitement (ex: capteur brut, semaine)
    metrics["donnees_brutes"] = {
        "semaine": int(row["SEMAINE"]) if "SEMAINE" in row.index and pd.notna(row["SEMAINE"]) else None,
        "source_file": row.get("__source_file"),
    }
    return metrics


def row_to_zones_vitesse(row: pd.Series) -> list[dict]:
    """Convertit une ligne standardisée en liste de zones de vitesse
    prêtes pour zones_vitesse_gps."""
    zones = []
    for col, vmin, vmax in _GPS_ZONES_GF1:
        if col in row.index and pd.notna(row[col]):
            zones.append({
                "vitesse_min": vmin,
                "vitesse_max": vmax,
                "distance": float(row[col]),
            })
    return zones


# ------------------------------------------------------------
# Tactique — conversion d'une ligne brute CSV en événement + tags
# ------------------------------------------------------------

# Colonnes "stables" qui vont dans evenements_match ; tout le reste
# de la ligne (Passe, Duel défensifs, Tir, Destination passe, Zone
# Création du deséquilibre, etc.) part en tags clé/valeur.
_EVENEMENT_CORE_COLS = {
    "Timeline": "timeline",
    "Start time": "start_time",
    "Duration": "duration",
    "Instance number": "instance_number",
    "Action": "action",
    "Poste": "poste",
    "Poste receveuse": "poste_receveuse",
    "X_localisation": "x_localisation",
    "Y_localisation": "y_localisation",
    "Issue d'action": "issue_action",
}


def deduce_categorie_from_filename(filename: str, default: str = "U19F") -> str:
    """Déduit la catégorie (ex. U19F) depuis un nom de fichier Drive.

    Cherche un motif Uxx suivi optionnellement de F/M. À défaut, retourne
    `default` — à ajuster le jour où plusieurs catégories cohabitent
    vraiment dans le même dossier Drive.
    """
    m = re.search(r'\bU(1[5-9]|2[0-3])\s*([FM])?\b', filename, re.IGNORECASE)
    if not m:
        return default
    age = m.group(1)
    sexe = (m.group(2) or "F").upper()
    return f"U{age}{sexe}"


def row_to_evenement_and_tags(row: pd.Series) -> tuple[dict, dict]:
    """Convertit une ligne brute du CSV tactique en (evenement, tags).

    NB : la colonne 'Row' (ex. 'MC 1', 'DCD', 'GB') identifie un profil de
    poste, pas une joueuse nommée dans ce format d'export — elle est donc
    stockée comme tag 'Row', pas comme joueuse_id.
    """
    evenement = {}
    for src_col, dest_key in _EVENEMENT_CORE_COLS.items():
        val = row.get(src_col)
        if pd.notna(val):
            evenement[dest_key] = val

    for k in ("start_time", "duration", "instance_number", "x_localisation", "y_localisation"):
        if k in evenement:
            try:
                evenement[k] = float(evenement[k])
            except (TypeError, ValueError):
                evenement.pop(k)

    tags = {}
    for col in row.index:
        if col in _EVENEMENT_CORE_COLS:
            continue
        val = row.get(col)
        if pd.notna(val) and str(val).strip() != "":
            tags[col] = val

    return evenement, tags
