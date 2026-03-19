# ============================================================
# PARIS FC - DATA CENTER (Streamlit)
# - PFC Matchs (CSV): stats + temps de jeu via segments Duration
# - EDF U19: comparaison vs référentiel EDF (moyenne par poste)
# - Référentiel noms: "Noms Prénoms Paris FC.xlsx"
# - GPS Entraînement: fichiers "GF1 ... .csv" (exports Drive, lecture robuste)
# ============================================================

import os
import io
import re
import unicodedata
import warnings
from typing import Any, Dict, List, Optional, Set, Tuple
from difflib import get_close_matches, SequenceMatcher
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from streamlit_option_menu import option_menu
from mplsoccer import PyPizza, Radar, FontManager, grid
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import time
import json
import textwrap

warnings.filterwarnings("ignore")

# =========================
# CONFIG
# =========================
DATA_FOLDER = "data"
PASSERELLE_FOLDER = "data/passerelle"
GPS_FOLDER = "data/gps"

# Dossiers Drive
DRIVE_MAIN_FOLDER_ID = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
DRIVE_PASSERELLE_FOLDER_ID = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"
DRIVE_GPS_FOLDER_ID = "1v4Iit4JlEDNACp2QWQVrP89j66zBqMFH"

# Photos joueuses (Drive)
DRIVE_PHOTOS_FOLDER_ID = "1h-BwepZc96K7VpidPiy8FEqNiE10GLdE"
PHOTOS_FOLDER_ID = DRIVE_PHOTOS_FOLDER_ID  # alias rétro-compat
PHOTOS_FOLDER = "data/photos"
PHOTO_MAPPING_PATH = "data/photo_mapping.json"  # mapping manuel persistant : canon → filename
GPS_NAME_MAP_PATH  = "data/gps_name_map.json"   # concordance GPS nom → nom canonique tactique

# Fichiers attendus
PERMISSIONS_FILENAME = "Classeurs permissions streamlit.xlsx"
EDF_JOUEUSES_FILENAME = "EDF_Joueuses.xlsx"
PASSERELLE_FILENAME = "Liste Joueuses Passerelles.xlsx"
REFERENTIEL_FILENAME = "Noms Prénoms Paris FC.xlsx"
OBJECTIFS_EVAL_FILENAME = "Evaluations Objectifs.csv"  # Export CSV du Google Sheet lié au Forms
DRIVE_OBJECTIFS_FOLDER_ID = ""  # À renseigner : ID du dossier Drive contenant le CSV des évaluations
OBJECTIFS_FOLDER = "data/objectifs"

# Colonnes "poste" dans les lignes match (lineups)
POST_COLS = ["ATT", "DCD", "DCG", "DD", "DG", "GB", "MCD", "MCG", "MD", "MDef", "MG"]

BAD_TOKENS = {"CORNER", "COUP-FRANC", "COUP FRANC", "PENALTY", "CARTON", "CARTONS"}
GPS_GF1_PREFIX = "GF1"
GPS_MATCH_FOLDER = "data/gps_match"
TACTICAL_FOLDER = "data"        # Les fichiers tactiques sont dans le dossier data principal
DRIVE_TACTICAL_FOLDER_ID = ""   # À renseigner si dossier Drive dédié
DRIVE_GPS_MATCH_FOLDER_ID = "1jzLW_jR5sMtsP4lOb4mN9mJlthw3pvbu"  # Dossier Drive GPS Match
DRIVE_LOGOS_FOLDER_ID = "1TCKyVOHzKynm6Z1fhKnNUKYDcN7NhMCj"  # Logos clubs adversaires
LOGOS_FOLDER = "data/logos"  # Cache local

# =========================
# UTILS
# =========================
def normalize_str(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.split()).lower()
    return s


def find_local_file_by_normalized_name(folder: str, target_name: str) -> Optional[str]:
    if not os.path.exists(folder):
        return None
    target_norm = normalize_str(target_name)
    for fn in os.listdir(folder):
        if normalize_str(fn) == target_norm:
            return os.path.join(folder, fn)
    return None


def safe_float(x, default=np.nan) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def safe_int_numeric_only(df: pd.DataFrame, round_first: bool = True) -> pd.DataFrame:
    """Evite les ValueError sur astype(int) si colonnes non-numériques."""
    if df is None or df.empty:
        return df
    out = df.copy()
    num_cols = out.select_dtypes(include=[np.number]).columns
    if len(num_cols) > 0:
        if round_first:
            out[num_cols] = out[num_cols].round()
        out[num_cols] = out[num_cols].fillna(0)
        out[num_cols] = out[num_cols].astype(int)
    return out


def _sanitize_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit TOUS les types incompatibles avec Excel en types sérialisables."""
    import numpy as np
    import datetime
    df = df.copy()

    EXCEL_SAFE = (str, int, float, bool, type(None), datetime.datetime, datetime.date)

    def _safe(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        # Datetime avec timezone → retirer le tzinfo (Excel ne supporte pas)
        if isinstance(v, datetime.datetime) and v.tzinfo is not None:
            return v.replace(tzinfo=None)
        if isinstance(v, datetime.date):
            return v
        if isinstance(v, EXCEL_SAFE):
            return v
        if hasattr(v, "item"):           # numpy scalar → python natif
            try: return v.item()
            except Exception: pass
        if isinstance(v, pd.Timestamp):
            try:
                ts = v.tz_localize(None) if v.tzinfo is not None else v
                return ts.to_pydatetime()
            except Exception: return str(v)
        if isinstance(v, np.datetime64):
            try: return pd.Timestamp(v).tz_localize(None).to_pydatetime()
            except Exception: return str(v)
        if isinstance(v, type(pd.NaT)):
            return None
        # Tout le reste (list, dict, set, timedelta, etc.) → str
        return str(v)

    for col in df.columns:
        # Retirer le timezone des colonnes datetime avec tz directement (plus rapide)
        try:
            if hasattr(df[col], "dt") and df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_localize(None)
                continue
        except Exception:
            pass
        try:
            df[col] = df[col].apply(_safe)
        except Exception:
            df[col] = df[col].astype(str)
    return df


def build_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        used = set()
        for name, df in sheets.items():
            if df is None:
                continue
            sheet = (str(name) or "Sheet1")[:31]
            base = sheet
            k = 1
            while sheet in used:
                suffix = f"_{k}"
                sheet = (base[:31 - len(suffix)] + suffix)[:31]
                k += 1
            used.add(sheet)
            if isinstance(df, pd.DataFrame) and not df.empty:
                _sanitize_df_for_excel(df).to_excel(writer, sheet_name=sheet, index=False)
            else:
                pd.DataFrame().to_excel(writer, sheet_name=sheet, index=False)
    output.seek(0)
    return output.read()


def nettoyer_nom_joueuse(nom):
    if not isinstance(nom, str):
        nom = str(nom) if nom is not None else ""
    s = nom.strip().upper()
    s = (
        s.replace("É", "E")
        .replace("È", "E")
        .replace("Ê", "E")
        .replace("À", "A")
        .replace("Ù", "U")
        .replace("Î", "I")
        .replace("Ï", "I")
        .replace("Ô", "O")
        .replace("Ö", "O")
        .replace("Â", "A")
        .replace("Ä", "A")
        .replace("Ç", "C")
    )
    s = " ".join(s.split())
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    if len(parts) > 1 and parts[0] == parts[1]:
        return parts[0]
    return s


def nom_tokens(nom: str) -> frozenset:
    """Retourne l'ensemble des tokens (mots) d'un nom, pour comparaison ordre-indépendante.
    Ex: 'Sharlie YERRO' et 'YERRO Sharlie' donnent le même frozenset.
    """
    return frozenset(nettoyer_nom_joueuse(nom).split())


def extract_any_date_from_string(s: str):
    """Extract a date from a filename / label with many possible formats.

    Supported examples:
    - 27-01-2026, 27/01/2026, 27.01.2026
    - 27-01-26, 27.01.26 (assumes 2000-2069 for yy<=69, else 1900s)
    - 2026-01-27
    - 20260127
    Returns pandas.Timestamp (naive) or None.
    """
    if not s:
        return None
    txt = str(s)

    patterns = [
        # dd-mm-yyyy / dd.mm.yyyy / dd/mm/yyyy
        r'(?P<d>\b\d{1,2})[\-\./](?P<m>\d{1,2})[\-\./](?P<y>\d{4})\b',
        # yyyy-mm-dd / yyyy.mm.dd / yyyy/mm/dd
        r'\b(?P<y>\d{4})[\-\./](?P<m>\d{1,2})[\-\./](?P<d>\d{1,2})\b',
        # dd-mm-yy / dd.mm.yy / dd/mm/yy
        r'(?P<d>\b\d{1,2})[\-\./](?P<m>\d{1,2})[\-\./](?P<y>\d{2})\b',
        # yyyymmdd
        r'\b(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})\b',
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
                # heuristic
                y = 2000 + y if y <= 69 else 1900 + y
            return pd.Timestamp(year=y, month=mth, day=d)
        except Exception:
            continue

    return None

def nettoyer_nom_equipe(nom: str) -> str:
    if nom is None:
        return ""
    s = str(nom).strip().upper()
    s = (
        s.replace("É", "E")
        .replace("È", "E")
        .replace("Ê", "E")
        .replace("À", "A")
        .replace("Ù", "U")
        .replace("Î", "I")
        .replace("Ï", "I")
        .replace("Ô", "O")
        .replace("Ö", "O")
        .replace("Â", "A")
        .replace("Ä", "A")
        .replace("Ç", "C")
    )
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        s = parts[0] if parts else s
    s = " ".join(s.split())
    return s


def looks_like_player(name: str) -> bool:
    n = nettoyer_nom_joueuse(str(name)) if name is not None else ""
    if not n or n in {"NAN", "NONE", "NULL"}:
        return False
    if any(tok in n for tok in BAD_TOKENS):
        return False
    if len(n) <= 2:
        return False
    if re.search(r"\d", n):
        return False
    return True


def split_if_comma(cell: str) -> List[str]:
    if cell is None:
        return []
    s = str(cell).strip()
    if not s or s.upper() in {"NAN", "NONE", "NULL"}:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts if len(parts) > 1 else [s]


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


def extract_season_from_filename(filename: str) -> Optional[str]:
    if not filename:
        return None
    s = str(filename)
    candidates = re.findall(r"\b\d{4}\b", s)
    for c in candidates:
        if c in {"2425", "2526"}:
            return c
    m = re.search(r"(2425|2526)", s)
    return m.group(1) if m else None


# =========================
# NAME NORMALIZATION
# =========================
PARTICLES = {"DE", "DU", "DES", "D", "DA", "DI", "DEL", "DELA", "DELLA", "LE", "LA", "LES"}

def strip_accents_upper(s: str) -> str:
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper()

def normalize_name_raw(s: str) -> str:
    s = strip_accents_upper(s)
    s = s.replace(",", " ")
    s = s.replace("'", "'")
    s = re.sub(r"[^A-Z' -]", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    toks = s.split()
    if len(toks) >= 2 and toks[0] == toks[1]:
        toks = toks[1:]
    return " ".join(toks)

def tokens_name(s: str) -> List[str]:
    s = normalize_name_raw(s)
    if not s:
        return []
    toks = s.split()
    out: List[str] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t == "D" and i + 1 < len(toks):
            out.append("D" + toks[i + 1])
            i += 2
            continue
        out.append(t)
        i += 1
    return out

def compact_name(s: str) -> str:
    s = strip_accents_upper(s)
    s = re.sub(r"[^A-Z]", "", s)
    return s

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def infer_opponent_from_columns(df: pd.DataFrame, equipe_pfc: str) -> Optional[str]:
    if df is None or df.empty:
        return None

    pfc_clean = nettoyer_nom_equipe(equipe_pfc)
    banned_clean = {nettoyer_nom_equipe(x) for x in ["ADVERSAIRE", "TEAMERSAIRE", "TEAMVERSAIRE", "OPPONENT", "OPPOSANT"]}

    for col in ["Adversaire", "Teamersaire"]:
        if col not in df.columns:
            continue

        s_raw = df[col].dropna().astype(str).map(lambda x: x.strip())
        s_raw = s_raw[s_raw != ""]
        if s_raw.empty:
            continue

        tmp = pd.DataFrame({"raw": s_raw})
        tmp["clean"] = tmp["raw"].map(nettoyer_nom_equipe)

        tmp = tmp[tmp["clean"] != ""]
        tmp = tmp[tmp["clean"] != pfc_clean]
        tmp = tmp[~tmp["clean"].isin(banned_clean)]
        tmp = tmp[~tmp["raw"].map(lambda x: looks_like_player(x))]

        if tmp.empty:
            continue

        clean_choice = tmp["clean"].value_counts().index[0]
        raw_choice = tmp.loc[tmp["clean"] == clean_choice, "raw"].value_counts().index[0]
        return raw_choice.strip()

    return None


def infer_opponent_from_filename(filename: str, equipe_pfc: str) -> Optional[str]:
    if not filename:
        return None
    base = os.path.splitext(os.path.basename(filename))[0]
    parts = base.split("_")
    if len(parts) >= 3:
        token = parts[2].strip()
        words = token.split()
        if words:
            opp = words[-1].strip()
            if opp and normalize_str(opp) != normalize_str(equipe_pfc):
                return opp
    return None


# =========================
# EXCEL READER
# =========================
def read_excel_auto(path: str, sheet_name=0) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xls":
        return pd.read_excel(path, sheet_name=sheet_name, engine="xlrd")
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


# =========================
# =========================
# PHOTOS — concordance 3 sources :
#   Liste déroulante  →  Référentiel (NOM + Prénom)  →  Fichier photo Drive
# =========================
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif"}

def _ensure_photos_folder():
    os.makedirs(PHOTOS_FOLDER, exist_ok=True)

def _quick_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def _photo_normalize(s: str) -> str:
    """Normalisation de base : supprime accents, majuscules, ponctuation → espaces."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    # tirets, underscores, virgules, points → espace
    s = re.sub(r"[-_,.]", " ", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _photo_key_spaced(s: str) -> str:
    """Clé normalisée avec espaces — utilisée dans l'index."""
    return _photo_normalize(s)

def _photo_key_compact(s: str) -> str:
    """Clé sans espaces — pour match exact compact."""
    return re.sub(r"\s+", "", _photo_normalize(s))

# rétrocompat
def _norm_txt(s: str) -> str:
    return _photo_key_spaced(s)


# ------------------------------------------------------------------
# GÉNÉRATION DES VARIANTES de nom à partir de NOM + Prénom
# ------------------------------------------------------------------
def _generate_name_variants(nom: str, prenom: str) -> List[str]:
    """
    Génère toutes les formes plausibles d'un nom de fichier photo
    à partir du NOM et du Prénom extraits du référentiel.

    Couvre les conventions mixtes courantes :
      - DUPONT ALICE  /  ALICE DUPONT
      - DUPONT_ALICE  /  ALICE_DUPONT
      - Dupont Alice  /  Alice Dupont
      - dupont alice  /  alice dupont
      - DUPONT        (nom seul)
      - ALICE DUPONT  (prénom d'abord)
      ... et toutes les combinaisons tiret/underscore
    """
    nom = str(nom or "").strip()
    prenom = str(prenom or "").strip()

    # Normalisation de base (sans accents)
    def _n(s):
        s = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in s if not unicodedata.combining(ch))

    n  = _n(nom).upper()
    p  = _n(prenom).upper()
    nc = nom.capitalize()   # Dupont
    pc = prenom.capitalize() # Alice

    seps = [" ", "_", "-", ""]
    variants = set()

    for sep in seps:
        if n and p:
            variants.add(f"{n}{sep}{p}")       # DUPONT ALICE
            variants.add(f"{p}{sep}{n}")       # ALICE DUPONT
            variants.add(f"{nc}{sep}{pc}")     # Dupont Alice
            variants.add(f"{pc}{sep}{nc}")     # Alice Dupont
            variants.add(f"{n}{sep}{pc}")      # DUPONT Alice
            variants.add(f"{nc}{sep}{p}")      # Dupont ALICE

        if n:
            variants.add(n)                    # DUPONT
            variants.add(nc)                   # Dupont

        if p:
            variants.add(p)                    # ALICE
            variants.add(pc)                   # Alice

    return [v for v in variants if v]


def _score_photo_vs_variants(photo_stem: str, variants: List[str]) -> float:
    """
    Score max entre le stem du fichier photo et toutes les variantes générées.
    Utilise 3 méthodes :
      1. Égalité exacte après normalisation → score 1.0
      2. Égalité compact (sans espaces/séparateurs) → score 0.95
      3. Ratio SequenceMatcher → score continu
    """
    stem_norm    = _photo_normalize(photo_stem)   # ex: "DUPONT ALICE"
    stem_compact = _photo_key_compact(photo_stem) # ex: "DUPONTALICE"
    stem_tokens  = set(stem_norm.split())

    best = 0.0
    for v in variants:
        v_norm    = _photo_normalize(v)
        v_compact = re.sub(r"\s+", "", v_norm)
        v_tokens  = set(v_norm.split())

        # 1. Exact normalisé
        if stem_norm == v_norm:
            return 1.0

        # 2. Exact compact
        if stem_compact == v_compact and stem_compact:
            best = max(best, 0.95)
            continue

        # 3. Ratio chaîne
        r = _quick_ratio(stem_norm, v_norm)

        # 4. Jaccard tokens (bonus si tous les tokens du nom sont dans le stem)
        if v_tokens and stem_tokens:
            inter = len(v_tokens & stem_tokens)
            union = len(v_tokens | stem_tokens)
            jaccard = inter / union if union else 0.0
            full_match_bonus = 0.15 if v_tokens <= stem_tokens or stem_tokens <= v_tokens else 0.0
            token_bonus = 0.08 if inter >= 1 else 0.0
            r = 0.55 * r + 0.30 * jaccard + full_match_bonus + token_bonus

        best = max(best, r)

    return best



# ------------------------------------------------------------------
# MAPPING MANUEL PERSISTANT  :  canon → nom_de_fichier_photo
# Stocké dans PHOTO_MAPPING_PATH (JSON), survit aux redémarrages
# ------------------------------------------------------------------
def load_photo_manual_mapping() -> Dict[str, str]:
    """Charge le mapping manuel depuis le fichier JSON local."""
    if not os.path.exists(PHOTO_MAPPING_PATH):
        return {}
    try:
        with open(PHOTO_MAPPING_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_photo_manual_mapping(mapping: Dict[str, str]) -> None:
    """Sauvegarde le mapping manuel dans le fichier JSON local."""
    os.makedirs(os.path.dirname(PHOTO_MAPPING_PATH), exist_ok=True)
    with open(PHOTO_MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────────────────────────
# CONCORDANCE GPS ↔ NOMS CANONIQUES
# ──────────────────────────────────────────────────────────────────
def load_gps_name_map() -> Dict[str, str]:
    """Charge le mapping GPS nom → canon depuis JSON. {gps_nom_clean: canon_tactique}"""
    if not os.path.exists(GPS_NAME_MAP_PATH):
        return {}
    try:
        with open(GPS_NAME_MAP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_gps_name_map(mapping: Dict[str, str]) -> None:
    os.makedirs(os.path.dirname(GPS_NAME_MAP_PATH), exist_ok=True)
    with open(GPS_NAME_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def get_gps_name_map() -> Dict[str, str]:
    m = st.session_state.get("gps_name_map")
    if m is None:
        m = load_gps_name_map()
        st.session_state["gps_name_map"] = m
    return m


def apply_gps_name_map(gps_nom_raw: str) -> str:
    """Retourne le nom canonique si une concordance manuelle existe, sinon le nom original."""
    m = get_gps_name_map()
    key = normalize_name_raw(str(gps_nom_raw))
    return m.get(key, gps_nom_raw)


def auto_gps_concordance(gps_names: list, tac_names: list) -> Dict[str, str]:
    """Propose des correspondances automatiques GPS → tactique par token-set matching.
    Retourne {gps_nom_clean: canon_tactique_clean} pour les cas trouvés automatiquement."""
    import difflib
    result = {}
    tac_clean = {normalize_name_raw(n): n for n in tac_names}
    tac_tokens = {normalize_name_raw(n): set(normalize_name_raw(n).split()) for n in tac_names}

    for gps in gps_names:
        gps_c = normalize_name_raw(gps)
        gps_toks = set(gps_c.split())

        # 1. Exact match
        if gps_c in tac_clean:
            result[gps_c] = gps_c; continue

        # 2. Token-set: tous les tokens GPS sont dans la cible (noms composés partiels)
        best_canon, best_score = None, 0
        for tc, toks in tac_tokens.items():
            common = gps_toks & toks
            if len(common) == 0: continue
            # Score = mots communs / max(len GPS, len TAC)
            score = len(common) / max(len(gps_toks), len(toks))
            if score > best_score:
                best_score = score
                best_canon = tc

        if best_canon and best_score >= 0.60:
            result[gps_c] = best_canon; continue

        # 3. Fuzzy fallback
        matches = difflib.get_close_matches(gps_c, list(tac_clean.keys()), n=1, cutoff=0.75)
        if matches:
            result[gps_c] = matches[0]

    return result


def render_gps_concordance_ui(gps_match_df, tac_players: list):
    """Affiche l'interface de concordance GPS ↔ noms tactiques dans les paramètres."""
    st.markdown("#### 🔗 Concordance GPS ↔ Noms tactiques")
    st.caption(
        "Le GPS exporte les noms au format **Prénom NOM** parfois tronqués (ex: 'Lana BOUDINE' vs 'BOUDINE FAERBER Lana'). "
        "Ici tu peux forcer la correspondance manuellement."
    )

    if gps_match_df is None or getattr(gps_match_df, "empty", True):
        st.info("Aucune donnée GPS match chargée.")
        return

    # Noms GPS disponibles
    gps_raw_names = sorted(
        gps_match_df["NOM"].dropna().astype(str).str.strip().unique().tolist()
    ) if "NOM" in gps_match_df.columns else []

    if not gps_raw_names:
        st.warning("Aucun nom trouvé dans la colonne 'NOM' du GPS.")
        return

    current_map = get_gps_name_map()
    # Propositions auto (ne pas écraser les manuelles)
    auto_map = auto_gps_concordance(gps_raw_names, tac_players)

    # Merge: manual overrides auto
    effective_map = {**auto_map, **{normalize_name_raw(k): v for k, v in current_map.items()}}

    st.markdown("**Correspondances détectées / configurées**")

    # Afficher chaque nom GPS avec un selectbox
    tac_options = ["(aucune correspondance)"] + sorted(tac_players)
    updated_map = {}
    changed = False

    cols_header = st.columns([3, 3, 2])
    cols_header[0].markdown("**Nom dans le GPS**")
    cols_header[1].markdown("**Nom tactique associé**")
    cols_header[2].markdown("**Statut**")

    for gps_name in gps_raw_names:
        gps_c = normalize_name_raw(gps_name)
        current_tac = effective_map.get(gps_c, None)
        manual_set = gps_c in {normalize_name_raw(k) for k in current_map}
        auto_found = gps_c in auto_map

        if current_tac and current_tac in tac_options:
            idx = tac_options.index(current_tac)
        else:
            # Try to find by normalized
            matches_norm = [t for t in tac_options if normalize_name_raw(t) == (normalize_name_raw(current_tac) if current_tac else "")]
            idx = tac_options.index(matches_norm[0]) if matches_norm else 0

        cols = st.columns([3, 3, 2])
        cols[0].markdown(f"`{gps_name}`")
        sel = cols[1].selectbox(
            label="",
            options=tac_options,
            index=idx,
            key=f"gps_map_{gps_c}",
            label_visibility="collapsed"
        )
        if sel != "(aucune correspondance)":
            updated_map[gps_c] = sel
            if manual_set:
                cols[2].markdown("✏️ Manuel")
            elif auto_found:
                cols[2].markdown("🤖 Auto")
            else:
                cols[2].markdown("⚠️ Nouveau")
        else:
            cols[2].markdown("❌ Ignoré")

    col_save, col_reset = st.columns(2)
    if col_save.button("💾 Sauvegarder la concordance", type="primary"):
        save_gps_name_map(updated_map)
        st.session_state["gps_name_map"] = updated_map
        st.success(f"✅ {len(updated_map)} correspondances sauvegardées.")
        st.rerun()

    if col_reset.button("🔄 Réinitialiser (auto seulement)"):
        save_gps_name_map(auto_map)
        st.session_state["gps_name_map"] = auto_map
        st.info("Concordance réinitialisée sur les correspondances automatiques.")
        st.rerun()

    # Diagnostic
    with st.expander("🔍 Diagnostic — joueuses non trouvées"):
        mapped_tac = set(updated_map.values())
        not_in_gps = [p for p in tac_players if p not in mapped_tac and normalize_name_raw(p) not in {normalize_name_raw(v) for v in updated_map.values()}]
        if not_in_gps:
            st.warning(f"{len(not_in_gps)} joueuse(s) tactiques sans GPS :")
            for p in not_in_gps:
                st.markdown(f"  - `{p}`")
        else:
            st.success("Toutes les joueuses tactiques ont une correspondance GPS.")

        gps_unmapped = [g for g in gps_raw_names if normalize_name_raw(g) not in updated_map]
        if gps_unmapped:
            st.info(f"{len(gps_unmapped)} nom(s) GPS non associé(s) (remplaçantes non jouées ?) :")
            for g in gps_unmapped:
                st.markdown(f"  - `{g}`")


def set_manual_photo(player_name: str, filename: str) -> None:
    """
    Associe manuellement un fichier photo à une joueuse.
    Persiste dans PHOTO_MAPPING_PATH et met à jour la session.
    """
    canon = normalize_name_raw(player_name)
    mapping = load_photo_manual_mapping()
    mapping[canon] = filename
    save_photo_manual_mapping(mapping)
    # Mettre à jour la session immédiatement
    st.session_state["photo_manual_mapping"] = mapping


def get_manual_photo_path(player_name: str) -> Optional[str]:
    """
    Retourne le chemin photo du mapping manuel si disponible.
    Cherche d'abord en session, sinon charge depuis le JSON.
    """
    mapping = st.session_state.get("photo_manual_mapping")
    if mapping is None:
        mapping = load_photo_manual_mapping()
        st.session_state["photo_manual_mapping"] = mapping

    canon = normalize_name_raw(player_name)
    filename = mapping.get(canon)
    if not filename:
        return None

    path = os.path.join(PHOTOS_FOLDER, filename)
    return path if os.path.exists(path) else None


# ------------------------------------------------------------------
# TABLE DE CONCORDANCE  :  canon → chemin photo
# Construite une fois à partir du référentiel + index photos locaux
# ------------------------------------------------------------------
def build_photo_concordance(
    ref_path: str,
    photos_index: Dict[str, str],   # stem normalisé → filepath
) -> Dict[str, str]:
    """
    Construit un dict  canon_name → photo_filepath

    Étapes :
      1. Charger le référentiel (NOM + Prénom) → extraire toutes les joueuses
      2. Pour chaque joueuse, générer toutes les variantes de nom de fichier
      3. Pour chaque fichier photo dans l'index, scorer contre les variantes
      4. Associer la meilleure photo (score >= seuil) à chaque canon
    """
    concordance: Dict[str, str] = {}

    if not ref_path or not os.path.exists(ref_path):
        return concordance
    if not photos_index:
        return concordance

    # --- Charger le référentiel ---
    try:
        ref_df = read_excel_auto(ref_path)
        if isinstance(ref_df, dict):
            ref_df = list(ref_df.values())[0] if ref_df else pd.DataFrame()
    except Exception:
        return concordance

    if not isinstance(ref_df, pd.DataFrame) or ref_df.empty:
        return concordance

    # Détecter colonnes NOM / Prénom
    cols_up = {str(c).strip().upper(): c for c in ref_df.columns}
    col_nom = cols_up.get("NOM")
    col_pre = (cols_up.get("PRÉNOM") or cols_up.get("PRENOM")
                or cols_up.get("PR\u00c9NOM"))   # unicode safe

    # Fallback : colonne unique "Nom de joueuse"
    col_full = (cols_up.get("NOM DE JOUEUSE")
                or next((c for ck, c in cols_up.items() if "JOUEUSE" in ck), None))

    joueuses: List[Tuple[str, str, str]] = []  # (canon, nom, prenom)

    if col_nom and col_pre:
        for _, row in ref_df.iterrows():
            nom    = str(row.get(col_nom, "") or "").strip()
            prenom = str(row.get(col_pre, "") or "").strip()
            if not nom:
                continue
            canon = normalize_name_raw(f"{nom} {prenom}")
            joueuses.append((canon, nom, prenom))
    elif col_full:
        for _, row in ref_df.iterrows():
            full = str(row.get(col_full, "") or "").strip()
            if not full:
                continue
            parts = full.split()
            nom    = parts[0] if parts else full
            prenom = " ".join(parts[1:]) if len(parts) > 1 else ""
            canon  = normalize_name_raw(full)
            joueuses.append((canon, nom, prenom))
    else:
        return concordance

    # --- Index photos : stem normalisé → filepath ---
    # (l'index passé en paramètre est déjà key_spaced → path)
    photo_stems = list(photos_index.keys())   # ex: ["DUPONT ALICE", "MARTIN LEA", ...]
    photo_paths = list(photos_index.values())

    SEUIL = 0.62  # score minimum pour accepter le match

    for canon, nom, prenom in joueuses:
        variants = _generate_name_variants(nom, prenom)
        best_score = 0.0
        best_path  = None

        for stem, path in zip(photo_stems, photo_paths):
            score = _score_photo_vs_variants(stem, variants)
            if score > best_score:
                best_score = score
                best_path  = path

        if best_path and best_score >= SEUIL:
            concordance[canon] = best_path
            # Aussi indexer les variantes directes pour lookup rapide
            for v in variants:
                canon_v = normalize_name_raw(v)
                if canon_v and canon_v not in concordance:
                    concordance[canon_v] = best_path

    return concordance


# ------------------------------------------------------------------
# INDEX LOCAL  +  SYNC
# ------------------------------------------------------------------
def build_photos_index_local() -> Dict[str, str]:
    """
    Index local : stem normalisé (avec espaces) → filepath.
    Ex: { 'DUPONT ALICE': '/data/photos/Dupont_Alice.jpg', ... }
    """
    _ensure_photos_folder()
    idx: Dict[str, str] = {}
    if not os.path.exists(PHOTOS_FOLDER):
        return idx

    for fn in os.listdir(PHOTOS_FOLDER):
        ext = os.path.splitext(fn)[1].lower()
        if ext not in IMAGE_EXTS:
            continue
        stem = os.path.splitext(fn)[0]
        key  = _photo_key_spaced(stem)
        if not key:
            continue
        path = os.path.join(PHOTOS_FOLDER, fn)
        if key not in idx:
            idx[key] = path
        else:
            try:
                if os.path.getmtime(path) > os.path.getmtime(idx[key]):
                    idx[key] = path
            except Exception:
                pass
    return idx


def photos_get_index(force_sync: bool = False) -> Tuple[Dict[str, str], Dict[str, Any]]:
    status: Dict[str, Any] = {
        "local_folder": PHOTOS_FOLDER,
        "folder_id": PHOTOS_FOLDER_ID,
        "synced": False,
        "n_index": 0,
        "n_local_files": 0,
        "error": None,
    }
    _ensure_photos_folder()

    def _count_local_images() -> int:
        try:
            return sum(1 for fn in os.listdir(PHOTOS_FOLDER)
                       if os.path.splitext(fn)[1].lower() in IMAGE_EXTS)
        except Exception:
            return 0

    idx = build_photos_index_local()
    status["n_local_files"] = _count_local_images()
    status["n_index"] = len(idx)

    if force_sync or len(idx) == 0:
        try:
            sync_photos_from_drive()
            status["synced"] = True
        except Exception as e:
            status["error"] = str(e)
        idx = build_photos_index_local()
        status["n_local_files"] = _count_local_images()
        status["n_index"] = len(idx)

    return idx, status


def get_photo_concordance(force_rebuild: bool = False) -> Dict[str, str]:
    """
    Retourne (et met en cache session) la table de concordance canon → photo.
    La reconstruit si absente, vide, ou si force_rebuild=True.
    """
    cached = st.session_state.get("photo_concordance", {})
    if cached and not force_rebuild:
        return cached

    photos_index = build_photos_index_local()
    if not photos_index:
        return {}

    ref_path = os.path.join(DATA_FOLDER, REFERENTIEL_FILENAME)
    if not os.path.exists(ref_path):
        ref_path = find_local_file_by_normalized_name(DATA_FOLDER, REFERENTIEL_FILENAME)

    concordance = build_photo_concordance(ref_path or "", photos_index)
    st.session_state["photo_concordance"] = concordance
    return concordance


# ------------------------------------------------------------------
# FONCTION PRINCIPALE : trouver la photo d'une joueuse
# ------------------------------------------------------------------
def find_photo_for_player(
    player_name: str,
    concordance: Optional[Dict[str, str]] = None,
    photos_index: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """
    Cherche la photo d'une joueuse en 3 passes :

    Passe 1 — Concordance référentiel (la plus fiable)
        Le canon du nom est cherché dans la table concordance.

    Passe 2 — Lookup direct dans l'index photos
        Comparaison normalisée stem ↔ nom cherché (fallback si hors référentiel).

    Passe 3 — Fuzzy large sur tous les fichiers
        Dernier recours avec seuil très bas pour couvrir les typos.
    """
    if not player_name:
        return None

    # --- Passe 0 : mapping manuel (priorité absolue) ---
    manual = get_manual_photo_path(player_name)
    if manual:
        return manual

    # --- Passe 1 : concordance référentiel ---
    if concordance:
        canon = normalize_name_raw(player_name)
        if canon in concordance:
            p = concordance[canon]
            if os.path.exists(p):
                return p

        # Essai avec les variantes directes du nom brut
        pn_norm = _photo_normalize(player_name)
        for k, path in concordance.items():
            if _photo_normalize(k) == pn_norm and os.path.exists(path):
                return path

    # --- Passe 2 : index photos direct ---
    if photos_index:
        pn = _photo_key_spaced(player_name)

        # Exact
        if pn in photos_index and os.path.exists(photos_index[pn]):
            return photos_index[pn]

        # Compact
        pn_c = _photo_key_compact(player_name)
        for k, path in photos_index.items():
            if re.sub(r"\s+", "", k) == pn_c and os.path.exists(path):
                return path

        # Fuzzy
        pn_tokens = set(pn.split())
        best_path, best_score = None, 0.0
        for k, path in photos_index.items():
            k_tokens = set(k.split())
            inter    = len(pn_tokens & k_tokens)
            union    = max(1, len(pn_tokens | k_tokens))
            jaccard  = inter / union
            subset   = 0.15 if (pn_tokens <= k_tokens or k_tokens <= pn_tokens) else 0.0
            hit      = 0.08 if inter >= 1 else 0.0
            ratio    = _quick_ratio(pn, k)
            score    = 0.50 * ratio + 0.30 * jaccard + subset + hit
            if score > best_score:
                best_score = score
                best_path  = path

        if best_path and best_score >= 0.50 and os.path.exists(best_path):
            return best_path

    return None


# Alias rétrocompat
def find_best_photo_for_player_relaxed(player_name: str, photos_index: Dict[str, str]) -> Optional[str]:
    concordance = st.session_state.get("photo_concordance", {})
    return find_photo_for_player(player_name, concordance=concordance, photos_index=photos_index)

def find_best_photo_for_player(player_name: str, photos_index: Dict[str, str]) -> Optional[str]:
    return find_best_photo_for_player_relaxed(player_name, photos_index)


# ------------------------------------------------------------------
# BLOC AFFICHAGE PHOTO (avec diagnostic)
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# SÉLECTEUR PHOTO MANUEL — grille cliquable dans l'onglet Passerelles
# ------------------------------------------------------------------
def _render_photo_picker(player_name: str, canon: str, photos_index: Dict[str, str]) -> None:
    """
    Affiche toutes les photos disponibles en grille.
    L'utilisateur clique sur la bonne → le choix est sauvegardé dans
    PHOTO_MAPPING_PATH (JSON) et en session_state.

    Affiche d'abord les photos les mieux scorées par le matcher automatique,
    puis toutes les autres.
    """
    if not photos_index:
        st.warning("Aucune photo disponible dans le dossier local.")
        return

    # Charger le mapping courant pour afficher la sélection active
    mapping = st.session_state.get("photo_manual_mapping") or load_photo_manual_mapping()
    current_file = mapping.get(canon, "")

    # Trier : meilleures correspondances fuzzy en premier
    pn = _photo_key_spaced(player_name)
    scored_files = sorted(
        [(_quick_ratio(pn, k), os.path.basename(v), v)
         for k, v in photos_index.items()],
        reverse=True
    )
    # Dédoublonner sur le nom de fichier (garde le meilleur score)
    seen, ordered_files = set(), []
    for sc, fn, path in scored_files:
        if fn not in seen:
            seen.add(fn)
            ordered_files.append((sc, fn, path))

    if current_file:
        st.caption(f"✅ Photo actuellement associée : **{current_file}**  —  cliquez sur une autre pour changer.")
    else:
        st.caption("Cliquez sur la photo correspondant à cette joueuse pour l'associer.")

    # Afficher en grille de 5 colonnes
    N_COLS = 5
    rows = [ordered_files[i:i + N_COLS] for i in range(0, len(ordered_files), N_COLS)]

    for row in rows:
        cols = st.columns(N_COLS)
        for col, (sc, fn, path) in zip(cols, row):
            with col:
                data = load_photo_bytes(path)
                if data:
                    st.image(data, width=110)
                else:
                    st.caption("🚫 illisible")

                # Indicateur sélection active
                is_selected = fn == current_file
                label = f"✅ {fn[:18]}" if is_selected else fn[:18]
                btn_type = "primary" if is_selected else "secondary"

                if st.button(
                    label,
                    key=f"pick_photo_{canon}_{fn}",
                    type=btn_type,
                    use_container_width=True,
                ):
                    set_manual_photo(player_name, fn)
                    st.success(f"✅ Photo **{fn}** associée à **{player_name}**")
                    st.rerun()

    # Option : effacer le mapping pour cette joueuse
    if current_file:
        st.divider()
        if st.button("🗑️ Supprimer l'association manuelle", key=f"del_photo_{canon}"):
            mapping = load_photo_manual_mapping()
            mapping.pop(canon, None)
            save_photo_manual_mapping(mapping)
            st.session_state["photo_manual_mapping"] = mapping
            st.info("Association supprimée.")
            st.rerun()


def show_photo_block(player_name: str, location: str = "stats") -> None:
    c1, c2 = st.columns([1, 4])
    with c1:
        force = st.button("🔄 Sync photos", key=f"photos_sync_{location}")
    with c2:
        st.caption("Photos synchronisées depuis Google Drive.")

    if force:
        with st.spinner("Sync + conversion en cours..."):
            sync_photos_from_drive()
            ok, fail, errs = reconvert_photos_to_jpeg()
            if fail > 0:
                st.warning(f"{fail} fichier(s) non convertible(s) : {', '.join(errs[:3])}")
            get_photo_concordance(force_rebuild=True)
            st.session_state["photos_index"] = build_photos_index_local()

    photos_index  = st.session_state.get("photos_index") or build_photos_index_local()
    concordance   = get_photo_concordance()
    photo_path    = find_photo_for_player(player_name, concordance=concordance, photos_index=photos_index)

    if photo_path and os.path.exists(photo_path):
        if safe_show_photo(photo_path, width=170):
            return

    # --- Diagnostic ---
    with st.expander("📷 Photo non trouvée — Diagnostic", expanded=True):
        canon = normalize_name_raw(player_name)
        st.write(f"**Nom cherché :** `{player_name}`")
        st.write(f"**Canon référentiel :** `{canon}`")
        st.write(f"**Photos locales :** {len(photos_index)} fichiers indexés")
        st.write(f"**Concordance référentiel :** {len(concordance)} entrées")

        if photos_index:
            pn = _photo_key_spaced(player_name)
            scored = sorted(
                [(_quick_ratio(pn, k), k, os.path.basename(p))
                 for k, p in photos_index.items()],
                reverse=True
            )[:6]
            st.write("**Fichiers photos les plus proches (par nom) :**")
            for sc, k, fn in scored:
                st.write(f"  `{fn}` — score : `{sc:.2f}`")

        st.info(
            "💡 La concordance utilise le référentiel **Noms Prénoms Paris FC.xlsx** (colonnes NOM + Prénom). "
            "Si la photo n'est pas trouvée, vérifiez que la joueuse est bien dans le référentiel "
            "et que le nom du fichier photo contient NOM et/ou Prénom."
        )


@st.cache_data(ttl=3600, show_spinner=False)
def load_photo_bytes(path: str) -> Optional[bytes]:
    """
    Charge une image depuis le disque et retourne des bytes JPEG compatibles Streamlit.
    Gère HEIC, HEIF, PNG, JPG et tout format supporté par Pillow.
    Retourne None si le fichier est invalide ou illisible.
    """
    if not path or not os.path.exists(path):
        return None
    try:
        from PIL import Image as PilImage
        ext = os.path.splitext(path)[1].lower()
        with open(path, "rb") as f:
            raw_bytes = f.read()
        # Enregistrer le support HEIC si pillow-heif est installé
        if ext in (".heic", ".heif"):
            try:
                import pillow_heif
                pillow_heif.register_heif_opener()
            except ImportError:
                pass
        # Ouvrir, valider et convertir en JPEG pour Streamlit
        im = PilImage.open(io.BytesIO(raw_bytes))
        im.load()
        im = im.convert("RGB")
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=92, optimize=True)
        return buf.getvalue()
    except Exception:
        return None


def safe_show_photo(path: str, width: int = 160) -> bool:
    """
    Affiche une photo de façon sécurisée (gère HEIC, HEIF, PNG, JPG...).
    Retourne True si affichée, False sinon.
    """
    data = load_photo_bytes(path)
    if data:
        st.image(data, width=width)
        return True
    return False


def debug_photo_suggestions(player_name: str, photos_index: dict, topn: int = 8):
    try:
        target = normalize_str(player_name)
        keys = list(photos_index.keys())
        close = get_close_matches(target, keys, n=topn, cutoff=0.0)
        out = []
        for k in close:
            pth = photos_index.get(k)
            if pth:
                out.append(os.path.basename(pth))
        return out
    except Exception:
        return []


def _download_drive_binary_to_path(service, file_id: str, out_path: str) -> str:
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(fh.read())
    return out_path


# ✅ FIX 4 : sync_photos_from_drive corrigée
# - signature avec paramètres par défaut
# - drive_service initialisé localement
# - list_all_files_in_folder_recursive remplacée par list_files_recursive
def reconvert_photos_to_jpeg(folder: str = PHOTOS_FOLDER) -> Tuple[int, int, List[str]]:
    """
    Parcourt tous les fichiers du dossier photos et tente de les convertir en JPEG.
    - HEIC/HEIF/WebP/PNG/etc. → renommés en .jpg et convertis
    - Fichiers déjà en .jpg valides → vérifiés, reconvertis si corrompus
    - Retourne (nb_ok, nb_echec, liste_erreurs)
    """
    if not os.path.exists(folder):
        return 0, 0, []

    ok, fail = 0, 0
    errors: List[str] = []

    # Enregistrer pillow-heif si disponible (HEIC/HEIF)
    try:
        import pillow_heif as _ph
        _ph.register_heif_opener()
    except ImportError:
        pass

    from PIL import Image as _PilImg

    files = [f for f in os.listdir(folder)
             if os.path.splitext(f)[1].lower() in IMAGE_EXTS]

    for fn in files:
        src_path = os.path.join(folder, fn)
        stem, ext = os.path.splitext(fn)
        dst_path = os.path.join(folder, stem + ".jpg")

        try:
            with open(src_path, "rb") as f_in:
                raw = f_in.read()

            # Tentative Pillow (pillow-heif enregistré au-dessus si dispo)
            jpeg_bytes = None
            try:
                im = _PilImg.open(io.BytesIO(raw))
                im.load()
                im = im.convert("RGB")
                buf = io.BytesIO()
                im.save(buf, format="JPEG", quality=92, optimize=True)
                jpeg_bytes = buf.getvalue()
            except Exception:
                pass

            if jpeg_bytes is None:
                # Fichier vraiment illisible (HEIC sans pillow-heif, format exotique...)
                fail += 1
                errors.append(f"{fn}: non supporté par Pillow (HEIC sans pillow-heif ?)")
                continue

            with open(dst_path, "wb") as f_out:
                f_out.write(jpeg_bytes)

            # Supprimer le fichier original s'il avait une extension différente
            if src_path != dst_path and os.path.exists(src_path):
                try:
                    os.remove(src_path)
                except Exception:
                    pass

            ok += 1

        except Exception as e:
            fail += 1
            errors.append(f"{fn}: {e}")

    return ok, fail, errors


# ─── LOGOS CLUBS ────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def sync_logos_from_drive() -> dict:
    """Télécharge les logos clubs depuis Drive et retourne un index {nom_normalisé: chemin_local}.
    Les fichiers sont nommés par le nom du club (ex: HAC.png, PSG.png, OL.png).
    """
    folder_id = DRIVE_LOGOS_FOLDER_ID
    local_folder = LOGOS_FOLDER
    index = {}

    if not folder_id:
        return index
    os.makedirs(local_folder, exist_ok=True)

    try:
        drive_service = authenticate_google_drive()
        items = list_files_recursive(drive_service, folder_id)
    except Exception:
        return index

    exts = ('.jpg', '.jpeg', '.png', '.webp', '.svg')
    for it in items:
        name = (it.get('name') or '').strip()
        if not any(name.lower().endswith(e) for e in exts):
            continue
        stem = os.path.splitext(name)[0]
        ext  = os.path.splitext(name)[1].lower()
        local_path = os.path.join(local_folder, name)
        # Télécharger si absent
        if not os.path.exists(local_path):
            try:
                req = drive_service.files().get_media(fileId=it['id'])
                buf = io.BytesIO()
                from googleapiclient.http import MediaIoBaseDownload
                dl = MediaIoBaseDownload(buf, req, chunksize=512*1024)
                done = False
                while not done:
                    _, done = dl.next_chunk()
                with open(local_path, 'wb') as f:
                    f.write(buf.getvalue())
            except Exception:
                continue
        # Indexer par nom normalisé
        key = normalize_str(stem)
        index[key] = local_path

    return index


def find_logo_for_club(club_name: str, logos_index: dict = None) -> str:
    """Retourne le chemin local du logo pour un club, ou '' si non trouvé.
    Cherche par correspondance exacte puis partielle sur le nom normalisé.
    """
    if not club_name or logos_index is None:
        return ""
    cn = normalize_str(club_name)
    # Exact
    if cn in logos_index:
        return logos_index[cn]
    # Partiel : le nom du fichier est contenu dans le nom du club ou vice versa
    for key, path in logos_index.items():
        if key in cn or cn in key:
            return path
    # Tokens : un token significatif du club matche un token du fichier
    tokens_club = set(t for t in re.split(r'[\s\-_]+', cn) if len(t) >= 3)
    for key, path in logos_index.items():
        tokens_key = set(t for t in re.split(r'[\s\-_]+', key) if len(t) >= 3)
        if tokens_club & tokens_key:
            return path
    return ""


def logo_path_to_b64(path: str) -> str:
    """Convertit un fichier logo en data URI base64."""
    if not path or not os.path.exists(path):
        return ""
    ext = os.path.splitext(path)[1].lower().lstrip('.')
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "webp": "image/webp", "svg": "image/svg+xml"}.get(ext, "image/png")
    try:
        import base64 as _b64l
        with open(path, 'rb') as f:
            return f"data:{mime};base64,{_b64l.b64encode(f.read()).decode()}"
    except Exception:
        return ""


def sync_photos_from_drive(folder_id: str = None, local_folder: str = None):
    """Télécharge (cache local) les photos des joueuses depuis Google Drive."""
    if folder_id is None:
        folder_id = PHOTOS_FOLDER_ID
    if local_folder is None:
        local_folder = PHOTOS_FOLDER

    if not folder_id:
        st.warning('Photos: PHOTOS_FOLDER_ID non configuré.')
        return
    os.makedirs(local_folder, exist_ok=True)

    try:
        # ✅ drive_service initialisé localement (n'existait pas dans la portée)
        drive_service = authenticate_google_drive()
        # ✅ list_all_files_in_folder_recursive remplacée par list_files_recursive
        items = list_files_recursive(drive_service, folder_id)
    except Exception as e:
        st.warning(f"Photos: impossible d'accéder au dossier Drive. Partage ce dossier avec le service account. Erreur: {e}")
        return

    if not items:
        st.warning('Photos: aucun fichier trouvé dans le dossier Drive (y compris sous-dossiers/raccourcis).')
        return

    exts = ('.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif')
    img_items = []
    for it in items:
        name = (it.get('name') or '').strip()
        mt = (it.get('mimeType') or '').lower()
        if name.lower().endswith(exts) or mt.startswith('image/'):
            img_items.append(it)

    if not img_items:
        st.warning('Photos: aucun fichier image (.jpg/.png/.heic…) trouvé dans le dossier Drive.')
        return

    downloaded = 0

    def _download_file(file_id: str, filename: str, size_str=None):
        nonlocal downloaded
        safe_name = filename.replace('/', '_').replace('\\', '_')
        ext_orig = os.path.splitext(safe_name)[1].lower()

        # Toujours stocker en JPEG pour garantir la lisibilité (Pillow universel)
        stem = os.path.splitext(safe_name)[0]
        safe_name_jpg = stem + ".jpg"
        local_path = os.path.join(local_folder, safe_name_jpg)

        try:
            # Si le fichier JPEG existe déjà et a une taille raisonnable, skip
            if os.path.exists(local_path):
                try:
                    if os.path.getsize(local_path) > 1024:
                        return
                except Exception:
                    pass

            # Télécharger en mémoire
            req = drive_service.files().get_media(fileId=file_id)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            raw_bytes = buf.getvalue()

            # Convertir en JPEG — 3 stratégies en cascade
            jpeg_bytes = None

            # Stratégie 1 : pillow-heif + Pillow (HEIC/HEIF natif)
            if ext_orig in (".heic", ".heif"):
                try:
                    import pillow_heif as _ph
                    _ph.register_heif_opener()
                    from PIL import Image as _PilImg
                    im = _PilImg.open(io.BytesIO(raw_bytes))
                    im.load()
                    im = im.convert("RGB")
                    out_buf = io.BytesIO()
                    im.save(out_buf, format="JPEG", quality=92, optimize=True)
                    jpeg_bytes = out_buf.getvalue()
                except Exception:
                    pass

            # Stratégie 2 : Pillow standard (JPG, PNG, WebP, et HEIC si s1 a réussi)
            if jpeg_bytes is None:
                try:
                    from PIL import Image as _PilImg
                    im = _PilImg.open(io.BytesIO(raw_bytes))
                    im.load()
                    im = im.convert("RGB")
                    out_buf = io.BytesIO()
                    im.save(out_buf, format="JPEG", quality=92, optimize=True)
                    jpeg_bytes = out_buf.getvalue()
                except Exception:
                    pass

            # Stratégie 3 (HEIC uniquement) : demander à Drive d'exporter en JPEG
            # Google Drive peut convertir les images HEIC côté serveur
            if jpeg_bytes is None and ext_orig in (".heic", ".heif"):
                try:
                    req_jpg = drive_service.files().export_media(
                        fileId=file_id, mimeType="image/jpeg"
                    )
                    buf_jpg = io.BytesIO()
                    dl_jpg = MediaIoBaseDownload(buf_jpg, req_jpg, chunksize=1024 * 1024)
                    done_jpg = False
                    while not done_jpg:
                        _, done_jpg = dl_jpg.next_chunk()
                    jpeg_bytes = buf_jpg.getvalue()
                except Exception:
                    pass

            if jpeg_bytes is not None:
                pass  # conversion réussie → local_path = stem + ".jpg" déjà défini
            else:
                # Aucune stratégie n'a fonctionné : stocker brut sous nom d'origine
                local_path = os.path.join(local_folder, safe_name)
                jpeg_bytes = raw_bytes

            with open(local_path, "wb") as f_out:
                f_out.write(jpeg_bytes)
            downloaded += 1

        except Exception as e:
            st.warning(f"Photos: impossible de télécharger {filename} -> {e}")

    for it in img_items:
        fid = it.get('id')
        name = it.get('name') or ''
        size = it.get('size')
        if fid and name:
            _download_file(fid, name, size)

    # Reconstruction de l'index après sync
    build_photos_index_local()
    if downloaded > 0:
        pass  # photos sync count recorded silently

@st.cache_resource(show_spinner=False)
def authenticate_google_drive():
    scopes = ["https://www.googleapis.com/auth/drive"]
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return build("drive", "v3", credentials=creds)


def _is_retryable_http_error(e: Exception) -> bool:
    if not isinstance(e, HttpError):
        return False
    status = getattr(e.resp, "status", None)
    return status in (429, 500, 502, 503, 504)


# =========================
# GPS DRIVE SYNC
# =========================
GPS_SYNC_STATE_PATH = os.path.join(DATA_FOLDER, "gps_sync_state.json")

def _load_gps_state() -> dict:
    if os.path.exists(GPS_SYNC_STATE_PATH):
        try:
            with open(GPS_SYNC_STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_modifiedTime": None, "folders_failed": {}}

def _save_gps_state(state: dict) -> None:
    os.makedirs(DATA_FOLDER, exist_ok=True)
    with open(GPS_SYNC_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _execute_with_retry(call, max_tries: int = 7):
    for attempt in range(max_tries):
        try:
            return call.execute()
        except Exception as e:
            if _is_retryable_http_error(e) and attempt < max_tries - 1:
                time.sleep((2 ** attempt) + 0.2 * attempt)
                continue
            raise

def list_files_in_folder_paged(service, folder_id: str, q_extra: str = "", page_size: int = 200) -> List[dict]:
    q = f"'{folder_id}' in parents and trashed=false"
    if q_extra:
        q += f" and ({q_extra})"

    out: List[dict] = []
    page_token = None
    while True:
        req = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, size, shortcutDetails, thumbnailLink)",
            pageSize=page_size,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        resp = _execute_with_retry(req)
        out.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out

def walk_drive_folders(service, root_folder_id: str, state: dict):
    stack = [root_folder_id]
    seen = set()
    now = time.time()

    while stack:
        fid = stack.pop()
        if fid in seen:
            continue
        seen.add(fid)

        last_fail = state.get("folders_failed", {}).get(fid)
        if last_fail and (now - float(last_fail)) < 600:
            continue

        yield fid

        try:
            subfolders = list_files_in_folder_paged(
                service,
                fid,
                q_extra="mimeType='application/vnd.google-apps.folder'",
                page_size=200
            )
            for sf in subfolders:
                stack.append(sf["id"])
        except Exception:
            state.setdefault("folders_failed", {})[fid] = time.time()
            continue

def _safe_local_path(filename: str, file_id: str, dest_folder: str = None) -> str:
    base_folder = dest_folder if dest_folder else GPS_FOLDER
    os.makedirs(base_folder, exist_ok=True)

    rel = "" if filename is None else str(filename)
    rel = os.path.normpath(rel).lstrip("/")

    rel_dir = os.path.dirname(rel)
    base = os.path.basename(rel)
    base_noext, ext = os.path.splitext(base)

    target_dir = os.path.join(base_folder, rel_dir) if rel_dir else base_folder
    os.makedirs(target_dir, exist_ok=True)

    return os.path.join(target_dir, f"{base_noext}__{file_id[:8]}{ext}")


def download_drive_file_to_local(service, file_id: str, file_name: str, mime_type: str) -> str:
    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        file_name = os.path.splitext(file_name)[0] + ".xlsx"
    else:
        request = service.files().get_media(fileId=file_id)

    final_path = _safe_local_path(file_name, file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    with open(final_path, "wb") as f:
        f.write(fh.read())

    return final_path

def download_drive_csv_to_local(service, file_id: str, file_name: str, dest_folder: str = None) -> str:
    request = service.files().get_media(fileId=file_id)
    if not str(file_name).lower().endswith(".csv"):
        file_name = os.path.splitext(str(file_name))[0] + ".csv"

    final_path = _safe_local_path(str(file_name), file_id, dest_folder=dest_folder)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    with open(final_path, "wb") as f:
        f.write(fh.read())

    return final_path


def export_sheet_to_csv_local(service, file_id: str, file_name: str, dest_folder: str = None) -> str:
    request = service.files().export_media(fileId=file_id, mimeType="text/csv")
    file_name = os.path.splitext(str(file_name))[0] + ".csv"

    final_path = _safe_local_path(str(file_name), file_id, dest_folder=dest_folder)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    with open(final_path, "wb") as f:
        f.write(fh.read())

    return final_path


def convert_xls_drive_to_xlsx_local(service, file_id: str, original_name: str) -> str:
    body = {
        "name": f"__tmp_convert__{original_name}",
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [DRIVE_GPS_FOLDER_ID],
    }
    copied = _execute_with_retry(service.files().copy(
        fileId=file_id,
        body=body,
        supportsAllDrives=True,
    ))
    gsheet_id = copied["id"]

    req = service.files().export_media(
        fileId=gsheet_id,
        mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    out_name = os.path.splitext(original_name)[0] + ".xlsx"
    final_path = _safe_local_path(out_name, file_id)
    with open(final_path, "wb") as f:
        f.write(fh.read())

    try:
        _execute_with_retry(service.files().delete(fileId=gsheet_id, supportsAllDrives=True))
    except Exception:
        pass

    return final_path

def sync_gps_from_drive_autonomous():
    service = authenticate_google_drive()
    state = _load_gps_state()
    last_m = state.get("last_modifiedTime")
    newest_modified = last_m

    def is_gps_candidate(f: dict) -> bool:
        name = (f.get("name") or "").lower()
        mt = f.get("mimeType") or ""
        if mt == "application/vnd.google-apps.folder":
            return False
        if not (name.endswith(".csv") or mt == "application/vnd.google-apps.spreadsheet"):
            return False
        # Séances d'entraînement (GF1)
        if ("gf1" in name) or ("seance" in name) or ("séance" in name) or ("gps" in name):
            return True
        # Fichiers de match : U19_, U17_, U16_, U15_, _J0x_, etc.
        if is_gps_match_file(name):
            return True
        return False

    for folder_id in walk_drive_folders(service, DRIVE_GPS_FOLDER_ID, state):
        try:
            q_extra = f"modifiedTime > '{last_m}'" if last_m else ""
            items = list_files_in_folder_paged(service, folder_id, q_extra=q_extra, page_size=200)

            for f in items:
                if not is_gps_candidate(f):
                    continue

                fid = f["id"]
                name = f.get("name", "")
                mt = f.get("mimeType", "")

                try:
                    # Router les fichiers match vers leur dossier dédié
                    _dest_folder = GPS_MATCH_FOLDER if is_gps_match_file(name) else GPS_FOLDER
                    os.makedirs(_dest_folder, exist_ok=True)
                    if mt == "application/vnd.google-apps.spreadsheet":
                        export_sheet_to_csv_local(service, fid, name, dest_folder=_dest_folder)
                    elif name.lower().endswith(".csv"):
                        download_drive_csv_to_local(service, fid, name, dest_folder=_dest_folder)
                except Exception as e:
                    st.warning(f"GPS: téléchargement/export CSV impossible {name} -> {e}")

                mtime = f.get("modifiedTime")
                if mtime and (newest_modified is None or mtime > newest_modified):
                    newest_modified = mtime

        except Exception:
            state.setdefault("folders_failed", {})[folder_id] = time.time()
            continue

    state["last_modifiedTime"] = newest_modified
    state["folders_failed"] = {k: v for k, v in state.get("folders_failed", {}).items() if (time.time() - float(v)) < 86400}
    _save_gps_state(state)


def list_files_in_folder(service, folder_id: str, include_folders: bool = False) -> List[dict]:
    query = f"'{folder_id}' in parents and trashed=false"
    fields = "nextPageToken, files(id, name, mimeType, modifiedTime, size, shortcutDetails, thumbnailLink)"

    page_token = None
    out: List[dict] = []

    while True:
        max_tries = 6
        resp = None
        for attempt in range(max_tries):
            try:
                resp = service.files().list(
                    q=query,
                    fields=fields,
                    pageSize=200,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                ).execute()
                break
            except Exception as e:
                if _is_retryable_http_error(e) and attempt < max_tries - 1:
                    time.sleep((2 ** attempt) + (0.1 * attempt))
                    continue
                raise

        if not resp:
            break

        items = resp.get("files", []) or []
        if not include_folders:
            items = [f for f in items if f.get("mimeType") != "application/vnd.google-apps.folder"]
        out.extend(items)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return out


def list_files_recursive(service, folder_id: str) -> List[dict]:
    """Parcourt récursivement un dossier Drive et retourne tous les fichiers (hors folders)."""
    stack = [folder_id]
    out: List[dict] = []
    seen = set()
    while stack:
        fid = stack.pop()
        if fid in seen:
            continue
        seen.add(fid)

        items = list_files_in_folder(service, fid, include_folders=True)
        for it in items:
            mt = it.get("mimeType")
            if mt == "application/vnd.google-apps.folder":
                stack.append(it["id"])
            else:
                out.append(it)
    return out

def download_file(service, file_id, file_name, output_folder, mime_type=None):
    os.makedirs(output_folder, exist_ok=True)
    final_path = os.path.join(output_folder, file_name)
    tmp_path = final_path + ".tmp"

    # Fichier déjà présent et non vide → skip (évite les re-téléchargements inutiles)
    if os.path.exists(final_path) and os.path.getsize(final_path) > 512:
        return final_path

    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        if not final_path.lower().endswith(".xlsx"):
            final_path = os.path.splitext(final_path)[0] + ".xlsx"
            tmp_path = final_path + ".tmp"
        # Re-check après renommage xlsx
        if os.path.exists(final_path) and os.path.getsize(final_path) > 512:
            return final_path
    elif mime_type and mime_type.startswith("application/vnd.google-apps."):
        # Type Google natif non exportable (Docs, Slides, etc.) → on skip
        raise ValueError(f"Type non téléchargeable : {mime_type}")
    else:
        request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    retries = 0
    while not done:
        try:
            _, done = downloader.next_chunk()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in (403, 404):
                raise  # fichier inaccessible → on remonte l'erreur
            if retries < 3 and status in (429, 500, 502, 503, 504):
                time.sleep(2 ** retries)
                retries += 1
                continue
            raise

    fh.seek(0)
    with open(tmp_path, "wb") as f:
        f.write(fh.read())

    os.replace(tmp_path, final_path)
    return final_path


def download_permissions_file():
    try:
        service = authenticate_google_drive()
        files = list_files_in_folder(service, DRIVE_MAIN_FOLDER_ID)

        target = normalize_str(PERMISSIONS_FILENAME)
        candidate = None
        for f in files:
            if normalize_str(f["name"]) == target:
                candidate = f
                break
        if not candidate:
            return None

        path = download_file(
            service, candidate["id"], candidate["name"], DATA_FOLDER, mime_type=candidate.get("mimeType")
        )

        try:
            _ = read_excel_auto(path)
        except Exception:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
            path = download_file(
                service, candidate["id"], candidate["name"], DATA_FOLDER, mime_type=candidate.get("mimeType")
            )

        return path
    except Exception as e:
        st.error(f"Erreur téléchargement permissions: {e}")
        return None


@st.cache_resource(show_spinner=False)
def load_permissions():
    try:
        permissions_path = download_permissions_file()
        if not permissions_path or not os.path.exists(permissions_path):
            return {}

        permissions_df = read_excel_auto(permissions_path)

        if isinstance(permissions_df, dict):
            permissions_df = list(permissions_df.values())[0] if len(permissions_df) else pd.DataFrame()

        if not isinstance(permissions_df, pd.DataFrame) or permissions_df.empty:
            return {}

        for col in ["Profil", "Mot de passe", "Permissions", "Joueuse"]:
            if col not in permissions_df.columns:
                permissions_df[col] = np.nan

        permissions = {}
        for _, row in permissions_df.iterrows():
            profile = str(row.get("Profil", "")).strip()
            if not profile:
                continue

            raw_perm = row.get("Permissions", np.nan)
            perm_list = [p.strip() for p in str(raw_perm).split(",") if p.strip()] if pd.notna(raw_perm) else []

            player = row.get("Joueuse", np.nan)
            player = nettoyer_nom_joueuse(str(player)) if pd.notna(player) else None

            permissions[profile] = {
                "password": str(row.get("Mot de passe", "")).strip(),
                "permissions": perm_list,
                "player": player,
            }
        return permissions
    except Exception as e:
        st.error(f"Erreur chargement permissions: {e}")
        return {}


# Types MIME Google natifs NON téléchargeables directement (hors Sheets)
_SKIP_MIME_TYPES = {
    "application/vnd.google-apps.document",       # Google Docs
    "application/vnd.google-apps.presentation",   # Google Slides
    "application/vnd.google-apps.form",           # Google Forms
    "application/vnd.google-apps.drawing",        # Google Drawings
    "application/vnd.google-apps.map",            # Google Maps
    "application/vnd.google-apps.folder",         # Dossiers
    "application/vnd.google-apps.shortcut",       # Raccourcis
    "application/vnd.google-apps.script",         # Apps Script
    "application/vnd.google-apps.site",           # Google Sites
}


def _is_downloadable(f: dict) -> bool:
    """Retourne True si le fichier est téléchargeable (CSV/Excel/Sheets seulement)."""
    name = str(f.get("name", ""))
    mime = str(f.get("mimeType", ""))
    if mime in _SKIP_MIME_TYPES:
        return False
    is_sheet = mime == "application/vnd.google-apps.spreadsheet"
    is_data_file = name.lower().endswith((".csv", ".xlsx", ".xls"))
    return is_sheet or is_data_file


def download_google_drive():
    service = authenticate_google_drive()
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(PASSERELLE_FOLDER, exist_ok=True)
    os.makedirs(GPS_FOLDER, exist_ok=True)

    files = list_files_in_folder(service, DRIVE_MAIN_FOLDER_ID)
    for f in files:
        if not _is_downloadable(f):
            continue
        try:
            download_file(service, f["id"], f["name"], DATA_FOLDER, mime_type=f.get("mimeType"))
        except Exception as e:
            _warn(f"Drive: impossible de télécharger '{f['name']}' → {e}")

    files_pass = list_files_in_folder(service, DRIVE_PASSERELLE_FOLDER_ID)
    for f in files_pass:
        if normalize_str(f["name"]) == normalize_str(PASSERELLE_FILENAME):
            try:
                download_file(service, f["id"], f["name"], PASSERELLE_FOLDER, mime_type=f.get("mimeType"))
            except Exception as e:
                _warn(f"Drive: impossible de télécharger le fichier passerelle → {e}")
            break

st.session_state["gps_drive_found"] = 0
st.session_state["gps_drive_downloaded"] = 0


# =========================
# REFERENTIEL NOMS
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def build_referentiel_players(ref_path: str) -> Tuple[Set[str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, Set[str]], Dict[str, Set[str]]]:
    ref = read_excel_auto(ref_path)

    if isinstance(ref, dict):
        if len(ref) == 0:
            raise ValueError("Référentiel vide (aucune feuille lisible).")
        ref = list(ref.values())[0]

    if not isinstance(ref, pd.DataFrame) or ref.empty:
        raise ValueError("Référentiel illisible ou vide.")

    cols_norm = {normalize_str(c): c for c in ref.columns}

    if "Nom de joueuse" in ref.columns:
        col_name = "Nom de joueuse"
        ref = ref.copy()
        ref["CANON"] = ref[col_name].astype(str).map(normalize_name_raw)
    elif cols_norm.get("nom de joueuse") is not None:
        col_name = cols_norm["nom de joueuse"]
        ref = ref.copy()
        ref["CANON"] = ref[col_name].astype(str).map(normalize_name_raw)
    else:
        cols = {str(c).strip().upper(): c for c in ref.columns}
        col_nom = cols.get("NOM") or cols_norm.get("nom")
        col_pre = cols.get("PRÉNOM") or cols.get("PRENOM") or cols_norm.get("prenom") or cols_norm.get("prénom")

        if not col_nom or not col_pre:
            raise ValueError(f"Référentiel: colonnes introuvables (NOM/Prénom ou 'Nom de joueuse'): {ref.columns.tolist()}")

        ref = ref.copy()
        ref["CANON"] = (ref[col_nom].astype(str) + " " + ref[col_pre].astype(str)).map(normalize_name_raw)

    ref = ref[ref["CANON"].astype(str).str.strip().ne("")].copy()
    ref_set = set(ref["CANON"].dropna().unique().tolist())

    alias_to_canon: Dict[str, str] = {}
    tokenkey_to_canon: Dict[str, str] = {}
    compact_to_canon: Dict[str, str] = {}
    first_to_canons: Dict[str, Set[str]] = {}
    last_to_canons: Dict[str, Set[str]] = {}

    def _add_index(d: Dict[str, Set[str]], k: str, canon: str):
        if not k:
            return
        if k not in d:
            d[k] = set()
        d[k].add(canon)

    for canon in ref_set:
        alias_to_canon[canon] = canon
        compact_to_canon[compact_name(canon)] = canon

        toks = tokens_name(canon)
        if toks:
            token_key = " ".join(sorted(toks))
            tokenkey_to_canon[token_key] = canon

            for t in toks:
                _add_index(first_to_canons, t, canon)
                _add_index(last_to_canons, t, canon)

            _add_index(first_to_canons, toks[-1], canon)
            _add_index(last_to_canons, toks[0], canon)

        if toks and len(toks) >= 2:
            inv1 = " ".join([toks[-1]] + toks[:-1])
            alias_to_canon[normalize_name_raw(inv1)] = canon

            if len(toks) >= 3:
                inv2 = " ".join(toks[-2:] + toks[:-2])
                alias_to_canon[normalize_name_raw(inv2)] = canon

        if toks and len(toks) >= 2:
            nom = " ".join(toks[:-1])
            prenom = toks[-1]
            alias_to_canon[normalize_name_raw(f"{nom}, {prenom}")] = canon
            alias_to_canon[normalize_name_raw(f"{prenom} {nom}")] = canon

        if toks and len(toks) >= 2:
            nom = " ".join(toks[:-1])
            prenom = toks[-1]
            alias_to_canon[normalize_name_raw(f"{nom} {prenom[0]}")] = canon
            alias_to_canon[normalize_name_raw(f"{nom} {prenom[0]}.")] = canon

    return ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons

def best_from_candidates(raw_clean: str, candidates: List[str], min_score: float = 0.88) -> Tuple[Optional[str], float, Optional[float]]:
    if not candidates:
        return None, 0.0, None

    best_canon = None
    best_score = 0.0
    second = 0.0

    for canon in candidates:
        sc = SequenceMatcher(None, raw_clean, canon).ratio()
        if sc > best_score:
            second = best_score
            best_score = sc
            best_canon = canon
        elif sc > second:
            second = sc

    if best_canon and best_score >= min_score and (best_score - second) >= 0.04:
        return best_canon, best_score, second
    return None, best_score, second


def map_player_name(
    raw_name: str,
    ref_set: Set[str],
    alias_to_canon: Dict[str, str],
    tokenkey_to_canon: Dict[str, str],
    compact_to_canon: Dict[str, str],
    first_to_canons: Dict[str, Set[str]],
    last_to_canons: Dict[str, Set[str]],
    cutoff_fuzzy: float = 0.90,
    cutoff_token: float = 0.92,
    cutoff_single: float = 0.90,
) -> Tuple[str, str, str]:
    if raw_name is None:
        return "", "unmatched", "empty"

    raw = str(raw_name).strip()
    if not raw or raw.upper() in {"NAN", "NONE", "NULL"}:
        return "", "unmatched", "empty"

    cleaned = normalize_name_raw(raw)
    if not cleaned:
        return "", "unmatched", raw

    if cleaned in ref_set:
        return cleaned, "exact", raw

    if cleaned in alias_to_canon:
        return alias_to_canon[cleaned], "alias", raw

    toks = tokens_name(cleaned)
    if toks:
        key = " ".join(sorted(toks))
        if key in tokenkey_to_canon:
            return tokenkey_to_canon[key], "token_set", raw

        best_canon = None
        best_score = 0.0
        for k, canon in tokenkey_to_canon.items():
            sc = SequenceMatcher(None, key, k).ratio()
            if sc > best_score:
                best_score = sc
                best_canon = canon
        if best_canon and best_score >= cutoff_token:
            return best_canon, f"token_fuzzy({best_score:.2f})", raw

    comp = compact_name(cleaned)
    if comp in compact_to_canon:
        return compact_to_canon[comp], "compact", raw

    if toks and len(toks) == 1:
        t = toks[0]
        cand: Set[str] = set()
        cand |= first_to_canons.get(t, set())
        cand |= last_to_canons.get(t, set())

        if not cand:
            keys = list(set(list(first_to_canons.keys()) + list(last_to_canons.keys())))
            near = get_close_matches(t, keys, n=8, cutoff=0.86)
            for nk in near:
                cand |= first_to_canons.get(nk, set())
                cand |= last_to_canons.get(nk, set())

        cand_list = list(cand)
        best, sc, sc2 = best_from_candidates(cleaned, cand_list, min_score=cutoff_single)
        if best:
            return best, f"single_token({sc:.2f})", raw

    best = get_close_matches(cleaned, list(ref_set), n=1, cutoff=cutoff_fuzzy)
    if best:
        return best[0], "fuzzy", raw

    return cleaned, "unmatched", raw


def normalize_players_in_df(
    df: pd.DataFrame,
    cols: List[str],
    ref_set: Set[str],
    alias_to_canon: Dict[str, str],
    tokenkey_to_canon: Dict[str, str],
    compact_to_canon: Dict[str, str],
    first_to_canons: Dict[str, Set[str]],
    last_to_canons: Dict[str, Set[str]],
    filename: str,
    report: List[dict],
    fuzzy_cutoff: float = 0.93,
) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        new_vals = []
        for v in out[col].tolist():
            mapped, status, raw = map_player_name(
                v,
                ref_set,
                alias_to_canon,
                tokenkey_to_canon,
                compact_to_canon,
                first_to_canons,
                last_to_canons,
                cutoff_fuzzy=fuzzy_cutoff,
                cutoff_token=0.92,
                cutoff_single=0.90,
            )
            if status not in {"exact", "alias", "token_set", "compact"} and str(v).strip():
                report.append({"file": filename, "column": col, "raw": raw, "mapped": mapped, "status": status})
            new_vals.append(mapped if looks_like_player(mapped) else v)
        out[col] = new_vals
    return out


# =========================
# PASSERELLES
# =========================
def _passerelle_fmt_date(v) -> str:
    """Convertit n'importe quelle valeur de date en 'dd/mm/yyyy' ou '' si vide."""
    if v is None:
        return ""
    # Timestamp pandas / datetime Python
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%d/%m/%Y")
        except Exception:
            return ""
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "nat", ""):
        return ""
    # Format ISO yyyy-mm-dd (pandas lit souvent les dates ainsi)
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        try:
            return pd.to_datetime(s).strftime("%d/%m/%Y")
        except Exception:
            pass
    # Format dd/mm/yyyy déjà correct
    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        return s
    # Dernier recours : laisser pandas interpréter
    try:
        return pd.to_datetime(s, dayfirst=True).strftime("%d/%m/%Y")
    except Exception:
        return s  # retourner tel quel si vraiment impossible


def _passerelle_clean(v) -> str:
    """Nettoie une valeur : None/NaN/NAT → '', sinon strip."""
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat", "") else s


def _find_col(df: pd.DataFrame, *candidates: str):
    """Trouve la première colonne correspondant à un des candidats (insensible casse/accents)."""
    col_map = {normalize_str(str(c)): str(c) for c in df.columns}
    # Correspondance exacte normalisée
    for cand in candidates:
        key = normalize_str(cand)
        if key in col_map:
            return col_map[key]
    # Correspondance partielle
    for cand in candidates:
        key = normalize_str(cand)
        for k, orig in col_map.items():
            if key and (key in k or k in key):
                return orig
    return None


@st.cache_data(ttl=600, show_spinner=False)
def load_passerelle_data():
    passerelle_data = {}
    passerelle_file = os.path.join(PASSERELLE_FOLDER, PASSERELLE_FILENAME)
    if not os.path.exists(passerelle_file):
        return passerelle_data
    try:
        df = read_excel_auto(passerelle_file)
        if isinstance(df, dict):
            df = list(df.values())[0] if df else pd.DataFrame()
        if not isinstance(df, pd.DataFrame) or df.empty:
            return passerelle_data

        # Détection flexible des colonnes (gère variations d'intitulé)
        col_nom    = _find_col(df, "Nom", "NOM", "Nom de famille")
        col_prenom = _find_col(df, "Prénom", "Prenom", "PRENOM", "Prénom")
        col_ddn    = _find_col(df,
                        "Date de naissance", "Date naissance",
                        "DateNaissance", "DOB", "Naissance",
                        "Date de Naissance", "date_naissance")
        col_photo  = _find_col(df, "Photo", "PHOTO", "Lien photo")
        col_p1     = _find_col(df, "Poste 1", "Poste1", "Poste principal", "Poste")
        col_p2     = _find_col(df, "Poste 2", "Poste2", "Poste secondaire")
        col_pied   = _find_col(df, "Pied Fort", "Pied", "Pied fort")
        col_taille   = _find_col(df, "Taille", "Taille (cm)", "Height")
        col_obj1     = _find_col(df, "Objectif 1", "Objectifs 1", "Obj1", "Objectif1")
        col_obj2     = _find_col(df, "Objectif 2", "Objectifs 2", "Obj2", "Objectif2")
        col_obj3     = _find_col(df, "Objectif 3", "Objectifs 3", "Obj3", "Objectif3")

        def _get(row, col):
            return row.get(col) if col else None

        for _, row in df.iterrows():
            nom = _passerelle_clean(_get(row, col_nom))
            if not nom:
                continue

            # Clé du dict : "NOM Prénom" (ou juste NOM si pas de prénom)
            prenom = _passerelle_clean(_get(row, col_prenom))
            key = f"{nom} {prenom}".strip() if prenom else nom

            passerelle_data[key] = {
                "Nom":    nom,
                "Prénom": prenom,
                "Photo":  _passerelle_clean(_get(row, col_photo)),
                "Date de naissance": _passerelle_fmt_date(_get(row, col_ddn)),
                "Poste 1":   _passerelle_clean(_get(row, col_p1)),
                "Poste 2":   _passerelle_clean(_get(row, col_p2)),
                "Pied Fort": _passerelle_clean(_get(row, col_pied)),
                "Taille":    _passerelle_clean(_get(row, col_taille)),
                "Objectif 1": _passerelle_clean(_get(row, col_obj1)),
                "Objectif 2": _passerelle_clean(_get(row, col_obj2)),
                "Objectif 3": _passerelle_clean(_get(row, col_obj3)),
            }
    except Exception as e:
        _warn(f"Passerelle: erreur lecture → {e}")
    return passerelle_data



# =========================
# ÉVALUATIONS OBJECTIFS (Google Forms → Google Sheet → CSV)
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def load_objectifs_evaluations() -> pd.DataFrame:
    """
    Charge le CSV exporté du Google Sheet lié au Google Forms d'évaluation des objectifs.
    Colonnes attendues dans le CSV :
      - Horodateur (timestamp de soumission)
      - Joueuse     (liste déroulante : Nom Prénom de la joueuse)
      - Objectif évalué  (texte de l'objectif, correspondant à Objectif 1/2/3)
      - Note         (entier 1-5)
      - Évaluateur   (optionnel)
    Retourne un DataFrame vide si le fichier n'existe pas encore.
    """
    import os, glob
    os.makedirs(OBJECTIFS_FOLDER, exist_ok=True)

    # Chercher un CSV dans le dossier local
    csv_files = sorted(glob.glob(os.path.join(OBJECTIFS_FOLDER, "*.csv")), reverse=True)
    if not csv_files:
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_files[0], encoding="utf-8-sig")
        # Normalisation des noms de colonnes (Google Forms génère des en-têtes longs)
        rename_map = {}
        for col in df.columns:
            col_low = col.lower().strip()
            if "joueuse" in col_low or "joueur" in col_low or "nom" in col_low:
                rename_map[col] = "Joueuse"
            elif "objectif" in col_low and "évalué" in col_low:
                rename_map[col] = "Objectif évalué"
            elif "note" in col_low or "évaluation" in col_low or "score" in col_low:
                rename_map[col] = "Note"
            elif "horodateur" in col_low or "timestamp" in col_low:
                rename_map[col] = "Horodateur"
            elif "évaluateur" in col_low or "evaluateur" in col_low or "staff" in col_low:
                rename_map[col] = "Évaluateur"
        df = df.rename(columns=rename_map)

        # Normaliser la colonne Note en numérique
        if "Note" in df.columns:
            df["Note"] = pd.to_numeric(df["Note"], errors="coerce")

        return df
    except Exception as e:
        _warn(f"Évaluations objectifs : erreur lecture CSV → {e}")
        return pd.DataFrame()


def sync_objectifs_from_drive() -> Tuple[int, int]:
    """
    Télécharge le CSV des évaluations depuis Google Drive (dossier DRIVE_OBJECTIFS_FOLDER_ID).
    Retourne (nb_téléchargés, nb_erreurs).
    """
    if not DRIVE_OBJECTIFS_FOLDER_ID:
        return 0, 0  # Pas encore configuré
    try:
        service = _get_drive_service()
        os.makedirs(OBJECTIFS_FOLDER, exist_ok=True)
        files = list_files_in_folder(service, DRIVE_OBJECTIFS_FOLDER_ID)
        ok, err = 0, 0
        for f in files:
            if f.get("name", "").endswith(".csv"):
                try:
                    _download_file(service, f["id"], f["name"], OBJECTIFS_FOLDER)
                    ok += 1
                except Exception as e:
                    _warn(f"Évaluations : échec téléchargement {f['name']} → {e}")
                    err += 1
        return ok, err
    except Exception as e:
        _warn(f"Évaluations objectifs : erreur Drive → {e}")
        return 0, 1


# =========================
# PERMISSIONS HELPERS
# =========================
def check_permission(user_profile, required_permission, permissions):
    if user_profile not in permissions:
        return False
    if "all" in permissions[user_profile]["permissions"]:
        return True
    return required_permission in permissions[user_profile]["permissions"]


def get_player_for_profile(profile, permissions):
    if profile in permissions:
        return permissions[profile].get("player", None)
    return None


# =========================
# TEMPS DE JEU (segments Duration)
# =========================
def infer_duration_unit(series: pd.Series) -> str:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return "seconds"
    total = s.sum()
    if 30 <= total <= 200:
        return "minutes"
    if 1500 <= total <= 20000:
        return "seconds"
    if s.median() < 10:
        return "seconds"
    return "minutes"


def extract_lineup_from_row(row: pd.Series, available_posts: List[str]) -> Set[str]:
    players = set()
    for poste in available_posts:
        if poste not in row.index:
            continue
        v = row.get(poste, "")
        for cand in split_if_comma(v):
            p = nettoyer_nom_joueuse(str(cand))
            if looks_like_player(p):
                players.add(p)
    return players


def players_duration(match: pd.DataFrame, home_team: str, away_team: str) -> pd.DataFrame:
    if match is None or match.empty:
        return pd.DataFrame()

    if "Duration" not in match.columns or "Row" not in match.columns:
        return pd.DataFrame()

    available_posts = [c for c in POST_COLS if c in match.columns]
    if not available_posts:
        return pd.DataFrame()

    m = match.copy()

    home_clean = nettoyer_nom_equipe(home_team)
    away_clean = nettoyer_nom_equipe(away_team)

    m["Row_team"] = m["Row"].astype(str).apply(nettoyer_nom_equipe)
    m = m[m["Row_team"].isin({home_clean, away_clean})].copy()
    if m.empty:
        return pd.DataFrame()

    unit = infer_duration_unit(m["Duration"])

    def to_seconds(x):
        try:
            x = float(x)
        except Exception:
            return 0.0
        if x <= 0:
            return 0.0
        return x * 60.0 if unit == "minutes" else x

    played_seconds: Dict[str, float] = {}

    for _, row in m.iterrows():
        dur_sec = to_seconds(row["Duration"])
        if dur_sec <= 0:
            continue

        lineup = extract_lineup_from_row(row, available_posts)
        if not lineup:
            continue

        for p in lineup:
            played_seconds[p] = played_seconds.get(p, 0.0) + dur_sec

    if not played_seconds:
        return pd.DataFrame()

    df = pd.DataFrame(
        {"Player": list(played_seconds.keys()), "Temps de jeu (en minutes)": [v / 60.0 for v in played_seconds.values()]}
    )
    return df.sort_values("Temps de jeu (en minutes)", ascending=False).reset_index(drop=True)


# =========================
# STATS ACTIONS
# =========================
def players_shots(joueurs):
    if joueurs is None or joueurs.empty or "Row" not in joueurs.columns:
        return pd.DataFrame()

    df = joueurs.copy()

    if "Action" in df.columns:
        mask_shot = df["Action"].astype(str).str.contains("Tir", na=False)
    else:
        mask_shot = pd.Series([False] * len(df), index=df.index)

    df = df[mask_shot].copy()
    if df.empty:
        return pd.DataFrame()

    df["Player"] = df["Row"].astype(str).apply(nettoyer_nom_joueuse)
    df["__shots"] = df["Action"].astype(str).apply(lambda s: s.count("Tir"))

    if "Tir" in df.columns:
        tir_txt = df["Tir"].astype(str)
        df["__on_target"] = tir_txt.apply(lambda s: s.count("Tir Cadré") + s.count("But"))
        df["__goals"] = tir_txt.apply(lambda s: s.count("But"))
    else:
        df["__on_target"] = 0
        df["__goals"] = 0

    out = (
        df.groupby("Player", as_index=False)
        .agg({"__shots": "sum", "__on_target": "sum", "__goals": "sum"})
        .rename(columns={"__shots": "Tirs", "__on_target": "Tirs cadrés", "__goals": "Buts"})
        .sort_values(by="Tirs", ascending=False)
        .reset_index(drop=True)
    )
    return out


def players_passes(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()

    short_, long_ = {}, {}
    ok_s, ok_l = {}, {}
    total_, ok_total = {}, {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if not (isinstance(action, str) and "Passe" in action):
            continue

        player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
        passe = joueurs.iloc[i].get("Passe", "") if "Passe" in joueurs.columns else ""
        passe = "" if pd.isna(passe) else str(passe)

        is_short = "Courte" in passe
        is_long = "Longue" in passe
        is_ok = "Réussie" in passe

        total_[player] = total_.get(player, 0) + 1
        if is_ok:
            ok_total[player] = ok_total.get(player, 0) + 1

        if is_short:
            short_[player] = short_.get(player, 0) + 1
            if is_ok:
                ok_s[player] = ok_s.get(player, 0) + 1
        elif is_long:
            long_[player] = long_.get(player, 0) + 1
            if is_ok:
                ok_l[player] = ok_l.get(player, 0) + 1

    if not total_:
        return pd.DataFrame()

    players = sorted(total_.keys())
    df = pd.DataFrame(
        {
            "Player": players,
            "Passes courtes": [short_.get(p, 0) for p in players],
            "Passes longues": [long_.get(p, 0) for p in players],
            "Passes réussies (courtes)": [ok_s.get(p, 0) for p in players],
            "Passes réussies (longues)": [ok_l.get(p, 0) for p in players],
            "Passes": [total_.get(p, 0) for p in players],
            "Passes réussies": [ok_total.get(p, 0) for p in players],
        }
    )
    df["Pourcentage de passes réussies"] = np.where(df["Passes"] > 0, (df["Passes réussies"] / df["Passes"]) * 100, 0)
    return df.sort_values(by="Passes", ascending=False).reset_index(drop=True)


def players_assists(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()

    assists = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if not (isinstance(action, str) and "Passe" in action):
            continue

        player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
        passe = joueurs.iloc[i].get("Passe", "") if "Passe" in joueurs.columns else ""
        passe = "" if pd.isna(passe) else str(passe)

        if "Passe Décisive" in passe:
            assists[player] = assists.get(player, 0) + 1

    if not assists:
        return pd.DataFrame()

    return pd.DataFrame({"Player": list(assists.keys()), "Passes décisives": list(assists.values())})


def players_pass_directions(joueurs):
    if joueurs is None or joueurs.empty:
        return pd.DataFrame()
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns or "Ungrouped" not in joueurs.columns:
        return pd.DataFrame()

    out_cols = [
        "Passes vers l'avant",
        "Passes vers l'avant réussies",
        "Passes vers l'arrière",
        "Passes vers l'arrière réussies",
        "Passes latérales Gauche",
        "Passes latérales Gauche réussies",
        "Passes latérales Droite",
        "Passes latérales Droite réussies",
        "Passes diagonales Gauche",
        "Passes diagonales Gauche réussies",
        "Passes diagonales Droite",
        "Passes diagonales Droite réussies",
    ]

    totals: Dict[str, Dict[str, int]] = {}

    def ensure(p):
        if p not in totals:
            totals[p] = {c: 0 for c in out_cols}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if not (isinstance(action, str) and "Passe" in action):
            continue

        player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
        if not looks_like_player(player):
            continue

        ung = joueurs.iloc[i].get("Ungrouped", "")
        ung_norm = normalize_str(ung)

        cat_total = None
        cat_ok = None

        if "diago gauche" in ung_norm or "diagonale gauche" in ung_norm:
            cat_total = "Passes diagonales Gauche"
            cat_ok = "Passes diagonales Gauche réussies"
        elif "diago droite" in ung_norm or "diagonale droite" in ung_norm:
            cat_total = "Passes diagonales Droite"
            cat_ok = "Passes diagonales Droite réussies"
        elif "laterale gauche" in ung_norm:
            cat_total = "Passes latérales Gauche"
            cat_ok = "Passes latérales Gauche réussies"
        elif "laterale droite" in ung_norm:
            cat_total = "Passes latérales Droite"
            cat_ok = "Passes latérales Droite réussies"
        elif "arriere" in ung_norm:
            cat_total = "Passes vers l'arrière"
            cat_ok = "Passes vers l'arrière réussies"
        elif "avant" in ung_norm:
            cat_total = "Passes vers l'avant"
            cat_ok = "Passes vers l'avant réussies"

        if not cat_total:
            continue

        ensure(player)
        totals[player][cat_total] += 1

        passe = joueurs.iloc[i].get("Passe", "")
        if isinstance(passe, str) and "Réussie" in passe:
            totals[player][cat_ok] += 1

    if not totals:
        return pd.DataFrame()

    rows = []
    for p, d in totals.items():
        r = {"Player": p}
        r.update(d)
        rows.append(r)

    df = pd.DataFrame(rows)
    for c in out_cols:
        if c not in df.columns:
            df[c] = 0
    return df


def players_dribbles(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()
    drb, drb_ok = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Dribble" in action:
            player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
            drb[player] = drb.get(player, 0) + action.count("Dribble")
            status = joueurs.iloc[i].get("Dribble", None) if "Dribble" in joueurs.columns else None
            if isinstance(status, str) and "Réussi" in status:
                drb_ok[player] = drb_ok.get(player, 0) + status.count("Réussi")
    if not drb:
        return pd.DataFrame()
    df = pd.DataFrame({"Player": list(drb.keys()), "Dribbles": list(drb.values()), "Dribbles réussis": [drb_ok.get(p, 0) for p in drb]})
    df["Pourcentage de dribbles réussis"] = (df["Dribbles réussis"] / df["Dribbles"] * 100).fillna(0)
    return df.sort_values(by="Dribbles", ascending=False).reset_index(drop=True)


def players_defensive_duels(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()
    duels, ok, faults = {}, {}, {}
    duels_col = "Duel défensifs" if "Duel défensifs" in joueurs.columns else ("Duel défensif" if "Duel défensif" in joueurs.columns else None)
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Duel défensif" in action:
            player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
            duels[player] = duels.get(player, 0) + action.count("Duel défensif")
            if duels_col:
                status = joueurs.iloc[i].get(duels_col, None)
                if isinstance(status, str):
                    if "Gagné" in status:
                        ok[player] = ok.get(player, 0) + status.count("Gagné")
                    if "Faute" in status:
                        faults[player] = faults.get(player, 0) + status.count("Faute")
    if not duels:
        return pd.DataFrame()
    df = pd.DataFrame({"Player": list(duels.keys()), "Duels défensifs": list(duels.values()), "Duels défensifs gagnés": [ok.get(p, 0) for p in duels], "Fautes": [faults.get(p, 0) for p in duels]})
    df["Pourcentage de duels défensifs gagnés"] = (df["Duels défensifs gagnés"] / df["Duels défensifs"] * 100).fillna(0)
    return df.sort_values(by="Duels défensifs", ascending=False).reset_index(drop=True)


def players_interceptions(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()
    inter = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Interception" in action:
            player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
            inter[player] = inter.get(player, 0) + action.count("Interception")
    if not inter:
        return pd.DataFrame()
    return pd.DataFrame({"Player": list(inter.keys()), "Interceptions": list(inter.values())}).sort_values(by="Interceptions", ascending=False).reset_index(drop=True)


def players_ball_losses(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()
    losses = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Perte de balle" in action:
            player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
            losses[player] = losses.get(player, 0) + action.count("Perte de balle")
    if not losses:
        return pd.DataFrame()
    return pd.DataFrame({"Player": list(losses.keys()), "Pertes de balle": list(losses.values())}).sort_values(by="Pertes de balle", ascending=False).reset_index(drop=True)


def creativity_helpers_from_events(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs is None or joueurs.empty or "Row" not in joueurs.columns:
        return pd.DataFrame()

    d = joueurs.copy()
    d["Player"] = d["Row"].astype(str).apply(nettoyer_nom_joueuse)

    total_passes = pd.Series(dtype=float)
    last_third = pd.Series(dtype=float)
    assists = pd.Series(dtype=float)

    if "Action" in d.columns and "Passe" in d.columns:
        mask_p = d["Action"].astype(str).str.contains("Passe", na=False)
        passe_txt = d.loc[mask_p, "Passe"].astype(str).fillna("")
        player_p = d.loc[mask_p, "Player"]

        total_passes = passe_txt.str.strip().ne("").groupby(player_p).sum().astype(float)
        last_third = passe_txt.str.count("Passe dans dernier 1/3").groupby(player_p).sum().astype(float)
        assists = passe_txt.str.count("Passe Décisive").groupby(player_p).sum().astype(float)

    deseq = pd.Series(dtype=float)
    team_total = 0.0
    if "Création de Deséquilibre" in d.columns:
        filled = d["Création de Deséquilibre"].notna() & d["Création de Deséquilibre"].astype(str).str.strip().ne("")
        deseq = filled.groupby(d["Player"]).sum().astype(float)
        team_total = float(filled.sum())

    players = sorted(set(d["Player"].dropna().unique().tolist()))
    out = pd.DataFrame({"Player": players})
    out["__total_passes"] = out["Player"].map(total_passes).fillna(0.0).astype(float)
    out["__last_third"] = out["Player"].map(last_third).fillna(0.0).astype(float)
    out["__assists"] = out["Player"].map(assists).fillna(0.0).astype(float)
    out["__deseq"] = out["Player"].map(deseq).fillna(0.0).astype(float)
    out["__team_deseq_total"] = team_total
    return out


# =========================
# METRICS / KPI / POSTES
# =========================
def create_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    required_cols = {
        "Timing": ["Duels défensifs", "Fautes"],
        "Force physique": ["Duels défensifs", "Duels défensifs gagnés"],
        "Intelligence tactique": ["Interceptions"],
        "Technique 1": ["Passes"],
        "Technique 2": ["Passes courtes", "Passes réussies (courtes)"],
        "Technique 3": ["Passes longues", "Passes réussies (longues)"],
        "Explosivité": ["Dribbles", "Dribbles réussis"],
        "Prise de risque": ["Dribbles"],
        "Précision": ["Tirs", "Tirs cadrés"],
        "Sang-froid": ["Tirs"],
    }

    for metric, cols in required_cols.items():
        if not all(c in df.columns for c in cols):
            continue

        if metric == "Timing":
            df[metric] = np.where(df[cols[0]] > 0, (df[cols[0]] - df.get(cols[1], 0)) / df[cols[0]], 0)
        elif metric == "Force physique":
            df[metric] = np.where(df[cols[0]] > 0, df.get(cols[1], 0) / df[cols[0]], 0)
        elif metric in ["Intelligence tactique", "Technique 1", "Prise de risque", "Sang-froid"]:
            mmax = pd.to_numeric(df[cols[0]], errors="coerce").max()
            df[metric] = np.where(df[cols[0]] > 0, df[cols[0]] / mmax, 0) if (mmax is not None and mmax > 0) else 0
        else:
            df[metric] = np.where(df[cols[0]] > 0, df.get(cols[1], 0) / df[cols[0]], 0)

    def _series_or_zeros(col: str) -> pd.Series:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0)
        return pd.Series(0, index=df.index, dtype=float)

    total_passes = _series_or_zeros("__total_passes")
    last_third = _series_or_zeros("__last_third")
    assists = _series_or_zeros("__assists")
    deseq = _series_or_zeros("__deseq")
    team_total = _series_or_zeros("__team_deseq_total")

    denom = total_passes.replace(0, np.nan)
    df["Créativité 1"] = ((last_third + 2 * assists) / denom * 100).fillna(0)

    denom_team = team_total.replace(0, np.nan)
    df["Créativité 2"] = (deseq / denom_team * 100).fillna(0)

    to_rank = list(required_cols.keys()) + ["Créativité 1", "Créativité 2"]
    for metric in to_rank:
        if metric in df.columns:
            df[metric] = (pd.to_numeric(df[metric], errors="coerce").rank(pct=True) * 100).fillna(0)

    return df


def create_kpis(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()

    if "Timing" in out.columns and "Force physique" in out.columns:
        out["Rigueur"] = (out["Timing"] + out["Force physique"]) / 2
    if "Intelligence tactique" in out.columns:
        out["Récupération"] = out["Intelligence tactique"]

    tech = [m for m in ["Technique 1", "Technique 2", "Technique 3"] if m in out.columns]
    if tech:
        out["Distribution"] = out[tech].mean(axis=1)

    if "Explosivité" in out.columns and "Prise de risque" in out.columns:
        out["Percussion"] = (out["Explosivité"] + out["Prise de risque"]) / 2

    if "Précision" in out.columns and "Sang-froid" in out.columns:
        out["Finition"] = (out["Précision"] + out["Sang-froid"]) / 2

    if "Créativité 1" in out.columns and "Créativité 2" in out.columns:
        out["Créativité"] = (out["Créativité 1"] + out["Créativité 2"]) / 2

    return out


def create_poste(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    required = ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition"]
    if not all(k in out.columns for k in required):
        return out

    out["Défenseur central"] = (
        out["Rigueur"] * 5 + out["Récupération"] * 5 + out["Distribution"] * 5 + out["Percussion"] * 1 + out["Finition"] * 1
    ) / 17
    out["Défenseur latéral"] = (
        out["Rigueur"] * 3 + out["Récupération"] * 3 + out["Distribution"] * 3 + out["Percussion"] * 3 + out["Finition"] * 3
    ) / 15
    out["Milieu défensif"] = (
        out["Rigueur"] * 4 + out["Récupération"] * 4 + out["Distribution"] * 4 + out["Percussion"] * 2 + out["Finition"] * 2
    ) / 16
    out["Milieu relayeur"] = (
        out["Rigueur"] * 3 + out["Récupération"] * 3 + out["Distribution"] * 3 + out["Percussion"] * 3 + out["Finition"] * 3
    ) / 15
    out["Milieu offensif"] = (
        out["Rigueur"] * 2 + out["Récupération"] * 2 + out["Distribution"] * 2 + out["Percussion"] * 4 + out["Finition"] * 4
    ) / 14
    out["Attaquant"] = (
        out["Rigueur"] * 1 + out["Récupération"] * 1 + out["Distribution"] * 1 + out["Percussion"] * 5 + out["Finition"] * 5
    ) / 13

    return out


# =========================
# CREATE DATA (PFC/EDF)
# =========================
def create_data(match, joueurs, is_edf, home_team=None, away_team=None):
    if is_edf:
        if "Player" not in joueurs.columns or "Temps de jeu" not in joueurs.columns or "Poste" not in joueurs.columns:
            return pd.DataFrame()
        df_duration = pd.DataFrame(
            {
                "Player": joueurs["Player"].apply(nettoyer_nom_joueuse),
                "Temps de jeu (en minutes)": pd.to_numeric(joueurs["Temps de jeu"], errors="coerce").fillna(0),
                "Poste": joueurs["Poste"],
            }
        )
        dfs = [df_duration]
    else:
        if not home_team or not away_team:
            return pd.DataFrame()
        df_duration = players_duration(match, home_team=home_team, away_team=away_team)
        dfs = [df_duration]

    for func in [
        players_shots,
        players_passes,
        players_assists,
        players_pass_directions,
        players_dribbles,
        players_defensive_duels,
        players_interceptions,
        players_ball_losses,
    ]:
        try:
            res = func(joueurs)
            if res is not None and not res.empty:
                dfs.append(res)
        except Exception:
            pass

    valid = []
    for d in dfs:
        if d is not None and not d.empty and "Player" in d.columns:
            dd = d.copy()
            dd["Player"] = dd["Player"].apply(nettoyer_nom_joueuse)
            valid.append(dd)

    if not valid:
        return pd.DataFrame()

    df = valid[0]
    for other in valid[1:]:
        df = df.merge(other, on="Player", how="outer")

    df.fillna(0, inplace=True)

    try:
        ch = creativity_helpers_from_events(joueurs)
        if ch is not None and not ch.empty:
            df = df.merge(ch, on="Player", how="left")
    except Exception:
        ch = None

    for c in ["__total_passes", "__last_third", "__assists", "__deseq", "__team_deseq_total"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df = df[(df.iloc[:, 1:] != 0).any(axis=1)]

    if "Temps de jeu (en minutes)" in df.columns:
        df = df[df["Temps de jeu (en minutes)"] >= 10]

    df = create_metrics(df)
    df = create_kpis(df)
    df = create_poste(df)
    return df


def filter_data_by_player(df, player_name):
    if not player_name or df is None or df.empty or "Player" not in df.columns:
        return df
    pn = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    out = tmp[tmp["Player_clean"] == pn].copy()
    out.drop(columns=["Player_clean"], inplace=True, errors="ignore")
    return out


def prepare_comparison_data(df, player_name, selected_matches=None):
    if df is None or df.empty or "Player" not in df.columns:
        return pd.DataFrame()
    pn = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    filtered = tmp[tmp["Player_clean"] == pn].copy()
    if selected_matches and "Adversaire" in filtered.columns:
        filtered = filtered[filtered["Adversaire"].isin(selected_matches)]
    if filtered.empty:
        return pd.DataFrame()

    aggregated = (
        filtered.groupby("Player")
        .agg({"Temps de jeu (en minutes)": "sum", "Buts": "sum"})
        .join(
            filtered.groupby("Player")
            .mean(numeric_only=True)
            .drop(columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore")
        )
        .reset_index()
    )

    return safe_int_numeric_only(aggregated)


def aggregate_player_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les stats d'une joueuse (moyenne pondérée par temps de jeu)."""
    if df is None or df.empty:
        return pd.DataFrame()

    sum_cols = ["Temps de jeu (en minutes)", "Buts"]
    existing_sum = [c for c in sum_cols if c in df.columns]

    mean_df = df.mean(numeric_only=True)
    agg = mean_df.to_frame().T

    for c in existing_sum:
        if c in df.columns:
            agg[c] = df[c].sum()

    if "Player" in df.columns:
        agg.insert(0, "Player", df["Player"].iloc[0])

    return safe_int_numeric_only(agg)


# =========================
# AGRÉGATION GLOBALE (export)
# =========================
def aggregate_global_players(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "Player" not in df.columns:
        return pd.DataFrame()

    d = df.copy()
    if "Temps de jeu (en minutes)" not in d.columns:
        d["Temps de jeu (en minutes)"] = 0.0

    meta_cols = {"Player", "Adversaire", "Journée", "Catégorie", "Date", "Saison"}

    score_cols = {
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
        "Créativité 1", "Créativité 2",
        "Rigueur", "Récupération", "Distribution", "Percussion", "Finition", "Créativité",
        "Défenseur central", "Défenseur latéral", "Milieu défensif", "Milieu relayeur", "Milieu offensif", "Attaquant",
    }

    minutes = pd.to_numeric(d["Temps de jeu (en minutes)"], errors="coerce").fillna(0.0)
    w = minutes.replace(0, np.nan)

    num_cols = [c for c in d.columns if c not in meta_cols and pd.api.types.is_numeric_dtype(d[c])]
    count_cols = [c for c in num_cols if c not in score_cols and "Pourcentage" not in c and c != "Temps de jeu (en minutes)"]

    for c in count_cols:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0) * minutes / 90.0

    def wavg(s):
        s = pd.to_numeric(s, errors="coerce")
        return np.nan if w.isna().all() else np.nansum(s * w) / np.nansum(w)

    agg_dict = {"Temps de jeu (en minutes)": "sum"}
    for c in num_cols:
        if c == "Temps de jeu (en minutes)":
            continue
        if c in score_cols or "Pourcentage" in c:
            agg_dict[c] = wavg
        else:
            agg_dict[c] = "sum"

    out = d.groupby("Player", as_index=False).agg(agg_dict)

    for c in out.columns:
        if c == "Player":
            continue
        if "Pourcentage" in c or c in score_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(1)
        else:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(0).astype("Int64")

    return out


def denormalize_match_rows_from_per90(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "Temps de jeu (en minutes)" not in df.columns:
        return df

    out = df.copy()
    minutes = pd.to_numeric(out["Temps de jeu (en minutes)"], errors="coerce")

    exclude = {
        "Player", "Adversaire", "Journée", "Journee", "Catégorie", "Categorie", "Date", "Saison",
        "Row", "Row_clean", "Row_team", "Player_clean", "Poste",
        "Temps de jeu", "Temps de jeu (en minutes)",
    }

    exclude_exact = {
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
        "Créativité 1", "Créativité 2", "Créativité",
        "Rigueur", "Récupération", "Recuperation", "Distribution", "Percussion", "Finition",
        "Défenseur central", "Defenseur central", "Défenseur latéral", "Defenseur lateral",
        "Milieu défensif", "Milieu defensif", "Milieu relayeur", "Milieu offensif", "Attaquant",
    }

    scaled_cols: List[str] = []
    for col in list(out.columns):
        if col in exclude:
            continue
        if isinstance(col, str) and "pourcentage" in col.lower():
            continue
        if col in exclude_exact:
            continue

        coerced = pd.to_numeric(out[col], errors="coerce")
        if coerced.notna().sum() == 0:
            continue

        out[col] = np.where(minutes > 0, coerced * (minutes / 90.0), coerced)
        scaled_cols.append(col)

    for col in scaled_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").round(0).astype("Int64")

    return out


# =========================
# GPS - FORMAT GF1 + LEGACY
# =========================
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

def is_gf1_export_format(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    cols = set(map(str, df.columns))
    return len(GPS_GF1_REQUIRED.intersection(cols)) >= 8


def standardize_gps_gf1_export(df: pd.DataFrame, filename: str) -> pd.DataFrame:
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

    # Convertir "Durée_min" (issu de "Temps joué") : format H:MM:SS → minutes
    if "Durée_min" in d.columns:
        def _parse_hmmss(val):
            s = str(val).strip()
            if not s or s.lower() in ("nan", "none", ""):
                return np.nan
            # Déjà numérique (minutes brutes) ?
            try:
                return float(s)
            except ValueError:
                pass
            # Format H:MM:SS ou MM:SS
            parts = s.split(":")
            try:
                if len(parts) == 3:          # H:MM:SS
                    h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
                    return round(h * 60 + m + sec / 60, 1)
                elif len(parts) == 2:        # MM:SS
                    m, sec = int(parts[0]), int(parts[1])
                    return round(m + sec / 60, 1)
            except Exception:
                pass
            return np.nan
        d["Durée_min"] = d["Durée_min"].apply(_parse_hmmss)

    if "DATE" in d.columns:
        # Parser avec gestion timezone : retirer tz et ne garder que la date (sans heure)
        _dates = pd.to_datetime(d["DATE"], errors="coerce", utc=True)
        d["DATE"] = _dates.dt.tz_localize(None).dt.normalize()  # minuit, sans tz
    else:
        dt = parse_date_from_gf1_filename(filename)
        d["DATE"] = pd.Timestamp(dt.date()) if dt else pd.NaT

    d["SEMAINE"] = d["DATE"].dt.isocalendar().week.astype("Int64")
    w_file = parse_week_from_gf1_filename(filename)
    if w_file is not None:
        d["SEMAINE"] = pd.Series([w_file] * len(d), index=d.index, dtype="Int64")

    for c in ["Distance (m)", "Sprints_23", "Sprints_25", "Vitesse max (km/h)", "Accélération maximale (m/s²)", "#accel/decel"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    def _num(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    v0_7    = _num("Distance par plage de vitesse (0-7 km/h)")
    v7_13   = _num("Distance par plage de vitesse (7-13 km/h)")
    v13_15  = _num("Distance par plage de vitesse (13-15 km/h)")
    v15_19  = _num("Distance par plage de vitesse (15-19 km/h)")
    v19_23  = _num("Distance par plage de vitesse (19-23 km/h)")
    v23_25  = _num("Distance par plage de vitesse (23-25 km/h)")
    v_sup25 = _num("Distance par plage de vitesse (>25 km/h)")

    d["V_0_7"]            = v0_7
    d["V_7_13"]           = v7_13
    d["Distance 13-19 (m)"] = v13_15 + v15_19
    d["Distance 19-23 (m)"] = v19_23
    d["Distance >23 (m)"]   = v23_25 + v_sup25

    d["__source_file"] = os.path.basename(filename)
    return d



# ─── GPS MATCH ──────────────────────────────────────────────────────

def is_gps_match_file(filename: str) -> bool:
    """Détecte un fichier GPS de match : U19_, U17_, J0x_, vs _, - _, etc."""
    fn = normalize_str(filename)
    match_patterns = ["u19", "u17", "u16", "u15", "_j0", "_j1", "_j2", " vs ", "match", "contre"]
    seance_patterns = ["gf1", "seance", "séance", "entrainement", "entraînement"]
    has_match = any(p in fn for p in match_patterns)
    has_seance = any(p in fn for p in seance_patterns)
    return has_match and not has_seance


def parse_match_info_from_filename(filename: str) -> dict:
    """Extrait adversaire, date et journée depuis le nom de fichier GPS match."""
    import re
    name = os.path.splitext(filename)[0]
    info = {"adversaire": "", "date": None, "journee": "", "label": name}

    # Date : format DDMMYYYY ou DD_MM_YY à la fin
    date_match = re.search(r'(\d{2})[_\-](\d{2})[_\-](\d{2,4})', name)
    if date_match:
        d, m, y = date_match.groups()
        y = "20" + y if len(y) == 2 else y
        try:
            info["date"] = pd.Timestamp(f"{y}-{m}-{d}")
        except Exception:
            pass

    # Journée : J0x ou J1x
    j_match = re.search(r'[_\-](J\d+)[_\-]', name, re.IGNORECASE)
    if j_match:
        info["journee"] = j_match.group(1).upper()

    # Adversaire : pattern "PFC - XXX" ou "XXX - PFC" ou "PFC_XXX"
    adv_match = re.search(r'Paris[_\s]FC[_\s\-]+([^_\-\d]+)|([^_\-\d]+)[_\s\-]+Paris[_\s]FC', name, re.IGNORECASE)
    if adv_match:
        adv = (adv_match.group(1) or adv_match.group(2) or "").strip().strip("_- ")
        # Nettoyer les mots parasites
        adv = re.sub(r'(U19|U17|U16|U15|J\d+)', '', adv, flags=re.IGNORECASE).strip().strip("_- ")
        info["adversaire"] = adv[:40] if adv else ""

    # Label lisible
    date_str = info["date"].strftime("%d/%m/%Y") if info["date"] else ""
    parts = [p for p in [info["journee"], info["adversaire"], date_str] if p]
    info["label"] = " · ".join(parts) if parts else name

    return info


@st.cache_data(ttl=600, show_spinner=False)
def load_gps_match(ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon,
                   first_to_canons, last_to_canons) -> pd.DataFrame:
    """Charge et normalise tous les fichiers GPS match détectés localement."""
    paths = []
    # Dossier dédié gps_match : TOUT fichier CSV est accepté, quel que soit son nom
    if os.path.exists(GPS_MATCH_FOLDER):
        for root, _, files in os.walk(GPS_MATCH_FOLDER):
            for f in files:
                if f.lower().endswith(".csv"):
                    full = os.path.join(root, f)
                    if full not in paths:
                        paths.append(full)
    # Dossier GPS général et DATA : filtrer par nom pour ne pas confondre avec les séances
    for d in [GPS_FOLDER, DATA_FOLDER]:
        if not os.path.exists(d):
            continue
        for root, _, files in os.walk(d):
            for f in files:
                if not f.lower().endswith(".csv"):
                    continue
                if is_gps_match_file(f):
                    full = os.path.join(root, f)
                    if full not in paths:
                        paths.append(full)

    if not paths:
        return pd.DataFrame()

    frames = []
    for p in sorted(paths):
        try:
            df = read_csv_auto(p)
            fname = os.path.basename(p)

            # Filtrer les lignes agrégats (Nom de joueur vide = totaux/moyennes)
            if "Nom de joueur" in df.columns:
                df = df[df["Nom de joueur"].notna() & (df["Nom de joueur"].astype(str).str.strip() != "")].copy()

            # Réutiliser la standardisation GF1 existante
            if is_gf1_export_format(df):
                df = standardize_gps_gf1_export(df, fname)
            else:
                continue  # format inconnu

            # Infos match depuis nom de fichier
            minfo = parse_match_info_from_filename(fname)
            df["__match_label"] = minfo["label"]
            df["__adversaire"] = minfo["adversaire"]
            df["__journee"] = minfo["journee"]
            # N'utiliser la date du filename que si la colonne DATE est absente ou entièrement vide
            if "DATE" not in df.columns or df["DATE"].isna().all():
                if minfo["date"] is not None:
                    df["DATE"] = minfo["date"]

            # Garder colonnes plages vitesse pour barres
            for col_orig, col_std in [
                ("Distance par plage de vitesse (0-7 km/h)", "V_0_7"),
                ("Distance par plage de vitesse (7-13 km/h)", "V_7_13"),
                ("Distance par plage de vitesse (13-15 km/h)", "V_13_15"),
                ("Distance par plage de vitesse (15-19 km/h)", "V_15_19"),
                ("Distance par plage de vitesse (19-23 km/h)", "V_19_23"),
                ("Distance par plage de vitesse (23-25 km/h)", "V_23_25"),
                ("Distance par plage de vitesse (>25 km/h)", "V_sup25"),
                ("# of Accelerations (>2 m/s²)", "Acc_2"),
                ("# of Accelerations (>3 m/s²)", "Acc_3"),
                ("# of Accelerations (>4 m/s²)", "Acc_4"),
                ("# of Decélerations (>2 m/s²)", "Dec_2"),
                ("# of Decélerations (>3 m/s²)", "Dec_3"),
                ("# of Decélerations (>4 m/s²)", "Dec_4"),
                ("Accélération maximale (m/s²)", "Acc_max"),
                ("#accel/decel", "#accel/decel"),
                ("Unnamed: 25", "#accel/decel"),
            ]:
                # Chercher dans le df original (avant standardize, qui l'a renommé)
                if col_orig in df.columns:
                    df[col_std] = pd.to_numeric(df[col_orig], errors="coerce")
                elif col_std not in df.columns:
                    # Chercher via le df original avant rename
                    pass

            df["__source_file"] = fname
            frames.append(df)
        except Exception as e:
            _warn(f"GPS Match: impossible de lire {os.path.basename(p)} → {e}")
            continue

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    # Mapper les noms vers les noms canoniques
    if "NOM" in result.columns:
        # Appliquer d'abord la concordance manuelle GPS → canon
        _gps_map = load_gps_name_map()
        mapped, statuses = [], []
        for v in result["NOM"].astype(str).tolist():
            # 1. Concordance manuelle GPS
            _vk = normalize_name_raw(v)
            if _vk in _gps_map:
                mapped.append(_gps_map[_vk])
                statuses.append("gps_manual")
                continue
            # 2. Matching automatique via référentiel
            m, status, _ = map_player_name(v, ref_set, alias_to_canon, tokenkey_to_canon,
                                           compact_to_canon, first_to_canons, last_to_canons, cutoff_fuzzy=0.88)
            mapped.append(m)
            statuses.append(status)
        result["Player"] = mapped
        result["__name_status"] = statuses

    # Convertir colonnes numériques
    for c in ["Durée_min", "Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
              "Sprints_23", "Sprints_25", "Vitesse max (km/h)",
              "V_0_7","V_7_13","V_13_15","V_15_19","V_19_23","V_23_25","V_sup25",
              "Acc_2","Acc_3","Acc_4","Dec_2","Dec_3","Dec_4","Acc_max","#accel/decel"]:
        if c in result.columns:
            result[c] = pd.to_numeric(result[c], errors="coerce")

    result["DATE"] = pd.to_datetime(result.get("DATE", pd.NaT), errors="coerce")
    return result


def sync_gps_match_from_drive() -> Tuple[int, int]:
    """Télécharge les fichiers GPS match depuis le dossier Drive dédié."""
    if not DRIVE_GPS_MATCH_FOLDER_ID:
        # Chercher automatiquement dans le dossier GPS principal
        return 0, 0
    os.makedirs(GPS_MATCH_FOLDER, exist_ok=True)
    service = authenticate_google_drive()
    ok, fail = 0, 0
    try:
        items = list_files_in_folder_paged(service, DRIVE_GPS_MATCH_FOLDER_ID, page_size=200)
        for f in items:
            name = f.get("name", "")
            if not name.lower().endswith(".csv"):
                continue
            fid = f["id"]
            dest = os.path.join(GPS_MATCH_FOLDER, name)
            if os.path.exists(dest):
                ok += 1
                continue
            try:
                download_drive_csv_to_local(service, fid, name, dest_folder=GPS_MATCH_FOLDER)
                ok += 1
            except Exception as e:
                _warn(f"GPS Match sync: {name} → {e}")
                fail += 1
    except Exception as e:
        _warn(f"GPS Match sync Drive: {e}")
    return ok, fail


# ─── DONNÉES TECHNICO-TACTIQUES ─────────────────────────────────────

def is_tactical_file(filename: str) -> bool:
    """Détecte un fichier tactique : PFC_VS__ ou contient des colonnes Timeline/Action."""
    fn = normalize_str(filename)
    return fn.startswith(normalize_str("PFC_VS")) or "pfc_vs" in fn


def parse_tactical_filename(filename: str) -> dict:
    """Extrait date, adversaire, journée et saison depuis le nom d'un fichier tactique.

    Formats connus :
      - PFC_VS__2526_U19F_HAC_J10_U19_07-12-2025.csv   -> 25/26 · J10 · HAC
      - PFC_VS_ 2425 U19F MHSC_P2J4_U19_02-03-2025.csv -> 24/25 · J04 · MHSC
      - U19_2_J02_Paris_FC_-_OL_Lyonnes_25_01_26.csv   -> J02 · OL Lyonnes
      - U19_2_J03_PSG_-_Paris_FC_07_02_26.csv          -> J03 · PSG
    """
    name = os.path.splitext(os.path.basename(filename))[0]
    info = {"date": None, "journee": "", "adversaire": "", "adv_norm": "", "saison": "", "label": name}

    # Saison : 4 chiffres AABB consecutifs (ex: 2526 = 25/26)
    sais_m = re.search(r'(?<![0-9])(\d{2})(\d{2})(?![0-9])', name)
    if sais_m:
        a1, a2 = int(sais_m.group(1)), int(sais_m.group(2))
        if 20 <= a1 <= 30 and a2 == a1 + 1:
            info["saison"] = f"{a1}/{a2}"

    # Date
    dt = extract_any_date_from_string(name)
    if dt is not None and pd.notna(dt):
        try:
            info["date"] = pd.Timestamp(dt).normalize()
        except Exception:
            pass

    # Journee P2J4 ou Jxx
    pj_m = re.search(r'[_\s]P\d+J(\d{1,2})[_\s]', name, re.IGNORECASE)
    j_m  = re.search(r'[_\s\-]J0*(\d{1,2})[_\s\-]', name, re.IGNORECASE)
    if pj_m:
        info["journee"] = pj_m.group(1).zfill(2)
    elif j_m:
        info["journee"] = j_m.group(1).zfill(2)

    # Adversaire
    adv = None
    # Cas 1 : PFC_VS__SSSS_CAT_ADV_Jxx
    adv_m = re.search(
        r'PFC_VS_[\s_]+\d{4}[\s_]+\w+[\s_]+([A-Za-z\u00C0-\u024F][\w\s]+?)[\s_]+(?:P\d+)?J\d+',
        name, re.IGNORECASE)
    if adv_m:
        adv = adv_m.group(1).replace("_", " ").strip()
    # Cas 2 : Paris_FC_-_ADV
    if not adv:
        m2 = re.search(r'Paris[_\s]+FC[_\s]*-[_\s]*([A-Za-z\u00C0-\u024F][A-Za-z\u00C0-\u024F_\s]+)', name, re.IGNORECASE)
        if m2:
            adv = m2.group(1).replace("_", " ").strip()
            adv = re.sub(r'\s*\d+\s*', ' ', adv).strip()
    # Cas 3 : ADV_-_Paris_FC
    if not adv:
        m3 = re.search(r'([A-Za-z\u00C0-\u024F][A-Za-z\u00C0-\u024F_\s]+?)[_\s]*-[_\s]*Paris', name, re.IGNORECASE)
        if m3:
            adv = m3.group(1).replace("_", " ").strip()
            adv = re.sub(r'\s*\d+\s*', ' ', adv).strip()
    if adv:
        adv = re.sub(r'\b(U19F?|U17|U16|U15|NAT|CSV)\b', '', adv, flags=re.IGNORECASE)
        adv = re.sub(r'\s{2,}', ' ', adv).strip(" _-")
        info["adversaire"] = adv
        info["adv_norm"]   = normalize_str(adv)

    return info

def _adv_similarity(a: str, b: str) -> float:
    """Score de similarité entre deux noms d'adversaires normalisés."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # L'un contient l'autre
    if a in b or b in a:
        return 0.85
    # Chevauchement de tokens
    ta, tb = set(re.findall(r"[a-z0-9]{2,}", a)), set(re.findall(r"[a-z0-9]{2,}", b))
    if not ta or not tb:
        return 0.0
    inter = ta & tb
    return len(inter) / max(len(ta), len(tb))


@st.cache_data(ttl=600, show_spinner=False)
def load_tactical_files() -> list:
    """Charge tous les fichiers tactiques CSV depuis TACTICAL_FOLDER.
    Retourne une liste de dicts : {path, date, journee, adversaire, adv_norm, df}
    """
    import re
    results = []
    search_dirs = [TACTICAL_FOLDER, "data/tactical"]
    seen = set()

    for folder in search_dirs:
        if not os.path.exists(folder):
            continue
        for f in os.listdir(folder):
            if not f.lower().endswith(".csv"):
                continue
            if not is_tactical_file(f):
                continue
            full = os.path.join(folder, f)
            if full in seen:
                continue
            seen.add(full)
            try:
                df = read_csv_auto(full)
                # Vérification : doit avoir Timeline et Action (colonnes tactiques)
                if "Timeline" not in df.columns or "Action" not in df.columns:
                    continue
                info = parse_tactical_filename(f)

                # ── Enrichissement depuis la colonne Timeline ──────────────────
                # Timeline contient ex: "U19N J10 Paris FC - HAC"
                _tl = str(df["Timeline"].dropna().iloc[0]) if not df["Timeline"].dropna().empty else ""
                if _tl:
                    # Adversaire depuis Timeline (prioritaire sur nom de fichier)
                    _adv_m = re.search(r"paris\s*fc\s*[-–]\s*(.+)|(.+)\s*[-–]\s*paris\s*fc", _tl, re.IGNORECASE)
                    if _adv_m:
                        _adv = (_adv_m.group(1) or _adv_m.group(2) or "").strip()
                        if _adv:
                            info["adversaire"] = _adv
                            info["adv_norm"]   = normalize_str(_adv)

                    # Journée depuis Timeline si absente
                    if not info["journee"]:
                        _pj = re.search(r'P\d+J(\d{1,2})', _tl, re.IGNORECASE)
                        _jm = re.search(r'\bJ(\d{1,2})\b', _tl, re.IGNORECASE)
                        if _pj:
                            info["journee"] = _pj.group(1).zfill(2)
                        elif _jm:
                            info["journee"] = _jm.group(1).zfill(2)

                    # Saison depuis Timeline si absente : ex "U19N" → compétition, pas saison
                    # La saison vient du nom de fichier uniquement (AABB pattern)

                    # Compétition depuis Timeline : token avant Jxx ou Paris FC
                    _comp_m = re.search(r'^([A-Za-z0-9]+(?:\s[A-Za-z0-9]+)?)\s+(?:P\d+)?J\d+', _tl, re.IGNORECASE)
                    if _comp_m:
                        info["competition"] = _comp_m.group(1).strip()

                results.append({**info, "path": full, "filename": f, "df": df})
            except Exception as e:
                _warn(f"Tactique: impossible de lire {f} → {e}")
                continue

    return results


def match_tactical_to_gps(gps_row: dict, tactical_files: list) -> "pd.DataFrame | None":
    """Associe un match GPS à son fichier tactique.
    Priorité : date exacte → (date + adversaire) → (journee + adversaire).
    Retourne le DataFrame tactique ou None.
    """
    gps_date = gps_row.get("date")        # pd.Timestamp ou None
    gps_adv  = normalize_str(str(gps_row.get("adversaire", "")))
    gps_j    = str(gps_row.get("journee", "")).lstrip("J").lstrip("0") or ""

    best_score = 0.0
    best_df = None

    for t in tactical_files:
        score = 0.0
        t_date = t.get("date")
        t_adv  = t.get("adv_norm", "")
        t_j    = str(t.get("journee", "")).lstrip("0") or ""

        # Date exacte = fort signal
        if gps_date and t_date and abs((gps_date - t_date).days) <= 1:
            score += 2.0

        # Adversaire
        adv_sim = _adv_similarity(gps_adv, t_adv)
        score += adv_sim * 1.5

        # Journée
        if gps_j and t_j and gps_j == t_j:
            score += 0.5

        if score > best_score:
            best_score = score
            best_df = t["df"]

    # Seuil minimal : au moins une date ou un adversaire reconnu
    return best_df if best_score >= 1.5 else None


def _filter_player_rows(df_tactic, player_name):
    if "Row" not in df_tactic.columns:
        return df_tactic.iloc[0:0]
    player_norm = normalize_str(player_name)
    mask = df_tactic["Row"].dropna().apply(
        lambda x: normalize_str(str(x)) == player_norm or
                  player_norm in normalize_str(str(x)) or
                  normalize_str(str(x)) in player_norm
    )
    d = df_tactic[mask].copy()
    if d.empty:
        for tok in [t for t in player_norm.split() if len(t) > 2]:
            m2 = df_tactic["Row"].dropna().apply(lambda x: tok in normalize_str(str(x)))
            if m2.sum() > 0:
                return df_tactic[m2].copy()
    return d


def compute_tactical_stats(df_tactic, player_name):
    """Calcule les stats technico-tactiques complètes pour une joueuse."""
    from collections import Counter
    d = _filter_player_rows(df_tactic, player_name)
    if d.empty:
        return {}
    stats = {"nom_row": d["Row"].iloc[0] if "Row" in d.columns else player_name}

    # PASSES
    pass_rows = d[d["Passe"].notna()].copy() if "Passe" in d.columns else d.iloc[0:0]
    all_pass = [a.strip() for cell in pass_rows["Passe"].dropna() for a in str(cell).split(",")]
    p_ok = all_pass.count("Réussie")
    p_ko = all_pass.count("Ratée")
    c_ok = sum(1 for _, r in pass_rows.iterrows() if "Courte" in str(r.get("Passe","")) and "Réussie" in str(r.get("Passe","")))
    c_ko = sum(1 for _, r in pass_rows.iterrows() if "Courte" in str(r.get("Passe","")) and "Ratée"   in str(r.get("Passe","")))
    l_ok = sum(1 for _, r in pass_rows.iterrows() if "Longue" in str(r.get("Passe","")) and "Réussie" in str(r.get("Passe","")))
    l_ko = sum(1 for _, r in pass_rows.iterrows() if "Longue" in str(r.get("Passe","")) and "Ratée"   in str(r.get("Passe","")))
    stats.update({"passes_ok":p_ok,"passes_ko":p_ko,"courtes_ok":c_ok,"courtes_ko":c_ko,"longues_ok":l_ok,"longues_ko":l_ko})

    # Coordonnées Sportscode : 1-80 sur les deux axes
    # SVG viewBox : 0-100 (longueur) x 0-68 (largeur)
    _FIELD_MAX = 80.0
    _SVG_W, _SVG_H = 100.0, 68.0

    # Détecter les instances MT2 pour inverser X (changement de côté)
    _mt2_inst: set = set()
    if "Row" in df_tactic.columns and "Instance number" in df_tactic.columns and "Mi-temps" in df_tactic.columns:
        _pfc_mt = df_tactic[df_tactic["Row"] == "PFC"][["Instance number", "Mi-temps"]].copy()
        _mt2_inst = set(_pfc_mt[_pfc_mt["Mi-temps"].apply(lambda x: "MT2" in str(x))]["Instance number"].tolist())

    def _inst(r):
        try: return int(str(r.get("Instance number","")).split(",")[0].strip())
        except: return None

    def _norm_x(v, inst=None):
        raw = float(v) * _SVG_W / _FIELD_MAX
        if inst is not None and inst in _mt2_inst:
            raw = _SVG_W - raw  # inversion côté MT2
        # PFC attaque vers la droite : x grand Sportscode = BUT PFC (droite SVG)
        return round(max(1.5, min(98.5, raw)), 1)

    def _norm_y(v): return round(max(1.5, min(66.5, _SVG_H - float(v) * _SVG_H / _FIELD_MAX)), 1)

    pass_map = []
    for _, r in pass_rows.iterrows():
        try:
            _i = _inst(r)
            x = _norm_x(str(r.get("X_localisation","")).split(",")[0].strip(), _i)
            y = _norm_y(str(r.get("Y_localisation","")).split(",")[0].strip())
            ok = "Réussie" in str(r.get("Passe",""))
            longue = "Longue" in str(r.get("Passe",""))
            pass_map.append({"x": x, "y": y, "ok": ok, "longue": longue})
        except Exception:
            pass
    stats["passes_map"] = pass_map

    # DRIBBLES
    drib_rows = d[d["Dribble"].notna()] if "Dribble" in d.columns else d.iloc[0:0]
    all_drib = [a.strip() for cell in drib_rows["Dribble"].dropna() for a in str(cell).split(",")]
    stats["drib_ok"] = all_drib.count("Réussi")
    stats["drib_ko"] = all_drib.count("Raté")

    # TIRS
    tir_rows = d[d["Tir"].notna()] if "Tir" in d.columns else d.iloc[0:0]
    all_tir = [a.strip() for cell in tir_rows["Tir"].dropna() for a in str(cell).split(",")]
    stats["tirs_tot"]    = len(tir_rows)
    stats["tirs_cadres"] = all_tir.count("Tir Cadré") + all_tir.count("But")
    stats["tirs_buts"]   = all_tir.count("But")

    # PERTES / BALLONS
    all_actions_flat = [a.strip() for cell in d["Action"].dropna() for a in str(cell).split(",")] if "Action" in d.columns else []
    stats["pertes"]        = sum(1 for a in all_actions_flat if "Perte" in a)
    stats["recuperations"] = sum(1 for a in all_actions_flat if "Interception" in a)
    stats["ballons"]       = len(d)

    # DUELS
    duel_rows = d[d["Duel défensifs"].notna()] if "Duel défensifs" in d.columns else d.iloc[0:0]
    all_duels = [a.strip() for cell in duel_rows["Duel défensifs"].dropna() for a in str(cell).split(",")]
    sol_list  = [r for _, r in duel_rows.iterrows() if "Sol"    in str(r.get("Duel défensifs",""))]
    aer_list  = [r for _, r in duel_rows.iterrows() if "Aérien" in str(r.get("Duel défensifs",""))]
    stats.update({
        "duels_gagnes": all_duels.count("Gagné"),
        "duels_perdus": all_duels.count("Perdu"),
        "sol_ok":  sum(1 for r in sol_list if "Gagné" in str(r.get("Duel défensifs",""))),
        "sol_ko":  sum(1 for r in sol_list if "Perdu" in str(r.get("Duel défensifs",""))),
        "aer_ok":  sum(1 for r in aer_list if "Gagné" in str(r.get("Duel défensifs",""))),
        "aer_ko":  sum(1 for r in aer_list if "Perdu" in str(r.get("Duel défensifs",""))),
        "interceptions": stats["recuperations"],
    })

    # LOCALISATION — normalisation coordonnées Sportscode (1-80) → SVG (0-100 × 0-68)
    locs = []
    for _, r in d.iterrows():
        try:
            _i = _inst(r)
            x = _norm_x(str(r.get("X_localisation","")).split(",")[0].strip(), _i)
            y = _norm_y(str(r.get("Y_localisation","")).split(",")[0].strip())
            locs.append({"x": x, "y": y})
        except Exception:
            pass
    stats["locs"] = locs

    # META
    stats["postes"]  = ", ".join(p for p in d["Poste"].dropna().apply(lambda x: str(x).split(",")[0].strip()).unique() if p and p.lower() not in ("nan","")) if "Poste" in d.columns else ""
    stats["systeme"] = d["Système de Jeu PFC"].dropna().apply(lambda x: str(x).split(",")[0].strip()).mode().iloc[0] if "Système de Jeu PFC" in d.columns and not d["Système de Jeu PFC"].dropna().empty else ""

    # Legacy
    stats["nb_actions"] = len(d); stats["nb_passes"] = p_ok+p_ko; stats["nb_tirs"] = stats["tirs_tot"]
    stats["nb_pertes"] = stats["pertes"]; stats["nb_dribbles"] = stats["drib_ok"]+stats["drib_ko"]
    stats["nb_duels_def"] = stats["duels_gagnes"]+stats["duels_perdus"]; stats["nb_interceptions"] = stats["interceptions"]
    return stats


def _build_all_player_stats(df_tactic):
    """Calcule les stats pour toutes les joueuses du fichier tactique."""
    if df_tactic is None or df_tactic.empty or "Row" not in df_tactic.columns:
        return {}
    skip = {"START","PFC","HAC",""}
    players = [r for r in df_tactic["Row"].dropna().unique()
               if r not in skip and not any(k in str(r) for k in ["Transition","Carton","def "])]
    return {p: s for p in players for s in [compute_tactical_stats(df_tactic, p)] if s}


def _get_match_context(df_tactic):
    """Extrait le contexte match depuis les lignes PFC du fichier tactique."""
    import re as _re
    if df_tactic is None or "Row" not in df_tactic.columns:
        return {}
    pfc = df_tactic[df_tactic["Row"] == "PFC"]
    tl  = str(df_tactic["Timeline"].dropna().iloc[0]) if "Timeline" in df_tactic.columns and not df_tactic["Timeline"].dropna().empty else ""
    adv_m = _re.search(r"paris\s*fc\s*[-–]\s*(.+)|(.+)\s*[-–]\s*paris\s*fc", tl, _re.IGNORECASE)
    ctx = {"timeline": tl, "pfc": "Paris FC",
           "adversaire": (adv_m.group(1) or adv_m.group(2) or "ADV").strip() if adv_m else "ADV"}
    if not pfc.empty:
        scores = pfc["Score"].dropna().apply(lambda x: str(x).split(",")[0].strip()) if "Score" in pfc.columns else pd.Series(dtype=str)
        sf = scores.iloc[-1] if not scores.empty else "0-0"
        pts = sf.split("-")
        ctx["score_pfc"]    = pts[0].strip() if len(pts)>=2 else "?"
        ctx["score_adv"]    = pts[1].strip() if len(pts)>=2 else "?"
        ctx["lieu"]         = pfc["Lieu"].dropna().apply(lambda x: x.split(",")[0].strip()).iloc[0] if "Lieu" in pfc.columns and not pfc["Lieu"].dropna().empty else ""
        ctx["journee"]      = str(pfc["Journée"].dropna().apply(lambda x: str(x).split(",")[0]).iloc[0]) if "Journée" in pfc.columns and not pfc["Journée"].dropna().empty else ""
        ctx["competition"]  = pfc["Compétition"].dropna().apply(lambda x: x.split(",")[0].strip()).iloc[0] if "Compétition" in pfc.columns and not pfc["Compétition"].dropna().empty else ""
        ctx["systeme"]      = pfc["Système de Jeu PFC"].dropna().apply(lambda x: x.split(",")[0].strip()).mode().iloc[0] if "Système de Jeu PFC" in pfc.columns and not pfc["Système de Jeu PFC"].dropna().empty else ""
        adv_name = pfc["Teamersaire"].dropna().iloc[0] if "Teamersaire" in pfc.columns and not pfc["Teamersaire"].dropna().empty else ctx["adversaire"]
        ctx["adversaire"] = adv_name
        adv_seq = len(df_tactic[df_tactic["Row"] == adv_name])
        total = len(pfc) + adv_seq
        ctx["poss_pfc"] = round(len(pfc)/total*100, 1) if total > 0 else 50.0
        ctx["poss_adv"] = round(100 - ctx["poss_pfc"], 1)
    else:
        ctx.update({"score_pfc":"?","score_adv":"?","lieu":"","journee":"","competition":"","systeme":"","poss_pfc":50.0,"poss_adv":50.0})
    return ctx



def get_gps_match_summary_for_player(gps_match_df: pd.DataFrame,
                                    player_name: str,
                                    match_date: Optional[pd.Timestamp] = None,
                                    match_label: Optional[str] = None) -> Optional[Dict[str, float]]:
    """Return a compact GPS summary for ONE player on ONE match (date/label based).

    Matching model:
    1) Prefer exact match on calendar day (DATE normalized).
    2) Fallback to ±1 day around the match date (timezone / midnight shifts).
    3) If match_date missing OR no rows found, try to extract a date from match_label / filename.
    4) If still none, pick the closest activity date (±3 days) with the largest duration/distance as a best-effort.
    """
    if gps_match_df is None or getattr(gps_match_df, "empty", True):
        return None

    df = gps_match_df.copy()
    df = ensure_date_column(df)

    if "Player" not in df.columns or "DATE" not in df.columns:
        return None

    p = nettoyer_nom_joueuse(player_name)
    p_tokens = nom_tokens(player_name)
    p_tokens_raw = set(normalize_name_raw(player_name).split())  # tokens sans tri

    # Filtrer d'abord les lignes agrégats (totaux équipe = NOM vide ou NaN)
    if "NOM" in df.columns:
        df = df[df["NOM"].notna() & (df["NOM"].astype(str).str.strip().str.lower() != "nan") & (df["NOM"].astype(str).str.strip() != "")].copy()

    def _match_player(val):
        v = str(val)
        # 1. Matching exact normalisé
        if nom_tokens(v) == p_tokens or nettoyer_nom_joueuse(v) == p:
            return True
        # 2. Matching via concordance manuelle GPS → canon
        v_mapped = apply_gps_name_map(v)
        if nettoyer_nom_joueuse(v_mapped) == p or nom_tokens(v_mapped) == p_tokens:
            return True
        # 3. Matching partiel : au moins 2 tokens communs ou un seul token si nom simple
        v_toks = set(normalize_name_raw(v).split())
        common = p_tokens_raw & v_toks
        if len(common) >= 2:
            return True
        if len(common) == 1 and (len(p_tokens_raw) == 1 or len(v_toks) == 1):
            return True
        return False

    # Essai 1 : colonne Player (issue de map_player_name)
    df_by_player = df[df["Player"].astype(str).apply(_match_player)].copy()
    # Essai 2 : colonne NOM brute (si map_player_name a mal mappé)
    df_by_nom = df[df["NOM"].astype(str).apply(_match_player)].copy() if "NOM" in df.columns else pd.DataFrame()
    # Prendre l'union (sans doublons) puis filtrer ensuite par match
    df = pd.concat([df_by_player, df_by_nom]).drop_duplicates().copy() if not df_by_player.empty or not df_by_nom.empty else pd.DataFrame()
    if df.empty:
        return None

    # ---- resolve match_date
    md = None
    if match_date is not None and pd.notna(match_date):
        md = pd.to_datetime(match_date, errors="coerce")
    if (md is None or pd.isna(md)) and match_label:
        md = extract_any_date_from_string(str(match_label))
    if md is not None and pd.notna(md):
        md = pd.Timestamp(md).normalize()

    # ---- Matching GPS : date exacte → ±1j → adversaire/journée → label
    df_work = pd.DataFrame()

    # 1) Par date exacte
    if md is not None and pd.notna(md):
        df_exact = df[df["DATE"].dt.normalize() == md].copy()
        if not df_exact.empty:
            df_work = df_exact
        else:
            # ±1 jour
            df_pm1 = df[(df["DATE"].dt.normalize() >= (md - pd.Timedelta(days=1))) &
                        (df["DATE"].dt.normalize() <= (md + pd.Timedelta(days=1)))].copy()
            if not df_pm1.empty:
                df_work = df_pm1

    # 2) Fallback par adversaire + journée dans __match_label / __adversaire / __journee
    if df_work.empty and match_label:
        ml_norm = normalize_str(str(match_label))
        # Chercher adversaire et journée extraits du label (ex: "25/26 · U19N · J02 · OL Lyonnes")
        _adv_tokens = set()
        _jrnee = ""
        _jm = re.search(r'\bJ(\d{1,2})\b', str(match_label), re.IGNORECASE)
        if _jm:
            _jrnee = _jm.group(1).zfill(2)
        # Tokens non-numériques non-génériques comme adversaire potentiel
        _generic = {'j', 'u19', 'u17', 'nat', 'u19n', 'u19f', 'pfc', 'paris', 'fc'}
        for tok in re.split(r'[\s·\-_]+', ml_norm):
            if tok and not tok.isdigit() and tok not in _generic and len(tok) >= 2:
                _adv_tokens.add(tok)

        if "__match_label" in df.columns:
            def _label_match(lbl):
                ln = normalize_str(str(lbl))
                # Match par journée
                if _jrnee:
                    lj = re.search(r'\bJ(\d{1,2})\b', ln, re.IGNORECASE)
                    if lj and lj.group(1).zfill(2) == _jrnee:
                        return True
                # Match par token adversaire
                return any(tok in ln for tok in _adv_tokens)
            df_lbl = df[df["__match_label"].astype(str).apply(_label_match)].copy()
            if not df_lbl.empty:
                df_work = df_lbl

        # Aussi essayer via __adversaire et __journee séparément
        if df_work.empty and ("__adversaire" in df.columns or "__journee" in df.columns):
            mask = pd.Series([True] * len(df), index=df.index)
            if _jrnee and "__journee" in df.columns:
                mask &= df["__journee"].astype(str).apply(
                    lambda j: re.sub(r'[^0-9]', '', str(j)).zfill(2) == _jrnee)
            if _adv_tokens and "__adversaire" in df.columns:
                mask &= df["__adversaire"].astype(str).apply(
                    lambda a: any(tok in normalize_str(a) for tok in _adv_tokens))
            df_adv = df[mask].copy()
            if not df_adv.empty:
                df_work = df_adv

    if df_work.empty:
        return None

    # ── Dédoublonnage : si plusieurs lignes pour la même joueuse sur le même match
    # (ex: 2 fichiers GPS exportés pour le même match : complet + par période),
    # garder UNIQUEMENT la ligne avec la plus grande distance (= session complète).
    if len(df_work) > 1 and "Distance (m)" in df_work.columns:
        dist_col = pd.to_numeric(df_work["Distance (m)"], errors="coerce")
        if dist_col.notna().any():
            best_idx = dist_col.idxmax()
            df_work = df_work.loc[[best_idx]].copy()

    def _num(col):
        if col not in df_work.columns:
            return np.array([np.nan])
        return pd.to_numeric(df_work[col], errors="coerce")

    out: Dict[str, float] = {}
    out["duration_min"] = float(np.nanmean(_num("Durée_min"))) if "Durée_min" in df_work.columns else (
        float(np.nanmean(_num("Durée"))) if "Durée" in df_work.columns else np.nan
    )
    out["distance_m"] = float(np.nansum(_num("Distance (m)"))) if "Distance (m)" in df_work.columns else np.nan
    out["hid13_m"] = float(np.nansum(_num("Distance HID (>13 km/h)"))) if "Distance HID (>13 km/h)" in df_work.columns else np.nan
    out["hid19_m"] = float(np.nansum(_num("Distance HID (>19 km/h)"))) if "Distance HID (>19 km/h)" in df_work.columns else np.nan
    out["d_13_19_m"] = float(np.nansum(_num("Distance 13-19 (m)"))) if "Distance 13-19 (m)" in df_work.columns else np.nan
    out["d_19_23_m"] = float(np.nansum(_num("Distance 19-23 (m)"))) if "Distance 19-23 (m)" in df_work.columns else np.nan
    out["d_23p_m"] = float(np.nansum(_num("Distance >23 (m)"))) if "Distance >23 (m)" in df_work.columns else np.nan
    out["acc_dec"] = float(np.nansum(_num("#accel/decel"))) if "#accel/decel" in df_work.columns else np.nan
    out["vmax_kmh"] = float(np.nanmax(_num("Vitesse max (km/h)"))) if "Vitesse max (km/h)" in df_work.columns else np.nan
    out["charge"] = float(np.nansum(_num("CHARGE"))) if "CHARGE" in df_work.columns else np.nan
    out["rpe"] = float(np.nanmean(_num("RPE"))) if "RPE" in df_work.columns else np.nan

    out["d_0_7"]     = float(np.nansum(_num("V_0_7")))      if "V_0_7"      in df_work.columns else np.nan
    out["d_7_13"]    = float(np.nansum(_num("V_7_13")))     if "V_7_13"     in df_work.columns else np.nan
    out["sprints_23"]= float(np.nansum(_num("Sprints_23"))) if "Sprints_23" in df_work.columns else np.nan
    out["sprints_25"]= float(np.nansum(_num("Sprints_25"))) if "Sprints_25" in df_work.columns else np.nan
    out["acc2"]      = float(np.nansum(_num("Acc_2")))            if "Acc_2"            in df_work.columns else np.nan
    out["acc3"]      = float(np.nansum(_num("Acc_3")))            if "Acc_3"            in df_work.columns else np.nan
    out["dec2"]      = float(np.nansum(_num("Dec_2")))            if "Dec_2"            in df_work.columns else np.nan
    out["dec3"]      = float(np.nansum(_num("Dec_3")))            if "Dec_3"            in df_work.columns else np.nan

    if all(pd.isna(v) for v in out.values()):
        return None
    return out

def read_csv_auto(path: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "latin1"]
    seps = [",", ";", "\t"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep)
                if df.shape[1] == 1 and sep != "\t":
                    continue
                return df
            except Exception as e:
                last_err = e
                continue
    raise last_err if last_err else ValueError(f"Impossible de lire le CSV: {path}")


def list_gps_files_local() -> List[str]:
    paths: List[str] = []

    gps_root = os.path.join(DATA_FOLDER, "gps")
    if os.path.exists(gps_root):
        for root, _, files in os.walk(gps_root):
            for f in files:
                if not f.lower().endswith(".csv"):
                    continue
                fn_norm = normalize_str(f)
                if ("gf1" in fn_norm) or ("seance" in fn_norm) or ("séance" in fn_norm) or ("gps" in fn_norm):
                    paths.append(os.path.join(root, f))

    if os.path.exists(DATA_FOLDER):
        for f in os.listdir(DATA_FOLDER):
            if not f.lower().endswith(".csv"):
                continue
            fn_norm = normalize_str(f)
            if ("gf1" in fn_norm) or ("seance" in fn_norm) or ("séance" in fn_norm) or ("gps" in fn_norm):
                paths.append(os.path.join(DATA_FOLDER, f))

    paths = sorted(list(dict.fromkeys(paths)))
    return paths


def standardize_gps_columns(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if is_gf1_export_format(df):
        return standardize_gps_gf1_export(df, filename)

    colmap = {}
    for c in df.columns:
        nc = normalize_str(c)
        if nc in {"nom", "name", "joueur", "joueuse"}:
            colmap[c] = "NOM"
        elif nc == "date":
            colmap[c] = "DATE"
        elif "semaine" in nc or nc == "week":
            colmap[c] = "SEMAINE"
        elif "duree" in nc or "durée" in nc:
            colmap[c] = "Durée"
        elif "distance" in nc and "(m)" in nc:
            colmap[c] = "Distance (m)"
        elif "hid" in nc and "13" in nc:
            colmap[c] = "Distance HID (>13 km/h)"
        elif "hid" in nc and "19" in nc:
            colmap[c] = "Distance HID (>19 km/h)"
        elif "charge" in nc:
            colmap[c] = "CHARGE"
        elif "rpe" in nc:
            colmap[c] = "RPE"

    out = df.rename(columns=colmap).copy()

    if "DATE" not in out.columns:
        d = parse_date_from_gf1_filename(filename)
        if d:
            out["DATE"] = pd.Timestamp(d.date())

    if "SEMAINE" not in out.columns and "DATE" in out.columns:
        out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")
        out["SEMAINE"] = out["DATE"].dt.isocalendar().week.astype("Int64")

    out["__source_file"] = os.path.basename(filename)
    return out


@st.cache_data(ttl=600, show_spinner=False)
def load_gps_raw(ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons) -> pd.DataFrame:
    files = list_gps_files_local()
    if not files:
        return pd.DataFrame()

    gf1_files = [p for p in files if normalize_str(os.path.basename(p)).startswith(normalize_str(GPS_GF1_PREFIX))]
    if not gf1_files:
        gf1_files = [p for p in files if "seance" in normalize_str(os.path.basename(p))]
    if not gf1_files:
        return pd.DataFrame()

    gf1_files_sorted = []
    for p in gf1_files:
        d = parse_date_from_gf1_filename(os.path.basename(p))
        gf1_files_sorted.append((d or datetime.min, p))
    gf1_files_sorted.sort(key=lambda t: t[0])

    frames = []
    for _, p in gf1_files_sorted:
        try:
            dfp = read_csv_auto(p)
            dfp = standardize_gps_columns(dfp, os.path.basename(p))
            dfp["__source_file"] = os.path.basename(p)
            frames.append(dfp)
        except Exception as e:
            _warn(f"GPS: impossible de lire {os.path.basename(p)} → {e}")
            continue

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty or "NOM" not in df.columns:
        return pd.DataFrame()

    mapped = []
    statuses = []
    for v in df["NOM"].astype(str).tolist():
        m, status, _ = map_player_name(v, ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons, cutoff_fuzzy=0.93)
        mapped.append(m)
        statuses.append(status)
    df["Player"] = mapped
    df["__name_status"] = statuses

    for c in [
        "Durée", "Durée_min",
        "Distance (m)",
        "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
        "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
        "CHARGE", "RPE",
        "Sprints_23", "Sprints_25",
        "Vitesse max (km/h)",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "Durée_min" not in df.columns and "Durée" in df.columns:
        df["Durée_min"] = pd.to_numeric(df["Durée"], errors="coerce")
    elif "Durée_min" in df.columns:
        df["Durée_min"] = pd.to_numeric(df["Durée_min"], errors="coerce")

    df["DATE"] = pd.to_datetime(df.get("DATE", pd.NaT), errors="coerce")
    return df


def compute_gps_weekly_metrics(df_gps: pd.DataFrame) -> pd.DataFrame:
    if df_gps is None or df_gps.empty:
        return pd.DataFrame()

    d = df_gps.copy()

    if "SEMAINE" not in d.columns:
        if "DATE" in d.columns:
            d["SEMAINE"] = pd.to_datetime(d["DATE"], errors="coerce").dt.isocalendar().week.astype("Int64")
        else:
            d["SEMAINE"] = pd.NA

    if "Durée_min" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée_min"], errors="coerce")
    elif "Durée" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée"], errors="coerce")
    else:
        d["Durée_min"] = np.nan

    if "CHARGE" not in d.columns and "RPE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["RPE"], errors="coerce").fillna(0) * d["Durée_min"].fillna(0)
    elif "CHARGE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["CHARGE"], errors="coerce")

    agg_map: Dict[str, str] = {}

    if "Distance (m)" in d.columns:
        d["Distance (m)"] = pd.to_numeric(d["Distance (m)"], errors="coerce")
        agg_map["Distance (m)"] = "sum"

    for col in ["Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
            agg_map[col] = "sum"

    for col in ["Distance HID (>13 km/h)", "Distance HID (>19 km/h)"]:
        if col in d.columns and col not in agg_map:
            d[col] = pd.to_numeric(d[col], errors="coerce")
            agg_map[col] = "sum"

    if "Durée_min" in d.columns:
        agg_map["Durée_min"] = "sum"
    if "CHARGE" in d.columns:
        agg_map["CHARGE"] = "sum"

    if not agg_map:
        return pd.DataFrame()

    out = d.groupby(["Player", "SEMAINE"], as_index=False).agg(agg_map)

    if "CHARGE" in out.columns:
        out = out.sort_values(["Player", "SEMAINE"])
        out["Aigue"] = out["CHARGE"]
        out["Chronique"] = out.groupby("Player")["Aigue"].transform(lambda s: s.rolling(4, min_periods=1).mean())
        out["ACWR"] = np.where(out["Chronique"] > 0, out["Aigue"] / out["Chronique"], np.nan)
    else:
        out["ACWR"] = np.nan

    return out


def compute_acwr(gps_raw: pd.DataFrame, player_name: str) -> pd.DataFrame:
    """Calcule l'ACWR hebdomadaire selon deux modèles pour une joueuse.

    Modèle 1 — Rolling Average (Gabbett 2016) :
        Aigu  = moyenne CHARGE sur les 7 derniers jours
        Chronique = moyenne CHARGE sur les 28 derniers jours
        ACWR_RA = Aigu / Chronique

    Modèle 2 — EWMA (Murray et al. 2016) :
        λ_a = 2 / (7 + 1)   → décroissance rapide (aigu)
        λ_c = 2 / (28 + 1)  → décroissance lente (chronique)
        EWMA_a(t) = CHARGE(t) × λ_a + EWMA_a(t-1) × (1 - λ_a)
        EWMA_c(t) = CHARGE(t) × λ_c + EWMA_c(t-1) × (1 - λ_c)
        ACWR_EWMA = EWMA_a(t) / EWMA_c(t)   (en fin de chaque semaine ISO)

    Retourne un DataFrame avec une ligne par semaine ISO et les colonnes :
        SEMAINE, DATE_FIN, CHARGE_semaine,
        Aigu_RA, Chronique_RA, ACWR_RA,
        Aigu_EWMA, Chronique_EWMA, ACWR_EWMA
    """
    if gps_raw is None or gps_raw.empty:
        return pd.DataFrame()

    df = gps_raw.copy()
    df = ensure_date_column(df)

    # Filtrer la joueuse
    _p = nettoyer_nom_joueuse(player_name)
    df = df[df["Player"].astype(str).apply(nettoyer_nom_joueuse) == _p].copy()
    if df.empty or "DATE" not in df.columns:
        return pd.DataFrame()

    df = df[df["DATE"].notna()].copy()
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.normalize()

    # Construire la colonne CHARGE si absente
    if "CHARGE" not in df.columns and "RPE" in df.columns and "Durée_min" in df.columns:
        df["CHARGE"] = (
            pd.to_numeric(df["RPE"], errors="coerce").fillna(0) *
            pd.to_numeric(df["Durée_min"], errors="coerce").fillna(0)
        )
    elif "CHARGE" in df.columns:
        df["CHARGE"] = pd.to_numeric(df["CHARGE"], errors="coerce").fillna(0)
    elif "Distance (m)" in df.columns:
        # Fallback : utiliser la distance comme proxy de charge
        df["CHARGE"] = pd.to_numeric(df["Distance (m)"], errors="coerce").fillna(0)
    else:
        return pd.DataFrame()

    # Agréger par jour (plusieurs sessions dans une journée → somme)
    daily = df.groupby("DATE")["CHARGE"].sum().reset_index()
    daily = daily.sort_values("DATE").reset_index(drop=True)

    # Créer une série continue jour par jour (combler les jours sans séance avec 0)
    if daily.empty:
        return pd.DataFrame()
    date_range = pd.date_range(daily["DATE"].min(), daily["DATE"].max(), freq="D")
    daily = daily.set_index("DATE").reindex(date_range, fill_value=0.0).rename_axis("DATE").reset_index()

    # ── Modèle 1 : Rolling Average ─────────────────────────────────────────────
    daily["Aigu_RA"]      = daily["CHARGE"].rolling(7,  min_periods=1).mean()
    daily["Chronique_RA"] = daily["CHARGE"].rolling(28, min_periods=1).mean()
    daily["ACWR_RA"]      = np.where(
        daily["Chronique_RA"] > 0,
        daily["Aigu_RA"] / daily["Chronique_RA"],
        np.nan
    )

    # ── Modèle 2 : EWMA (Murray et al. 2016) ───────────────────────────────────
    lam_a = 2 / (7  + 1)   # λ aigu
    lam_c = 2 / (28 + 1)   # λ chronique

    ewma_a = np.zeros(len(daily))
    ewma_c = np.zeros(len(daily))
    charges = daily["CHARGE"].values

    for i, c in enumerate(charges):
        if i == 0:
            ewma_a[i] = c
            ewma_c[i] = c
        else:
            ewma_a[i] = c * lam_a + ewma_a[i - 1] * (1 - lam_a)
            ewma_c[i] = c * lam_c + ewma_c[i - 1] * (1 - lam_c)

    daily["Aigu_EWMA"]      = ewma_a
    daily["Chronique_EWMA"] = ewma_c
    daily["ACWR_EWMA"]      = np.where(
        daily["Chronique_EWMA"] > 0,
        daily["Aigu_EWMA"] / daily["Chronique_EWMA"],
        np.nan
    )

    # ── Agréger par semaine ISO (fin de semaine = dimanche) ────────────────────
    daily["SEMAINE"] = daily["DATE"].dt.isocalendar().week.astype(int)
    daily["ANNEE"]   = daily["DATE"].dt.isocalendar().year.astype(int)

    # Fin de semaine = dernier jour de la semaine ISO (dimanche)
    weekly = (
        daily.groupby(["ANNEE", "SEMAINE"], sort=True)
        .agg(
            DATE_FIN        =("DATE",          "last"),
            CHARGE_semaine  =("CHARGE",        "sum"),
            Aigu_RA         =("Aigu_RA",       "last"),
            Chronique_RA    =("Chronique_RA",  "last"),
            ACWR_RA         =("ACWR_RA",       "last"),
            Aigu_EWMA       =("Aigu_EWMA",     "last"),
            Chronique_EWMA  =("Chronique_EWMA","last"),
            ACWR_EWMA       =("ACWR_EWMA",     "last"),
        )
        .reset_index()
    )

    weekly["Label_semaine"] = weekly.apply(
        lambda r: f"S{int(r['SEMAINE']):02d} ({r['DATE_FIN'].strftime('%d/%m')})", axis=1
    )

    return weekly


# =========================
# GPS UI HELPERS
# =========================
def ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    d = df.copy()

    src = None
    for cand in ["Activity Date", "activity date", "DATE", "Date"]:
        if cand in d.columns:
            src = cand
            break

    if src is not None:
        s = pd.to_datetime(d[src], errors="coerce", utc=True)
        try:
            s = s.dt.tz_convert(None)
        except Exception:
            pass
        d["DATE"] = s
    else:
        d["DATE"] = pd.NaT

    if "__source_file" in d.columns:
        missing = d["DATE"].isna()
        if missing.any():
            extracted = (
                d.loc[missing, "__source_file"]
                .astype(str)
                .str.extract(r"(\d{2}[\./-]\d{2}[\./-]\d{2,4})", expand=False)
            )
            parsed = pd.to_datetime(extracted, dayfirst=True, errors="coerce")
            d.loc[missing, "DATE"] = parsed.values

    d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce", utc=True)
    try:
        d["DATE"] = d["DATE"].dt.tz_convert(None)
    except Exception:
        pass

    return d

def _gps_get_numeric(d: pd.DataFrame, col: str) -> pd.Series:
    if d is None or d.empty or col not in d.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(d[col], errors="coerce")

def build_md_window_summary(d_player: pd.DataFrame, end_date, days: int = 7) -> pd.DataFrame:
    if d_player is None or d_player.empty or "DATE" not in d_player.columns:
        return pd.DataFrame()

    end_date = pd.Timestamp(end_date).normalize()
    start_date = end_date - pd.Timedelta(days=days - 1)

    d = d_player.copy()
    d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    d = d[d["DATE"].notna()].copy()
    d = d[(d["DATE"] >= start_date) & (d["DATE"] <= end_date)].copy()
    if d.empty:
        return pd.DataFrame()

    if "Distance relative (m/min)" not in d.columns:
        dist = _gps_get_numeric(d, "Distance (m)")
        dur = _gps_get_numeric(d, "Durée_min")
        d["Distance relative (m/min)"] = (dist / dur.replace(0, np.nan)).fillna(0)

    vars_map = {
        "Distance (m)": "Moyenne de Distance (m)",
        "Distance HID (>13 km/h)": "Moyenne de Distance HID (>13 km/h)",
        "Distance par plage de vitesse (15-19 km/h)": "Moyenne de Distance par plage de vitesse (15-19 km/h)",
        "Distance 13-19 (m)": "Moyenne de Distance 13-19 (m)",
        "Distance HID (>19 km/h)": "Moyenne de Distance HID (>19 km/h)",
        "Distance 19-23 (m)": "Moyenne de Distance 19-23 (m)",
        "Distance par plage de vitesse (>25 km/h)": "Moyenne de Distance par plage de vitesse (>25 km/h)",
        "Distance >23 (m)": "Moyenne de Distance >23 (m)",
        "Distance relative (m/min)": "Moyenne de Distance relative (m/min)",
        "#accel/decel": "Moyenne de # Acc/Dec",
    }

    agg_cols = [c for c in vars_map.keys() if c in d.columns]
    if not agg_cols:
        return pd.DataFrame()

    d["DATE_DAY"] = d["DATE"].dt.normalize()
    dd = d.groupby("DATE_DAY", as_index=False)[agg_cols].mean(numeric_only=True)

    dd["delta"] = (end_date - dd["DATE_DAY"]).dt.days.astype(int)
    dd = dd[(dd["delta"] >= 0) & (dd["delta"] <= (days - 1))].copy()
    dd["MD"] = dd["delta"].map(lambda k: "MD" if k == 0 else f"MD-{k}")

    out = dd.groupby("MD", as_index=False)[agg_cols].mean(numeric_only=True)

    order = [f"MD-{k}" for k in range(days - 1, 0, -1)] + ["MD"]
    out["__ord"] = out["MD"].map({lab: i for i, lab in enumerate(order)})
    out = out.sort_values("__ord").drop(columns="__ord")

    out = out.rename(columns=vars_map)
    return out

def plot_gps_md_graph(summary: pd.DataFrame, selected_lines=None):
    if summary is None or summary.empty or "MD" not in summary.columns:
        return None

    import numpy as np

    d = summary.copy()

    md_order = [f"MD-{k}" for k in range(6, 0, -1)] + ["MD"]
    d["__ord"] = d["MD"].astype(str).map({lab: i for i, lab in enumerate(md_order)})
    d = d.sort_values("__ord").drop(columns="__ord")

    x_labels = d["MD"].astype(str).tolist()
    x = np.arange(len(x_labels))

    bar_col = "Moyenne de Distance (m)"
    candidates = [
        "Moyenne de Distance HID (>13 km/h)",
        "Moyenne de Distance 13-19 (m)",
        "Moyenne de Distance 19-23 (m)",
        "Moyenne de Distance >23 (m)",
        "Moyenne de # Acc/Dec",
        "Moyenne de Distance relative (m/min)",
        "Moyenne de Distance HID (>19 km/h)",
        "Moyenne de Distance par plage de vitesse (15-19 km/h)",
        "Moyenne de Distance par plage de vitesse (>25 km/h)",
    ]
    available_lines = [c for c in candidates if c in d.columns]

    if selected_lines:
        lines_to_plot = [c for c in selected_lines if c in available_lines]
    else:
        lines_to_plot = available_lines[:5]

    fig, ax1 = plt.subplots(figsize=(11.2, 5.6), dpi=170)
    fig.patch.set_facecolor("#061a2e")
    ax1.set_facecolor("#061a2e")

    ax1.grid(True, axis="y", linestyle="--", alpha=0.25)
    for sp in ax1.spines.values():
        sp.set_alpha(0.35)

    ax1.tick_params(axis="x", colors="white")
    ax1.tick_params(axis="y", colors="white")
    ax1.yaxis.label.set_color("white")

    if bar_col in d.columns:
        y_bar = pd.to_numeric(d[bar_col], errors="coerce").fillna(0.0).values
    else:
        y_bar = np.zeros(len(d))

    bar_width = 0.55
    bars = ax1.bar(
        x,
        y_bar,
        width=bar_width,
        alpha=0.45,
        edgecolor="white",
        linewidth=0.7,
        label=bar_col if bar_col in d.columns else "Distance (m)",
    )
    ax1.set_ylabel("Distance (m)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels, rotation=0, ha="center", color="white")

    if len(y_bar) and np.nanmax(y_bar) > 0:
        ax1.set_ylim(0, float(np.nanmax(y_bar)) * 1.18)

    ax2 = ax1.twinx()
    ax2.set_facecolor("none")
    ax2.tick_params(axis="y", colors="white")
    ax2.yaxis.label.set_color("white")
    ax2.set_ylabel("Valeurs (axe droit)")

    palette = ["#2EC4B6", "#FF9F1C", "#E71D36", "#A06CD5", "#9BC53D", "#5BC0EB", "#FDE74C"]
    handles = []
    labels = []

    handles.append(bars)
    labels.append(bar_col if bar_col in d.columns else "Distance (m)")

    for i, col in enumerate(lines_to_plot):
        y = pd.to_numeric(d[col], errors="coerce").fillna(0.0).values
        color = palette[i % len(palette)]
        line, = ax2.plot(
            x,
            y,
            marker="o",
            markersize=5.0,
            linewidth=2.4,
            alpha=0.95,
            color=color,
            label=col,
        )
        handles.append(line)
        labels.append(col)

    try:
        y_all = []
        for col in lines_to_plot:
            y_all.extend(pd.to_numeric(d[col], errors="coerce").fillna(0.0).tolist())
        if y_all and np.nanmax(y_all) > 0:
            ax2.set_ylim(0, float(np.nanmax(y_all)) * 1.15)
    except Exception:
        pass

    leg = ax1.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.18),
        ncol=2,
        frameon=False,
        fontsize=9,
    )
    for txt in leg.get_texts():
        txt.set_color("white")

    ax1.set_title("Microcycle (MD-6 → MD)", color="white", pad=10, fontsize=13)

    fig.tight_layout()
    return fig

def gps_last_7_days_summary(gps_raw: pd.DataFrame, player_sel: str, end_date=None):
    if gps_raw is None or gps_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    d = gps_raw.copy()

    canon = nettoyer_nom_joueuse(player_sel)
    if "Player" in d.columns:
        d = d[d["Player"].astype(str).apply(nettoyer_nom_joueuse) == canon].copy()
    else:
        return pd.DataFrame(), pd.DataFrame()

    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    d = ensure_date_column(d)
    if "DATE" not in d.columns:
        return pd.DataFrame(), pd.DataFrame()

    d = d[d["DATE"].notna()].copy()
    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    d = d[d["DATE"].notna()].copy()
    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    try:
        d["DATE"] = d["DATE"].dt.tz_localize(None)
    except Exception:
        pass

    if end_date is None:
        end_dt = pd.to_datetime(d["DATE"].max()).normalize()
    else:
        end_dt = pd.to_datetime(end_date).normalize()

    start_dt = end_dt - pd.Timedelta(days=6)
    end_inclusive = end_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    df_7j = d[(d["DATE"] >= start_dt) & (d["DATE"] <= end_inclusive)].copy()
    if df_7j.empty:
        return pd.DataFrame(), pd.DataFrame()

    metric_cols = [c for c in [
        "Durée", "Durée_min",
        "Distance (m)",
        "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
        "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
        "CHARGE", "RPE",
        "Sprints_23", "Sprints_25",
        "Vitesse max (km/h)", "#accel/decel",
    ] if c in df_7j.columns]

    for c in metric_cols:
        df_7j[c] = pd.to_numeric(df_7j[c], errors="coerce")

    means = df_7j[metric_cols].mean(numeric_only=True)
    sums = df_7j[metric_cols].sum(numeric_only=True)

    summary = pd.DataFrame([{
        "Player": canon,
        "Période": f"{start_dt.date()} → {end_dt.date()}",
        **{f"Moyenne 7j - {k}": (float(v) if pd.notna(v) else np.nan) for k, v in means.items()},
        **{f"Total 7j - {k}": (float(v) if pd.notna(v) else np.nan) for k, v in sums.items()},
        "Nb jours avec données (7j)": int(df_7j["DATE"].dt.date.nunique()),
        "Nb lignes": int(len(df_7j)),
    }])

    return df_7j, summary


def _warn(msg: str) -> None:
    """Accumule les avertissements système dans session_state sans les afficher."""
    try:
        buf = st.session_state.setdefault("_system_warnings", [])
        if msg not in buf:
            buf.append(msg)
    except Exception:
        pass  # hors contexte Streamlit (tests, import)



def _run_initial_sync():
    """
    Télécharge depuis Drive et synchronise GPS + Photos.
    Exécutée UNE SEULE FOIS par session (guard _sync_done).
    Re-déclenchée par le bouton "Mettre à jour la base".
    """
    st.session_state["_system_warnings"] = []

    with st.spinner("🔄 Chargement des données depuis Drive..."):
        try:
            download_google_drive()
        except Exception as e:
            _warn(f"Drive: téléchargement principal échoué → {e}")

        try:
            sync_gps_from_drive_autonomous()
        except Exception as e:
            _warn(f"GPS: sync autonome échouée → {e}")

        try:
            sync_gps_match_from_drive()
        except Exception as e:
            _warn(f"GPS Match: sync échouée → {e}")

        try:
            sync_photos_from_drive()
        except Exception as e:
            _warn(f"Photos: sync échouée → {e}")

        # Index photos + concordance
        photos_index_local = build_photos_index_local()
        st.session_state["photos_index"] = photos_index_local
        ref_path = os.path.join(DATA_FOLDER, REFERENTIEL_FILENAME)
        if not os.path.exists(ref_path):
            ref_path = find_local_file_by_normalized_name(DATA_FOLDER, REFERENTIEL_FILENAME) or ""
        try:
            concordance = build_photo_concordance(ref_path, photos_index_local)
            st.session_state["photo_concordance"] = concordance
        except Exception as e:
            _warn(f"Photos: concordance échouée → {e}")
            st.session_state["photo_concordance"] = {}

    st.session_state["_sync_done"] = True
    # Vider TOUS les caches après sync pour forcer le rechargement des nouveaux fichiers
    st.cache_data.clear()


@st.cache_data(ttl=600, show_spinner=False)
def collect_data(selected_season=None):
    """
    Chargement principal des données PFC + EDF + GPS.
    Mis en cache 5 min (ttl=300) — ne re-télécharge pas Drive à chaque rerun.
    Les syncs Drive/GPS/Photos sont faits UNE SEULE FOIS au démarrage
    (voir _run_initial_sync) ou via le bouton "Mettre à jour".
    """
    ref_path = os.path.join(DATA_FOLDER, REFERENTIEL_FILENAME)
    if not os.path.exists(ref_path):
        ref_path = find_local_file_by_normalized_name(DATA_FOLDER, REFERENTIEL_FILENAME)
    if not ref_path or not os.path.exists(ref_path):
        return pd.DataFrame(), pd.DataFrame()

    ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons = build_referentiel_players(ref_path)
    name_report: List[dict] = []

    pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

    if not os.path.exists(DATA_FOLDER):
        return pd.DataFrame(), pd.DataFrame()

    fichiers = [
        f
        for f in os.listdir(DATA_FOLDER)
        if f.endswith((".csv", ".xlsx", ".xls")) and normalize_str(f) != normalize_str(PERMISSIONS_FILENAME)
    ]

    if selected_season and selected_season != "Toutes les saisons":
        keep_always_prefixes = ("EDF_",)
        keep_always_names = {EDF_JOUEUSES_FILENAME, REFERENTIEL_FILENAME, PASSERELLE_FILENAME}
        fichiers = [
            f for f in fichiers
            if (selected_season in f) or f.startswith(keep_always_prefixes) or (f in keep_always_names)
        ]

    gps_raw = load_gps_raw(ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons)
    gps_week = compute_gps_weekly_metrics(gps_raw)
    gps_match = load_gps_match(ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons)

    # ======================================================
    # EDF (référentiel par poste)
    # ======================================================
    edf_path = os.path.join(DATA_FOLDER, EDF_JOUEUSES_FILENAME)
    if os.path.exists(edf_path):
        try:
            edf_joueuses = read_excel_auto(edf_path)
            if isinstance(edf_joueuses, dict):
                edf_joueuses = list(edf_joueuses.values())[0] if len(edf_joueuses) else pd.DataFrame()

            needed = {"Player", "Poste", "Temps de jeu"}
            if not needed.issubset(set(edf_joueuses.columns)):
                _warn(f"EDF_Joueuses.xlsx: colonnes manquantes → {edf_joueuses.columns.tolist()}")
            else:
                edf_j = edf_joueuses.copy()
                edf_j["Player_raw"] = edf_j["Player"].astype(str)

                canon_list = []
                for v in edf_j["Player_raw"].tolist():
                    canon, _, _ = map_player_name(v, ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons, cutoff_fuzzy=0.93)
                    canon_list.append(canon)
                edf_j["PlayerCanon"] = canon_list

                _tj = edf_j["Temps de jeu"] if "Temps de jeu" in edf_j.columns else pd.Series([0] * len(edf_j))
                edf_j["Temps de jeu"] = pd.Series(pd.to_numeric(_tj, errors="coerce"), index=edf_j.index).fillna(0)

                matchs_csv = [f for f in fichiers if f.startswith("EDF_U19_Match") and f.endswith(".csv")]
                all_edf_rows = []

                for csv_file in matchs_csv:
                    d = pd.read_csv(os.path.join(DATA_FOLDER, csv_file))
                    if "Row" not in d.columns:
                        continue

                    d = d.copy()
                    d["Player_raw"] = d["Row"].astype(str)

                    canon_d = []
                    for v in d["Player_raw"].tolist():
                        canon, _, _ = map_player_name(v, ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons, cutoff_fuzzy=0.93)
                        canon_d.append(canon)
                    d["PlayerCanon"] = canon_d

                    d = d.merge(edf_j[["PlayerCanon", "Poste", "Temps de jeu"]], on="PlayerCanon", how="left")

                    if "Poste" not in d.columns or d["Poste"].isna().mean() > 0.9:
                        _warn(f"EDF: merge faible sur {csv_file} (Poste NaN {d['Poste'].isna().mean():.0%})")
                        continue

                    df_duration = edf_j[["PlayerCanon", "Poste", "Temps de jeu"]].copy()
                    df_duration = df_duration.rename(columns={"PlayerCanon": "Player"})
                    df_duration["Temps de jeu (en minutes)"] = df_duration["Temps de jeu"]
                    df_duration = df_duration.drop(columns=["Temps de jeu"])

                    joueurs_edf = d.copy()
                    joueurs_edf["Row"] = joueurs_edf["PlayerCanon"]
                    joueurs_edf["Player"] = joueurs_edf["PlayerCanon"]

                    dfs = [df_duration]

                    for func in [
                        players_shots,
                        players_passes,
                        players_pass_directions,
                        players_dribbles,
                        players_defensive_duels,
                        players_interceptions,
                        players_ball_losses,
                    ]:
                        try:
                            res = func(joueurs_edf)
                            if res is not None and not res.empty:
                                dfs.append(res)
                        except Exception:
                            pass

                    df_edf = dfs[0]
                    for other in dfs[1:]:
                        df_edf = df_edf.merge(other, on="Player", how="outer")

                    df_edf.fillna(0, inplace=True)
                    df_edf = df_edf[df_edf["Temps de jeu (en minutes)"] >= 10].copy()

                    df_edf = create_metrics(df_edf)
                    df_edf = create_kpis(df_edf)
                    df_edf = create_poste(df_edf)

                    if not df_edf.empty and "Poste" in df_edf.columns:
                        all_edf_rows.append(df_edf)

                if all_edf_rows:
                    edf_full = pd.concat(all_edf_rows, ignore_index=True)
                    edf_kpi = edf_full.groupby("Poste").mean(numeric_only=True).reset_index()
                    edf_kpi["Poste"] = edf_kpi["Poste"].astype(str) + " moyenne (EDF)"

        except Exception as e:
            _warn(f"EDF: erreur chargement/calcul référentiel → {e}")

    # ======================================================
    # PFC Matchs
    # ======================================================
    for filename in fichiers:
        if not (filename.endswith(".csv") and "PFC" in filename):
            continue

        path = os.path.join(DATA_FOLDER, filename)

        try:
            parts = filename.split(".")[0].split("_")
            if len(parts) < 6:
                continue

            journee = parts[3]
            categorie = parts[4]
            date = parts[5]

            data = pd.read_csv(path)
            if "Row" not in data.columns:
                continue

            cols_to_fix = ["Row"] + [c for c in POST_COLS if c in data.columns]
            data = normalize_players_in_df(
                data,
                cols=cols_to_fix,
                ref_set=ref_set,
                alias_to_canon=alias_to_canon,
                tokenkey_to_canon=tokenkey_to_canon,
                compact_to_canon=compact_to_canon,
                first_to_canons=first_to_canons,
                last_to_canons=last_to_canons,
                filename=filename,
                report=name_report,
            )

            d2 = data.copy()
            d2["Row_clean"] = d2["Row"].astype(str).apply(nettoyer_nom_equipe)
            available_posts = [c for c in POST_COLS if c in d2.columns]

            if "Duration" in d2.columns and available_posts:
                mask_lineup = d2["Duration"].notna() & d2[available_posts].notna().any(axis=1)
            else:
                mask_lineup = pd.Series(False, index=d2.index)

            teams_found = d2.loc[mask_lineup, "Row_clean"].dropna().unique().tolist()

            if len(teams_found) < 2:
                candidates_team_like = []
                for v in d2["Row_clean"].dropna().unique().tolist():
                    if not looks_like_player(v) and v not in BAD_TOKENS and len(str(v).strip()) > 2:
                        candidates_team_like.append(v)
                if candidates_team_like:
                    vc = d2[d2["Row_clean"].isin(candidates_team_like)]["Row_clean"].value_counts()
                    teams_found = vc.index.tolist()

            if "PFC" in teams_found:
                equipe_pfc = "PFC"
                others = [t for t in teams_found if t != "PFC"]
                equipe_adv_team = others[0] if others else None
            else:
                equipe_pfc = teams_found[0] if len(teams_found) else str(parts[0]).strip()
                equipe_adv_team = teams_found[1] if len(teams_found) > 1 else None

            adv_label = infer_opponent_from_columns(data, equipe_pfc) or infer_opponent_from_filename(filename, equipe_pfc)
            if not adv_label:
                adv_label = "Adversaire inconnu"

            if not equipe_adv_team:
                equipe_adv_team = adv_label

            home_clean = nettoyer_nom_equipe(equipe_pfc)
            away_clean = nettoyer_nom_equipe(equipe_adv_team)

            match = d2[d2["Row_clean"].isin({home_clean, away_clean})].copy()
            if match.empty:
                continue

            mask_joueurs = ~d2["Row_clean"].str.contains("CORNER|COUP-FRANC|COUP FRANC|PENALTY|CARTON", na=False)
            mask_joueurs &= ~d2.index.isin(match.index)
            joueurs = d2[mask_joueurs].copy()
            if joueurs.empty:
                joueurs = pd.DataFrame(columns=["Row", "Action"])

            df = create_data(match, joueurs, False, home_team=equipe_pfc, away_team=equipe_adv_team)
            if df.empty:
                continue

            if "Temps de jeu (en minutes)" in df.columns:
                num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != "Temps de jeu (en minutes)"]
                for idx, r in df.iterrows():
                    tp = safe_float(r.get("Temps de jeu (en minutes)", np.nan), default=np.nan)
                    if np.isnan(tp) or tp <= 0:
                        continue
                    scale = 90.0 / tp
                    for col in num_cols:
                        if col == "Buts":
                            continue
                        if "Pourcentage" in col:
                            continue
                        df.loc[idx, col] = r[col] * scale

            df = create_metrics(df)
            df = create_kpis(df)
            df = create_poste(df)

            adversaire = adv_label
            saison = extract_season_from_filename(filename) or "Inconnue"
            df.insert(1, "Saison", saison)
            df.insert(2, "Adversaire", f"{journee} - {adversaire}")
            df.insert(3, "Journée", journee)
            df.insert(4, "Catégorie", categorie)
            df.insert(5, "Date", date)

            pfc_kpi = pd.concat([pfc_kpi, df], ignore_index=True)

        except Exception as e:
            _warn(f"Match: impossible de lire {filename} → {e}")
            continue

    return pfc_kpi, edf_kpi, gps_raw, gps_week, gps_match, pd.DataFrame(name_report).drop_duplicates() if name_report else pd.DataFrame()


# =========================
# RADARS
# =========================
def fig_to_b64(fig) -> str:
    """Convertit une figure matplotlib en data URI base64 PNG."""
    import base64 as _b64
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    buf.seek(0)
    return "data:image/png;base64," + _b64.b64encode(buf.read()).decode()


@st.cache_data(ttl=300, show_spinner=False)
def create_individual_radar(df: pd.DataFrame):
    if df is None or df.empty or "Player" not in df.columns:
        return None

    ordered_params = [
        "Timing", "Force physique",
        "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque",
        "Précision", "Sang-froid",
        "Créativité 1", "Créativité 2",
    ]

    available = [p for p in ordered_params if p in df.columns]
    if len(available) < 3:
        return None

    player = df.iloc[0].copy()

    FAMILY_COLOR = {
        "Timing": "#2FB8FF",
        "Force physique": "#2FB8FF",
        "Intelligence tactique": "#FFA06E",
        "Technique 1": "#FF6B6B",
        "Technique 2": "#FF6B6B",
        "Technique 3": "#FF6B6B",
        "Explosivité": "#7B84FF",
        "Prise de risque": "#7B84FF",
        "Précision": "#BFBFBF",
        "Sang-froid": "#BFBFBF",
        "Créativité 1": "#8E9BFF",
        "Créativité 2": "#8E9BFF",
    }
    slice_colors = [FAMILY_COLOR.get(p, "#9AA4B2") for p in available]

    values = [float(pd.to_numeric(player[p], errors="coerce")) if p in player else 0.0 for p in available]
    values = [0.0 if pd.isna(v) else max(0.0, min(100.0, v)) for v in values]
    values = [round(v) for v in values]  # entiers → pas de décimales dans le radar

    pizza = PyPizza(
        params=available,
        background_color="#08090D",
        straight_line_color="#1A2A3A",
        last_circle_color="#00A3E0",
        straight_line_lw=0.8,
        last_circle_lw=1.5,
        other_circle_lw=0.6,
        other_circle_color="#1A2A3A",
    )

    fig, ax = pizza.make_pizza(
        values=values,
        figsize=(7, 7),
        slice_colors=slice_colors,
        value_colors=["#FFFFFF"] * len(available),
        kwargs_slices=dict(edgecolor="#08090D", linewidth=1.8),
        kwargs_params=dict(color="#C8D8E8", fontsize=8.5, fontproperties="monospace"),
        kwargs_values=dict(
            color="#FFFFFF",
            fontsize=9,
            bbox=dict(
                edgecolor="#00A3E0",
                facecolor="#0C1220",
                boxstyle="round,pad=0.2",
                lw=1.0
            ),
        ),
    )

    import matplotlib.patches as patches

    zone_specs = [
        (40, "#FF6B6B", 0.08),
        (70, "#FFA06E", 0.07),
        (100, "#2ED47A", 0.05),
    ]

    for r, col, alpha in sorted(zone_specs, key=lambda x: x[0], reverse=True):
        circ = patches.Circle((0, 0), r, transform=ax.transData._b, color=col, alpha=alpha, zorder=0)
        ax.add_artist(circ)

    center = patches.Circle((0, 0), 4.0, transform=ax.transData._b, color="#08090D", zorder=10)
    ax.add_artist(center)

    fig.subplots_adjust(top=0.90, bottom=0.18)

    player_name = str(player.get("Player", "")).strip()

    vals = []
    for p in available:
        try:
            v = float(player.get(p, np.nan))
        except Exception:
            v = np.nan
        if not np.isnan(v):
            vals.append((p, v))

    vals_desc = sorted(vals, key=lambda t: t[1], reverse=True)
    vals_asc = sorted(vals, key=lambda t: t[1])

    top_n = vals_desc[:2]
    low_n = vals_asc[:2]

    top_txt = " • ".join([f"{k} ({v:.0f})" for k, v in top_n]) if top_n else "—"
    low_txt = " • ".join([f"{k} ({v:.0f})" for k, v in low_n]) if low_n else "—"

    forces_txt = f"✅ Forces : {top_txt}"
    axes_txt = f"⚠️ Axes : {low_txt}"
    forces_wrapped = "\n".join(textwrap.wrap(forces_txt, width=70))
    axes_wrapped = "\n".join(textwrap.wrap(axes_txt, width=70))

    fig.text(0.5, 0.10, forces_wrapped, ha="center", va="center", fontsize=12, color="#C8D8E8")
    fig.text(0.5, 0.05, axes_wrapped, ha="center", va="center", fontsize=12, color="#6A8090")
    fig.set_facecolor("#08090D")
    return fig


@st.cache_data(ttl=300, show_spinner=False)
def create_comparison_radar(df, player1_name=None, player2_name=None, exclude_creativity: bool = False):
    if df is None or df.empty or len(df) < 2:
        return None

    metrics = [
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
    ]
    if not exclude_creativity:
        metrics += ["Créativité 1", "Créativité 2"]

    available = [m for m in metrics if m in df.columns]
    if len(available) < 3:
        return None

    d = df.copy()

    for c in available:
        d[c] = pd.to_numeric(d[c], errors="coerce").clip(lower=0, upper=100).fillna(0.0)

    d2 = d.iloc[:2].copy()
    v1 = d2.iloc[0][available].values.astype(float)
    v2 = d2.iloc[1][available].values.astype(float)

    p1 = str(player1_name) if player1_name else str(d2.iloc[0].get("Player", "Joueuse A"))
    p2 = str(player2_name) if player2_name else str(d2.iloc[1].get("Player", "Joueuse B"))

    low, high = [0] * len(available), [100] * len(available)
    radar = Radar(available, low, high, num_rings=4, ring_width=1, center_circle_radius=1)


    fig = plt.figure(figsize=(8, 8))
    ax = fig.add_subplot(111)
    fig.patch.set_facecolor("#08090D")
    ax.set_facecolor("#08090D")

    radar.setup_axis(ax=ax, facecolor="None")
    radar.draw_circles(ax=ax, facecolor="#0C1220", edgecolor="#1A2A3A", lw=1.0)

    radar.draw_radar_compare(
        v1,
        v2,
        ax=ax,
        kwargs_radar={"facecolor": "#00A3E0", "alpha": 0.50, "edgecolor": "#00A3E0", "lw": 2.5},
        kwargs_compare={"facecolor": "#003189", "alpha": 0.55, "edgecolor": "#FFFFFF", "lw": 1.5},
    )

    radar.draw_range_labels(ax=ax, fontsize=10, color="#6A8090")
    radar.draw_param_labels(ax=ax, fontsize=12, color="#C8D8E8")

    # Titre discret centré tout en haut
    fig.text(0.5, 0.978, "COMPARAISON  ·  0 – 100", ha="center", va="top",
             fontsize=11, color="#6A8090", fontweight="normal")
    # Séparateur
    from matplotlib.lines import Line2D
    fig.add_artist(Line2D([0.03, 0.97], [0.960, 0.960], transform=fig.transFigure,
                          color="#1A2A3A", linewidth=0.8))
    # Noms bien espacés en dessous, plus petits pour éviter tout chevauchement
    fig.text(0.03, 0.955, f"\u25cf  {p1}", ha="left", va="top", fontsize=13, color="#00A3E0", fontweight="bold")
    fig.text(0.97, 0.955, f"{p2}  \u25cf", ha="right", va="top", fontsize=13, color="#4A7FFF", fontweight="bold")

    delta = pd.Series(v1 - v2, index=available)

    top_pos = delta.sort_values(ascending=False).head(3)
    top_neg = delta.sort_values(ascending=True).head(3)

    def _fmt_series(s: pd.Series) -> str:
        parts = []
        for k, v in s.items():
            sign = "+" if v >= 0 else ""
            parts.append(f"{k} ({sign}{v:.0f})")
        return " • ".join(parts) if parts else "—"

    txt_pos = _fmt_series(top_pos)
    txt_neg = _fmt_series(top_neg)

    fig.subplots_adjust(top=0.90, bottom=0.18)

    fig.text(0.5, 0.11, f"✅ Avantages {p1} vs {p2} : {txt_pos}", ha="center", va="center", fontsize=11.5, color="#C8D8E8")
    fig.text(0.5, 0.075, f"⚠️ Axes d'amélioration {p1} vs {p2} : {txt_neg}", ha="center", va="center", fontsize=11.5, color="#C8D8E8")

    fig.text(0.98, 0.02, "Δ = (profil A - profil B)", ha="right", va="bottom", fontsize=9, color="#6A8090")
    fig.set_dpi(90)  # DPI réduit pour rendu plus rapide

    return fig


def _make_match_bar_chart(labels, datasets, title, ylabel, figsize=(9,3.5), stacked=False):
    """Crée un graphique matplotlib barres groupées ou empilées."""
    fig, ax = plt.subplots(figsize=figsize, dpi=90)
    fig.patch.set_facecolor("#08090D")
    ax.set_facecolor("#08090D")

    n = len(labels)
    x = range(n)
    width = 0.8 / max(len(datasets), 1) if not stacked else 0.6

    bottom = [0.0] * n if stacked else None
    for i, (data, label, color) in enumerate(datasets):
        vals = [float(v) if v is not None and str(v) not in ("nan","") else 0.0 for v in data]
        if stacked:
            ax.bar(x, vals, width, label=label, color=color, bottom=bottom, edgecolor="#08090D", linewidth=0.5)
            bottom = [b + v for b, v in zip(bottom, vals)]
        else:
            offset = (i - len(datasets)/2 + 0.5) * width
            ax.bar([xi + offset for xi in x], vals, width, label=label, color=color, edgecolor="#08090D", linewidth=0.5)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=9, color="#C8D8E8")
    ax.set_ylabel(ylabel, color="#6A8090", fontsize=9)
    ax.tick_params(colors="#6A8090", labelsize=9)
    ax.spines["bottom"].set_color("#1A2A3A")
    ax.spines["left"].set_color("#1A2A3A")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.grid(True, color="#1A2A3A", linewidth=0.5, alpha=0.7)
    ax.set_axisbelow(True)
    if len(datasets) > 1:
        ax.legend(fontsize=8, facecolor="#0C1220", edgecolor="#1A2A3A", labelcolor="#C8D8E8", loc="upper right")
    fig.subplots_adjust(bottom=0.28, top=0.95, left=0.08, right=0.97)
    return fig


def get_playing_time_from_gps(gps_match_df, player_canon: str) -> str:
    """Retourne le temps de jeu en minutes (entier) depuis la colonne Durée_min du fichier GPS match.
    Durée_min est déjà converti en minutes (depuis H:MM:SS) lors de la standardisation.
    Retourne "—" si la joueuse ou la donnée est introuvable.
    """
    if gps_match_df is None or getattr(gps_match_df, "empty", True) or not player_canon:
        return "—"
    if "Durée_min" not in gps_match_df.columns:
        return "—"

    _p          = nettoyer_nom_joueuse(player_canon)
    _p_toks     = set(normalize_name_raw(player_canon).split())
    _p_nom_toks = nom_tokens(player_canon)

    def _matches(val: str) -> bool:
        v = str(val).strip()
        if not v or v.lower() in ("nan", "none", ""):
            return False
        if nettoyer_nom_joueuse(v) == _p or nom_tokens(v) == _p_nom_toks:
            return True
        v_mapped = apply_gps_name_map(v)
        if nettoyer_nom_joueuse(v_mapped) == _p or nom_tokens(v_mapped) == _p_nom_toks:
            return True
        v_toks = set(normalize_name_raw(v).split())
        common = _p_toks & v_toks
        return len(common) >= 2 or (len(common) == 1 and (len(_p_toks) == 1 or len(v_toks) == 1))

    try:
        df = gps_match_df.copy()
        if "NOM" in df.columns:
            df = df[df["NOM"].notna() & (df["NOM"].astype(str).str.strip() != "")
                    & (df["NOM"].astype(str).str.strip().str.lower() != "nan")]

        mask = pd.Series(False, index=df.index)
        if "Player" in df.columns:
            mask |= df["Player"].astype(str).apply(_matches)
        if "NOM" in df.columns:
            mask |= df["NOM"].astype(str).apply(_matches)

        df_p = df[mask]
        if df_p.empty:
            return "—"

        # Si plusieurs lignes, prendre celle avec la plus grande distance (= session complète)
        if len(df_p) > 1 and "Distance (m)" in df_p.columns:
            dist = pd.to_numeric(df_p["Distance (m)"], errors="coerce")
            if dist.notna().any():
                df_p = df_p.loc[[dist.idxmax()]]

        val = pd.to_numeric(df_p["Durée_min"].iloc[0], errors="coerce")
        if pd.isna(val) or val <= 0:
            return "—"
        return str(int(round(float(val))))

    except Exception:
        return "—"


def build_tactical_report_html(
    df_tactic,
    player_canon: str,
    gps_summary=None,
    photo_b64: str = "",
    match_info: dict = None,
    pfc_kpi_row=None,
    radar_b64: str = "",
    gps_match_df=None,
) -> str:
    """Rapport match A4 HTML v4 — photo à côté du nom, polices grandes, layout lisible."""
    import json as _json, math as _math

    player_label = str(player_canon or "").strip()
    mi = match_info or {}

    # ── GPS ───────────────────────────────────────────────────────────────────
    _gps = None
    if isinstance(gps_summary, dict) and gps_summary:
        _gps = gps_summary
    elif isinstance(gps_summary, pd.DataFrame) and not gps_summary.empty:
        _gps = gps_summary.iloc[0].to_dict()

    def _g(key, fmt="{:.0f}", fb="—"):
        if _gps is None: return fb
        v = pd.to_numeric(_gps.get(key, None), errors="coerce")
        return fb if pd.isna(v) else fmt.format(float(v))

    def _gf(key, fb=0.0):
        if _gps is None: return fb
        v = pd.to_numeric(_gps.get(key, None), errors="coerce")
        return float(v) if not pd.isna(v) else fb

    import math as _m
    # Temps de jeu : uniquement depuis gps_summary (colonne "Temps joué" du fichier GPS,
    # convertie en minutes lors de la standardisation via _parse_hmmss H:MM:SS → minutes)
    _tps_raw  = pd.to_numeric(_gps.get("duration_min", None), errors="coerce") if _gps else float("nan")
    temps_gps = str(int(round(float(_tps_raw)))) if _gps and not _m.isnan(float(_tps_raw)) else "—"

    # ── Stats tactiques ────────────────────────────────────────────────────────
    s = compute_tactical_stats(df_tactic, player_canon) if df_tactic is not None else {}

    def _si(key, fb=0):
        v = s.get(key, fb)
        try: return int(float(v) if v is not None else fb)
        except: return fb

    p_ok=_si("passes_ok"); p_ko=_si("passes_ko"); p_tot=p_ok+p_ko
    p_pct=int(p_ok/p_tot*100) if p_tot else 0
    c_ok=_si("courtes_ok"); c_ko=_si("courtes_ko"); c_tot=c_ok+c_ko
    l_ok=_si("longues_ok"); l_ko=_si("longues_ko"); l_tot=l_ok+l_ko
    d_ok=_si("drib_ok"); d_ko=_si("drib_ko"); d_tot=d_ok+d_ko
    d_pct=int(d_ok/d_tot*100) if d_tot else 0
    du_ok=_si("duels_gagnes"); du_ko=_si("duels_perdus"); du_tot=du_ok+du_ko
    du_pct=int(du_ok/du_tot*100) if du_tot else 0
    sol_ok=_si("sol_ok"); sol_ko=_si("sol_ko"); sol_tot=sol_ok+sol_ko
    aer_ok=_si("aer_ok"); aer_ko=_si("aer_ko"); aer_tot=aer_ok+aer_ko
    t_tot=_si("tirs_tot"); t_cad=_si("tirs_cadres"); t_but=_si("tirs_buts")
    recup=_si("recuperations"); pertes=_si("pertes"); ballons=_si("ballons")
    poste=str(s.get("postes","") or ""); systeme=str(s.get("systeme","") or "")

    creation_deseq=0; passes_dt=0; passes_en1=0
    if df_tactic is not None and not df_tactic.empty:
        try:
            d_rows=_filter_player_rows(df_tactic, player_canon)
            for col in d_rows.columns:
                if "quilibre" in col and "Zone" not in col:
                    # La colonne contient "Création de Deséquilibre" si tagué, sinon vide
                    creation_deseq=int(d_rows[col].apply(
                        lambda v: str(v).strip() not in ("","nan","None")
                    ).sum()); break
            if "Passe" in d_rows.columns:
                pr=d_rows[d_rows["Passe"].notna()]
                ap=[a.strip() for cell in pr["Passe"].dropna() for a in str(cell).split(",")]
                passes_dt=ap.count("Passe dans dernier 1/3")
                passes_en1=ap.count("En 1")
        except: pass

    dest_counts={}
    if df_tactic is not None and not df_tactic.empty:
        try:
            d_rows=_filter_player_rows(df_tactic, player_canon)
            if "Destination passe" in d_rows.columns and "Passe" in d_rows.columns:
                for _,r in d_rows.iterrows():
                    dest=str(r.get("Destination passe","")).strip()
                    if dest and dest.lower() not in ("","nan") and str(r.get("Passe","")).strip():
                        dest_counts[dest]=dest_counts.get(dest,0)+1
        except: pass

    locs_json=_json.dumps(s.get("locs",[]))
    passes_json=_json.dumps(s.get("passes_map",[]))
    locs=s.get("locs",[])
    cx=sum(l["x"] for l in locs)/len(locs) if locs else 50.0
    cy=sum(l["y"] for l in locs)/len(locs) if locs else 34.0

    # ── Centroïdes par poste — moyenne des centroïdes individuels par joueuse
    def _parse_coord(v):
        v=str(v).strip()
        if not v or v in ("nan","None",""): return None
        try: return float(v.split(",")[0].strip())
        except: return None

    # Étape 1 : centroïde individuel de chaque joueuse pour chaque poste joué
    _player_poste_pts = {}  # {(player, poste): [(svgX, svgY)]}
    if df_tactic is not None and not df_tactic.empty:
        _skip = {"PFC","HAC","START",""}
        for _,_r in df_tactic.iterrows():
            _rn = str(_r.get("Row","") or "").strip()
            if not _rn or _rn in _skip or "Transition" in _rn or "Carton" in _rn: continue
            _xr = _parse_coord(_r.get("X_localisation",""))
            _yr = _parse_coord(_r.get("Y_localisation",""))
            _pr = str(_r.get("Poste","") or "").strip().split(",")[0].strip()
            if _xr is None or _yr is None or not _pr: continue
            _svgx_raw = _xr * 100/80
            # PFC attaque vers la droite : même mapping que _norm_x, sans inversion
            _svgx = round(max(1.5, min(98.5, _svgx_raw)), 2)
            _svgy = round(max(1.5, min(66.5, 68.0 - _yr * 68/80)), 2)
            _player_poste_pts.setdefault((_rn, _pr), []).append((_svgx, _svgy))

    # Étape 2 : centroïde individuel par (joueuse, poste)
    _indiv_centroids = {
        (pl, po): (sum(x for x,y in pts)/len(pts), sum(y for x,y in pts)/len(pts))
        for (pl, po), pts in _player_poste_pts.items() if pts
    }

    # Étape 3 : centroïde du poste = moyenne des centroïdes individuels de ce poste
    _poste_indiv = {}  # {poste: [(cx_indiv, cy_indiv)]}
    for (pl, po), (cx2, cy2) in _indiv_centroids.items():
        _poste_indiv.setdefault(po, []).append((cx2, cy2))
    _poste_centroids = {
        po: (sum(x for x,y in items)/len(items), sum(y for x,y in items)/len(items))
        for po, items in _poste_indiv.items()
    }

    # Étape 4 : poste principal de la joueuse sélectionnée = poste le plus joué (max n actions)
    _player_postes = {po: len(pts) for (pl, po), pts in _player_poste_pts.items()
                      if pl == player_label and pts}
    _player_main_poste = max(_player_postes, key=_player_postes.get) if _player_postes else poste

    # Étape 5 : centroïde propre de la joueuse à son poste principal
    _player_own_centroid = _indiv_centroids.get((player_label, _player_main_poste), (None, None))

    # JSON pour JS : centroïdes de référence par poste + centroïde propre de la joueuse
    _ref_centroids_json = _json.dumps([
        {"poste": po, "cx": round(cx2,2), "cy": round(cy2,2),
         "isPlayer": False}
        for po, (cx2, cy2) in sorted(_poste_centroids.items())
    ])
    # Centroïde propre de la joueuse (rouge) — séparé pour l'afficher différemment
    _player_cx, _player_cy = _player_own_centroid if _player_own_centroid[0] is not None else (cx, cy)
    _player_centroid_json = _json.dumps({
        "poste": _player_main_poste,
        "cx": round(_player_cx, 2),
        "cy": round(_player_cy, 2)
    })

    # GPS vitesses
    spd_data=[
        ("0–7",   _gf("d_0_7"),     "#2D4060"),
        ("7–13",  _gf("d_7_13"),    "#3B5478"),
        ("13–19", _gf("d_13_19_m"), "#00A3E0"),
        ("19–23", _gf("d_19_23_m"), "#38BDF8"),
        (">23",   _gf("d_23p_m"),   "#7DD3FC"),
    ]
    max_spd=max(v for _,v,_ in spd_data) or 1.0
    spd_bars=""
    for lbl,val,col in spd_data:
        pct=int(val/max_spd*100)
        spd_bars+=(
            f'<div style="display:grid;grid-template-columns:30px 1fr 52px;align-items:center;gap:4px;margin-bottom:3px;">'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:9px;color:#7A9AB8;">{lbl}</span>'
            f'<div style="height:6px;background:#0A1520;border-radius:3px;overflow:hidden;">'
            f'<div style="height:100%;width:{pct}%;background:{col};border-radius:3px;"></div></div>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:9px;color:#A0BDD0;text-align:right;">{int(val):,} m</span>'
            f'</div>'
        )

    # Match info
    adversaire=mi.get("adversaire","") or ""; journee=mi.get("journee","") or ""
    score=mi.get("score","") or ""; lieu=mi.get("lieu","") or ""
    match_label=mi.get("label","") or ""; competition=mi.get("competition","") or "Match"
    match_date=""
    try:
        raw_dt=mi.get("date",None)
        if raw_dt: match_date=pd.Timestamp(raw_dt).strftime("%d/%m/%Y")
    except: pass
    journee_tag=f"J{journee}" if journee else ""
    meta_line=" · ".join(p for p in [match_date,lieu] if p)

    # Logos
    PFC_LOGO = "https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"

    # Logo adversaire : chercher dans l'index Drive local
    adv_logo_url = mi.get("logo_adversaire", "") or ""
    if not adv_logo_url and adversaire:
        try:
            _logos_idx = sync_logos_from_drive()
            _logo_path = find_logo_for_club(adversaire, _logos_idx)
            if _logo_path:
                adv_logo_url = logo_path_to_b64(_logo_path)
        except Exception:
            pass
    adv_init=adversaire[:3].upper() if adversaire else "ADV"
    adv_logo_html=(
        f'<img src="{adv_logo_url}" style="width:44px;height:44px;object-fit:contain;" '
        f'onerror="this.outerHTML=\'<div style=&quot;width:44px;height:44px;border-radius:50%;'
        f'background:#0A1520;border:1px solid #1A2E44;display:flex;align-items:center;'
        f'justify-content:center;font-size:11px;font-weight:700;color:#2A4060;'
        f'font-family:Barlow Condensed,sans-serif;&quot;>{adv_init}</div>\'"/ >'
    ) if adv_logo_url else (
        f'<div style="width:44px;height:44px;border-radius:50%;background:#0A1520;'
        f'border:1px solid #1A2E44;display:flex;align-items:center;justify-content:center;'
        f'font-family:Barlow Condensed,sans-serif;font-size:11px;font-weight:700;color:#2A4060;">{adv_init}</div>'
    )

    # Photo
    initials="".join(w[0].upper() for w in player_label.split()[:2]) if player_label else "??"
    if photo_b64:
        photo_html=(
            f'<img src="{photo_b64}" style="width:76px;height:92px;'
            f'object-fit:cover;object-position:top center;'
            f'border-radius:6px;border:2px solid #00A3E0;display:block;flex-shrink:0;"/>'
        )
    else:
        photo_html=(
            f'<div style="width:76px;height:92px;border-radius:6px;background:#0A1520;'
            f'border:2px solid #1A2E44;display:flex;align-items:center;justify-content:center;'
            f'font-family:Barlow Condensed,sans-serif;font-size:28px;font-weight:700;'
            f'color:#2A4060;flex-shrink:0;">{initials}</div>'
        )

    # Destinations
    dest_html=""
    if dest_counts:
        sorted_dest=sorted(dest_counts.items(),key=lambda x:-x[1])
        max_dest=sorted_dest[0][1] if sorted_dest else 1
        for name,cnt in sorted_dest[:7]:
            pct2=int(cnt/max_dest*100)
            short=" ".join(name.split()[-2:]) if len(name.split())>2 else name
            dest_html+=(
                f'<div style="display:grid;grid-template-columns:1fr 50px 20px;'
                f'align-items:center;gap:4px;margin-bottom:4px;">'
                f'<span style="font-size:11px;color:#A8C4D8;font-family:Barlow,sans-serif;">{short}</span>'
                f'<div style="height:5px;background:#0A1520;border-radius:3px;overflow:hidden;">'
                f'<div style="height:100%;width:{pct2}%;background:#00A3E0;border-radius:3px;"></div></div>'
                f'<span style="font-family:JetBrains Mono,monospace;font-size:9.5px;'
                f'color:#E0EDF5;text-align:right;font-weight:600;">{cnt}</span>'
                f'</div>'
            )

    # Pitch SVG
    PITCH=(
        '<rect width="100" height="68" fill="#070E04"/>'
        '<rect x="1" y="1" width="16" height="66" fill="rgba(255,255,255,0.008)"/>'
        '<rect x="33" y="1" width="16" height="66" fill="rgba(255,255,255,0.008)"/>'
        '<rect x="67" y="1" width="16" height="66" fill="rgba(255,255,255,0.008)"/>'
        '<rect x="1" y="1" width="98" height="66" fill="none" stroke="#1A3D12" stroke-width=".7"/>'
        '<line x1="50" y1="1" x2="50" y2="67" stroke="#1A3D12" stroke-width=".6"/>'
        '<circle cx="50" cy="34" r="9.15" fill="none" stroke="#1A3D12" stroke-width=".6"/>'
        '<circle cx="50" cy="34" r=".6" fill="#1A3D12"/>'
        '<rect x="1" y="13.84" width="16.5" height="40.32" fill="none" stroke="#1A3D12" stroke-width=".6"/>'
        '<rect x="1" y="24.84" width="5.5" height="18.32" fill="none" stroke="#1A3D12" stroke-width=".6"/>'
        '<rect x="82.5" y="13.84" width="16.5" height="40.32" fill="none" stroke="#1A3D12" stroke-width=".6"/>'
        '<rect x="93.5" y="24.84" width="5.5" height="18.32" fill="none" stroke="#1A3D12" stroke-width=".6"/>'
        '<rect x="0" y="29.34" width="1" height="9.32" fill="#1A3D12"/>'
        '<rect x="99" y="29.34" width="1" height="9.32" fill="#1A3D12"/>'
    )

    def srow(lbl, pct, col, detail=""):
        return (
            f'<div style="margin-bottom:6px;">'
            f'<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px;">'
            f'<span style="font-family:Barlow,sans-serif;font-size:12px;color:#A8C4D8;">{lbl}</span>'
            f'<span style="font-family:JetBrains Mono,monospace;font-size:13px;font-weight:600;color:{col};">{pct}%</span>'
            f'</div>'
            f'<div style="height:6px;background:#0A1520;border-radius:3px;overflow:hidden;margin-bottom:2px;">'
            f'<div style="height:100%;width:{pct}%;background:{col};border-radius:3px;"></div></div>'
            f'<div style="font-size:10px;color:#6A8898;">{detail}</div>'
            f'</div>'
        )

    def mcard(t, v, sub, col="#7A9AB8"):
        return (
            f'<div style="background:#080F1C;border:1px solid #101E2E;border-radius:5px;padding:5px 7px;">'
            f'<div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:700;'
            f'letter-spacing:.8px;text-transform:uppercase;color:#5A7A98;margin-bottom:2px;">{t}</div>'
            f'<div style="font-family:Barlow Condensed,sans-serif;font-size:18px;font-weight:800;'
            f'color:{col};line-height:1;">{v}</div>'
            f'<div style="font-size:9.5px;color:#5A7A98;margin-top:1px;">{sub}</div>'
            f'</div>'
        )

    def stitle(txt):
        return (
            f'<div style="font-family:Barlow Condensed,sans-serif;font-size:10.5px;font-weight:700;'
            f'letter-spacing:1.6px;text-transform:uppercase;color:#00A3E0;'
            f'display:flex;align-items:center;gap:6px;margin-bottom:7px;">'
            f'{txt}<div style="flex:1;height:1px;background:#0F1E2E;"></div></div>'
        )

    sol_pct=int(sol_ok/sol_tot*100) if sol_tot else 0
    c_pct=int(c_ok/c_tot*100) if c_tot else 0
    l_pct=int(l_ok/l_tot*100) if l_tot else 0
    grn_du="#22C55E" if du_pct>=50 else "#EF4444"
    t_but_str=f"⚽ {t_but} but{'s' if t_but>1 else ''}" if t_but else "0 but"
    deseq_pct=min(100,int(creation_deseq/max(ballons,1)*300)) if creation_deseq else 0

    return f"""<!DOCTYPE html>
<html lang="fr"><head>
<meta charset="UTF-8"/>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800;900&family=Barlow:wght@300;400;500&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:#030608;-webkit-print-color-adjust:exact;print-color-adjust:exact;
  display:flex;justify-content:center;padding:6px;}}
.page{{width:210mm;height:297mm;overflow:hidden;background:#07111C;
  display:flex;flex-direction:column;font-family:Barlow,sans-serif;}}
@media print{{
  body{{background:#030608!important;padding:0;}}
  .page{{page-break-inside:avoid;}}
  @page{{size:A4;margin:0;}}
}}
</style></head>
<body><div class="page">

<!-- HEADER -->
<div style="display:grid;grid-template-columns:1fr auto;gap:0;
  background:#060F1A;border-bottom:2.5px solid #00A3E0;flex-shrink:0;">

  <!-- Gauche : logo + photo + nom -->
  <div style="display:flex;align-items:center;gap:14px;padding:11px 16px;">
    <img src="{PFC_LOGO}" alt="PFC"
      style="width:54px;height:54px;object-fit:contain;flex-shrink:0;"
      onerror="this.style.display='none'"/>
    {photo_html}
    <div style="display:flex;flex-direction:column;gap:5px;">
      <div style="font-family:'Barlow Condensed',sans-serif;font-size:34px;font-weight:900;
        letter-spacing:.5px;color:#FFFFFF;line-height:1;text-transform:uppercase;">{player_label}</div>
      <div style="display:flex;gap:5px;flex-wrap:wrap;align-items:center;">
        {'<span style="font-family:Barlow Condensed,sans-serif;font-size:11.5px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;padding:3px 10px;border-radius:3px;border:1.5px solid #00A3E0;color:#00A3E0;background:rgba(0,163,224,0.1);">'+poste+'</span>' if poste else ''}
        {'<span style="font-family:Barlow Condensed,sans-serif;font-size:11.5px;font-weight:600;letter-spacing:.6px;text-transform:uppercase;padding:3px 10px;border-radius:3px;border:1px solid #1A2E44;color:#8AABCA;background:#09131E;">'+systeme+'</span>' if systeme else ''}
        <span style="font-family:Barlow Condensed,sans-serif;font-size:11.5px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;padding:3px 10px;border-radius:3px;border:1.5px solid #00A3E0;color:#00A3E0;background:rgba(0,163,224,0.1);">{temps_gps} MIN</span>
        <span style="font-family:Barlow Condensed,sans-serif;font-size:11.5px;font-weight:600;letter-spacing:.6px;text-transform:uppercase;padding:3px 10px;border-radius:3px;border:1px solid #1A2E44;color:#8AABCA;background:#09131E;">{journee_tag} · {competition}</span>
      </div>
      <div style="font-size:10.5px;color:#5A7A98;font-family:Barlow Condensed,sans-serif;letter-spacing:.5px;">{meta_line}</div>
    </div>
  </div>

  <!-- Droite : score + logo adverse -->
  <div style="display:flex;align-items:center;gap:14px;padding:11px 16px;border-left:1px solid #0F1E2E;">
    <div style="text-align:center;">
      <div style="font-family:Barlow Condensed,sans-serif;font-size:10px;font-weight:600;color:#5A7A98;letter-spacing:1.2px;text-transform:uppercase;margin-bottom:3px;">Paris FC vs {adversaire}</div>
      {'<div style="font-family:JetBrains Mono,monospace;font-size:30px;font-weight:600;color:#FFFFFF;background:#09131E;border:1.5px solid #1A2E44;border-radius:8px;padding:3px 14px;letter-spacing:4px;">'+score+'</div>' if score else ''}
      <div style="font-family:Barlow Condensed,sans-serif;font-size:9px;color:#5A7A98;letter-spacing:.6px;text-transform:uppercase;margin-top:3px;">Rapport match</div>
    </div>
    <div style="display:flex;flex-direction:column;align-items:center;gap:4px;">
      {adv_logo_html}
      <div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:600;color:#5A7A98;letter-spacing:.5px;text-transform:uppercase;">{adversaire or "ADV"}</div>
    </div>
  </div>
</div><!-- /header -->

<!-- BODY 2 colonnes -->
<div style="display:grid;grid-template-columns:1fr 1fr;flex:1;min-height:0;overflow:hidden;">

  <!-- COLONNE GAUCHE -->
  <div style="display:flex;flex-direction:column;border-right:1px solid #0F1E2E;overflow:hidden;">

    <!-- 4 KPI -->
    <div style="display:grid;grid-template-columns:repeat(4,1fr);border-bottom:1px solid #0F1E2E;background:#060F1A;flex-shrink:0;">
      <div style="padding:5px 8px;text-align:center;border-right:1px solid #0F1E2E;">
        <div style="font-family:Barlow Condensed,sans-serif;font-size:24px;font-weight:800;color:#FFF;line-height:1;">{p_tot}</div>
        <div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#5A7A98;margin-top:1px;">Passes</div></div>
      <div style="padding:5px 8px;text-align:center;border-right:1px solid #0F1E2E;">
        <div style="font-family:Barlow Condensed,sans-serif;font-size:24px;font-weight:800;color:#FFF;line-height:1;">{t_tot}</div>
        <div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#5A7A98;margin-top:1px;">Tirs ({t_but_str})</div></div>
      <div style="padding:5px 8px;text-align:center;border-right:1px solid #0F1E2E;">
        <div style="font-family:Barlow Condensed,sans-serif;font-size:24px;font-weight:800;color:#FFF;line-height:1;">{d_tot}</div>
        <div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#5A7A98;margin-top:1px;">Dribbles</div></div>
      <div style="padding:5px 8px;text-align:center;">
        <div style="font-family:Barlow Condensed,sans-serif;font-size:24px;font-weight:800;color:{grn_du};line-height:1;">{du_ok}/{du_tot}</div>
        <div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#5A7A98;margin-top:1px;">Duels déf.</div></div>
    </div>

    <!-- TECHNICO-TACTIQUE -->
    <div style="padding:9px 11px;border-bottom:1px solid #0F1E2E;flex:1;overflow:hidden;">
      {stitle("Technico-Tactique")}
      {srow("Passes réussies", p_pct, "#22C55E",
        f"{p_ok} réussies · {p_ko} ratées" + (f" · {passes_dt} dern.tiers" if passes_dt else "") + (f" · {passes_en1} en 1 tch." if passes_en1 else ""))}
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:7px;">
        {mcard("Courtes", f"{c_ok}/{c_tot}", f"Réussite {c_pct}%", "#22C55E")}
        {mcard("Longues", f"{l_ok}/{l_tot}", f"Réussite {l_pct}%", "#E8B30A")}
      </div>
      {srow("Dribbles réussis", d_pct, "#00A3E0", f"{d_ok} réussi{'s' if d_ok!=1 else ''} · {d_ko} raté{'s' if d_ko!=1 else ''}")}
      {srow("Duels défensifs gagnés", du_pct, "#F4830A", f"{du_ok} gagnés · {du_ko} perdus")}
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:7px;">
        {mcard("Sol", f"{sol_ok}G / {sol_tot}", f"{sol_pct}% réussite", "#F4830A")}
        {mcard("Aérien", f"{aer_ok}G / {aer_tot}" if aer_tot else "—", "duels aériens" if aer_tot else "aucun", "#3A5570" if not aer_tot else "#F4830A")}
      </div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-bottom:7px;">
        {mcard("Tirs cadrés", f"{t_cad}/{t_tot}", t_but_str, "#22C55E")}
        {mcard("Récupérations", str(recup), "interceptions", "#22C55E")}
        {mcard("Pertes balle", str(pertes), f"/{ballons} ballons", "#EF4444")}
      </div>
      {srow("Créations déséquilibre", deseq_pct, "#38BDF8", f"{creation_deseq} actions") if creation_deseq else ""}

      <div style="height:1px;background:#0F1E2E;margin:6px 0 7px;"></div>
      {stitle("Physique Match GPS")}
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:4px;margin-bottom:5px;">
        {mcard("Distance", _g("distance_m","{:,.0f}"), "m", "#38BDF8")}
        {mcard("HID >13", _g("hid13_m","{:,.0f}"), "m", "#38BDF8")}
        {mcard("HID >19", _g("hid19_m","{:,.0f}"), "m", "#38BDF8")}
        {mcard("V. max", _g("vmax_kmh","{:.1f}"), "km/h", "#38BDF8")}
        {mcard("Sprints >23", _g("sprints_23","{:.0f}"), "nb", "#38BDF8")}
        {mcard("Acc/Déc tot.", _g("acc_dec","{:.0f}"), "nb", "#38BDF8")}
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:3px;margin-bottom:5px;">
        {mcard("Acc >2", _g("acc2","{:.0f}"), "nb", "#5A7A98")}
        {mcard("Acc >3", _g("acc3","{:.0f}"), "nb", "#5A7A98")}
        {mcard("Déc >2", _g("dec2","{:.0f}"), "nb", "#5A7A98")}
        {mcard("Déc >3", _g("dec3","{:.0f}"), "nb", "#5A7A98")}
      </div>
      <div style="font-family:Barlow Condensed,sans-serif;font-size:9.5px;font-weight:700;
        letter-spacing:1.2px;text-transform:uppercase;color:#6A8898;margin-bottom:4px;">Répartition vitesse</div>
      {spd_bars}
    </div>

  </div><!-- /col gauche -->

  <!-- COLONNE DROITE -->
  <div style="display:flex;flex-direction:column;overflow:hidden;">

    <!-- RADAR DU MATCH -->
    {f'''
    <div style="padding:9px 10px;border-bottom:1px solid #0F1E2E;flex-shrink:0;">
      <div style="font-family:Barlow Condensed,sans-serif;font-size:10px;font-weight:700;
        letter-spacing:1.4px;text-transform:uppercase;color:#00A3E0;margin-bottom:5px;">
        ◈ Radar du match
      </div>
      <img src="{radar_b64}" style="width:100%;display:block;border-radius:5px;background:#08090D;" alt="Radar"/>
    </div>
    ''' if radar_b64 else ''}

    <!-- HEATMAP -->
    <div style="padding:9px 10px;border-bottom:1px solid #0F1E2E;flex-shrink:0;">
      {stitle("Heatmap — Zone d'action")}
      <svg viewBox="0 0 100 68" width="100%" style="border-radius:5px;display:block;"
           xmlns="http://www.w3.org/2000/svg">
        <defs>
          <clipPath id="cph"><rect x="1" y="1" width="98" height="66"/></clipPath>
          <radialGradient id="hg" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stop-color="#00A3E0" stop-opacity="0.95"/>
            <stop offset="55%" stop-color="#00A3E0" stop-opacity="0.35"/>
            <stop offset="100%" stop-color="#00A3E0" stop-opacity="0"/>
          </radialGradient>
        </defs>
        {PITCH}
        <text x="3" y="65.5" font-size="4" fill="#1A3D10" font-family="Barlow Condensed,sans-serif">◀ BUT ADV</text>
        <text x="73" y="65.5" font-size="4" fill="#1A3D10" font-family="Barlow Condensed,sans-serif">BUT PFC ▶</text>
        <g id="heat-g" clip-path="url(#cph)"></g>
      </svg>
    </div>

    <!-- PASSES + ROSE -->
    <div style="padding:9px 10px;border-bottom:1px solid #0F1E2E;flex-shrink:0;">
      {stitle("Rose des directions de passe")}
      <svg id="svg-pass" viewBox="0 0 100 68" width="100%" style="border-radius:5px;display:block;"
           xmlns="http://www.w3.org/2000/svg">
        <defs>
          <clipPath id="cpp"><rect x="1" y="1" width="98" height="66"/></clipPath>
          <marker id="mOk" markerWidth="5" markerHeight="5" refX="4" refY="2.5" orient="auto">
            <path d="M0,0.5 L4.5,2.5 L0,4.5 Z" fill="#22C55E"/></marker>
          <marker id="mKo" markerWidth="5" markerHeight="5" refX="4" refY="2.5" orient="auto">
            <path d="M0,0.5 L4.5,2.5 L0,4.5 Z" fill="#EF4444"/></marker>
        </defs>
        {PITCH}
        <g id="pass-g" clip-path="url(#cpp)"></g>
      </svg>
      <div style="display:flex;gap:10px;margin:3px 0 4px;">
        <span style="font-size:9.5px;color:#5A7A98;display:flex;align-items:center;gap:3px;">
          <span style="width:7px;height:7px;border-radius:50%;background:#00A3E0;display:inline-block;"></span>Centroïde joueuse</span>
        <span style="font-size:9px;color:#4A6A88;">↑ AV=vers but adverse · ↓ AR=arrière</span>
      </div>
    </div>

    <!-- DESTINATIONS -->
    <div style="padding:9px 10px;flex:1;overflow:hidden;">
      {stitle("Destinations de passes")}
      {dest_html if dest_html else '<div style="font-size:11px;color:#1A2E44;font-style:italic;">Non disponible</div>'}
    </div>

  </div><!-- /col droite -->
</div><!-- /body -->

<!-- FOOTER -->
<div style="height:18px;display:flex;align-items:center;justify-content:space-between;
  padding:0 11px;background:#060F1A;border-top:1px solid #0F1E2E;flex-shrink:0;">
  <span style="font-family:Barlow Condensed,sans-serif;font-size:9px;letter-spacing:.8px;text-transform:uppercase;color:#3A5A70;">Paris Football Club · Rapport individuel de match</span>
  <span style="font-family:Barlow Condensed,sans-serif;font-size:9px;letter-spacing:.8px;text-transform:uppercase;color:#3A5A70;">{match_label}</span>
  <span style="font-family:Barlow Condensed,sans-serif;font-size:9px;letter-spacing:.8px;text-transform:uppercase;color:#00A3E0;font-weight:700;">Confidentiel</span>
</div>

</div><!-- .page -->
<script>
var NS="http://www.w3.org/2000/svg";
var LD={locs_json};
var PD={passes_json};
var CX={cx:.2f},CY={cy:.2f};
var RC={_ref_centroids_json};
var PC={_player_centroid_json};

// ── HEATMAP avec centroïdes par poste ────────────────────────────────────────
(function(){{
  var g=document.getElementById("heat-g");if(!g)return;
  // Centroïdes par poste (petits losanges gris + label)
  RC.forEach(function(rc){{
    if(rc.isPlayer) return;
    var cx2=rc.cx,cy2=rc.cy;
    var sz=2.0;
    var pts=[cx2+","+( cy2-sz)+" "+(cx2+sz)+","+cy2+" "+cx2+","+(cy2+sz)+" "+(cx2-sz)+","+cy2];
    var d=document.createElementNS(NS,"polygon");
    d.setAttribute("points",pts.join(""));
    d.setAttribute("fill","#3A5570");d.setAttribute("stroke","#07111C");d.setAttribute("stroke-width","0.5");
    d.setAttribute("opacity","0.9");
    g.appendChild(d);
    var t=document.createElementNS(NS,"text");
    t.setAttribute("x",(cx2+2.4).toFixed(1));t.setAttribute("y",(cy2-2.2).toFixed(1));
    t.setAttribute("font-size","3.8");t.setAttribute("font-family","Barlow Condensed,sans-serif");
    t.setAttribute("font-weight","700");t.setAttribute("fill","#3A5570");
    t.textContent=rc.poste;g.appendChild(t);
  }});
  // Centroïde activité joueuse (cyan — position moyenne de toutes ses actions)
  var c=document.createElementNS(NS,"circle");
  c.setAttribute("cx",CX.toFixed(1));c.setAttribute("cy",CY.toFixed(1));
  c.setAttribute("r","2.2");c.setAttribute("fill","#00A3E0");
  c.setAttribute("stroke","#060F1A");c.setAttribute("stroke-width","0.8");
  g.appendChild(c);
  var ring=document.createElementNS(NS,"circle");
  ring.setAttribute("cx",CX.toFixed(1));ring.setAttribute("cy",CY.toFixed(1));
  ring.setAttribute("r","4.5");ring.setAttribute("fill","none");
  ring.setAttribute("stroke","#00A3E0");ring.setAttribute("stroke-width","0.7");ring.setAttribute("opacity","0.5");
  g.appendChild(ring);
}})();
(function(){{
  var g=document.getElementById("pass-g");if(!g)return;
  // Centroïde uniquement (pas de flèches)
  var cd=document.createElementNS(NS,"circle");
  cd.setAttribute("cx",CX.toFixed(1));cd.setAttribute("cy",CY.toFixed(1));
  cd.setAttribute("r","2.5");cd.setAttribute("fill","#00A3E0");
  cd.setAttribute("stroke","#060F1A");cd.setAttribute("stroke-width","1");
  g.appendChild(cd);
  var ring2=document.createElementNS(NS,"circle");
  ring2.setAttribute("cx",CX.toFixed(1));ring2.setAttribute("cy",CY.toFixed(1));
  ring2.setAttribute("r","4.5");ring2.setAttribute("fill","none");
  ring2.setAttribute("stroke","#00A3E0");ring2.setAttribute("stroke-width","0.6");ring2.setAttribute("opacity","0.4");
  g.appendChild(ring2);
  // Rose des directions intégrée au terrain, centrée sur le centroïde de la joueuse
  var NS2="http://www.w3.org/2000/svg";
  var svgPass=document.getElementById("svg-pass");
  // Le viewBox du terrain est "0 0 100 68"
  // On place la rose centrée sur le centroïde (CX, CY), rayon max = 18 unités terrain
  var rcx=CX, rcy=CY, rRMax=17, rRMin=2;
  var SC2=[
    {{l:"AV",     a:0,   c:"#00A3E0"}},
    {{l:"D▸AV",   a:45,  c:"#38BDF8"}},
    {{l:"LAT▸",   a:90,  c:"#64748B"}},
    {{l:"D▸AR",   a:135, c:"#475569"}},
    {{l:"AR",     a:180, c:"#F4830A"}},
    {{l:"G▸AR",   a:-135,c:"#475569"}},
    {{l:"◂LAT",   a:-90, c:"#64748B"}},
    {{l:"G▸AV",   a:-45, c:"#38BDF8"}},
  ];
  // Cercles de fond
  [0.33,0.66,1.0].forEach(function(fr){{
    var r2=document.createElementNS(NS2,"circle");
    r2.setAttribute("cx",rcx.toFixed(1));r2.setAttribute("cy",rcy.toFixed(1));
    r2.setAttribute("r",(rRMin+(rRMax-rRMin)*fr).toFixed(1));
    r2.setAttribute("fill","#060F1A");r2.setAttribute("fill-opacity","0.55");
    r2.setAttribute("stroke","#0D1B2A");r2.setAttribute("stroke-width","0.5");
    svgPass.appendChild(r2);
  }});
  // Spokes
  for(var si=0;si<8;si++){{
    var a2=si*45*Math.PI/180;
    var sp=document.createElementNS(NS2,"line");
    sp.setAttribute("x1",rcx.toFixed(1));sp.setAttribute("y1",rcy.toFixed(1));
    sp.setAttribute("x2",(rcx+(rRMax+1)*Math.cos(a2)).toFixed(1));
    sp.setAttribute("y2",(rcy+(rRMax+1)*Math.sin(a2)).toFixed(1));
    sp.setAttribute("stroke","#0D1B2A");sp.setAttribute("stroke-width","0.4");
    svgPass.appendChild(sp);
  }}
  // Comptage secteurs
  var counts2=new Array(8).fill(0);
  PD.forEach(function(p){{
    if(p.x==null)return;
    // AV = vers BUT PFC = droite SVG = dx positif
    // Y inversé dans SVG : dy positif SVG = vers bas = vers DG (gauche terrain)
    var dx=p.x-CX,dy=CY-p.y;
    var angle=Math.atan2(dy,dx)*180/Math.PI;
    var norm=(angle+360)%360;
    var sector=Math.round(norm/45)%8;
    counts2[sector]++;
  }});
  var maxC2=Math.max.apply(null,counts2)||1;
  // Pétales
  SC2.forEach(function(s,i){{
    var n=counts2[i];
    var aBase=i*45*Math.PI/180;
    var hw=16*Math.PI/180;
    var rFull2=rRMin+(rRMax-rRMin)*1.0;
    var framePts=[[rcx,rcy],
      [rcx+rFull2*Math.cos(aBase-hw),rcy+rFull2*Math.sin(aBase-hw)],
      [rcx+rFull2*Math.cos(aBase),   rcy+rFull2*Math.sin(aBase)],
      [rcx+rFull2*Math.cos(aBase+hw),rcy+rFull2*Math.sin(aBase+hw)]
    ].map(function(p2){{return p2[0].toFixed(1)+","+p2[1].toFixed(1);}}).join(" ");
    var frame=document.createElementNS(NS2,"polygon");
    frame.setAttribute("points",framePts);frame.setAttribute("fill",s.c);
    frame.setAttribute("fill-opacity","0.07");frame.setAttribute("stroke","none");
    svgPass.appendChild(frame);
    if(n===0){{
      var lr2=rFull2+4.5;
      var t2=document.createElementNS(NS2,"text");
      t2.setAttribute("x",(rcx+lr2*Math.cos(aBase)).toFixed(1));
      t2.setAttribute("y",(rcy+lr2*Math.sin(aBase)).toFixed(1));
      t2.setAttribute("text-anchor","middle");t2.setAttribute("dominant-baseline","central");
      t2.setAttribute("font-size","2.8");t2.setAttribute("font-family","Barlow Condensed,sans-serif");
      t2.setAttribute("font-weight","600");t2.setAttribute("fill","#1E3050");
      t2.textContent=s.l+":0";svgPass.appendChild(t2);return;
    }}
    var r3=rRMin+(rRMax-rRMin)*n/maxC2;
    var pts2=[[rcx,rcy],
      [rcx+r3*Math.cos(aBase-hw),rcy+r3*Math.sin(aBase-hw)],
      [rcx+r3*Math.cos(aBase),   rcy+r3*Math.sin(aBase)],
      [rcx+r3*Math.cos(aBase+hw),rcy+r3*Math.sin(aBase+hw)]
    ].map(function(p2){{return p2[0].toFixed(1)+","+p2[1].toFixed(1);}}).join(" ");
    var poly2=document.createElementNS(NS2,"polygon");
    poly2.setAttribute("points",pts2);poly2.setAttribute("fill",s.c);
    poly2.setAttribute("fill-opacity","0.85");poly2.setAttribute("stroke",s.c);poly2.setAttribute("stroke-width","0.3");
    svgPass.appendChild(poly2);
    var lr3=r3+4.5;
    var t3=document.createElementNS(NS2,"text");
    t3.setAttribute("x",(rcx+lr3*Math.cos(aBase)).toFixed(1));
    t3.setAttribute("y",(rcy+lr3*Math.sin(aBase)).toFixed(1));
    t3.setAttribute("text-anchor","middle");t3.setAttribute("dominant-baseline","central");
    t3.setAttribute("font-size","3.2");t3.setAttribute("font-family","Barlow Condensed,sans-serif");
    t3.setAttribute("font-weight","700");t3.setAttribute("fill","#C8DCF0");
    t3.textContent=s.l+":"+n;svgPass.appendChild(t3);
  }});
  // Point central cyan
  var cdc=document.createElementNS(NS2,"circle");
  cdc.setAttribute("cx",rcx.toFixed(1));cdc.setAttribute("cy",rcy.toFixed(1));
  cdc.setAttribute("r","1.5");cdc.setAttribute("fill","#00A3E0");
  svgPass.appendChild(cdc);
}})();
// ── FIN ──
</script></body></html>"""


def _render_gps_match_tab(gps_match: "pd.DataFrame", player_name: str, permissions: dict, user_profile: str, tactical_files: list = None):
    """Affiche l'onglet GPS Match dans la page Données Physiques."""

    if gps_match is None or (hasattr(gps_match, "empty") and gps_match.empty):
        st.info("Aucun fichier GPS match détecté.")
        st.markdown(
            "<div style='background:#0C1220;border:1px solid rgba(0,163,224,0.2);border-radius:4px;padding:16px;'>"
            "<div style='font-family:Oswald,sans-serif;font-size:13px;color:#6A8090;text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>Comment ajouter des matchs ?</div>"
            "<div style='font-family:Inter,sans-serif;font-size:13px;color:#C8D8E8;line-height:1.7;'>"
            "Placez vos fichiers CSV GPS match dans le dossier Drive GPS.<br>"
            "Format attendu : <code style='color:#00A3E0;'>U19_2_J02_Paris_FC_-_OL_Lyonnes_25_01_26.csv</code>"
            "</div></div>",
            unsafe_allow_html=True,
        )
        return

    all_players_m = sorted(gps_match["Player"].dropna().astype(str).unique().tolist()) if "Player" in gps_match.columns else []
    is_admin = check_permission(user_profile, "all", permissions) or check_permission(user_profile, "update_data", permissions)

    # ── Sélection joueuse ──────────────────────────────────────────
    if player_name and any(nettoyer_nom_joueuse(player_name) == nettoyer_nom_joueuse(p) for p in all_players_m):
        selected_player = player_name
        st.caption(f"Joueuse : **{selected_player}**")
    else:
        selected_player = st.selectbox("Joueuse", ["Toutes"] + all_players_m, key="gps_match_player_sel")

    dm = gps_match[gps_match["Player"].astype(str).apply(nettoyer_nom_joueuse) == nettoyer_nom_joueuse(selected_player)].copy() \
        if selected_player and selected_player != "Toutes" else gps_match.copy()

    if dm.empty:
        st.info("Aucune donnée match pour cette joueuse.")
        return

    if "DATE" in dm.columns:
        dm = dm.sort_values("DATE")
    labels = dm["__match_label"].fillna("?").astype(str).tolist() if "__match_label" in dm.columns else [str(i) for i in range(len(dm))]

    # ── Métriques clés ─────────────────────────────────────────────
    nb_matchs   = dm["__match_label"].nunique() if "__match_label" in dm.columns else len(dm)
    dist_moy    = dm["Distance (m)"].mean() if "Distance (m)" in dm.columns else None
    vmax_max    = dm["Vitesse max (km/h)"].max() if "Vitesse max (km/h)" in dm.columns else None
    sprints_moy = dm["Sprints_23"].mean() if "Sprints_23" in dm.columns else None
    temps_moy   = dm["Durée_min"].mean() if "Durée_min" in dm.columns else None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Matchs", f"{nb_matchs}")
    c2.metric("Distance moy.", f"{dist_moy/1000:.2f} km" if dist_moy else "—")
    c3.metric("Vitesse max", f"{vmax_max:.1f} km/h" if vmax_max else "—")
    c4.metric("Sprints moy. (>23)", f"{sprints_moy:.1f}" if sprints_moy else "—")
    c5.metric("Temps moy.", f"{temps_moy:.0f} min" if temps_moy else "—")

    st.divider()

    def _col(key): return dm[key].fillna(0).tolist() if key in dm.columns else [0]*len(labels)

    # ── Distance par match ─────────────────────────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("#### 📏 Distance par match")

        dist_vals  = _col("Distance (m)")
        hid13_vals = _col("Distance HID (>13 km/h)")
        hid19_vals = _col("Distance HID (>19 km/h)")

        fig, ax1 = plt.subplots(figsize=(9, 3.8), dpi=90)
        fig.patch.set_facecolor("#08090D")
        ax1.set_facecolor("#08090D")

        x = list(range(len(labels)))
        w_tot  = 0.55   # barre Distance totale (large, en fond)
        w_hid  = 0.22   # barres HID (fines, groupées à droite)

        # Distance totale — axe gauche
        ax1.bar(x, dist_vals, w_tot, label="Distance totale", color="#1E3A5F",
                edgecolor="#08090D", linewidth=0.5, zorder=2)

        # Axe droit pour HID
        ax2 = ax1.twinx()
        ax2.bar([xi + w_hid*0.6 for xi in x], hid13_vals, w_hid,
                label="HID >13", color="#4db8e8", edgecolor="#08090D", linewidth=0.5, zorder=3)
        ax2.bar([xi + w_hid*0.6 + w_hid for xi in x], hid19_vals, w_hid,
                label="HID >19", color="#00A3E0", edgecolor="#08090D", linewidth=0.5, zorder=3)

        # Ligne % HID>13 sur axe gauche (en %)
        ax3 = ax1.twinx()
        ax3.spines["right"].set_position(("axes", 1.08))
        pct_hid = [h/d*100 if d and d > 0 else 0 for h, d in zip(hid13_vals, dist_vals)]
        ax3.plot(x, pct_hid, "o--", color="#FFD700", linewidth=1.5, markersize=4,
                 label="% HID >13", zorder=4, alpha=0.85)
        ax3.set_ylabel("% HID >13", color="#FFD700", fontsize=8)
        ax3.tick_params(axis="y", colors="#FFD700", labelsize=8)
        ax3.spines["right"].set_color("#FFD700")
        ax3.set_ylim(0, max(max(pct_hid)*1.5, 30) if pct_hid else 30)

        # Styling axes
        for spine in ["bottom","left","top"]:
            ax1.spines[spine].set_color("#1A2A3A")
        ax1.spines["right"].set_visible(False)
        ax1.spines["top"].set_visible(False)
        ax2.spines["right"].set_color("#1A2A3A")
        ax2.spines["top"].set_visible(False)
        ax2.spines["left"].set_visible(False)
        ax2.spines["bottom"].set_visible(False)

        ax1.set_xticks(x)
        ax1.set_xticklabels(labels, rotation=30, ha="right", fontsize=9, color="#C8D8E8")
        ax1.set_ylabel("Distance totale (m)", color="#6A8090", fontsize=9)
        ax2.set_ylabel("Distance HID (m)", color="#4db8e8", fontsize=9)
        ax1.tick_params(colors="#6A8090", labelsize=9)
        ax2.tick_params(axis="y", colors="#4db8e8", labelsize=9)
        ax1.yaxis.grid(True, color="#1A2A3A", linewidth=0.5, alpha=0.7)
        ax1.set_axisbelow(True)

        # Légende combinée
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        h3, l3 = ax3.get_legend_handles_labels()
        ax1.legend(h1+h2+h3, l1+l2+l3, fontsize=7, facecolor="#0C1220",
                   edgecolor="#1A2A3A", labelcolor="#C8D8E8", loc="upper left")

        fig.subplots_adjust(bottom=0.28, top=0.95, left=0.10, right=0.86)
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

    # ── Sprints & vitesse max ──────────────────────────────────────
    with col_r:
        st.markdown("#### ⚡ Sprints & Vitesse max")
        fig2, ax2 = plt.subplots(figsize=(9, 3.5))
        fig2.patch.set_facecolor("#08090D")
        ax2.set_facecolor("#08090D")
        fig2.set_dpi(90)
        x = list(range(len(labels)))
        w = 0.35
        if "Sprints_23" in dm.columns:
            ax2.bar([xi - w/2 for xi in x], _col("Sprints_23"), w, label="Sprints >23", color="#00A3E0", edgecolor="#08090D")
        if "Sprints_25" in dm.columns:
            ax2.bar([xi + w/2 for xi in x], _col("Sprints_25"), w, label="Sprints >25", color="#FFFFFF", edgecolor="#08090D")
        ax2b = ax2.twinx()
        if "Vitesse max (km/h)" in dm.columns:
            vmax_vals = dm["Vitesse max (km/h)"].fillna(0).tolist()
            ax2b.plot(x, vmax_vals, "o--", color="#FFD700", linewidth=2, markersize=6, label="Vmax")
            for xi, v in zip(x, vmax_vals):
                ax2b.annotate(f"{v:.1f}", (xi, v), textcoords="offset points", xytext=(0,7),
                              ha="center", fontsize=8, color="#FFD700")
            ax2b.set_ylabel("km/h", color="#FFD700", fontsize=9)
            ax2b.tick_params(colors="#6A8090")
            ax2b.spines["right"].set_color("#1A2A3A")
        for spine in ["bottom","left","top","right"]:
            ax2.spines[spine].set_color("#1A2A3A")
        ax2.spines["top"].set_visible(False)
        ax2.set_xticks(x); ax2.set_xticklabels(labels, rotation=30, ha="right", fontsize=9, color="#C8D8E8")
        ax2.set_ylabel("Nb sprints", color="#6A8090", fontsize=9)
        ax2.tick_params(colors="#6A8090")
        ax2.yaxis.grid(True, color="#1A2A3A", linewidth=0.5, alpha=0.7)
        ax2.set_axisbelow(True)
        lines1, labs1 = ax2.get_legend_handles_labels()
        lines2, labs2 = ax2b.get_legend_handles_labels()
        ax2.legend(lines1+lines2, labs1+labs2, fontsize=8, facecolor="#0C1220", edgecolor="#1A2A3A", labelcolor="#C8D8E8")
        fig2.subplots_adjust(bottom=0.28, top=0.95, left=0.08, right=0.93)
        st.pyplot(fig2, use_container_width=True)
        plt.close(fig2)

    # ── Plages de vitesse (stacked) ────────────────────────────────
    st.markdown("#### 🏃 Répartition des plages de vitesse")
    speed_data = [
        ("V_0_7",   "0-7 km/h",    "#1A2A3A"),
        ("V_7_13",  "7-13 km/h",   "#1E3A5F"),
        ("V_13_15", "13-15 km/h",  "#2A5A8F"),
        ("V_15_19", "15-19 km/h",  "#3A80C0"),
        ("V_19_23", "19-23 km/h",  "#50A0D8"),
        ("V_23_25", "23-25 km/h",  "#00A3E0"),
        ("V_sup25", ">25 km/h",    "#FFFFFF"),
    ]
    avail_speed = [(k,l,c) for k,l,c in speed_data if k in dm.columns]
    if avail_speed:
        fig3 = _make_match_bar_chart(labels,
            [(_col(k), l, c) for k,l,c in avail_speed],
            "Distance (m)", "m", figsize=(12, 3.5), stacked=True)
        st.pyplot(fig3, use_container_width=True)
        plt.close(fig3)

    # ── Accélérations / Décélérations ─────────────────────────────
    st.markdown("#### 🔀 Accélérations & Décélérations")
    ad_data = [
        ("Acc_2", "Acc >2 m/s²", "#1A6090"),
        ("Acc_3", "Acc >3 m/s²", "#00A3E0"),
        ("Acc_4", "Acc >4 m/s²", "#80D4F0"),
        ("Dec_2", "Déc >2 m/s²", "#902020"),
        ("Dec_3", "Déc >3 m/s²", "#D04040"),
        ("Dec_4", "Déc >4 m/s²", "#F08080"),
    ]
    avail_ad = [(k,l,c) for k,l,c in ad_data if k in dm.columns]
    if avail_ad:
        fig4 = _make_match_bar_chart(labels,
            [(_col(k), l, c) for k,l,c in avail_ad],
            "Nombre", "nb", figsize=(12, 3.5))
        st.pyplot(fig4, use_container_width=True)
        plt.close(fig4)

    # ── Tableau détaillé ──────────────────────────────────────────
    with st.expander("📋 Tableau détaillé", expanded=False):
        show_cols_m = [c for c in [
            "__match_label","__journee","__adversaire","DATE","Player","Durée_min",
            "Distance (m)","Distance HID (>13 km/h)","Distance HID (>19 km/h)",
            "V_0_7","V_7_13","V_13_15","V_15_19","V_19_23","V_23_25","V_sup25",
            "Sprints_23","Sprints_25","Vitesse max (km/h)","Acc_max",
            "Acc_2","Acc_3","Dec_2","Dec_3","#accel/decel",
        ] if c in dm.columns]
        rename_display = {
            "__match_label":"Match","__journee":"J.","__adversaire":"Adversaire",
            "Durée_min":"Tps(min)","V_0_7":"0-7km/h","V_7_13":"7-13km/h",
            "V_13_15":"13-15km/h","V_15_19":"15-19km/h","V_19_23":"19-23km/h",
            "V_23_25":"23-25km/h","V_sup25":">25km/h","Sprints_23":"Spr.23",
            "Sprints_25":"Spr.25","Acc_max":"Acc.max","Acc_2":"Acc>2",
            "Acc_3":"Acc>3","Dec_2":"Déc>2","Dec_3":"Déc>3",
        }
        disp = dm[show_cols_m].rename(columns=rename_display)
        if "DATE" in disp.columns:
            disp = disp.sort_values("DATE", ascending=False)
        st.dataframe(disp, use_container_width=True)

    # ── Section Technico-Tactique ──────────────────────────────────
    if tactical_files:
        st.divider()
        st.markdown("#### 🎯 Rapport Technico-Tactique")

        # Construire la liste des matchs depuis les fichiers tactiques (source primaire)
        # enrichie par les infos GPS quand disponibles
        match_rows = []
        gps_match_df_ref = st.session_state.get("gps_match_df", pd.DataFrame())

        for tac in tactical_files:
            tac_date        = tac.get("date")
            tac_adv         = tac.get("adversaire", "")
            tac_jrnee       = tac.get("journee", "")
            tac_label       = tac.get("filename", tac.get("label", ""))
            # competition et saison déjà enrichis dans load_tactical_files

            # Enrichir avec GPS si dispo
            gps_label = ""
            if not gps_match_df_ref.empty and "__match_label" in gps_match_df_ref.columns:
                # Chercher un match GPS proche en date ou adversaire
                for _, gps_row_r in gps_match_df_ref.drop_duplicates(subset=["__match_label"]).iterrows():
                    _gl = str(gps_row_r.get("__match_label", ""))
                    _gd = gps_row_r.get("DATE")
                    _ga = str(gps_row_r.get("__adversaire", ""))
                    date_match = (tac_date is not None and pd.notna(_gd) and
                                  abs((pd.Timestamp(tac_date) - pd.Timestamp(_gd).normalize()).days) <= 2)
                    adv_match  = (tac_adv and _ga and
                                  normalize_str(tac_adv)[:4] in normalize_str(_ga) or
                                  normalize_str(_ga)[:4] in normalize_str(tac_adv)) if tac_adv and _ga else False
                    if date_match or adv_match:
                        gps_label = _gl
                        if not tac_adv and _ga:
                            tac_adv = _ga
                        if not tac_jrnee and gps_row_r.get("__journee"):
                            tac_jrnee = str(gps_row_r.get("__journee", "")).lstrip("J")
                        break

            # Construire le libellé : "25/26 · U19N · J10 · HAC"
            parts = []
            saison = tac.get("saison", "")
            if saison:
                saison_court = re.sub(r"20(\d{2})/20(\d{2})", r"\1/\2", saison)
                parts.append(saison_court)
            competition = tac.get("competition", "")
            if competition:
                parts.append(competition)
            if tac_jrnee:
                parts.append(f"J{tac_jrnee}")
            if tac_adv:
                parts.append(tac_adv)
            display_label = " · ".join(parts) if parts else tac_label

            match_rows.append({
                "label":         tac_label,
                "display":       display_label,
                "date":          tac_date,
                "adversaire":    tac_adv,
                "journee":       tac_jrnee,
                "gps_label":     gps_label,
                "tac_obj":       tac,
            })

        # Trier par date décroissante
        match_rows.sort(key=lambda m: m["date"] if m["date"] is not None and pd.notna(m["date"]) else pd.Timestamp("1970-01-01"), reverse=True)

        if not match_rows:
            st.info("Aucun fichier tactique trouvé.")
        else:
            display_labels = [m["display"] for m in match_rows]
            sel_display = st.selectbox("Match", display_labels, key="tactical_match_sel")
            sel_row = next((m for m in match_rows if m["display"] == sel_display), match_rows[0])
            sel_match = sel_row["label"]

            # Le fichier tactique est directement dans sel_row["tac_obj"]
            df_tactic = sel_row["tac_obj"].get("df") if sel_row.get("tac_obj") else None

            if df_tactic is None:
                st.info(f"Aucun fichier tactique associé trouvé pour **{sel_match}**.")
                st.caption("Vérifiez que le fichier CSV tactique est bien dans le dossier `data/` avec le format `PFC_VS__...csv`")
            else:
                # Lister les joueuses présentes dans ce fichier tactique
                skip_rows = {"START", "PFC", "HAC", ""}
                tac_players = [
                    r for r in df_tactic["Row"].dropna().unique()
                    if r not in skip_rows
                    and not any(k in str(r) for k in ["Transition", "Carton", "def "])
                ] if "Row" in df_tactic.columns else []

                if not tac_players:
                    st.warning("Aucune joueuse trouvée dans ce fichier tactique.")
                else:
                    # Sélecteur de joueuse — pré-sélectionner si contexte joueuse
                    default_idx = 0
                    if selected_player and selected_player != "Toutes":
                        _pnorm = normalize_str(selected_player)
                        for _i, _p in enumerate(tac_players):
                            if normalize_str(_p) == _pnorm or _pnorm in normalize_str(_p) or normalize_str(_p) in _pnorm:
                                default_idx = _i
                                break
                    sel_tac_player = st.selectbox(
                        "Joueuse", tac_players,
                        index=default_idx,
                        key="tactical_player_sel"
                    )

                    # Générer et afficher le rapport HTML pour la joueuse sélectionnée
                    import streamlit.components.v1 as _components
                    gps_match_df = st.session_state.get("gps_match_df", pd.DataFrame())
                    gps_summary = get_gps_match_summary_for_player(
                        gps_match_df,
                        sel_tac_player,
                        match_date=pd.to_datetime(sel_row.get("date", None), errors="coerce") if isinstance(sel_row, dict) else None,
                        match_label=sel_row.get("gps_label") or sel_row.get("display", "") or sel_match,
                    )

                    # Score + contexte depuis le fichier tactique (source fiable)
                    _tac_ctx = _get_match_context(df_tactic)
                    _tac_match_info = dict(sel_row)  # copie pour ne pas muter l'original
                    _tac_match_info["score"] = f"{_tac_ctx.get('score_pfc','?')} – {_tac_ctx.get('score_adv','?')}"
                    if not _tac_match_info.get("adversaire"):
                        _tac_match_info["adversaire"] = _tac_ctx.get("adversaire", "")
                    if not _tac_match_info.get("lieu"):
                        _tac_match_info["lieu"] = _tac_ctx.get("lieu", "")
                    if not _tac_match_info.get("journee"):
                        _tac_match_info["journee"] = _tac_ctx.get("journee", "")
                    _tac_match_info["competition"] = _tac_ctx.get("competition", _tac_match_info.get("competition", ""))

                    # Photo joueuse — via concordance passerelle (même logique que l'onglet Passerelles)
                    _tac_photo_b64 = ''
                    try:
                        _cc = st.session_state.get('photo_concordance', {})
                        _pi = st.session_state.get('photos_index', {})
                        # Essayer d'abord avec le nom canonique du fichier tactique
                        _pp = find_photo_for_player(sel_tac_player, concordance=_cc, photos_index=_pi)
                        # Fallback : chercher dans la passerelle_data par correspondance de nom
                        if not _pp:
                            _pass_data = load_passerelle_data()
                            _pnorm = nettoyer_nom_joueuse(sel_tac_player)
                            for _pk, _pv in _pass_data.items():
                                _full = f"{_pv.get('Nom','')} {_pv.get('Prénom','')}".strip()
                                if nettoyer_nom_joueuse(_full) == _pnorm or nettoyer_nom_joueuse(_pk) == _pnorm:
                                    _src = _pv.get("Photo", "")
                                    if _src and os.path.exists(str(_src)):
                                        _pp = str(_src)
                                    break
                        if _pp and os.path.exists(str(_pp)):
                            import base64 as _b64r
                            _pb = load_photo_bytes(str(_pp))
                            if _pb:
                                _tac_photo_b64 = 'data:image/jpeg;base64,' + _b64r.b64encode(_pb).decode()
                    except Exception:
                        pass

                    # KPI joueuse
                    _tac_kpi_row = None
                    try:
                        _kpi_all = st.session_state.get('pfc_kpi_all', pd.DataFrame())
                        if not _kpi_all.empty and 'Player' in _kpi_all.columns:
                            _nm = nettoyer_nom_joueuse(sel_tac_player)
                            _kdf = _kpi_all[_kpi_all['Player'].astype(str).apply(nettoyer_nom_joueuse) == _nm]
                            if not _kdf.empty:
                                _tac_kpi_row = _kdf.iloc[0]
                    except Exception:
                        pass

                    # Radar du match — même logique que l'onglet Radar
                    _tac_radar_b64 = ""
                    try:
                        _kpi_all_r = st.session_state.get('pfc_kpi_all', pd.DataFrame())
                        if not _kpi_all_r.empty and 'Player' in _kpi_all_r.columns:
                            _nm_r = nettoyer_nom_joueuse(sel_tac_player)
                            _kdf_r = _kpi_all_r[_kpi_all_r['Player'].astype(str).apply(nettoyer_nom_joueuse) == _nm_r]
                            if not _kdf_r.empty:
                                _fig_r = create_individual_radar(_kdf_r.iloc[[0]])
                                if _fig_r is not None:
                                    _tac_radar_b64 = fig_to_b64(_fig_r)
                                    import matplotlib.pyplot as _plt_r
                                    _plt_r.close(_fig_r)
                    except Exception:
                        pass

                    html_report = build_tactical_report_html(
                        df_tactic, sel_tac_player,
                        gps_summary=gps_summary,
                        photo_b64=_tac_photo_b64,
                        match_info=_tac_match_info,
                        pfc_kpi_row=_tac_kpi_row,
                        radar_b64=_tac_radar_b64,
                        gps_match_df=gps_match_df,
                    )
                    _components.html(html_report, height=1120, scrolling=False)

                # Données brutes en expander (optionnel)
                with st.expander("📋 Données tactiques brutes", expanded=False):
                    show_tac_cols = [c for c in ["Row", "Action", "Poste", "Zone Départ action",
                                                  "Mi-temps", "Start time", "Passe", "Tir", "Dribble",
                                                  "Système de Jeu PFC", "Score", "Lieu"] if c in df_tactic.columns]
                    df_disp = df_tactic[show_tac_cols] if show_tac_cols else df_tactic
                    if selected_player and selected_player != "Toutes":
                        _pnorm = normalize_str(selected_player)
                        _mask  = df_tactic["Row"].dropna().apply(
                            lambda x: normalize_str(str(x)) == _pnorm or _pnorm in normalize_str(str(x)))
                        df_disp = df_tactic[_mask][show_tac_cols] if show_tac_cols else df_tactic[_mask]
                    st.dataframe(df_disp, use_container_width=True)

    if is_admin and DRIVE_GPS_MATCH_FOLDER_ID:
        st.divider()
        if st.button("🔄 Synchroniser GPS Match depuis Drive", key="sync_gps_match_btn"):
            with st.spinner("Synchronisation…"):
                ok, fail = sync_gps_match_from_drive()
                st.cache_data.clear()
                st.success(f"✅ {ok} fichier(s) OK — {fail} échec(s)")
                st.rerun()


def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
    st.sidebar.markdown(
        "<div style='display:flex;flex-direction:column;align-items:center;padding:24px 0 16px 0;border-bottom:1px solid rgba(0,163,224,0.15);margin-bottom:8px;'>"
        "<img src='https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png' style='width:90px;height:90px;object-fit:contain;'>"
        "<div style='font-family:Oswald,sans-serif;font-size:9px;font-weight:500;letter-spacing:0.18em;text-transform:uppercase;color:#6A8090;margin-top:10px;'>Centre de Formation F&eacute;minin</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    player_name = get_player_for_profile(user_profile, permissions)
    st.sidebar.markdown(
        f"<div style='font-family:Oswald,sans-serif;font-size:11px;font-weight:500;letter-spacing:0.14em;"
        f"text-transform:uppercase;color:#6A8090;padding:0 12px 2px 12px;'>Connecté</div>"
        f"<div style='font-family:Oswald,sans-serif;font-size:20px;font-weight:700;letter-spacing:0.06em;"
        f"text-transform:uppercase;color:#FFFFFF;padding:0 12px 12px 12px;'>{user_profile}</div>",
        unsafe_allow_html=True,
    )

    if player_name:
        st.sidebar.write(f"Joueuse associée : {player_name}")

    saison_options = ["Toutes les saisons", "2425", "2526"]
    selected_saison = st.sidebar.selectbox("Saison", saison_options)

    if st.sidebar.button("🔒 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user_profile = None
        st.rerun()

    if check_permission(user_profile, "update_data", permissions) or check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Mettre à jour la base"):
            st.session_state["_sync_done"] = False  # forcer re-sync
            st.cache_data.clear()
            st.rerun()

        if st.sidebar.button("🖼️ Reconvertir les photos"):
            with st.spinner("Reconversion en JPEG..."):
                ok, fail, errs = reconvert_photos_to_jpeg()
                new_idx = build_photos_index_local()
                st.session_state["photos_index"] = new_idx
                ref_path = os.path.join(DATA_FOLDER, REFERENTIEL_FILENAME)
                st.session_state["photo_concordance"] = build_photo_concordance(ref_path, new_idx)
            if fail == 0:
                st.sidebar.success(f"✅ {ok} photo(s) converties en JPEG")
            else:
                st.sidebar.warning(f"✅ {ok} OK — ⚠️ {fail} échec(s) : {', '.join(errs[:3])}")
            st.rerun()

    # Filtrer par saison si nécessaire (collect_data déjà appelé dans main())
    if selected_saison != "Toutes les saisons":
        pfc_kpi, edf_kpi, _gps, _gpsw, _gps_match, _nr = collect_data(selected_saison)
        st.session_state["name_report_df"] = _nr
        st.session_state["gps_raw_df"] = _gps
        st.session_state["gps_weekly_df"] = _gpsw
        st.session_state["gps_match_df"] = _gps_match
    # else : on garde pfc_kpi/edf_kpi passés en paramètre (déjà calculés)

    # Badge d'avertissements — affiché dans sidebar APRÈS collect_data
    _sys_warns = st.session_state.get("_system_warnings", [])
    if _sys_warns:
        n = len(_sys_warns)
        with st.sidebar.expander(f"⚠️ {n} avertissement{'s' if n > 1 else ''}", expanded=False):
            for w in _sys_warns:
                st.caption(f"• {w}")

    # Toujours garder une copie non-filtrée pour l'export et les comparaisons
    if "pfc_kpi_all" not in st.session_state or selected_saison != st.session_state.get("_last_saison"):
        pfc_kpi_all = pfc_kpi.copy() if isinstance(pfc_kpi, pd.DataFrame) else pd.DataFrame()
        edf_kpi_all = edf_kpi.copy() if isinstance(edf_kpi, pd.DataFrame) else pd.DataFrame()
        st.session_state["pfc_kpi_all"] = pfc_kpi_all
        st.session_state["edf_kpi_all"] = edf_kpi_all
        st.session_state["_last_saison"] = selected_saison
    else:
        pfc_kpi_all = st.session_state["pfc_kpi_all"]
        edf_kpi_all = st.session_state["edf_kpi_all"]

    if player_name and pfc_kpi is not None and not pfc_kpi.empty and "Player" in pfc_kpi.columns:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)

    # =========================
    # EXPORT EXCEL
    # =========================
    export_is_admin = check_permission(user_profile, "all", permissions)
    export_pfc = pfc_kpi_all if export_is_admin else pfc_kpi
    export_edf = edf_kpi_all if export_is_admin else edf_kpi
    export_gps_week = st.session_state.get("gps_weekly_df", pd.DataFrame())
    export_gps_raw = st.session_state.get("gps_raw_df", pd.DataFrame())
    export_names_report = st.session_state.get("name_report_df", pd.DataFrame())

    with st.sidebar.expander("📤 Export Excel", expanded=False):
        scope_label = "Toute la base" if export_is_admin else "Données (selon profil/filtres)"
        st.caption(f"Contenu : {scope_label}")

        export_season = st.selectbox(
            "Filtrer l'export par saison",
            ["Toutes les saisons", "2425", "2526"],
            index=0,
            key="export_season_select",
        )

        base_pfc = export_pfc.copy()

        if export_season != "Toutes les saisons" and "Saison" in base_pfc.columns:
            base_pfc = base_pfc[base_pfc["Saison"].astype(str) == export_season].copy()

        base_pfc_detail = denormalize_match_rows_from_per90(base_pfc)
        global_players = aggregate_global_players(base_pfc)

        if st.button("Générer le fichier Excel", key="btn_generate_export_xlsx"):
            sheets = {
                "PFC_Detail": base_pfc_detail,
                "PFC_Global_Joueuses": global_players,
                "EDF_Referentiel": export_edf,
                "GPS_Hebdo": export_gps_week,
                "GPS_Brut": export_gps_raw,
                "Noms_Mapping_Report": export_names_report,
            }
            st.session_state["export_xlsx_bytes"] = build_excel_bytes(sheets)

        if st.session_state.get("export_xlsx_bytes"):
            season_tag = "all" if export_season == "Toutes les saisons" else export_season
            fname = f"parisfc_export_{season_tag}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.download_button(
                "⬇️ Télécharger l'Excel",
                data=st.session_state["export_xlsx_bytes"],
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_export_xlsx",
            )

    options = ["Rapports de matchs", "Comparaison", "Données Physiques", "Joueuses Passerelles", "Médical", "Recrutement"]
    if check_permission(user_profile, "all", permissions):
        options.insert(2, "Gestion")

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=options,
            icons=["graph-up-arrow", "people", "gear", "activity", "people-fill", "heart-pulse", "search"][: len(options)],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={
                "container": {"padding": "6px 4px !important", "background-color": "transparent"},
                "icon": {"color": "#00A3E0", "font-size": "16px"},
                "nav-link": {
                    "font-size": "13px", "font-family": "Oswald, sans-serif",
                    "font-weight": "500", "letter-spacing": "0.07em", "text-transform": "uppercase",
                    "text-align": "left", "margin": "1px 6px", "--hover-color": "rgba(0,163,224,0.12)",
                    "border-radius": "3px", "color": "#C8D8E8",
                },
                "nav-link-selected": {
                    "background-color": "#00A3E0",
                    "color": "#08090D", "font-weight": "600",
                },
            },
        )

    # =====================
    # RAPPORTS DE MATCHS
    # =====================
    if page == "Rapports de matchs":
        st.header("📋 Rapports de matchs")

        if pfc_kpi is None or pfc_kpi.empty:
            st.warning("Aucune donnée disponible.")
            return

        if player_name:
            st.subheader(f"Stats pour {player_name}")
            df_player = pfc_kpi
        else:
            player_sel = st.selectbox("Choisissez une joueuse", pfc_kpi["Player"].unique())
            df_player = pfc_kpi[pfc_kpi["Player"] == player_sel].copy()

        if df_player.empty:
            st.warning("Aucune donnée pour cette joueuse.")
            return

        if "Adversaire" in df_player.columns:
            matches = df_player["Adversaire"].unique()
            game = st.multiselect("Choisissez un ou plusieurs matchs", matches)
            filtered = df_player[df_player["Adversaire"].isin(game)] if game else df_player
            # --- GPS du match sélectionné (si 1 seul match) ---
            if game and len(game) == 1:
                try:
                    gps_raw_df = st.session_state.get("gps_raw_df", pd.DataFrame())
                    if gps_raw_df is not None and not gps_raw_df.empty:
                        gps_raw_df = ensure_date_column(gps_raw_df)

                        # Date du match depuis les données technico-tactiques
                        _row = df_player[df_player["Adversaire"].astype(str) == str(game[0])].head(1)
                        match_date_val = _row["Date"].iloc[0] if (not _row.empty and "Date" in _row.columns) else None
                        match_dt = pd.to_datetime(match_date_val, errors="coerce", dayfirst=True)

                        if pd.notna(match_dt):
                            match_dt = pd.Timestamp(match_dt).normalize()
                            gm = gps_raw_df[gps_raw_df["DATE"].dt.normalize() == match_dt].copy()

                            # Filtre joueuse (concordance robuste)
                            sel_clean = nettoyer_nom_joueuse(player_sel)
                            if "Player" in gm.columns:
                                gm = gm[gm["Player"].astype(str) == sel_clean].copy()

                            if not gm.empty:
                                st.markdown("#### 🛰️ Données physiques (GPS) — match sélectionné")
                                cols_pref = [
                                    "Durée_min",
                                    "Distance (m)",
                                    "Distance HID (>13 km/h)",
                                    "Distance HID (>19 km/h)",
                                    "Distance 13-19 (m)",
                                    "Distance 19-23 (m)",
                                    "Distance >23 (m)",
                                    "Sprints_23",
                                    "Sprints_25",
                                    "Vitesse max (km/h)",
                                    "# Acc/Dec",
                                    "Distance relative (m/min)",
                                    "CHARGE",
                                    "RPE",
                                ]
                                show_cols = [c for c in cols_pref if c in gm.columns]

                                # Plusieurs lignes le même jour -> on agrège
                                agg = {}
                                for c in show_cols:
                                    cl = c.lower()
                                    if "vitesse" in cl:
                                        agg[c] = "max"
                                    elif "rpe" in cl:
                                        agg[c] = "mean"
                                    else:
                                        agg[c] = "sum"

                                gnum = gm[show_cols].apply(pd.to_numeric, errors="coerce") if show_cols else pd.DataFrame()
                                gsum = gnum.agg(agg) if not gnum.empty else pd.Series(dtype=float)

                                c1, c2, c3, c4 = st.columns(4)
                                if "Distance (m)" in gsum.index:
                                    c1.metric("Distance", f"{gsum['Distance (m)']:.0f} m" if pd.notna(gsum['Distance (m)']) else "—")
                                if "Distance HID (>13 km/h)" in gsum.index:
                                    c2.metric("HID >13", f"{gsum['Distance HID (>13 km/h)']:.0f} m" if pd.notna(gsum['Distance HID (>13 km/h)']) else "—")
                                if "Distance HID (>19 km/h)" in gsum.index:
                                    c3.metric("HID >19", f"{gsum['Distance HID (>19 km/h)']:.0f} m" if pd.notna(gsum['Distance HID (>19 km/h)']) else "—")
                                if "Vitesse max (km/h)" in gsum.index:
                                    c4.metric("Vmax", f"{gsum['Vitesse max (km/h)']:.1f} km/h" if pd.notna(gsum['Vitesse max (km/h)']) else "—")

                                with st.expander("Voir le détail GPS (lignes sources)"):
                                    cols_show = ["DATE"] + (["__source_file"] if "__source_file" in gm.columns else []) + show_cols
                                    st.dataframe(gm.sort_values("DATE", ascending=False)[cols_show], use_container_width=True)
                            else:
                                st.info("GPS: aucune ligne trouvée pour ce match (date) et cette joueuse.")
                        else:
                            st.info("GPS: date de match non exploitable.")
                except Exception as _e:
                    st.warning(f"GPS: impossible d'afficher les données du match sélectionné → {_e}")

        else:
            filtered = df_player

        if filtered.empty:
            st.warning("Aucune donnée pour cette sélection.")
            return

        aggregated = (
            filtered.groupby("Player")
            .agg({"Temps de jeu (en minutes)": "sum", "Buts": "sum"})
            .join(
                filtered.groupby("Player")
                .mean(numeric_only=True)
                .drop(columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore")
            )
            .reset_index()
        )
        aggregated = safe_int_numeric_only(aggregated)

        c1, c2 = st.columns(2)
        with c1:
            st.metric("Temps de jeu", f"{int(aggregated['Temps de jeu (en minutes)'].iloc[0])} minutes")
        with c2:
            st.metric("Buts", f"{int(aggregated['Buts'].iloc[0])}")

        tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
        with tab1:
            fig = create_individual_radar(aggregated)
            if fig:
                st.pyplot(fig, use_container_width=True)
                plt.close(fig)  # libère la mémoire

        with tab2:
            kpi_order = [
                ("Rigueur", "Rigueur"),
                ("Récupération", "Récupération"),
                ("Distribution", "Distribution"),
                ("Percussion", "Percussion"),
                ("Finition", "Finition"),
                ("Créativité", "Créativité"),
            ]
            available_kpis = [(label, col) for (label, col) in kpi_order if col in aggregated.columns]
            if available_kpis:
                cols = st.columns(len(available_kpis))
                for col_ui, (label, colname) in zip(cols, available_kpis):
                    with col_ui:
                        st.metric(label, f"{int(aggregated[colname].iloc[0])}/100")
            else:
                st.info("KPIs non disponibles sur cette sélection.")

        with tab3:
            poste_order = [
                ("DC", "Défenseur central"),
                ("DL", "Défenseur latéral"),
                ("MD", "Milieu défensif"),
                ("MR", "Milieu relayeur"),
                ("MO", "Milieu offensif"),
                ("ATT", "Attaquant"),
            ]
            if all(colname in aggregated.columns for _, colname in poste_order):
                cols = st.columns(len(poste_order))
                for col_ui, (label, colname) in zip(cols, poste_order):
                    with col_ui:
                        st.metric(label, f"{int(aggregated[colname].iloc[0])}/100")
            else:
                st.info("Notes de poste non disponibles sur cette sélection.")

        # ══════════════════════════════════════════════════════════════════
        # RAPPORT TECHNICO-TACTIQUE — en bas de page
        # ══════════════════════════════════════════════════════════════════
        st.divider()
        st.markdown("## 🎯 Rapport Technico-Tactique de match")

        _tac_files_stat = load_tactical_files()
        if not _tac_files_stat:
            st.info("Aucun fichier tactique trouvé dans le dossier `data/`.")
        else:
            import streamlit.components.v1 as _components_stat

            # ── Sélecteurs match + joueuse ─────────────────────────────
            _match_rows_stat = []
            for _tac in _tac_files_stat:
                _tl_str = ""
                if _tac.get("df") is not None and "Timeline" in _tac["df"].columns:
                    _tl_vals = _tac["df"]["Timeline"].dropna()
                    _tl_str = str(_tl_vals.iloc[0]) if not _tl_vals.empty else ""

                _saison = _tac.get("saison", "")
                _saison_c = re.sub(r"20(\d{2})/20(\d{2})", r"\1/\2", _saison) if _saison else ""
                _comp    = _tac.get("competition", "")
                _jrnee   = _tac.get("journee", "")
                _adv     = _tac.get("adversaire", "")
                _parts   = [p for p in [_saison_c, _comp, f"J{_jrnee}" if _jrnee else "", _adv] if p]
                _disp    = " · ".join(_parts) if _parts else _tac.get("filename", "")

                _match_rows_stat.append({
                    "display": _disp,
                    "date":    _tac.get("date"),
                    "adversaire": _adv,
                    "journee": _jrnee,
                    "saison":  _saison,
                    "tac_obj": _tac,
                    "gps_label": "",
                })

            _match_rows_stat.sort(
                key=lambda m: m["date"] if m["date"] is not None and pd.notna(m["date"]) else pd.Timestamp("1970-01-01"),
                reverse=True
            )

            _col_match, _col_player = st.columns([3, 2])
            with _col_match:
                _sel_disp_stat = st.selectbox(
                    "Match", [m["display"] for m in _match_rows_stat],
                    key="stat_tac_match_sel"
                )
            _sel_row_stat = next((m for m in _match_rows_stat if m["display"] == _sel_disp_stat), _match_rows_stat[0])
            _df_tac_stat  = _sel_row_stat["tac_obj"].get("df")

            if _df_tac_stat is not None:
                _skip_stat = {"START", "PFC", "HAC", ""}
                _tac_players_stat = [
                    r for r in _df_tac_stat["Row"].dropna().unique()
                    if r not in _skip_stat and not any(k in str(r) for k in ["Transition", "Carton", "def "])
                ] if "Row" in _df_tac_stat.columns else []

                with _col_player:
                    _def_idx = 0
                    if player_name and player_name != "Toutes":
                        _pnorm = normalize_str(player_name)
                        for _ii, _pp in enumerate(_tac_players_stat):
                            if normalize_str(_pp) == _pnorm or _pnorm in normalize_str(_pp):
                                _def_idx = _ii; break
                    _sel_player_stat = st.selectbox(
                        "Joueuse", _tac_players_stat,
                        index=_def_idx, key="stat_tac_player_sel"
                    ) if _tac_players_stat else None

                if _sel_player_stat:
                    # GPS summary
                    _gps_df_stat = st.session_state.get("gps_match_df", pd.DataFrame())

                    # Bouton sync GPS match si admin et données vides/incorrectes
                    if check_permission(user_profile, "all", permissions) or check_permission(user_profile, "update_data", permissions):
                        _col_sync, _col_info = st.columns([2, 3])
                        with _col_sync:
                            if st.button("🔄 Actualiser données GPS Match", key="stat_gps_sync_btn"):
                                with st.spinner("Synchronisation GPS Match depuis Drive…"):
                                    try:
                                        sync_gps_match_from_drive()
                                        st.cache_data.clear()
                                        st.session_state["gps_match_df"] = load_gps_match(
                                            *[st.session_state.get(k) for k in
                                              ["ref_set","alias_to_canon","tokenkey_to_canon",
                                               "compact_to_canon","first_to_canons","last_to_canons"]]
                                        ) if all(st.session_state.get(k) is not None for k in
                                                 ["ref_set","alias_to_canon","tokenkey_to_canon",
                                                  "compact_to_canon","first_to_canons","last_to_canons"]) else pd.DataFrame()
                                        st.success("✅ GPS Match mis à jour")
                                        st.rerun()
                                    except Exception as _e:
                                        st.error(f"Erreur sync GPS: {_e}")
                        with _col_info:
                            _n_gps = len(_gps_df_stat) if not _gps_df_stat.empty else 0
                            _n_files = _gps_df_stat["__source_file"].nunique() if not _gps_df_stat.empty and "__source_file" in _gps_df_stat.columns else 0
                            st.caption(f"📊 GPS chargé : {_n_gps} lignes · {_n_files} fichier(s)")

                    _gps_sum_stat = get_gps_match_summary_for_player(
                        _gps_df_stat, _sel_player_stat,
                        match_date=pd.to_datetime(_sel_row_stat.get("date"), errors="coerce"),
                        match_label=_sel_row_stat.get("gps_label") or _sel_row_stat.get("display", ""),
                    )

                    # Contexte match
                    _ctx_stat = _get_match_context(_df_tac_stat)
                    _minfo_stat = {
                        "adversaire": _sel_row_stat.get("adversaire") or _ctx_stat.get("adversaire",""),
                        "journee":    _sel_row_stat.get("journee") or _ctx_stat.get("journee",""),
                        "saison":     _sel_row_stat.get("saison",""),
                        "score":      f"{_ctx_stat.get('score_pfc','?')} – {_ctx_stat.get('score_adv','?')}",
                        "lieu":       _ctx_stat.get("lieu",""),
                        "competition":_ctx_stat.get("competition",""),
                    }

                    # Photo
                    _photo_b64_stat = ""
                    try:
                        _cc = st.session_state.get("photo_concordance", {})
                        _pi = st.session_state.get("photos_index", {})
                        _pp = find_photo_for_player(_sel_player_stat, concordance=_cc, photos_index=_pi)
                        if _pp and os.path.exists(str(_pp)):
                            import base64 as _b64s
                            _pb = load_photo_bytes(str(_pp))
                            if _pb:
                                _photo_b64_stat = "data:image/jpeg;base64," + _b64s.b64encode(_pb).decode()
                    except Exception:
                        pass

                    # KPI row
                    _kpi_row_stat = None
                    try:
                        _kpi_all = st.session_state.get("pfc_kpi_all", pd.DataFrame())
                        if not _kpi_all.empty and "Player" in _kpi_all.columns:
                            _nm = nettoyer_nom_joueuse(_sel_player_stat)
                            _kdf2 = _kpi_all[_kpi_all["Player"].astype(str).apply(nettoyer_nom_joueuse) == _nm]
                            if not _kdf2.empty:
                                _kpi_row_stat = _kdf2.iloc[0]
                    except Exception:
                        pass

                    # Radar du match — même logique que l'onglet Radar
                    _radar_b64_stat = ""
                    try:
                        _kpi_all_s = st.session_state.get("pfc_kpi_all", pd.DataFrame())
                        if not _kpi_all_s.empty and "Player" in _kpi_all_s.columns:
                            _nm_s = nettoyer_nom_joueuse(_sel_player_stat)
                            _kdf_s = _kpi_all_s[_kpi_all_s["Player"].astype(str).apply(nettoyer_nom_joueuse) == _nm_s]
                            if not _kdf_s.empty:
                                _fig_s = create_individual_radar(_kdf_s.iloc[[0]])
                                if _fig_s is not None:
                                    _radar_b64_stat = fig_to_b64(_fig_s)
                                    import matplotlib.pyplot as _plt_s
                                    _plt_s.close(_fig_s)
                    except Exception:
                        pass

                    _html_stat = build_tactical_report_html(
                        _df_tac_stat, _sel_player_stat,
                        gps_summary=_gps_sum_stat,
                        photo_b64=_photo_b64_stat,
                        match_info=_minfo_stat,
                        pfc_kpi_row=_kpi_row_stat,
                        radar_b64=_radar_b64_stat,
                        gps_match_df=st.session_state.get("gps_match_df", pd.DataFrame()),
                    )

                    # ── Bouton Imprimer A4 ──────────────────────────────
                    _print_js = f"""
<script>
function printA4Report() {{
    var w = window.open('', '_blank', 'width=900,height=1200');
    w.document.write(`<!DOCTYPE html><html><head>
    <meta charset="utf-8">
    <title>Rapport Match</title>
    <style>
      @page {{ size: A4 portrait; margin: 0; }}
      @media print {{
        html, body {{ width: 210mm; height: 297mm; margin: 0; padding: 0; }}
        body > * {{ page-break-inside: avoid; }}
      }}
      body {{ margin: 0; padding: 0; background: #060F1A; }}
    </style>
    </head><body>{_html_stat.replace('`', chr(96)).replace('</script>', '<\\/script>')}</body></html>`);
    w.document.close();
    setTimeout(function() {{ w.print(); }}, 800);
}}
</script>
<button onclick="printA4Report()" style="
    display:inline-flex;align-items:center;gap:8px;
    background:#00A3E0;color:#060F1A;border:none;border-radius:4px;
    padding:8px 18px;font-family:Oswald,sans-serif;font-size:13px;
    font-weight:700;letter-spacing:.08em;text-transform:uppercase;
    cursor:pointer;margin-bottom:10px;">
    🖨️ Imprimer / Zoom A4
</button>
"""
                    _components_stat.html(_print_js, height=55)
                    _components_stat.html(_html_stat, height=1120, scrolling=False)

    # =====================
    # COMPARAISON
    # =====================
    elif page == "Comparaison":
        st.header("Comparaison")

        if pfc_kpi is None or pfc_kpi.empty:
            st.warning("Aucune donnée PFC.")
            return

        def _matches_for_player(pname: str):
            if "Adversaire" not in pfc_kpi.columns:
                return []
            d = pfc_kpi[pfc_kpi["Player"].apply(nettoyer_nom_joueuse) == nettoyer_nom_joueuse(pname)].copy()
            if d.empty:
                return []
            return sorted(d["Adversaire"].dropna().unique().tolist())

        def _aggregate_player(pname: str, selected_matches=None):
            return prepare_comparison_data(pfc_kpi, pname, selected_matches=selected_matches)

        mode = st.selectbox(
            "Mode de comparaison",
            [
                "Joueuse vs elle-même (matchs)",
                "Joueuse vs une autre joueuse",
                "Joueuse vs Référentiel EDF U19 (poste)",
            ],
            key="compare_mode_select",
        )

        st.divider()

        if mode == "Joueuse vs elle-même (matchs)":
            if player_name:
                p = player_name
                st.info(f"Joueuse : {p}")
            else:
                p = st.selectbox("Joueuse", sorted(pfc_kpi["Player"].dropna().unique().tolist()), key="self_player")
                show_photo_block(p, location="stats")

            if "Adversaire" not in pfc_kpi.columns:
                st.warning("Colonne 'Adversaire' manquante : impossible de comparer par match.")
                return

            matches = _matches_for_player(p)
            if not matches:
                st.warning("Aucun match trouvé pour cette joueuse.")
                return

            st.write("Sélectionne plusieurs matchs, puis choisis **2 matchs** à comparer en radar.")
            selected_pool = st.multiselect("Matchs disponibles", matches, default=[], key="self_matches_pool")

            if len(selected_pool) < 2:
                st.info("Sélectionne au moins 2 matchs.")
                return

            comp_rows = []
            for mlabel in selected_pool:
                md = pfc_kpi[
                    (pfc_kpi["Player"].apply(nettoyer_nom_joueuse) == nettoyer_nom_joueuse(p))
                    & (pfc_kpi["Adversaire"] == mlabel)
                ].copy()
                if md.empty:
                    continue

                agg = (
                    md.groupby("Player")
                    .agg({"Temps de jeu (en minutes)": "sum", "Buts": "sum"})
                    .join(
                        md.groupby("Player")
                        .mean(numeric_only=True)
                        .drop(columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore")
                    )
                    .reset_index()
                )

                agg = safe_int_numeric_only(agg)
                if not agg.empty:
                    agg["Player"] = f"{p} ({mlabel})"
                    comp_rows.append(agg)

            if len(comp_rows) < 2:
                st.warning("Pas assez de données pour comparer ces matchs.")
                return

            players_data = pd.concat(comp_rows, ignore_index=True)

            with st.expander("Voir le tableau (tous les matchs sélectionnés)"):
                st.dataframe(players_data)

            labels = players_data["Player"].tolist()
            c1, c2 = st.columns(2)
            with c1:
                left = st.selectbox("Match A", labels, index=0, key="self_left_match")
            with c2:
                right = st.selectbox("Match B", [x for x in labels if x != left], index=0, key="self_right_match")

            if st.button("Afficher le radar (Match A vs Match B)", key="btn_self_radar"):
                df2 = players_data[players_data["Player"].isin([left, right])].copy()
                df2 = df2.set_index("Player").loc[[left, right]].reset_index()
                fig = create_comparison_radar(df2, player1_name=left, player2_name=right)
                if fig:
                    st.pyplot(fig, use_container_width=True)
                    plt.close(fig)  # libère la mémoire
                else:
                    st.warning("Radar indisponible (données insuffisantes sur les métriques).")

        elif mode == "Joueuse vs une autre joueuse":
            if player_name:
                p1 = player_name
                st.info(f"Joueuse A (profil) : {p1}")
                p2 = st.selectbox(
                    "Joueuse B",
                    [p for p in sorted(pfc_kpi["Player"].dropna().unique().tolist()) if nettoyer_nom_joueuse(p) != nettoyer_nom_joueuse(p1)],
                    key="p2_other_player",
                )
            else:
                p1 = st.selectbox("Joueuse A", sorted(pfc_kpi["Player"].dropna().unique().tolist()), key="p1_other_player")
                p2 = st.selectbox(
                    "Joueuse B",
                    [p for p in sorted(pfc_kpi["Player"].dropna().unique().tolist()) if nettoyer_nom_joueuse(p) != nettoyer_nom_joueuse(p1)],
                    key="p2_other_player",
                )

            if "Adversaire" in pfc_kpi.columns:
                st.write("Filtres (optionnels) : tu peux limiter les matchs de chaque joueuse.")
                colA, colB = st.columns(2)

                with colA:
                    m1 = _matches_for_player(p1)
                    sel_m1 = st.multiselect("Matchs (Joueuse A)", m1, default=[], key="p1_matches_filter")

                with colB:
                    m2 = _matches_for_player(p2)
                    sel_m2 = st.multiselect("Matchs (Joueuse B)", m2, default=[], key="p2_matches_filter")
            else:
                sel_m1, sel_m2 = None, None

            if st.button("Comparer Joueuse A vs Joueuse B", key="btn_compare_players"):
                d1 = _aggregate_player(p1, selected_matches=sel_m1 if sel_m1 else None)
                d2 = _aggregate_player(p2, selected_matches=sel_m2 if sel_m2 else None)

                if d1.empty or d2.empty:
                    st.warning("Pas assez de données pour afficher la comparaison (vérifie filtres / temps de jeu).")
                    return

                players_data = pd.concat([d1, d2], ignore_index=True)
                fig = create_comparison_radar(players_data, player1_name=p1, player2_name=p2)
                if fig:
                    st.pyplot(fig, use_container_width=True)
                    plt.close(fig)  # libère la mémoire
                else:
                    st.warning("Radar indisponible (données insuffisantes sur les métriques).")

        else:
            if player_name:
                p = player_name
                st.info(f"Joueuse : {p}")
            else:
                p = st.selectbox("Joueuse", sorted(pfc_kpi["Player"].dropna().unique().tolist()), key="edf_player")

            if edf_kpi is None or edf_kpi.empty or "Poste" not in edf_kpi.columns:
                st.warning("Aucune donnée EDF disponible pour la comparaison.")
                return

            postes_display = sorted(edf_kpi["Poste"].dropna().astype(str).unique().tolist())
            poste = st.selectbox("Poste (référentiel EDF)", postes_display, key="edf_poste_ref")

            edf_line = edf_kpi[edf_kpi["Poste"] == poste].copy()
            edf_line = edf_line.rename(columns={"Poste": "Player"})
            edf_label = f"EDF {poste}"

            player_df = prepare_comparison_data(pfc_kpi, p)

            if player_df.empty:
                st.info("Pas assez de données match pour cette joueuse.")
            elif edf_line.empty:
                st.info("Référentiel EDF indisponible pour ce poste.")
            else:
                players_data = pd.concat([player_df, edf_line], ignore_index=True, sort=False)
                fig = create_comparison_radar(players_data, player1_name=p, player2_name=edf_label, exclude_creativity=True)
                if fig:
                    st.pyplot(fig, use_container_width=True)
                    plt.close(fig)  # libère la mémoire
                else:
                    st.warning("Radar indisponible (données insuffisantes).")

    elif page == "Données Physiques":
        st.header("📊 Données Physiques (GPS)")

        gps_raw = st.session_state.get("gps_raw_df", pd.DataFrame())
        gps_weekly = st.session_state.get("gps_weekly_df", pd.DataFrame())

        if gps_raw is None or gps_raw.empty:
            st.warning("Aucune donnée GPS brute trouvée.")
            return

        gps_raw = ensure_date_column(gps_raw)

        all_players = sorted(set(gps_raw.get("Player", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()))
        if not all_players:
            st.warning("Aucune joueuse détectée dans les données GPS.")
            return

        gps_match = st.session_state.get("gps_match_df", pd.DataFrame())

        tab_raw, tab_week, tab_graph, tab_match, tab_charge, tab_concordance = st.tabs(
            ["🧾 Données brutes par joueuse", "📅 Moyennes 7 jours (glissant)", "📈 Graphique MD-6 → MD", "⚽ GPS Match", "⚖️ Suivi de charge", "🔗 Concordance noms"]
        )

        with tab_raw:
            st.subheader("Données brutes (par joueuse)")

            player_sel = player_name if player_name else st.selectbox("Joueuse", all_players, key="gps_raw_player_sel")
            d = gps_raw[gps_raw["Player"].astype(str) == nettoyer_nom_joueuse(player_sel)].copy()
            d = ensure_date_column(d)

            if d.empty:
                st.info("Aucune ligne GPS pour cette joueuse.")
            elif d["DATE"].notna().sum() == 0:
                st.info("Aucune date exploitable pour cette joueuse.")
            else:
                c1, c2 = st.columns(2)
                with c1:
                    min_date = d["DATE"].min().date()
                    max_date = d["DATE"].max().date()
                    date_range = st.date_input(
                        "Période",
                        value=(min_date, max_date),
                        min_value=min_date,
                        max_value=max_date,
                        key="gps_raw_date_range",
                    )
                with c2:
                    if "__source_file" in d.columns:
                        srcs = ["Tous"] + sorted(d["__source_file"].dropna().astype(str).unique().tolist())
                        src_sel = st.selectbox("Fichier source (optionnel)", srcs, key="gps_raw_src_sel")
                    else:
                        src_sel = "Tous"

                if isinstance(date_range, tuple) and len(date_range) == 2:
                    d = d[(d["DATE"] >= pd.Timestamp(date_range[0])) & (d["DATE"] <= pd.Timestamp(date_range[1]))].copy()

                if src_sel != "Tous" and "__source_file" in d.columns:
                    d = d[d["__source_file"].astype(str) == str(src_sel)].copy()

                show_cols = [c for c in [
                    "DATE", "SEMAINE", "Player", "NOM",
                    "Durée", "Durée_min",
                    "Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
                    "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
                    "CHARGE", "RPE",
                    "Sprints_23", "Sprints_25",
                    "Vitesse max (km/h)",
                    "__name_status", "__source_file"
                ] if c in d.columns]

                st.dataframe(d.sort_values("DATE", ascending=False)[show_cols], use_container_width=True)

        with tab_week:
            st.subheader("Moyennes sur une fenêtre glissante de 7 jours")

            player_sel = player_name if player_name else st.selectbox("Joueuse", all_players, key="gps_7d_player_sel")

            tmp = gps_raw[gps_raw["Player"].astype(str) == nettoyer_nom_joueuse(player_sel)].copy()
            tmp = ensure_date_column(tmp)
            tmp = tmp[tmp["DATE"].notna()].copy()

            if tmp.empty:
                st.info("Pas de dates exploitables pour cette joueuse.")
                return

            min_d = tmp["DATE"].min().date()
            max_d = tmp["DATE"].max().date()

            end_date_ui = st.date_input(
                "Date de fin (fenêtre = 7 jours précédents inclus)",
                value=max_d,
                min_value=min_d,
                max_value=max_d,
                key="gps_end_date_7d",
            )

            df_7j, summary = gps_last_7_days_summary(gps_raw, player_sel, end_date=pd.Timestamp(end_date_ui))

            if summary is None or summary.empty:
                st.info("Aucune donnée sur cette fenêtre de 7 jours.")
                return

            st.dataframe(summary, use_container_width=True)

            with st.expander("Voir le détail (lignes brutes sur la période 7 jours)"):
                show_cols = [c for c in [
                    "DATE", "SEMAINE", "Player", "NOM",
                    "Durée", "Durée_min",
                    "Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
                    "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
                    "CHARGE", "RPE",
                    "__name_status", "__source_file"
                ] if c in df_7j.columns]
                st.dataframe(df_7j.sort_values("DATE", ascending=False)[show_cols], use_container_width=True)

            if gps_weekly is not None and not gps_weekly.empty and "SEMAINE" in gps_weekly.columns:
                st.divider()
                st.caption("Vue hebdomadaire (somme par semaine ISO) — optionnelle")
                dw = gps_weekly[gps_weekly["Player"].astype(str) == nettoyer_nom_joueuse(player_sel)].copy()
                if not dw.empty:
                    st.dataframe(dw.sort_values("SEMAINE"), use_container_width=True)

        with tab_graph:
            st.subheader("Graphique microcycle (MD-6 → MD)")

            player_sel_g = player_name if player_name else st.selectbox("Joueuse", all_players, key="gps_graph_player_sel")
            dg = gps_raw[gps_raw["Player"].astype(str) == nettoyer_nom_joueuse(player_sel_g)].copy()
            dg = ensure_date_column(dg)
            dg = dg[dg["DATE"].notna()].copy()

            if dg.empty:
                st.info("Pas de dates exploitables pour cette joueuse.")
                return

            max_date = dg["DATE"].max().normalize()
            min_date = dg["DATE"].min().normalize()

            end_date = st.date_input(
                "Date de référence (MD)",
                value=max_date.date(),
                min_value=min_date.date(),
                max_value=max_date.date(),
                key="gps_md_ref_date",
            )

            summary_md = build_md_window_summary(dg, pd.Timestamp(end_date), days=7)

            if summary_md is None or summary_md.empty:
                st.info("Aucune donnée sur cette fenêtre de 7 jours.")
                return

            st.dataframe(summary_md, use_container_width=True)

            metric_cols = [c for c in summary_md.columns if c != "MD"]
            default_lines = [c for c in [
                "Moyenne de Distance HID (>13 km/h)",
                "Moyenne de Distance 13-19 (m)",
                "Moyenne de Distance 19-23 (m)",
                "Moyenne de Distance >23 (m)",
                "Moyenne de # Acc/Dec",
                "Moyenne de Distance relative (m/min)",
            ] if c in metric_cols]

            selected_lines = st.multiselect(
                "Indicateurs (courbes) affichés (axe droit)",
                options=metric_cols,
                default=default_lines,
                key="gps_md_selected_lines",
            )

            fig = plot_gps_md_graph(summary_md, selected_lines=selected_lines)
            if fig is not None:
                st.pyplot(fig, use_container_width=True)
                plt.close(fig)  # libère la mémoire

        with tab_match:
            _render_gps_match_tab(gps_match, player_name, permissions, user_profile, tactical_files=None)

        # ── SUIVI DE CHARGE ACWR ───────────────────────────────────────────────
        with tab_charge:
            st.subheader("⚖️ Suivi de charge — Ratio Aigu:Chronique (ACWR)")

            player_sel_charge = player_name if player_name else st.selectbox(
                "Joueuse", all_players, key="charge_player_sel"
            )

            weekly_acwr = compute_acwr(gps_raw, player_sel_charge)

            if weekly_acwr.empty:
                st.info(
                    "Pas de données de charge disponibles pour cette joueuse. "
                    "Vérifiez que les colonnes RPE et Durée_min (ou CHARGE) sont présentes dans les fichiers GPS."
                )
            else:
                # Sélecteur de période : 4 dernières semaines par défaut
                n_semaines_total = len(weekly_acwr)
                n_semaines_sel = st.slider(
                    "Nombre de semaines affichées", 4, max(4, n_semaines_total),
                    min(8, n_semaines_total), key="charge_n_semaines"
                )
                weekly_display = weekly_acwr.tail(n_semaines_sel).copy()

                # ── Métriques clés (dernière semaine) ──────────────────────────
                last = weekly_display.iloc[-1]
                st.markdown("##### Dernière semaine enregistrée")
                kc1, kc2, kc3, kc4, kc5 = st.columns(5)

                def _zone_color(acwr):
                    if pd.isna(acwr): return "⚪"
                    if acwr < 0.8:    return "🔵"   # sous-charge
                    if acwr <= 1.5:   return "🟢"   # zone optimale
                    return "🔴"                      # sur-charge / risque blessure

                kc1.metric("Semaine", last["Label_semaine"])
                kc2.metric("Charge semaine", f"{last['CHARGE_semaine']:.0f}")
                kc3.metric(
                    f"ACWR RA {_zone_color(last['ACWR_RA'])}",
                    f"{last['ACWR_RA']:.2f}" if not pd.isna(last['ACWR_RA']) else "—"
                )
                kc4.metric(
                    f"ACWR EWMA {_zone_color(last['ACWR_EWMA'])}",
                    f"{last['ACWR_EWMA']:.2f}" if not pd.isna(last['ACWR_EWMA']) else "—"
                )
                kc5.metric(
                    "Zone",
                    "🟢 Optimale" if not pd.isna(last['ACWR_EWMA']) and 0.8 <= last['ACWR_EWMA'] <= 1.5
                    else ("🔴 Sur-charge" if not pd.isna(last['ACWR_EWMA']) and last['ACWR_EWMA'] > 1.5
                    else "🔵 Sous-charge")
                )

                st.divider()

                # ── Graphique ACWR ─────────────────────────────────────────────
                import matplotlib.pyplot as _plt
                import matplotlib.patches as _mpatches

                labels   = weekly_display["Label_semaine"].tolist()
                acwr_ra  = weekly_display["ACWR_RA"].tolist()
                acwr_ew  = weekly_display["ACWR_EWMA"].tolist()
                charges  = weekly_display["CHARGE_semaine"].tolist()
                x        = list(range(len(labels)))

                fig, ax1 = _plt.subplots(figsize=(max(8, len(x) * 0.9), 5))
                fig.patch.set_facecolor("#0C1220")
                ax1.set_facecolor("#0C1220")

                # Barres de charge (axe gauche)
                ax1.bar(x, charges, color="#1A3A5C", alpha=0.6, label="Charge hebdo", zorder=2)
                ax1.set_ylabel("Charge (UA)", color="#6A8090", fontsize=10)
                ax1.tick_params(axis="y", colors="#6A8090")
                ax1.tick_params(axis="x", colors="#C8D8E8")
                ax1.set_xticks(x)
                ax1.set_xticklabels(labels, rotation=35, ha="right", fontsize=9, color="#C8D8E8")
                ax1.spines[:].set_color("#1A2A3A")

                # ACWR (axe droit)
                ax2 = ax1.twinx()
                ax2.set_facecolor("#0C1220")
                ax2.set_ylabel("ACWR", color="#C8D8E8", fontsize=10)
                ax2.tick_params(axis="y", colors="#C8D8E8")
                ax2.spines[:].set_color("#1A2A3A")

                # Zone optimale
                ax2.axhspan(0.8, 1.5, color="#22C55E", alpha=0.08, zorder=1)
                ax2.axhline(0.8, color="#22C55E", lw=0.8, ls="--", alpha=0.5)
                ax2.axhline(1.5, color="#EF4444", lw=0.8, ls="--", alpha=0.5)

                # Courbes ACWR
                ax2.plot(x, acwr_ra, color="#F4830A", lw=2, marker="o", ms=6,
                         label="ACWR Rolling Avg (Gabbett)", zorder=5)
                ax2.plot(x, acwr_ew, color="#00A3E0", lw=2, marker="s", ms=6,
                         label="ACWR EWMA (Murray et al.)", zorder=5)

                # Colorier les points selon la zone
                for xi, (ra, ew) in enumerate(zip(acwr_ra, acwr_ew)):
                    for val, col in [(ra, "#F4830A"), (ew, "#00A3E0")]:
                        if not pd.isna(val):
                            fc = "#22C55E" if 0.8 <= val <= 1.5 else ("#EF4444" if val > 1.5 else "#3B82F6")
                            ax2.scatter(xi, val, color=fc, edgecolors=col, s=60, zorder=6, linewidths=1.5)

                # Limites axe ACWR
                all_vals = [v for v in acwr_ra + acwr_ew if not pd.isna(v)]
                if all_vals:
                    ymin = max(0, min(all_vals) - 0.3)
                    ymax = max(all_vals) + 0.3
                    ax2.set_ylim(ymin, max(ymax, 1.8))

                # Légende
                handles = [
                    _mpatches.Patch(color="#1A3A5C", alpha=0.8, label="Charge hebdo"),
                    _plt.Line2D([0], [0], color="#F4830A", lw=2, marker="o", ms=6, label="ACWR RA (Gabbett)"),
                    _plt.Line2D([0], [0], color="#00A3E0", lw=2, marker="s", ms=6, label="ACWR EWMA (Murray)"),
                    _mpatches.Patch(color="#22C55E", alpha=0.15, label="Zone optimale (0.8–1.5)"),
                ]
                ax2.legend(handles=handles, loc="upper left", fontsize=8,
                           facecolor="#0C1220", edgecolor="#1A2A3A", labelcolor="#C8D8E8")

                fig.tight_layout()
                st.pyplot(fig)
                _plt.close(fig)

                st.caption(
                    "🔵 Sous-charge (<0.8) · 🟢 Zone optimale (0.8–1.5) · 🔴 Sur-charge / risque blessure (>1.5)  |  "
                    "**RA** = Rolling Average (Gabbett, 2016) · **EWMA** = Exponentially Weighted Moving Average (Murray et al., 2016)"
                )

                st.divider()

                # ── Tableau détaillé ───────────────────────────────────────────
                st.markdown("##### Tableau détaillé")
                tbl = weekly_display[[
                    "Label_semaine", "CHARGE_semaine",
                    "Aigu_RA", "Chronique_RA", "ACWR_RA",
                    "Aigu_EWMA", "Chronique_EWMA", "ACWR_EWMA"
                ]].copy()
                tbl.columns = [
                    "Semaine", "Charge (UA)",
                    "Aigu RA", "Chronique RA", "ACWR RA",
                    "Aigu EWMA", "Chronique EWMA", "ACWR EWMA"
                ]
                for c in ["Charge (UA)", "Aigu RA", "Chronique RA", "Aigu EWMA", "Chronique EWMA"]:
                    tbl[c] = tbl[c].apply(lambda v: f"{v:.1f}" if not pd.isna(v) else "—")
                for c in ["ACWR RA", "ACWR EWMA"]:
                    tbl[c] = tbl[c].apply(lambda v: f"{v:.2f}" if not pd.isna(v) else "—")

                st.dataframe(tbl.reset_index(drop=True), use_container_width=True, hide_index=True)

                st.info(
                    "**Interprétation** : La charge utilisée est le produit RPE × Durée (Unités Arbitraires). "
                    "L'ACWR EWMA pondère les charges récentes plus fortement que les charges anciennes, "
                    "le rendant plus sensible aux pics de charge et donc plus adapté aux calendriers irréguliers."
                )

        # ── CONCORDANCE GPS NOMS ────────────────────────────────────────────────
        with tab_concordance:
            # Récupérer les joueuses tactiques disponibles depuis toutes sources
            _tac_players_all = []
            for _ss_key in ["kpi_df", "pfc_kpi_df"]:
                _kdf = st.session_state.get(_ss_key)
                if _kdf is not None and not getattr(_kdf, "empty", True) and "Player" in _kdf.columns:
                    _tac_players_all += _kdf["Player"].dropna().astype(str).unique().tolist()
            # Compléter avec les noms GPS déjà mappés (pour ne pas perdre des associations)
            _gps_match_df = st.session_state.get("gps_match_df", pd.DataFrame())
            if _tac_players_all:
                _tac_players_all = sorted(set(_tac_players_all))
            else:
                st.info("Charge d'abord des données tactiques pour disposer de la liste des joueuses.")
            render_gps_concordance_ui(_gps_match_df, _tac_players_all)


    elif page == "Joueuses Passerelles":
        st.header("🔄 Joueuses Passerelles")

        passerelle_data = load_passerelle_data()
        if not passerelle_data:
            st.warning("Aucune donnée passerelle.")
            return

        pfc_source = pfc_kpi_all if isinstance(pfc_kpi_all, pd.DataFrame) and not pfc_kpi_all.empty else pfc_kpi
        edf_source = edf_kpi_all if isinstance(edf_kpi_all, pd.DataFrame) and not edf_kpi_all.empty else edf_kpi

        selected = st.selectbox("Sélectionnez une joueuse", list(passerelle_data.keys()), key="passerelle_player_sel")

        # Récupérer index + concordance depuis la session (construits dans collect_data)
        photos_index = st.session_state.get("photos_index") or build_photos_index_local()
        concordance  = st.session_state.get("photo_concordance") or get_photo_concordance()

        # Chercher la photo (mapping manuel en priorité absolue, puis concordance, puis fuzzy)
        photo_path = find_photo_for_player(selected, concordance=concordance, photos_index=photos_index)
        canon_selected = normalize_name_raw(selected)

        selected_clean = nettoyer_nom_joueuse(selected)
        info = passerelle_data[selected]

        def _resolve_best_player_name(pass_key: str, pass_info: dict, candidates: list) -> str:
            if not candidates:
                return pass_key

            nom = str(pass_info.get("Nom", "") or pass_key).strip()
            prenom = str(pass_info.get("Prénom", "")).strip()
            full = f"{nom} {prenom}".strip()

            try:
                base = normalize_str(full) if full else normalize_str(pass_key)
            except Exception:
                base = (full or pass_key).lower()

            norm_map = {}
            for c in candidates:
                try:
                    norm_map[c] = normalize_str(str(c))
                except Exception:
                    norm_map[c] = str(c).lower()

            for c, cn in norm_map.items():
                if cn == base:
                    return c

            for c, cn in norm_map.items():
                if base and base in cn:
                    return c

            best_norm = get_close_matches(base, list(norm_map.values()), n=1, cutoff=0.55)
            if best_norm:
                inv = {v: k for k, v in norm_map.items()}
                return inv.get(best_norm[0], pass_key)

            return pass_key

        stats_candidates = []
        if isinstance(pfc_source, pd.DataFrame) and not pfc_source.empty:
            for col in ["Joueuse", "Player", "Nom", "NOM", "Joueur"]:
                if col in pfc_source.columns:
                    stats_candidates = sorted(pfc_source[col].dropna().astype(str).unique().tolist())
                    break

        gps_candidates = []
        gps_raw_all = st.session_state.get("gps_raw_df", pd.DataFrame())
        if isinstance(gps_raw_all, pd.DataFrame) and not gps_raw_all.empty and "Player" in gps_raw_all.columns:
            gps_candidates = sorted(gps_raw_all["Player"].dropna().astype(str).unique().tolist())

        candidates = sorted(set(stats_candidates + gps_candidates))
        resolved_player = _resolve_best_player_name(selected, info, candidates)

        # ── Carte Identité — design fiche joueur ────────────────────────
        # Préparer les données d'affichage
        prenom_val  = info.get("Prénom", "") or ""
        nom_val     = info.get("Nom", selected) or selected
        poste1_val  = info.get("Poste 1", "") or ""
        poste2_val  = info.get("Poste 2", "") or ""
        pied_val    = info.get("Pied Fort", "") or ""
        taille_val  = info.get("Taille", "") or ""
        ddn_val = info.get("Date de naissance", "") or ""  # déjà nettoyé dans load_passerelle_data

        # DEBUG TEMPORAIRE — à retirer après validation
        with st.expander("🔍 Debug date de naissance", expanded=False):
            st.write(f"**Clé sélectionnée :** `{selected}`")
            st.write(f"**Contenu brut de info :** `{info}`")
            st.write(f"**ddn_val final :** `{repr(ddn_val)}`")
            all_keys = list(passerelle_data.keys())[:10]
            st.write(f"**10 premières clés du dict :** {all_keys}")

        if str(taille_val).lower() in ("nan", "none", ""): taille_val = ""
        if str(poste1_val).lower() in ("nan", "none", ""): poste1_val = ""
        if str(poste2_val).lower() in ("nan", "none", ""): poste2_val = ""
        if str(pied_val).lower()   in ("nan", "none", ""): pied_val   = ""

        # Photo bytes pour base64
        photo_b64 = ""
        photo_src = photo_path if (photo_path and os.path.exists(photo_path)) else None
        if photo_src is None and info.get("Photo"):
            photo_src = info["Photo"]
        if photo_src and os.path.exists(str(photo_src)):
            _cache_key = f"_photo_b64_{photo_src}"
            if _cache_key in st.session_state:
                photo_b64 = st.session_state[_cache_key]
            else:
                _pb = load_photo_bytes(photo_src)
                if _pb:
                    import base64 as _b64
                    photo_b64 = "data:image/jpeg;base64," + _b64.b64encode(_pb).decode()
                    st.session_state[_cache_key] = photo_b64

        # Badges postes
        def _badge(txt, color="#00A3E0"):
            return f"<span style='background:transparent;color:{color};padding:3px 10px;border-radius:2px;font-size:11px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;margin-right:8px;border:1px solid {color};font-family:Oswald,sans-serif;'>{txt}</span>"

        postes_html = ""
        if poste1_val: postes_html += _badge(poste1_val, "#00A3E0")
        if poste2_val: postes_html += _badge(poste2_val, "#6ECFEF")

        pied_icon  = "🦶 " + pied_val if pied_val else ""
        taille_str = f"📏 {taille_val}" if taille_val else ""
        ddn_str    = f"🎂 {ddn_val}"  if ddn_val    else ""

        details_lines = [x for x in [ddn_str, taille_str, pied_icon] if x]
        details_html = "".join(
            f"<div style='color:#6A8090;font-size:13px;margin-top:7px;font-family:Inter,sans-serif;letter-spacing:0.04em;'>{d}</div>"
            for d in details_lines
        )

        photo_html = (
            f"<img src='{photo_b64}' style='width:160px;height:200px;object-fit:cover;object-position:top;"
            f"border-radius:4px;box-shadow:0 0 0 1px rgba(0,163,224,0.3),0 8px 32px rgba(0,0,0,0.6);display:block;'/>"
            if photo_b64 else
            "<div style='width:160px;height:200px;background:#0C1220;border-radius:4px;"
            "display:flex;align-items:center;justify-content:center;"
            "font-size:56px;border:1px solid rgba(0,163,224,0.2);'>👤</div>"
        )

        card_html = f"""
        <div style='
            background: #0C1220;
            border: 1px solid rgba(0,163,224,0.2);
            border-top: 2px solid #00A3E0;
            border-radius: 4px;
            padding: 28px 32px;
            display: flex;
            gap: 32px;
            align-items: flex-start;
            margin-bottom: 8px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.5);
        '>
            <!-- Photo -->
            <div style='flex-shrink:0;'>
                {photo_html}
            </div>
            <!-- Infos -->
            <div style='flex:1;min-width:0;'>
                <div style='color:#00A3E0;font-family:Oswald,sans-serif;font-size:10px;font-weight:500;letter-spacing:0.20em;text-transform:uppercase;margin-bottom:10px;'>Paris FC — Joueuse Passerelle</div>
                <div style='color:#C8D8E8;font-family:Inter,sans-serif;font-size:26px;font-weight:300;line-height:1;margin-bottom:2px;'>{prenom_val}</div>
                <div style='color:#FFFFFF;font-family:Oswald,sans-serif;font-size:36px;font-weight:700;letter-spacing:0.05em;text-transform:uppercase;line-height:1;margin-bottom:18px;'>{nom_val.upper()}</div>
                <div style='margin-bottom:18px;'>{postes_html}</div>
                <div style='width:32px;height:1px;background:#00A3E0;margin-bottom:14px;opacity:0.5;'></div>
                {details_html}
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)

        # Bouton changer / associer la photo sous la carte
        btn_label = "🔄 Changer la photo" if photo_src else "📷 Associer une photo"
        with st.expander(btn_label, expanded=(photo_src is None)):
            _render_photo_picker(selected, canon_selected, photos_index)

        if resolved_player and resolved_player != selected:
            st.caption(f"↳ Données liées à : **{resolved_player}**")


        st.divider()

        tab_stats, tab_obj, tab_edf, tab_gps, tab_medical = st.tabs(["📈 Statistiques", "🎯 Objectifs", "🆚 Comparaison EDF", "🏃 Données physiques (GPS)", "🏥 Médical"])

        with tab_obj:
            # ── Récupération des objectifs depuis le fichier passerelle ──
            _obj1 = (info.get("Objectif 1", "") or "") if info else ""
            _obj2 = (info.get("Objectif 2", "") or "") if info else ""
            _obj3 = (info.get("Objectif 3", "") or "") if info else ""
            _objectifs = [(f"Objectif {i+1}", o) for i, o in enumerate([_obj1, _obj2, _obj3]) if o.strip()]

            if not _objectifs:
                st.info("Aucun objectif défini pour cette joueuse. Ajoutez les colonnes **Objectif 1**, **Objectif 2**, **Objectif 3** dans le fichier Excel passerelle.")
            else:
                # ── Affichage des objectifs texte ──
                for _onum, _otxt in _objectifs:
                    st.markdown(
                        f"<div style='background:#0C1220; border-left:2px solid #00A3E0; "
                        f"border-radius:2px; padding:12px 16px; margin-bottom:8px; color:#C8D8E8; font-size:15px;'>"
                        f"<span style='color:#00A3E0; font-family:Oswald,sans-serif; font-weight:600; font-size:11px; text-transform:uppercase; "
                        f"letter-spacing:0.12em;'>{_onum}</span><br/><span style='font-size:15px;font-family:Inter,sans-serif;'>{_otxt}</span></div>",
                        unsafe_allow_html=True
                    )

                # ── Évaluations depuis le CSV Google Forms ──
                st.markdown("---")
                st.markdown("**Évaluations du staff**")
                _evals_df = load_objectifs_evaluations()

                if _evals_df.empty:
                    st.caption("📋 Aucune évaluation disponible. Une fois le Google Forms créé, synchronisez le CSV via le bouton ci-dessous.")
                else:
                    _joueur_col = "Joueuse" if "Joueuse" in _evals_df.columns else None
                    _obj_col    = "Objectif évalué" if "Objectif évalué" in _evals_df.columns else None
                    _note_col   = "Note" if "Note" in _evals_df.columns else None

                    if _joueur_col and _obj_col and _note_col:
                        _sel_norm = normalize_str(selected) if selected else ""
                        _mask = _evals_df[_joueur_col].apply(
                            lambda x: _sel_norm in normalize_str(str(x)) or normalize_str(str(x)) in _sel_norm
                        )
                        _player_evals = _evals_df[_mask].copy()

                        if _player_evals.empty:
                            st.caption(f"Aucune évaluation enregistrée pour **{selected}**.")
                        else:
                            _cols_diag = st.columns(min(len(_objectifs), 3))
                            for _ci, (_onum, _otxt) in enumerate(_objectifs):
                                _obj_mask = _player_evals[_obj_col].apply(
                                    lambda x: normalize_str(str(x)) in normalize_str(_otxt)
                                             or normalize_str(_otxt) in normalize_str(str(x))
                                )
                                _obj_evals = _player_evals[_obj_mask][_note_col].dropna()
                                with _cols_diag[_ci % 3]:
                                    if _obj_evals.empty:
                                        st.caption(f"Pas encore d'évaluation pour **{_onum}**.")
                                    else:
                                        _moy = _obj_evals.mean()
                                        _n_evals = len(_obj_evals)
                                        # Jauge matplotlib (demi-cercle)
                                        import numpy as _np
                                        _gfig, _gax = plt.subplots(figsize=(3.5, 2.2), subplot_kw=dict(aspect="equal"))
                                        _gfig.patch.set_facecolor("#08090D")
                                        _gax.set_facecolor("#08090D")
                                        _gfig.set_dpi(90)
                                        # Zones de couleur
                                        for _start, _end, _col in [(0,2,"#3A1010"),(2,3.5,"#3A2A00"),(3.5,5,"#003A50")]:
                                            _th = _np.linspace(_np.pi, _np.pi*(1 - _start/5), 50)
                                            _th2 = _np.linspace(_np.pi*(1 - _start/5), _np.pi*(1 - _end/5), 50)
                                            _th_zone = _np.linspace(_np.pi*(1 - _start/5), _np.pi*(1 - _end/5), 50)
                                            _gax.barh(0, (_end-_start)/5*_np.pi, left=_np.pi*(1-_end/5), height=0.5,
                                                      color=_col, align="center")
                                        # Arc de fond
                                        _theta_bg = _np.linspace(_np.pi, 0, 200)
                                        _gax.plot(_np.cos(_theta_bg)*0.85, _np.sin(_theta_bg)*0.85, color="#1A2A3A", lw=8)
                                        # Arc valeur
                                        _theta_v = _np.linspace(_np.pi, _np.pi*(1 - _moy/5), 200)
                                        _gax.plot(_np.cos(_theta_v)*0.85, _np.sin(_theta_v)*0.85, color="#00A3E0", lw=8)
                                        # Aiguille
                                        _angle = _np.pi*(1 - _moy/5)
                                        _gax.annotate("", xy=(_np.cos(_angle)*0.7, _np.sin(_angle)*0.7),
                                                      xytext=(0,0), arrowprops=dict(arrowstyle="-|>", color="#00A3E0", lw=2))
                                        # Texte valeur
                                        _gax.text(0, -0.15, f"{_moy:.2f}/5", ha="center", va="center",
                                                  fontsize=16, fontweight="bold", color="#00A3E0")
                                        _gax.text(0, -0.45, f"{_onum}", ha="center", va="center",
                                                  fontsize=9, color="#C8D8E8", fontfamily="monospace")
                                        _short_txt = _otxt[:35]+"…" if len(_otxt)>35 else _otxt
                                        _gax.text(0, -0.65, _short_txt, ha="center", va="center",
                                                  fontsize=7, color="#6A8090")
                                        _gax.set_xlim(-1.1, 1.1)
                                        _gax.set_ylim(-0.8, 1.1)
                                        _gax.axis("off")
                                        _gfig.subplots_adjust(top=1, bottom=0, left=0, right=1)
                                        st.pyplot(_gfig, use_container_width=True)
                                        plt.close(_gfig)
                                        st.caption(f"Moyenne sur {_n_evals} évaluation{'s' if _n_evals > 1 else ''}")
                    else:
                        st.caption("⚠️ Format du CSV inattendu — colonnes attendues : Joueuse, Objectif évalué, Note.")

                # Bouton sync admin
                if check_permission(user_profile, "all", permissions):
                    st.markdown("---")
                    if st.button("🔄 Sync évaluations objectifs", key="sync_obj_evals"):
                        _ok, _err = sync_objectifs_from_drive()
                        if _ok:
                            st.success(f"{_ok} fichier(s) synchronisé(s).")
                            st.rerun()
                        elif not DRIVE_OBJECTIFS_FOLDER_ID:
                            st.info("Renseignez **DRIVE_OBJECTIFS_FOLDER_ID** dans le code (ligne ~60) pour activer la synchronisation Drive.")
                        else:
                            st.error("Aucun fichier récupéré. Vérifiez l'ID du dossier Drive.")

        with tab_stats:
            st.subheader("Statistiques joueuse")

            if not isinstance(pfc_source, pd.DataFrame) or pfc_source.empty:
                st.warning("Aucune donnée statistiques PFC disponible.")
            else:
                player_col = None
                for col in ["Joueuse", "Player", "Nom", "NOM", "Joueur"]:
                    if col in pfc_source.columns:
                        player_col = col
                        break

                if player_col is None:
                    st.warning("Impossible d'identifier la colonne 'joueuse' dans les statistiques PFC.")
                else:
                    pfc_player_df = pfc_source[pfc_source[player_col].astype(str) == str(resolved_player)].copy()
                    if pfc_player_df.empty:
                        try:
                            base = normalize_str(str(resolved_player))
                            pfc_player_df = pfc_source[pfc_source[player_col].astype(str).map(lambda x: normalize_str(str(x)) == base)].copy()
                        except Exception:
                            pass

                    if pfc_player_df.empty:
                        st.info("Aucune ligne statistique trouvée pour cette joueuse.")
                    else:
                        try:
                            aggregated = aggregate_player_stats(pfc_player_df)
                        except Exception:
                            num_cols = pfc_player_df.select_dtypes(include="number").columns.tolist()
                            aggregated = pfc_player_df[num_cols].mean(numeric_only=True).to_frame().T
                            aggregated.insert(0, "Joueuse", resolved_player)

                        try:
                            fig = create_individual_radar(aggregated)
                            if fig:
                                st.pyplot(fig, use_container_width=True)
                                plt.close(fig)  # libère la mémoire
                        except Exception as e:
                            st.warning(f"Radar indisponible : {e}")

                        with st.expander("Voir les données agrégées"):
                            st.dataframe(aggregated, use_container_width=True)

        with tab_edf:
            st.subheader("Comparaison avec le référentiel EDF")

            if not isinstance(edf_source, pd.DataFrame) or edf_source.empty:
                st.warning("Aucune donnée EDF disponible.")
            else:
                poste_default = str(info.get("Poste 1", "")).strip()
                postes = []
                poste_col = None
                for col in ["Poste", "POSTE", "Position", "POS"]:
                    if col in edf_source.columns:
                        postes = sorted(edf_source[col].dropna().astype(str).unique().tolist())
                        poste_col = col
                        break

                if poste_col is None or not postes:
                    st.warning("Impossible d'identifier la colonne 'Poste' dans le référentiel EDF.")
                else:
                    idx_default = 0
                    if poste_default:
                        try:
                            m = get_close_matches(poste_default, postes, n=1, cutoff=0.4)
                            if m:
                                idx_default = postes.index(m[0])
                        except Exception:
                            pass

                    poste_sel = st.selectbox("Poste EDF de référence", postes, index=idx_default, key="passerelle_poste_edf_sel")

                    player_df = prepare_comparison_data(pfc_source, resolved_player, selected_matches=None)

                    edf_line = edf_source[edf_source["Poste"] == poste_sel].copy()
                    if player_df is None or player_df.empty:
                        st.info("Pas assez de données match pour cette joueuse.")
                    elif edf_line.empty:
                        st.info("Référentiel EDF indisponible pour ce poste.")
                    else:
                        edf_label = str(poste_sel)
                        edf_line = edf_line.copy()
                        edf_line["Player"] = edf_label
                        if "Poste" in edf_line.columns:
                            edf_line = edf_line.drop(columns=["Poste"])
                        players_data = pd.concat([player_df, edf_line], ignore_index=True, sort=False)

                        fig = create_comparison_radar(
                            players_data,
                            player1_name=str(resolved_player),
                            player2_name=edf_label,
                            exclude_creativity=True,
                        )
                        if fig is not None:
                            st.pyplot(fig, use_container_width=True)
                            plt.close(fig)  # libère la mémoire
                        else:
                            st.info("Impossible de générer le radar de comparaison.")

        with tab_gps:
            st.subheader("Données physiques (GPS)")

            gps_raw = st.session_state.get("gps_raw_df", pd.DataFrame())
            gps_weekly = st.session_state.get("gps_weekly_df", pd.DataFrame())

            if gps_raw is None or gps_raw.empty:
                st.warning("Aucune donnée GPS brute trouvée.")
            else:
                gps_raw = ensure_date_column(gps_raw)

                dgps = gps_raw[gps_raw.get("Player", pd.Series(dtype=str)).astype(str) == str(resolved_player)].copy()
                if dgps.empty:
                    try:
                        base = normalize_str(str(resolved_player))
                        dgps = gps_raw[gps_raw.get("Player", pd.Series(dtype=str)).astype(str).map(lambda x: normalize_str(str(x)) == base)].copy()
                    except Exception:
                        pass

                if dgps.empty:
                    st.info("Aucune ligne GPS pour cette joueuse.")
                else:
                    tab_raw_g, tab_week_g, tab_graph_g = st.tabs(
                        ["🧾 Brutes", "📅 7 jours (glissant)", "📈 Microcycle MD-6 → MD"]
                    )

                    with tab_raw_g:
                        d = ensure_date_column(dgps.copy())

                        c1, c2 = st.columns(2)
                        with c1:
                            if d["DATE"].notna().sum() == 0:
                                st.info("Aucune date exploitable.")
                                date_range = None
                            else:
                                min_date = d["DATE"].min()
                                max_date = d["DATE"].max()
                                default_range = (min_date.date(), max_date.date())
                                date_range = st.date_input("Période", value=default_range, key="passerelle_gps_raw_date_range")

                        with c2:
                            if "__source_file" in d.columns:
                                srcs = ["Tous"] + sorted(d["__source_file"].dropna().astype(str).unique().tolist())
                                src_sel = st.selectbox("Fichier source (optionnel)", srcs, key="passerelle_gps_raw_src_sel")
                            else:
                                src_sel = "Tous"

                        if isinstance(date_range, tuple) and len(date_range) == 2 and date_range[0] and date_range[1]:
                            d = d[(d["DATE"] >= pd.Timestamp(date_range[0])) & (d["DATE"] <= pd.Timestamp(date_range[1]))].copy()
                        if src_sel != "Tous" and "__source_file" in d.columns:
                            d = d[d["__source_file"].astype(str) == str(src_sel)].copy()

                        show_cols = [c for c in [
                            "DATE", "SEMAINE", "Player", "NOM",
                            "Durée", "Durée_min",
                            "Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
                            "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
                            "CHARGE", "RPE",
                            "Sprints_23", "Sprints_25",
                            "Vitesse max (km/h)",
                            "__name_status", "__source_file"
                        ] if c in d.columns]

                        st.dataframe(d.sort_values("DATE", ascending=False)[show_cols], use_container_width=True)

                    with tab_week_g:
                        tmp = dgps.copy()
                        tmp = tmp[tmp["DATE"].notna()].copy()
                        if tmp.empty:
                            st.info("Pas de dates exploitables pour cette joueuse.")
                        else:
                            tmp["DATE"] = pd.to_datetime(tmp["DATE"], errors="coerce")
                            min_d = tmp["DATE"].min().date()
                            max_d = tmp["DATE"].max().date()

                            end_date_ui = st.date_input(
                                "Date de fin (fenêtre = 7 jours précédents inclus)",
                                value=max_d,
                                min_value=min_d,
                                max_value=max_d,
                                key="passerelle_gps_end_date_7d",
                            )

                            df_7j, summary = gps_last_7_days_summary(gps_raw, resolved_player, end_date=pd.Timestamp(end_date_ui))

                            if summary is None or summary.empty:
                                st.info("Aucune donnée sur cette fenêtre de 7 jours.")
                            else:
                                st.dataframe(summary, use_container_width=True)

                                with st.expander("Voir le détail (lignes brutes sur la période 7 jours)"):
                                    show_cols = [c for c in [
                                        "DATE", "SEMAINE", "Player", "NOM",
                                        "Durée", "Durée_min",
                                        "Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
                                        "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
                                        "CHARGE", "RPE",
                                        "__name_status", "__source_file"
                                    ] if c in df_7j.columns]
                                    st.dataframe(df_7j.sort_values("DATE", ascending=False)[show_cols], use_container_width=True)

                                if gps_weekly is not None and not gps_weekly.empty and "SEMAINE" in gps_weekly.columns:
                                    st.divider()
                                    st.caption("Vue hebdomadaire (somme par semaine ISO)")
                                    dw = gps_weekly[gps_weekly["Player"].astype(str) == str(resolved_player)].copy()
                                    if not dw.empty:
                                        st.dataframe(dw.sort_values("SEMAINE"), use_container_width=True)

                    with tab_graph_g:
                        dg = dgps.copy()
                        dg = dg[dg["DATE"].notna()].copy()

                        if dg.empty:
                            st.info("Pas de dates exploitables pour cette joueuse.")
                        else:
                            max_date = dg["DATE"].max().normalize()
                            min_date = dg["DATE"].min().normalize()

                            end_date = st.date_input(
                                "Date de référence (MD)",
                                value=max_date.date(),
                                min_value=min_date.date(),
                                max_value=max_date.date(),
                                key="passerelle_gps_md_ref_date",
                            )

                            summary_md = build_md_window_summary(dg, pd.Timestamp(end_date), days=7)

                            if summary_md is None or summary_md.empty:
                                st.info("Aucune donnée sur cette fenêtre de 7 jours.")
                            else:
                                st.dataframe(summary_md, use_container_width=True)
                                try:
                                    default_lines = [c for c in [
                                        "Moyenne de Distance (m)",
                                        "Moyenne de Distance HID (>13 km/h)",
                                        "Moyenne de Distance 13-19 (m)",
                                        "Moyenne de Distance 19-23 (m)",
                                        "Moyenne de Distance >23 (m)",
                                    ] if c in summary_md.columns]
                                    selected_lines = st.multiselect(
                                        "Courbes affichées (axe droit)",
                                        [c for c in summary_md.columns if c not in ["MD", "MD_num"]],
                                        default=default_lines,
                                        key="passerelle_gps_selected_lines"
                                    )
                                    fig = plot_gps_md_graph(summary_md, selected_lines=selected_lines)
                                    if fig is not None:
                                        st.pyplot(fig, use_container_width=True)
                                        plt.close(fig)  # libère la mémoire
                                except Exception as e:
                                    st.warning(f"Graphique indisponible : {e}")




# =========================
# MAIN
# =========================
def main():
    st.set_page_config(page_title="Paris FC - Centre de Formation Féminin", layout="wide")

    st.markdown(
        """
    <style>
    /* ═══════════════════════════════════════════════
       PARIS FC — CHARTE GRAPHIQUE OFFICIELLE 2025
       Fond noir   : #08090D  |  Bleu ciel : #00A3E0
       Bleu foncé  : #0C1B33  |  Blanc     : #FFFFFF
       Gris texte  : #A8B8C8
    ═══════════════════════════════════════════════ */

    @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600&display=swap');

    :root {
        --pfc-black:  #08090D;
        --pfc-dark:   #0C1220;
        --pfc-navy:   #0C1B33;
        --pfc-blue:   #00A3E0;
        --pfc-blue2:  #007AB8;
        --pfc-white:  #FFFFFF;
        --pfc-text:   #C8D8E8;
        --pfc-muted:  #6A8090;
        --pfc-border: rgba(0, 163, 224, 0.18);
        --pfc-card:   rgba(12, 18, 32, 0.9);
    }

    /* ── Fond général ── */
    .stApp {
        background-color: var(--pfc-black) !important;
        font-family: 'Inter', sans-serif;
        color: var(--pfc-text);
    }
    /* Lueur bleue subtile en haut */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0; left: 0; right: 0;
        height: 400px;
        background: radial-gradient(ellipse 70% 40% at 50% 0%, rgba(0,163,224,0.10) 0%, transparent 80%);
        pointer-events: none;
        z-index: 0;
    }

    /* ── Contenu principal ── */
    .main .block-container {
        background: transparent !important;
        padding: 1.5rem 2.5rem 3rem 2.5rem !important;
        max-width: 1440px;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: var(--pfc-dark) !important;
        border-right: 1px solid var(--pfc-border) !important;
    }
    [data-testid="stSidebar"] * { color: var(--pfc-text) !important; }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        font-family: 'Oswald', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--pfc-white) !important;
    }

    /* ── Boutons ── */
    .stButton > button {
        background: var(--pfc-blue) !important;
        color: var(--pfc-black) !important;
        border: none !important;
        border-radius: 3px !important;
        font-family: 'Oswald', sans-serif !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        letter-spacing: 0.10em !important;
        text-transform: uppercase !important;
        padding: 9px 20px !important;
        transition: all 0.18s ease !important;
    }
    .stButton > button:hover {
        background: #22BBEE !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 16px rgba(0,163,224,0.35) !important;
    }

    /* ── Onglets ── */
    .stTabs [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid var(--pfc-border) !important;
        gap: 0 !important;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        color: var(--pfc-muted) !important;
        font-family: 'Oswald', sans-serif !important;
        font-weight: 500 !important;
        font-size: 14px !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
        padding: 10px 22px !important;
        border-bottom: 2px solid transparent !important;
        transition: color 0.15s !important;
    }
    .stTabs [data-baseweb="tab"]:hover { color: var(--pfc-blue) !important; }
    .stTabs [aria-selected="true"] {
        color: var(--pfc-white) !important;
        border-bottom: 2px solid var(--pfc-blue) !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        background: transparent !important;
        padding-top: 1.8rem !important;
    }

    /* ── Métriques ── */
    [data-testid="stMetric"] {
        background: var(--pfc-card) !important;
        border: 1px solid var(--pfc-border) !important;
        border-top: 2px solid var(--pfc-blue) !important;
        border-radius: 4px !important;
        padding: 18px 22px !important;
    }
    [data-testid="stMetric"] label {
        color: var(--pfc-muted) !important;
        font-family: 'Oswald', sans-serif !important;
        font-size: 11px !important;
        letter-spacing: 0.12em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--pfc-white) !important;
        font-family: 'Oswald', sans-serif !important;
        font-size: 30px !important;
        font-weight: 600 !important;
    }

    /* ── Selectbox / inputs ── */
    .stSelectbox > div > div,
    .stMultiselect > div > div,
    .stTextInput > div > div > input,
    .stDateInput > div > div > input {
        background: var(--pfc-card) !important;
        color: var(--pfc-white) !important;
        border: 1px solid var(--pfc-border) !important;
        border-radius: 3px !important;
    }
    .stSelectbox svg, .stMultiselect svg { fill: var(--pfc-blue) !important; }

    /* ── DataFrames ── */
    .stDataFrame {
        border: 1px solid var(--pfc-border) !important;
        border-radius: 4px !important;
        overflow: hidden !important;
    }
    .stDataFrame table {
        color: var(--pfc-text) !important;
        background: var(--pfc-card) !important;
    }
    .stDataFrame thead th {
        background: rgba(0,163,224,0.12) !important;
        color: var(--pfc-blue) !important;
        font-family: 'Oswald', sans-serif !important;
        font-size: 11px !important;
        letter-spacing: 0.10em !important;
        text-transform: uppercase !important;
        border-bottom: 1px solid var(--pfc-border) !important;
    }
    .stDataFrame tbody tr:hover td {
        background: rgba(0,163,224,0.07) !important;
    }

    /* ── Expanders ── */
    .streamlit-expanderHeader {
        background: var(--pfc-card) !important;
        border: 1px solid var(--pfc-border) !important;
        border-radius: 4px !important;
        color: var(--pfc-text) !important;
        font-family: 'Oswald', sans-serif !important;
        font-weight: 500 !important;
        letter-spacing: 0.06em !important;
        text-transform: uppercase !important;
        font-size: 13px !important;
    }
    .streamlit-expanderContent {
        background: rgba(12,18,32,0.7) !important;
        border: 1px solid var(--pfc-border) !important;
        border-top: none !important;
        border-radius: 0 0 4px 4px !important;
    }

    /* ── Titres ── */
    h1, h2, h3 {
        font-family: 'Oswald', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.05em !important;
        text-transform: uppercase !important;
        color: var(--pfc-white) !important;
    }
    h1 { font-size: 2.2rem !important; }
    h2 { font-size: 1.6rem !important; border-bottom: 1px solid var(--pfc-border); padding-bottom: 8px; }
    h3 { font-size: 1.2rem !important; color: var(--pfc-blue) !important; }

    /* ── Alerts ── */
    .stAlert {
        background: var(--pfc-card) !important;
        border-radius: 3px !important;
        border-left: 3px solid var(--pfc-blue) !important;
        color: var(--pfc-text) !important;
    }

    /* ── Divider ── */
    hr { border-color: var(--pfc-border) !important; margin: 1rem 0 !important; }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--pfc-black); }
    ::-webkit-scrollbar-thumb { background: var(--pfc-blue2); border-radius: 2px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--pfc-blue); }

    /* ── Caption ── */
    .stCaption, [data-testid="stCaptionContainer"] {
        color: var(--pfc-muted) !important;
        font-size: 12px !important;
    }

    /* ── Option menu (sidebar nav) ── */
    .nav-link {
        border-radius: 3px !important;
        margin: 1px 6px !important;
        font-family: 'Oswald', sans-serif !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        letter-spacing: 0.07em !important;
        text-transform: uppercase !important;
        color: var(--pfc-text) !important;
        transition: background 0.12s !important;
    }
    .nav-link:hover { background: rgba(0,163,224,0.12) !important; }
    .nav-link-selected {
        background: var(--pfc-blue) !important;
        color: var(--pfc-black) !important;
        font-weight: 600 !important;
    }
    .nav-link-selected .icon { color: var(--pfc-black) !important; }
    </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "<div style='background:#08090D;border-bottom:1px solid rgba(0,163,224,0.2);padding:1.6rem 2.5rem;margin:-1.5rem -2.5rem 2rem -2.5rem;display:flex;align-items:center;justify-content:center;gap:2rem;position:relative;overflow:hidden;'>"
        "<div style='position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,#00A3E0,transparent);'></div>"
        "<div style='position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:300px;height:200px;background:radial-gradient(circle,rgba(0,163,224,0.08) 0%,transparent 70%);pointer-events:none;'></div>"
        "<img src='https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png' alt='Paris FC' style='width:80px;height:80px;object-fit:contain;flex-shrink:0;position:relative;z-index:1;'>"
        "<div style='position:relative;z-index:1;text-align:center;'>"
        "<div style='font-family:Oswald,sans-serif;font-size:32px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;line-height:1;color:#FFFFFF;'>Centre de Formation F&eacute;minin</div>"
        "<div style='font-family:Inter,sans-serif;font-size:13px;font-weight:300;letter-spacing:0.16em;color:#00A3E0;margin-top:8px;text-transform:uppercase;'>Analyse &amp; Suivi des Joueuses</div>"
        "</div></div>",
        unsafe_allow_html=True,
    )

    permissions = load_permissions()
    if not permissions:
        st.error("Impossible de charger les permissions. Vérifie le fichier de permissions sur Drive.")
        st.stop()

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user_profile" not in st.session_state:
        st.session_state.user_profile = None

    if not st.session_state.authenticated:
        with st.form("login_form"):
            username = st.text_input("Nom d'utilisateur (profil)")
            password = st.text_input("Mot de passe", type="password")
            submitted = st.form_submit_button("Valider")
            if submitted:
                if username in permissions and password == permissions[username]["password"]:
                    st.session_state.authenticated = True
                    st.session_state.user_profile = username
                    st.rerun()
                else:
                    st.error("Nom d'utilisateur ou mot de passe incorrect")
        st.stop()

    # Sync Drive uniquement au premier chargement de la session
    if not st.session_state.get("_sync_done"):
        _run_initial_sync()
        st.cache_data.clear()  # invalider le cache après sync

    with st.spinner("Chargement des données…"):
        pfc_kpi, edf_kpi, gps_raw_df, gps_week_df, gps_match_df, name_report_df = collect_data()
    st.session_state["name_report_df"] = name_report_df
    st.session_state["gps_raw_df"] = gps_raw_df
    st.session_state["gps_weekly_df"] = gps_week_df
    st.session_state["gps_match_df"] = gps_match_df

    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)


if __name__ == "__main__":
    main()
