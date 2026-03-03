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
DRIVE_GPS_MATCH_FOLDER_ID = ""  # À renseigner : ID du dossier Drive GPS Match

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

    if "DATE" in d.columns:
        d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    else:
        dt = parse_date_from_gf1_filename(filename)
        d["DATE"] = pd.Timestamp(dt.date()) if dt else pd.NaT

    d["SEMAINE"] = d["DATE"].dt.isocalendar().week.astype("Int64")
    w_file = parse_week_from_gf1_filename(filename)
    if w_file is not None:
        d["SEMAINE"] = pd.Series([w_file] * len(d), index=d.index, dtype="Int64")

    for c in ["Durée_min", "Distance (m)", "Sprints_23", "Sprints_25", "Vitesse max (km/h)", "Accélération maximale (m/s²)", "#accel/decel"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    def _num(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    v13_15 = _num("Distance par plage de vitesse (13-15 km/h)")
    v15_19 = _num("Distance par plage de vitesse (15-19 km/h)")
    v19_23 = _num("Distance par plage de vitesse (19-23 km/h)")
    v23_25 = _num("Distance par plage de vitesse (23-25 km/h)")
    v_sup25 = _num("Distance par plage de vitesse (>25 km/h)")

    d["Distance 13-19 (m)"] = v13_15 + v15_19
    d["Distance 19-23 (m)"] = v19_23
    d["Distance >23 (m)"] = v23_25 + v_sup25

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
    # Chercher dans le dossier dédié + dossier GPS général
    search_dirs = [GPS_MATCH_FOLDER, GPS_FOLDER, DATA_FOLDER]
    paths = []
    for d in search_dirs:
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
            if minfo["date"] is not None and ("DATE" not in df.columns or df["DATE"].isna().all()):
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
        mapped, statuses = [], []
        for v in result["NOM"].astype(str).tolist():
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
        kwargs_params=dict(color="#C8D8E8", fontsize=11, fontproperties="monospace"),
        kwargs_values=dict(
            color="#FFFFFF",
            fontsize=11,
            bbox=dict(
                edgecolor="#00A3E0",
                facecolor="#0C1220",
                boxstyle="round,pad=0.25",
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
    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_facecolor("#08090D")
    ax.set_facecolor("#08090D")
    ax.set_dpi(90)

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


def _render_gps_match_tab(gps_match: "pd.DataFrame", player_name: str, permissions: dict, user_profile: str):
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
        fig = _make_match_bar_chart(labels, [
            (_col("Distance (m)"),              "Distance totale",  "#1E3A5F"),
            (_col("Distance HID (>13 km/h)"),   "HID >13 km/h",    "rgba(0,163,224,0.7)".replace("rgba(","#").replace(",0.7)","B3") if False else "#4db8e8"),
            (_col("Distance HID (>19 km/h)"),   "HID >19 km/h",    "#00A3E0"),
        ], "Distance (m)", "m")
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

    options = ["Statistiques", "Comparaison", "Données Physiques", "Joueuses Passerelles", "Médical", "Recrutement"]
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
    # STATISTIQUES
    # =====================
    if page == "Statistiques":
        st.header("Statistiques")

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

        tab_raw, tab_week, tab_graph, tab_match = st.tabs(
            ["🧾 Données brutes par joueuse", "📅 Moyennes 7 jours (glissant)", "📈 Graphique MD-6 → MD", "⚽ GPS Match"]
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
            _render_gps_match_tab(gps_match, player_name, permissions, user_profile)

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
