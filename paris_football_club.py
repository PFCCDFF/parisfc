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
from typing import Dict, List, Optional, Set, Tuple
from difflib import get_close_matches, SequenceMatcher
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
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

# Fichiers attendus
PERMISSIONS_FILENAME = "Classeurs permissions streamlit.xlsx"
EDF_JOUEUSES_FILENAME = "EDF_Joueuses.xlsx"
PASSERELLE_FILENAME = "Liste Joueuses Passerelles.xlsx"
REFERENTIEL_FILENAME = "Noms Prénoms Paris FC.xlsx"

# Colonnes "poste" dans les lignes match (lineups)
POST_COLS = ["ATT", "DCD", "DCG", "DD", "DG", "GB", "MCD", "MCG", "MD", "MDef", "MG"]

BAD_TOKENS = {"CORNER", "COUP-FRANC", "COUP FRANC", "PENALTY", "CARTON", "CARTONS"}
GPS_GF1_PREFIX = "GF1"

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


def build_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """
    Construit un fichier Excel en mémoire (bytes) avec une feuille par DataFrame.
    Les noms de feuilles sont tronqués à 31 caractères (limite Excel).
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        used = set()
        for name, df in sheets.items():
            if df is None:
                continue
            sheet = (str(name) or "Sheet1")[:31]
            # éviter doublons de noms de feuilles
            base = sheet
            k = 1
            while sheet in used:
                suffix = f"_{k}"
                sheet = (base[:31 - len(suffix)] + suffix)[:31]
                k += 1
            used.add(sheet)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=sheet, index=False)
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
    # Cas "LOSC, LOSC" => on prend le 1er token
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
    """Extrait une semaine ISO depuis un nom de fichier du type 'GF1 S16 ...'.

    Exemple: 'GF1 S16 séance 66 - 10.11.25.xlsx' -> 16
    """
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
    """Extrait une saison type '2425' / '2526' depuis le nom de fichier."""
    if not filename:
        return None
    s = str(filename)
    candidates = re.findall(r"\b\d{4}\b", s)
    for c in candidates:
        if c in {"2425", "2526"}:
            return c
    # fallback: pattern collé (rare)
    m = re.search(r"(2425|2526)", s)
    return m.group(1) if m else None


# =========================
# NAME NORMALIZATION (robuste: inversions / noms collés / doubles noms)
# =========================
from difflib import SequenceMatcher

PARTICLES = {"DE", "DU", "DES", "D", "DA", "DI", "DEL", "DELA", "DELLA", "LE", "LA", "LES"}

def strip_accents_upper(s: str) -> str:
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper()

def normalize_name_raw(s: str) -> str:
    # Normalisation agressive: accents, virgules, tirets, espaces, caractères parasites
    s = strip_accents_upper(s)
    s = s.replace(",", " ")
    s = s.replace("’", "'")
    s = re.sub(r"[^A-Z' -]", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # supprime doublons type "DUPONT DUPONT"
    toks = s.split()
    if len(toks) >= 2 and toks[0] == toks[1]:
        toks = toks[1:]
    return " ".join(toks)

def tokens_name(s: str) -> List[str]:
    s = normalize_name_raw(s)
    if not s:
        return []
    toks = s.split()

    # fusion "D" + "A" => "DA" (exports parfois bizarres)
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
    # pour capter "DUPONTALICE" vs "DUPONT ALICE"
    s = strip_accents_upper(s)
    s = re.sub(r"[^A-Z]", "", s)
    return s

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def infer_opponent_from_columns(df: pd.DataFrame, equipe_pfc: str) -> Optional[str]:
    """
    Retourne le nom d'adversaire depuis les colonnes explicites du fichier si disponibles.
    Priorité: 'Adversaire' puis 'Teamersaire' (orthographe rencontrée dans certains exports).

    Robustesse:
    - ignore les valeurs "Adversaire"/"Teamersaire" (cellules polluées)
    - ignore les valeurs qui ressemblent à une joueuse
    - ignore la valeur égale à l'équipe PFC
    - renvoie une valeur "humaine" (raw le plus fréquent) plutôt qu'un libellé normalisé.
    """
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
    """Fallback si les colonnes Adversaire/Teamersaire n'existent pas ou sont vides."""
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
        # IMPORTANT: nécessite xlrd installé dans ton env Streamlit
        return pd.read_excel(path, sheet_name=sheet_name, engine="xlrd")
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")



# =========================
# PHOTOS - concordance noms + sync Drive
# =========================
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif"}

def _ensure_photos_folder():
    os.makedirs(PHOTOS_FOLDER, exist_ok=True)

def _normalize_for_photo_match(s: str) -> str:
    """Normalise un nom pour rapprocher joueuse <-> nom de fichier photo.
    - majuscules, sans accents
    - supprime ponctuation
    - espaces normalisés
    """
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = s.replace(",", " ").replace("-", " ").replace("_", " ")
    s = re.sub(r"[^A-Z ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _photo_tokens(s: str) -> List[str]:
    s = _normalize_for_photo_match(s)
    return [t for t in s.split() if t]

def _photo_key_compact(s: str) -> str:
    return re.sub(r"[^A-Z]", "", _normalize_for_photo_match(s))

def build_photos_index_local() -> Dict[str, str]:
    """Index local: renvoie dict key_compact -> filepath (valeur 'meilleure')"""
    _ensure_photos_folder()
    idx: Dict[str, str] = {}
    if not os.path.exists(PHOTOS_FOLDER):
        return idx

    for fn in os.listdir(PHOTOS_FOLDER):
        ext = os.path.splitext(fn)[1].lower()
        if ext not in IMAGE_EXTS:
            continue
        stem = os.path.splitext(fn)[0]
        key = _photo_key_compact(stem)
        if not key:
            continue
        # si doublon: on garde le plus récent
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

def find_best_photo_for_player(player_name: str, photos_index: Dict[str, str], cutoff: float = 0.82) -> Optional[str]:
    """Trouve la meilleure photo pour une joueuse (fuzzy sur key compact)."""
    if not player_name:
        return None
    if not photos_index:
        return None

    # 1) match exact sur compact
    key = _photo_key_compact(player_name)
    if key in photos_index:
        return photos_index[key]

    # 2) match tokens dans n'importe quel ordre
    toks = set(_photo_tokens(player_name))
    if toks:
        # candidates dont tous les tokens sont inclus dans le nom de fichier
        best_path = None
        best_score = 0.0
        for k, path in photos_index.items():
            # k est compact -> on approx via tokens du stem
            stem = os.path.splitext(os.path.basename(path))[0]
            stem_toks = set(_photo_tokens(stem))
            if toks.issubset(stem_toks) or stem_toks.issubset(toks):
                sc = SequenceMatcher(None, _normalize_for_photo_match(player_name), _normalize_for_photo_match(stem)).ratio()
                if sc > best_score:
                    best_score = sc
                    best_path = path
        if best_path and best_score >= cutoff:
            return best_path

    # 3) fuzzy sur compact (dernier recours)
    keys = list(photos_index.keys())
    if keys:
        best = get_close_matches(key, keys, n=1, cutoff=cutoff)
        if best:
            return photos_index.get(best[0])

    return None

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


def sync_photos_from_drive() -> None:
    """Synchronise les photos depuis le dossier Drive PHOTOS_FOLDER_ID vers data/photos/.

    Robustesse:
    - Parcours récursif (folders + shortcuts vers folders)
    - Télécharge images + shortcuts vers images (en résolvant targetId)
    - Ajoute un suffixe __<id> pour éviter les collisions de noms
    - Si 0 fichier trouvé: affiche un message d'aide (partage du dossier avec le service account)
    """
    service = authenticate_google_drive()

    # --- Vérifier l'accès au dossier (et afficher l'email du service account si besoin) ---
    sa_email = None
    try:
        sa_email = (st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON") or {}).get("client_email")
    except Exception:
        sa_email = None

    try:
        meta = _execute_with_retry(service.files().get(
            fileId=PHOTOS_FOLDER_ID,
            fields="id,name,mimeType,capabilities(canListChildren)",
            supportsAllDrives=True,
        ))
        if meta.get("mimeType") != "application/vnd.google-apps.folder":
            st.warning("Photos: l'ID fourni ne correspond pas à un dossier Drive (ou c'est un raccourci).")
    except Exception as e:
        st.error(
            "Photos: impossible d'accéder au dossier Drive. "
            + (f"Partage ce dossier avec le service account: {sa_email}. " if sa_email else "")
            + f"Erreur: {e}"
        )
        return

    os.makedirs(PHOTOS_FOLDER, exist_ok=True)

    IMG_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif")
    downloaded = 0
    found = 0

    def _is_image_like(name: str, mime: str) -> bool:
        n = (name or "").lower()
        mt = (mime or "").lower()
        if mt.startswith("image/"):
            return True
        return n.endswith(IMG_EXTS)

    def _safe_name(name: str) -> str:
        name = (name or "photo").replace("/", "_").replace("\\", "_")
        name = re.sub(r"\s+", " ", name).strip()
        return name

    def _download_file(file_id: str, file_name: str, mime_type: str = "", thumb: str | None = None, updated: str | None = None):
        nonlocal downloaded

        safe_name = file_name or f"{file_id}.img"
        local_path = os.path.join(local_folder, safe_name)

        # déjà présent ?
        if os.path.exists(local_path):
            return local_path

        try:
            download_drive_file_to_local(service, file_id, local_path)
            downloaded += 1

            # Si .HEIC/.heif: Streamlit/Pillow ne sait pas toujours l'afficher côté serveur.
            # On génère un aperçu JPEG via thumbnailLink (fourni par Drive) quand dispo.
            try:
                ext = os.path.splitext(local_path)[1].lower()
                if ext in (".heic", ".heif") and (thumb or ""):
                    creds = getattr(getattr(service, "_http", None), "credentials", None)
                    token = None
                    if creds is not None:
                        try:
                            from google.auth.transport.requests import Request as _GRequest
                            if (not getattr(creds, "valid", False)) or getattr(creds, "expired", False):
                                creds.refresh(_GRequest())
                        except Exception:
                            pass
                        token = getattr(creds, "token", None)

                    import requests
                    headers = {"Authorization": f"Bearer {token}"} if token else {}
                    r = requests.get(thumb, headers=headers, timeout=30)
                    if r.ok and r.content:
                        preview_path = os.path.splitext(local_path)[0] + ".jpg"
                        with open(preview_path, "wb") as pf:
                            pf.write(r.content)
            except Exception:
                pass

            return local_path
        except Exception as e:
            st.warning(f"Photos: impossible de télécharger {safe_name}: {e}")
            return None

    stack = [PHOTOS_FOLDER_ID]
    visited = set()

    while stack:
        fid = stack.pop()
        if fid in visited:
            continue
        visited.add(fid)

        try:
            items = list_files_in_folder_paged(service, fid, q_extra="", page_size=200)
        except Exception as e:
            st.warning(f"Photos: impossible de lister un sous-dossier -> {e}")
            continue

        for it in items:
            mt = it.get("mimeType", "")
            name = it.get("name", "")
            item_id = it.get("id")

            # folders
            if mt == "application/vnd.google-apps.folder":
                stack.append(item_id)
                continue

            # shortcuts (vers folder ou image)
            if mt == "application/vnd.google-apps.shortcut":
                sd = it.get("shortcutDetails") or {}
                tgt_id = sd.get("targetId")
                tgt_mime = sd.get("targetMimeType", "")
                if tgt_mime == "application/vnd.google-apps.folder" and tgt_id:
                    stack.append(tgt_id)
                    continue
                if tgt_id and _is_image_like(name, tgt_mime):
                    found += 1
                    _download_file(tgt_id, name)
                continue

            # images
            if _is_image_like(name, mt):
                found += 1
                _download_file(item_id, name, mime_type=mt, thumb=it.get('thumbnailLink'), updated=it.get('modifiedTime'))

    st.session_state["photos_drive_found"] = found
    st.session_state["photos_drive_downloaded"] = downloaded

    if found == 0:
        st.warning(
            "Photos: aucun fichier image trouvé dans le dossier Drive. "
            + ("Vérifie que le dossier est bien partagé avec le service account: " + str(sa_email) if sa_email else "Vérifie les droits du service account.")
        )

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
# GPS DRIVE SYNC (autonome, sans index)
# - évite les listings géants => limite les erreurs Drive 500 sur pagination
# - sync incrémental (modifiedTime)
# - conversion .xls -> Google Sheet -> export .xlsx (pas besoin de xlrd)
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

def _safe_local_path(filename: str, file_id: str) -> str:
    """Construit un chemin local sûr sous data/gps.

    Important:
    - Certains appels passent un 'filename' avec un sous-dossier (ex: 'gps/xxx.csv').
      On évite alors de créer 'data/gps/gps/xxx...' et on place toujours sous 'data/gps/'.
    - On ajoute un suffixe avec l'id Drive pour éviter les collisions de noms.
    """
    os.makedirs(GPS_FOLDER, exist_ok=True)

    rel = "" if filename is None else str(filename)
    rel = os.path.normpath(rel).lstrip("/")

    rel_dir = os.path.dirname(rel)
    base = os.path.basename(rel)
    base_noext, ext = os.path.splitext(base)

    target_dir = os.path.join(GPS_FOLDER, rel_dir) if rel_dir else GPS_FOLDER
    os.makedirs(target_dir, exist_ok=True)

    return os.path.join(target_dir, f"{base_noext}__{file_id[:8]}{ext}")


def download_drive_file_to_local(service, file_id: str, file_name: str, mime_type: str) -> str:
    # Google Sheet -> export xlsx
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

def download_drive_csv_to_local(service, file_id: str, file_name: str) -> str:
    """Télécharge un CSV (Drive binaire) vers data/gps/ (ou sous-dossiers éventuels)."""
    request = service.files().get_media(fileId=file_id)
    if not str(file_name).lower().endswith(".csv"):
        file_name = os.path.splitext(str(file_name))[0] + ".csv"

    # IMPORTANT: ne pas préfixer 'gps/' ici; _safe_local_path gère les sous-dossiers si présents.
    final_path = _safe_local_path(str(file_name), file_id)

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


def export_sheet_to_csv_local(service, file_id: str, file_name: str) -> str:
    """Exporte un Google Sheet en CSV vers data/gps/."""
    request = service.files().export_media(fileId=file_id, mimeType="text/csv")
    file_name = os.path.splitext(str(file_name))[0] + ".csv"

    # IMPORTANT: ne pas préfixer 'gps/' ici; _safe_local_path gère les sous-dossiers si présents.
    final_path = _safe_local_path(str(file_name), file_id)

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
    # 1) copy+convert -> Google Sheet (temp)
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

    # 2) export -> xlsx
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

    # 3) cleanup temp
    try:
        _execute_with_retry(service.files().delete(fileId=gsheet_id, supportsAllDrives=True))
    except Exception:
        pass

    return final_path

def sync_gps_from_drive_autonomous():
    """Synchronise les fichiers GPS depuis Drive, de manière autonome et incrémentale.

    Objectifs:
    - éviter un listing récursif gigantesque (source de 500 Internal Error sur pageToken)
    - parcourir dossier par dossier, avec skip temporaire des dossiers en échec
    - ne rapatrier que les fichiers modifiés depuis la dernière sync (modifiedTime)
    - télécharger/exporter les fichiers GPS en .csv (Google Sheets export -> CSV, CSV natifs téléchargés)
    """
    service = authenticate_google_drive()
    state = _load_gps_state()
    last_m = state.get("last_modifiedTime")  # RFC3339 str ou None
    newest_modified = last_m

    def is_gps_candidate(f: dict) -> bool:
        name = (f.get("name") or "").lower()
        mt = f.get("mimeType") or ""
        if mt == "application/vnd.google-apps.folder":
            return False
        # GPS: CSV natif ou Google Sheet (export CSV)
        if not (name.endswith(".csv") or mt == "application/vnd.google-apps.spreadsheet"):
            return False
        return ("gf1" in name) or ("seance" in name) or ("séance" in name) or ("gps" in name)

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
                    if mt == "application/vnd.google-apps.spreadsheet":
                        export_sheet_to_csv_local(service, fid, name)
                    elif name.lower().endswith(".csv"):
                        download_drive_csv_to_local(service, fid, name)
                except Exception as e:
                    st.warning(f"GPS: téléchargement/export CSV impossible {name} -> {e}")

                mtime = f.get("modifiedTime")
                if mtime and (newest_modified is None or mtime > newest_modified):
                    newest_modified = mtime

        except Exception:
            state.setdefault("folders_failed", {})[folder_id] = time.time()
            continue

    state["last_modifiedTime"] = newest_modified
    # purge des échecs vieux de +24h
    state["folders_failed"] = {k: v for k, v in state.get("folders_failed", {}).items() if (time.time() - float(v)) < 86400}
    _save_gps_state(state)


def list_files_in_folder(service, folder_id: str, include_folders: bool = False) -> List[dict]:
    """Liste les fichiers d'un dossier Drive (1 niveau) avec pagination + retries.

    - Retry/backoff pour les erreurs transitoires (500/503/504 etc.)
    - supportsAllDrives/includeItemsFromAllDrives: robuste aux drives partagés
    """
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
                    # backoff exponentiel
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

    # Google Sheet -> export xlsx
    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        if not final_path.lower().endswith(".xlsx"):
            final_path = os.path.splitext(final_path)[0] + ".xlsx"
            tmp_path = final_path + ".tmp"
    else:
        request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

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

        # retry once if corrupted
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


def download_google_drive():
    service = authenticate_google_drive()
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(PASSERELLE_FOLDER, exist_ok=True)
    os.makedirs(GPS_FOLDER, exist_ok=True)

    # Main folder
    files = list_files_in_folder(service, DRIVE_MAIN_FOLDER_ID)
    for f in files:
        is_sheet = f.get("mimeType") == "application/vnd.google-apps.spreadsheet"
        if f["name"].endswith((".csv", ".xlsx", ".xls")) or is_sheet:
            download_file(service, f["id"], f["name"], DATA_FOLDER, mime_type=f.get("mimeType"))

    # Passerelle
    files_pass = list_files_in_folder(service, DRIVE_PASSERELLE_FOLDER_ID)
    for f in files_pass:
        if normalize_str(f["name"]) == normalize_str(PASSERELLE_FILENAME):
            download_file(service, f["id"], f["name"], PASSERELLE_FOLDER, mime_type=f.get("mimeType"))
            break

# GPS : la synchronisation est gérée par sync_gps_from_drive_autonomous()
# (collecte incrémentale + conversion .xls -> .xlsx), afin d'éviter les erreurs Drive 500
# sur les listings paginés de gros dossiers.
st.session_state["gps_drive_found"] = 0
st.session_state["gps_drive_downloaded"] = 0

# =========================
# REFERENTIEL NOMS
# =========================
def build_referentiel_players(ref_path: str) -> Tuple[Set[str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, Set[str]], Dict[str, Set[str]]]:
    """Construit la base canon des joueuses depuis le référentiel.

    Supporte 2 formats:
    - Ancien: colonnes NOM / Prénom
    - Nouveau: colonne 'Nom de joueuse' (nom complet)

    Retourne:
      - ref_set: ensemble des CANON
      - alias_to_canon: alias directs -> CANON
      - tokenkey_to_canon: clé tokens triés -> CANON (insensible à l'ordre NOM/PRENOM)
      - compact_to_canon: forme sans espaces -> CANON (capte noms collés/décollés)
      - first_to_canons / last_to_canons: index tokens -> set(CANON) pour gérer prénom seul / nom seul + typos
    """
    ref = read_excel_auto(ref_path)

    if isinstance(ref, dict):
        if len(ref) == 0:
            raise ValueError("Référentiel vide (aucune feuille lisible).")
        ref = list(ref.values())[0]

    if not isinstance(ref, pd.DataFrame) or ref.empty:
        raise ValueError("Référentiel illisible ou vide.")

    cols_norm = {normalize_str(c): c for c in ref.columns}

    # --- Nouveau format prioritaire ---
    if "Nom de joueuse" in ref.columns:
        col_name = "Nom de joueuse"
        ref = ref.copy()
        ref["CANON"] = ref[col_name].astype(str).map(normalize_name_raw)
    elif cols_norm.get("nom de joueuse") is not None:
        col_name = cols_norm["nom de joueuse"]
        ref = ref.copy()
        ref["CANON"] = ref[col_name].astype(str).map(normalize_name_raw)
    else:
        # --- Ancien format NOM / PRENOM ---
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

            # Index tokens -> canons (prénom seul / nom seul)
            # Heuristique: on indexe TOUS les tokens + extrémités
            for t in toks:
                _add_index(first_to_canons, t, canon)
                _add_index(last_to_canons, t, canon)

            _add_index(first_to_canons, toks[-1], canon)  # prénom probable
            _add_index(last_to_canons, toks[0], canon)    # nom probable

        # Aliases d'inversion fréquents (PRENOM NOM)
        if toks and len(toks) >= 2:
            inv1 = " ".join([toks[-1]] + toks[:-1])
            alias_to_canon[normalize_name_raw(inv1)] = canon

            if len(toks) >= 3:
                inv2 = " ".join(toks[-2:] + toks[:-2])
                alias_to_canon[normalize_name_raw(inv2)] = canon

        # Alias virgule
        if toks and len(toks) >= 2:
            nom = " ".join(toks[:-1])
            prenom = toks[-1]
            alias_to_canon[normalize_name_raw(f"{nom}, {prenom}")] = canon
            alias_to_canon[normalize_name_raw(f"{prenom} {nom}")] = canon

        # Alias initiale (ex: DUPONT A)
        if toks and len(toks) >= 2:
            nom = " ".join(toks[:-1])
            prenom = toks[-1]
            alias_to_canon[normalize_name_raw(f"{nom} {prenom[0]}")] = canon
            alias_to_canon[normalize_name_raw(f"{nom} {prenom[0]}.")] = canon

    return ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons

def best_from_candidates(raw_clean: str, candidates: List[str], min_score: float = 0.88) -> Tuple[Optional[str], float, Optional[float]]:
    """Retourne le meilleur canon si non ambigu.
    - min_score: score minimum pour accepter
    - anti-ambiguïté: écart >= 0.04 avec le 2e meilleur
    """
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
    """Mappe un nom brut vers le CANON du référentiel.

    Cascade:
      exact -> alias -> token_set -> token_fuzzy -> compact -> single_token -> fuzzy -> unmatched

    Le mode single_token (prénom seul / nom seul / tronqué) est protégé par une règle
    anti-ambiguïté (écart avec le 2e meilleur).
    """
    if raw_name is None:
        return "", "unmatched", "empty"

    raw = str(raw_name).strip()
    if not raw or raw.upper() in {"NAN", "NONE", "NULL"}:
        return "", "unmatched", "empty"

    cleaned = normalize_name_raw(raw)
    if not cleaned:
        return "", "unmatched", raw

    # 1) exact
    if cleaned in ref_set:
        return cleaned, "exact", raw

    # 2) alias
    if cleaned in alias_to_canon:
        return alias_to_canon[cleaned], "alias", raw

    # 3) token-set exact + token-fuzzy
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

    # 4) compact (noms collés/décollés)
    comp = compact_name(cleaned)
    if comp in compact_to_canon:
        return compact_to_canon[comp], "compact", raw

    # 5) single token : prénom seul / nom seul / tronqué
    if toks and len(toks) == 1:
        t = toks[0]
        cand: Set[str] = set()
        cand |= first_to_canons.get(t, set())
        cand |= last_to_canons.get(t, set())

        # élargissement si faute sur le token
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

    # 6) fuzzy final global (dernier recours)
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
def load_passerelle_data():
    passerelle_data = {}
    passerelle_file = os.path.join(PASSERELLE_FOLDER, PASSERELLE_FILENAME)
    if not os.path.exists(passerelle_file):
        return passerelle_data
    try:
        df = read_excel_auto(passerelle_file)
        if isinstance(df, dict):
            df = list(df.values())[0] if len(df) else pd.DataFrame()
        for _, row in df.iterrows():
            nom = row.get("Nom", None)
            if nom:
                passerelle_data[nom] = {
                    "Prénom": row.get("Prénom", ""),
                    "Photo": row.get("Photo", ""),
                    "Date de naissance": row.get("Date de naissance", ""),
                    "Poste 1": row.get("Poste 1", ""),
                    "Poste 2": row.get("Poste 2", ""),
                    "Pied Fort": row.get("Pied Fort", ""),
                    "Taille": row.get("Taille", ""),
                }
    except Exception:
        pass
    return passerelle_data


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
    """Calcule le temps de jeu des joueuses à partir des segments Duration."""
    if match is None or match.empty:
        return pd.DataFrame()

    if "Duration" not in match.columns or "Row" not in match.columns:
        return pd.DataFrame()

    available_posts = [c for c in POST_COLS if c in match.columns]
    if not available_posts:
        return pd.DataFrame()

    m = match.copy()

    # Normalisation des noms d'équipes
    home_clean = nettoyer_nom_equipe(home_team)
    away_clean = nettoyer_nom_equipe(away_team)

    m["Row_team"] = m["Row"].astype(str).apply(nettoyer_nom_equipe)

    # garder lignes équipes
    m = m[m["Row_team"].isin({home_clean, away_clean})].copy()
    if m.empty:
        return pd.DataFrame()

    # unité Duration
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
    """
    Compte les tirs / tirs cadrés / buts à partir des événements.
    Règle: les buts = occurrences de "But" dans la colonne "Tir"
    sur les lignes où Action contient "Tir".
    """
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
    """Compte les passes (1 passe = 1 ligne) et la réussite."""
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
    """Compte les passes décisives (1 ligne = 1 passe)."""
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
    """
    ✅ Version unique (nettoyée) : compte la direction des passes à partir de la colonne 'Ungrouped'
    (uniquement si Action contient 'Passe').

    Catégories :
    - avant / arrière
    - latérale gauche / droite
    - diagonale gauche / droite

    Une passe est réussie si la colonne 'Passe' contient 'Réussie'.
    """
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
    """Construit les colonnes nécessaires à Créativité 1 & 2 à partir des events."""
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
    """Crée les métriques (0-100 via rang percentile)."""
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

    # Créativité 1 & 2 (colonnes internes)
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

    # Rang percentiles 0-100
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

    # Helpers créativité (à partir des events)
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


# =========================
# AGRÉGATION GLOBALE (export)
# =========================
def aggregate_global_players(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège la base PFC par joueuse pour l'export Excel."""
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
    """Convert per-90 volumes back to real volumes for match-by-match export only."""
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
    """Standardise un export GF1 (Activity Date, Nom de joueur, Temps joué, Distance par plages...)."""
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

    # Date
    if "DATE" in d.columns:
        d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    else:
        dt = parse_date_from_gf1_filename(filename)
        d["DATE"] = pd.Timestamp(dt.date()) if dt else pd.NaT

    d["SEMAINE"] = d["DATE"].dt.isocalendar().week.astype("Int64")
    w_file = parse_week_from_gf1_filename(filename)
    if w_file is not None:
        d["SEMAINE"] = pd.Series([w_file] * len(d), index=d.index, dtype="Int64")

    # Numériques essentiels
    for c in ["Durée_min", "Distance (m)", "Sprints_23", "Sprints_25", "Vitesse max (km/h)", "Accélération maximale (m/s²)", "#accel/decel"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    # Plages -> passerelle
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

    # Source
    d["__source_file"] = os.path.basename(filename)
    return d


def read_csv_auto(path: str) -> pd.DataFrame:
    """Lecture CSV robuste (',' ou ';', encodages fréquents)."""
    # essai UTF-8 (avec BOM) puis latin-1
    encodings = ["utf-8-sig", "utf-8", "latin1"]
    seps = [",", ";", "\t"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep)
                # Heuristique: si 1 seule colonne et le fichier contient des séparateurs, mauvais sep
                if df.shape[1] == 1 and sep != "\t":
                    continue
                return df
            except Exception as e:
                last_err = e
                continue
    raise last_err if last_err else ValueError(f"Impossible de lire le CSV: {path}")


def list_gps_files_local() -> List[str]:
    """Liste des CSV GPS synchronisés localement.

    - Parcourt récursivement data/gps (car certains téléchargements peuvent créer des sous-dossiers)
    - Fallback: data/ (non récursif) si certains CSV ont été déposés à la racine.
    """
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

    # fallback data/ (1 niveau)
    if os.path.exists(DATA_FOLDER):
        for f in os.listdir(DATA_FOLDER):
            if not f.lower().endswith(".csv"):
                continue
            fn_norm = normalize_str(f)
            if ("gf1" in fn_norm) or ("seance" in fn_norm) or ("séance" in fn_norm) or ("gps" in fn_norm):
                paths.append(os.path.join(DATA_FOLDER, f))

    # dédoublonnage
    paths = sorted(list(dict.fromkeys(paths)))
    return paths


def standardize_gps_columns(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Détecte d'abord le format GF1 export, sinon applique le mapping legacy."""
    if df is None or df.empty:
        return df

    if is_gf1_export_format(df):
        return standardize_gps_gf1_export(df, filename)

    # fallback legacy
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


def load_gps_raw(ref_set: Set[str], alias_to_canon: Dict[str, str], tokenkey_to_canon: Dict[str, str], compact_to_canon: Dict[str, str], first_to_canons: Dict[str, Set[str]], last_to_canons: Dict[str, Set[str]]) -> pd.DataFrame:
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
            # Ici on est dans la lecture des fichiers GPS (pas les matchs)
            st.warning(f"GPS: impossible de lire {os.path.basename(p)} -> {e}")
            continue

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty or "NOM" not in df.columns:
        return pd.DataFrame()

    # Mapping référentiel
    mapped = []
    statuses = []
    for v in df["NOM"].astype(str).tolist():
        m, status, _ = map_player_name(v, ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons, cutoff_fuzzy=0.93)
        mapped.append(m)
        statuses.append(status)
    df["Player"] = mapped
    df["__name_status"] = statuses

    # Numériques (compat formats)
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

    # Harmoniser Durée_min
    if "Durée_min" not in df.columns and "Durée" in df.columns:
        df["Durée_min"] = pd.to_numeric(df["Durée"], errors="coerce")
    elif "Durée_min" in df.columns:
        df["Durée_min"] = pd.to_numeric(df["Durée_min"], errors="coerce")

    df["DATE"] = pd.to_datetime(df.get("DATE", pd.NaT), errors="coerce")
    return df


def compute_gps_weekly_metrics(df_gps: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données GPS par joueuse et par semaine.

    Colonnes gérées (si présentes) :
    - Distance Totale : 'Distance (m)'
    - Bandes vitesse : 'Distance 13-19 (m)', 'Distance 19-23 (m)', 'Distance >23 (m)'
      (ou leurs équivalents HID si c'est ce qui est disponible)
    - Charge : 'CHARGE' (ou calcul via RPE * Durée)
    - Durée : 'Durée_min' (ou 'Durée')

    Retourne aussi (si CHARGE disponible) :
    - Aigue, Chronique (rolling 4 semaines), ACWR
    """
    if df_gps is None or df_gps.empty:
        return pd.DataFrame()

    d = df_gps.copy()

    # Semaine
    if "SEMAINE" not in d.columns:
        if "DATE" in d.columns:
            d["SEMAINE"] = pd.to_datetime(d["DATE"], errors="coerce").dt.isocalendar().week.astype("Int64")
        else:
            d["SEMAINE"] = pd.NA

    # Durée minutes
    if "Durée_min" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée_min"], errors="coerce")
    elif "Durée" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée"], errors="coerce")
    else:
        d["Durée_min"] = np.nan

    # Charge
    if "CHARGE" not in d.columns and "RPE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["RPE"], errors="coerce").fillna(0) * d["Durée_min"].fillna(0)
    elif "CHARGE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["CHARGE"], errors="coerce")

    # Colonnes à sommer
    agg_map: Dict[str, str] = {}

    # Distance totale
    if "Distance (m)" in d.columns:
        d["Distance (m)"] = pd.to_numeric(d["Distance (m)"], errors="coerce")
        agg_map["Distance (m)"] = "sum"

    # Bandes demandées
    for col in ["Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
            agg_map[col] = "sum"

    # Fallback HID si les bandes ne sont pas là
    for col in ["Distance HID (>13 km/h)", "Distance HID (>19 km/h)"]:
        if col in d.columns and col not in agg_map:
            d[col] = pd.to_numeric(d[col], errors="coerce")
            agg_map[col] = "sum"

    # Durée et charge
    if "Durée_min" in d.columns:
        agg_map["Durée_min"] = "sum"
    if "CHARGE" in d.columns:
        agg_map["CHARGE"] = "sum"

    # Si rien à agréger, on renvoie un DF vide (UI affichera un message)
    if not agg_map:
        return pd.DataFrame()

    out = d.groupby(["Player", "SEMAINE"], as_index=False).agg(agg_map)

    # ACWR si charge dispo
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
    """Garantit une colonne DATE (tz-naive) en datetime64[ns].

    Priorité:
    1) 'Activity Date' / 'activity date' (exports GPS)
    2) 'DATE'
    3) 'Date'
    4) date dans __source_file au format JJ.MM.AAAA

    Robustesse:
    - gère timestamps tz-aware (convertit en naïf)
    - gère formats texte variés (JJ.MM.AAAA, JJ/MM/AAAA, ISO)
    """
    if df is None or df.empty:
        return df

    d = df.copy()

    # Trouver une colonne source potentielle
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

    # Fallback: date dans le nom de fichier (JJ.MM.AAAA / JJ.MM.AA / JJ/MM/AAAA)
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

    # Dernier filet: normaliser en tz-naive
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

def build_md_window_summary(d_player: pd.DataFrame, end_date: pd.Timestamp, days: int = 7) -> pd.DataFrame:
    """Construit un tableau MD-(days-1) .. MD sur une fenêtre glissante.

    - Fenêtre: [end_date-(days-1) ; end_date] (inclus)
    - Si plusieurs lignes le même jour: moyenne journalière, puis agrégation par MD
    """
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

    # Distance relative si absente: Distance / Durée_min
    if "Distance relative (m/min)" not in d.columns:
        dist = _gps_get_numeric(d, "Distance (m)")
        dur = _gps_get_numeric(d, "Durée_min")
        d["Distance relative (m/min)"] = (dist / dur.replace(0, np.nan)).fillna(0)

    # Variables candidates (on ne force pas tout)
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

    # Moyenne journalière
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

def plot_gps_md_graph(summary: pd.DataFrame, selected_lines: Optional[List[str]] = None):
    """Graphique microcycle (MD-6 → MD) plus lisible :
    - barres plus étroites (distance totale)
    - lignes avec marqueurs + meilleure lisibilité sur fond sombre
    - légende compacte sous le graphique
    """
    if summary is None or summary.empty or "MD" not in summary.columns:
        return None

    import matplotlib.pyplot as plt
    import numpy as np

    d = summary.copy()

    # Ordre MD (si jamais des MD manquent)
    md_order = [f"MD-{k}" for k in range(6, 0, -1)] + ["MD"]
    d["__ord"] = d["MD"].astype(str).map({lab: i for i, lab in enumerate(md_order)})
    d = d.sort_values("__ord").drop(columns="__ord")

    x_labels = d["MD"].astype(str).tolist()
    x = np.arange(len(x_labels))

    # Colonnes possibles (selon les exports)
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
        # défaut: 4-5 courbes max (sinon illisible)
        lines_to_plot = available_lines[:5]

    # ---------- Figure ----------
    fig, ax1 = plt.subplots(figsize=(11.2, 5.6), dpi=170)
    fig.patch.set_facecolor("#061a2e")
    ax1.set_facecolor("#061a2e")

    # Grid + axes
    ax1.grid(True, axis="y", linestyle="--", alpha=0.25)
    for sp in ax1.spines.values():
        sp.set_alpha(0.35)

    ax1.tick_params(axis="x", colors="white")
    ax1.tick_params(axis="y", colors="white")
    ax1.yaxis.label.set_color("white")

    # ---------- Barres (Distance totale) ----------
    if bar_col in d.columns:
        y_bar = pd.to_numeric(d[bar_col], errors="coerce").fillna(0.0).values
    else:
        y_bar = np.zeros(len(d))

    bar_width = 0.55  # ✅ barres plus étroites
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

    # marge en Y pour éviter que les barres touchent le haut
    if len(y_bar) and np.nanmax(y_bar) > 0:
        ax1.set_ylim(0, float(np.nanmax(y_bar)) * 1.18)

    # ---------- Lignes (axe droit) ----------
    ax2 = ax1.twinx()
    ax2.set_facecolor("none")
    ax2.tick_params(axis="y", colors="white")
    ax2.yaxis.label.set_color("white")
    ax2.set_ylabel("Valeurs (axe droit)")

    # Palette lisible sur fond sombre
    palette = ["#2EC4B6", "#FF9F1C", "#E71D36", "#A06CD5", "#9BC53D", "#5BC0EB", "#FDE74C"]
    handles = []
    labels = []

    # Ajoute d'abord la barre dans la légende
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

    # Améliore la lisibilité : petite marge sur l’axe droit
    try:
        y_all = []
        for col in lines_to_plot:
            y_all.extend(pd.to_numeric(d[col], errors="coerce").fillna(0.0).tolist())
        if y_all and np.nanmax(y_all) > 0:
            ax2.set_ylim(0, float(np.nanmax(y_all)) * 1.15)
    except Exception:
        pass

    # ---------- Légende (sous le graphe) ----------
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

    # Titre discret
    ax1.set_title("Microcycle (MD-6 → MD)", color="white", pad=10, fontsize=13)

    fig.tight_layout()
    return fig

def gps_last_7_days_summary(gps_raw: pd.DataFrame, player_sel: str, end_date: Optional[pd.Timestamp] = None):
    """Fenêtre glissante 7 jours (inclus) pour une joueuse.

    Retourne:
      - df_7j: lignes brutes sur la période
      - summary: tableau (1 ligne) avec moyennes et totaux sur la période
    """
    if gps_raw is None or gps_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    d = gps_raw.copy()

    # Sélection joueuse (canon)
    canon = nettoyer_nom_joueuse(player_sel)
    if "Player" in d.columns:
        d = d[d["Player"].astype(str).apply(nettoyer_nom_joueuse) == canon].copy()
    else:
        return pd.DataFrame(), pd.DataFrame()

    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Assurer DATE
    d = ensure_date_column(d)
    if "DATE" not in d.columns:
        return pd.DataFrame(), pd.DataFrame()

    d = d[d["DATE"].notna()].copy()
    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Normaliser DATE (tz-naive)
    d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    d = d[d["DATE"].notna()].copy()
    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    d["DATE"] = d["DATE"].dt.tz_localize(None)

    # Date de fin (par défaut: dernière date dispo)
    if end_date is None:
        end_dt = pd.to_datetime(d["DATE"].max()).normalize()
    else:
        end_dt = pd.to_datetime(end_date).normalize()

    start_dt = end_dt - pd.Timedelta(days=6)
    end_inclusive = end_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    df_7j = d[(d["DATE"] >= start_dt) & (d["DATE"] <= end_inclusive)].copy()
    if df_7j.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Colonnes numériques GPS possibles
    metric_cols = [c for c in [
        "Durée", "Durée_min",
        "Distance (m)",
        "Distance 13-19 (m)", "Distance 19-23 (m)", "Distance >23 (m)",
        "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
        "CHARGE", "RPE",
        "Sprints_23", "Sprints_25",
        "Vitesse max (km/h)", "#accel/decel",
    ] if c in df_7j.columns]

    # Coerce numériques
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

@st.cache_data
def collect_data(selected_season=None):
    download_google_drive()

    # Référentiel
    ref_path = os.path.join(DATA_FOLDER, REFERENTIEL_FILENAME)
    if not os.path.exists(ref_path):
        ref_path = find_local_file_by_normalized_name(DATA_FOLDER, REFERENTIEL_FILENAME)
    if not ref_path or not os.path.exists(ref_path):
        st.error(f"Référentiel introuvable dans '{DATA_FOLDER}'.")
        return pd.DataFrame(), pd.DataFrame()

    ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons = build_referentiel_players(ref_path)
    name_report: List[dict] = []

    pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

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
    # GPS: sync autonome (Drive -> local data/gps) + conversion .xls si nécessaire
    try:
        sync_gps_from_drive_autonomous()
    except Exception as e:
        st.warning(f"GPS: sync autonome échouée -> {e}")

    # Photos: sync Drive -> local + index
    try:
        sync_photos_from_drive()
    except Exception as e:
        st.warning(f"Photos: sync échouée -> {e}")

    st.session_state["photos_index"] = build_photos_index_local()

    # GPS
    gps_raw = load_gps_raw(ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons)
    gps_week = compute_gps_weekly_metrics(gps_raw)
    st.session_state["gps_weekly_df"] = gps_week
    st.session_state["gps_raw_df"] = gps_raw

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
                st.warning(f"EDF_Joueuses.xlsx: colonnes manquantes, trouvé: {edf_joueuses.columns.tolist()}")
            else:
                edf_j = edf_joueuses.copy()
                edf_j["Player_raw"] = edf_j["Player"].astype(str)

                canon_list = []
                for v in edf_j["Player_raw"].tolist():
                    canon, _, _ = map_player_name(v, ref_set, alias_to_canon, tokenkey_to_canon, compact_to_canon, first_to_canons, last_to_canons, cutoff_fuzzy=0.93)
                    canon_list.append(canon)
                edf_j["PlayerCanon"] = canon_list

                # Temps de jeu minutes
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
                        st.warning(
                            f"EDF: merge faible sur {csv_file} (Poste NaN {d['Poste'].isna().mean():.0%})."
                        )
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
            st.warning(f"EDF: erreur chargement/calcul référentiel: {e}")

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

            # Normalisation per-90 (sauf buts, %)
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
            st.warning(f"Match: impossible de lire {filename} -> {e}")
            continue

    st.session_state["name_report_df"] = pd.DataFrame(name_report).drop_duplicates() if name_report else pd.DataFrame()
    return pfc_kpi, edf_kpi


# =========================
# RADARS
# =========================
def create_individual_radar(df: pd.DataFrame):
    """
    Radar 'Club Pro' (Version A) :
    - Couleurs par KPI (familles)
    - Ordre des axes logique
    - Grille fine
    - Zones de perf (rouge/orange/vert en fond)
    - Top 2 forces / Top 2 axes de progrès
    """
    if df is None or df.empty or "Player" not in df.columns:
        return None

    # --- Ordre logique (familles) ---
    # Rigueur -> Lecture -> Distribution -> Percussion -> Finition -> Créativité
    ordered_params = [
        # Rigueur
        "Timing", "Force physique",
        # Lecture
        "Intelligence tactique",
        # Distribution
        "Technique 1", "Technique 2", "Technique 3",
        # Percussion
        "Explosivité", "Prise de risque",
        # Finition
        "Précision", "Sang-froid",
        # Créativité
        "Créativité 1", "Créativité 2",
    ]

    available = [p for p in ordered_params if p in df.columns]
    if len(available) < 3:
        return None

    player = df.iloc[0].copy()

    # --- Couleurs par KPI (familles cohérentes) ---
    # (tu peux ajuster si tu veux coller à ta charte)
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

    # --- Valeurs ---
    values = [float(pd.to_numeric(player[p], errors="coerce")) if p in player else 0.0 for p in available]
    values = [0.0 if pd.isna(v) else max(0.0, min(100.0, v)) for v in values]

    # --- Top forces / axes de progrès (sur les métriques disponibles) ---
    s = pd.Series(values, index=available).sort_values(ascending=False)
    top2 = s.head(2)
    low2 = s.tail(2).sort_values(ascending=True)

    # --- Construction PyPizza ---
    # Grille plus fine + look plus clean
    pizza = PyPizza(
        params=available,
        background_color="#002B5C",
        straight_line_color="#FFFFFF",
        last_circle_color="#FFFFFF",
        straight_line_lw=1.0,      # plus fin
        last_circle_lw=1.4,        # un peu plus épais
        other_circle_lw=0.8,       # plus fin
        other_circle_color="#8FA3BF",
    )

    fig, ax = pizza.make_pizza(
        values=values,
        figsize=(8, 8),
        slice_colors=slice_colors,
        value_colors=["#FFFFFF"] * len(available),
        kwargs_slices=dict(edgecolor="#FFFFFF", linewidth=2.2),
        kwargs_params=dict(color="#FFFFFF", fontsize=12, fontproperties="monospace"),
        kwargs_values=dict(
            color="#FFFFFF",
            fontsize=11,
            bbox=dict(
                edgecolor="#FFFFFF",
                facecolor="#002B5C",
                boxstyle="round,pad=0.25",
                lw=1.2
            ),
        ),
    )

    # --- Zones de performance (fond léger) ---
    # Rouge <40 / Orange 40-70 / Vert >70
    # PyPizza est en coordonnées polaires mais on peut superposer des cercles
    import matplotlib.patches as patches

    # cercle externe = 100
    zone_specs = [
        (40, "#FF6B6B", 0.08),   # fragile
        (70, "#FFA06E", 0.07),   # développement
        (100, "#2ED47A", 0.05),  # point fort
    ]

    # On dessine de l’extérieur vers l’intérieur (100->70->40) pour un rendu propre
    for r, col, alpha in sorted(zone_specs, key=lambda x: x[0], reverse=True):
        circ = patches.Circle((0, 0), r, transform=ax.transData._b, color=col, alpha=alpha, zorder=0)
        ax.add_artist(circ)

    # --- Petite pastille centrale plus discrète ---
    center = patches.Circle((0, 0), 4.0, transform=ax.transData._b, color="#001E40", zorder=10)
    ax.add_artist(center)


    # Espace pour afficher Forces / Axes sous le graphique
    fig.subplots_adjust(top=0.90, bottom=0.18)
    # --- Titre + résumé forces/axes ---
    player_name = str(player.get("Player", "")).strip()
    # --- Forces / Axes : bloc lisible SOUS le graphique ---
    # On wrappe pour éviter les débordements sur petits écrans
    # --- calcul Forces / Axes (Top 2 / Bottom 2) ---
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

    fig.text(
        0.5, 0.10,
        forces_wrapped,
        ha="center", va="center",
        fontsize=12, color="#DDE8F7",
    )
    fig.text(
        0.5, 0.05,
        axes_wrapped,
        ha="center", va="center",
        fontsize=12, color="#DDE8F7",
    )
    fig.set_facecolor("#002B5C")
    return fig



# =========================
# RADAR COMPARAISON (2 profils)
# =========================
def create_comparison_radar(df, player1_name=None, player2_name=None, exclude_creativity: bool = False):
    """Radar de comparaison (2 profils) + résumé des écarts sous le graphique.

    Option A (choisie): même format radar, mais mise en page plus propre :
    - titres sans bandeaux blancs
    - espace réservé en bas pour un résumé lisible (forces / axes d'amélioration vs comparatif)
    """
    if df is None or df.empty or len(df) < 2:
        return None

    # métriques
    metrics = [
        "Timing",
        "Force physique",
        "Intelligence tactique",
        "Technique 1",
        "Technique 2",
        "Technique 3",
        "Explosivité",
        "Prise de risque",
        "Précision",
        "Sang-froid",
    ]
    if not exclude_creativity:
        metrics += ["Créativité 1", "Créativité 2"]

    # colonnes dispo
    available = [m for m in metrics if m in df.columns]
    if len(available) < 3:
        return None

    d = df.copy()

    # clamp + numeric
    for c in available:
        d[c] = pd.to_numeric(d[c], errors="coerce").clip(lower=0, upper=100).fillna(0.0)

    # on force 2 lignes
    d2 = d.iloc[:2].copy()
    v1 = d2.iloc[0][available].values.astype(float)
    v2 = d2.iloc[1][available].values.astype(float)

    # labels
    p1 = str(player1_name) if player1_name else str(d2.iloc[0].get("Player", "Joueuse A"))
    p2 = str(player2_name) if player2_name else str(d2.iloc[1].get("Player", "Joueuse B"))

    # radar
    low, high = [0] * len(available), [100] * len(available)
    radar = Radar(available, low, high, num_rings=4, ring_width=1, center_circle_radius=1)

    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111)
    fig.patch.set_facecolor("#002B5C")
    ax.set_facecolor("#002B5C")

    radar.setup_axis(ax=ax, facecolor="None")
    radar.draw_circles(ax=ax, facecolor="#0c4281", edgecolor="#0c4281", lw=1.5)

    # radar compare
    radar.draw_radar_compare(
        v1,
        v2,
        ax=ax,
        kwargs_radar={"facecolor": "#00f2c1", "alpha": 0.45, "edgecolor": "#00f2c1", "lw": 2},
        kwargs_compare={"facecolor": "#d80499", "alpha": 0.40, "edgecolor": "#d80499", "lw": 2},
    )

    # labels
    radar.draw_range_labels(ax=ax, fontsize=10, color="#bcd0e6")
    radar.draw_param_labels(ax=ax, fontsize=12, color="#fcfcfc")

    # titres (sans bandeaux)
    fig.text(0.03, 0.965, p1, ha="left", va="top", fontsize=16, color="#00f2c1", fontweight="bold")
    fig.text(0.97, 0.965, p2, ha="right", va="top", fontsize=16, color="#d80499", fontweight="bold")
    fig.text(0.5, 0.965, "Comparaison (0-100)", ha="center", va="top", fontsize=14, color="#ffffff", fontweight="bold")

    # résumé des écarts sous le radar
    # delta = p1 - p2
    delta = pd.Series(v1 - v2, index=available)

    # top écarts
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

    # espace en bas
    fig.subplots_adjust(top=0.90, bottom=0.18)

    fig.text(0.5, 0.11, f"✅ Avantages {p1} vs {p2} : {txt_pos}", ha="center", va="center",
             fontsize=11.5, color="#ffffff")
    fig.text(0.5, 0.075, f"⚠️ Axes d'amélioration {p1} vs {p2} : {txt_neg}", ha="center", va="center",
             fontsize=11.5, color="#ffffff")

    # petite légende en bas à droite
    fig.text(0.98, 0.02, "Δ = (profil A - profil B)", ha="right", va="bottom", fontsize=9, color="#bcd0e6")

    return fig

def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
    st.sidebar.markdown(
        "<div style='display:flex;justify-content:center;'><img src='https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png' width='100'></div>",
        unsafe_allow_html=True,
    )

    player_name = get_player_for_profile(user_profile, permissions)
    st.sidebar.title(f"Connecté : {user_profile}")
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
            with st.spinner("Mise à jour..."):
                download_google_drive()
                _p, _e = collect_data(selected_saison)
            st.cache_data.clear()
            st.success("✅ Mise à jour terminée")
            st.rerun()

    if selected_saison != "Toutes les saisons":
        pfc_kpi, edf_kpi = collect_data(selected_saison)
    else:
        pfc_kpi, edf_kpi = collect_data()

    pfc_kpi_all = pfc_kpi.copy() if isinstance(pfc_kpi, pd.DataFrame) else pd.DataFrame()
    edf_kpi_all = edf_kpi.copy() if isinstance(edf_kpi, pd.DataFrame) else pd.DataFrame()

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

    options = ["Statistiques", "Comparaison", "Données Physiques", "Joueuses Passerelles"]
    if check_permission(user_profile, "all", permissions):
        options.insert(2, "Gestion")

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=options,
            icons=["graph-up-arrow", "people", "gear", "activity", "people-fill"][: len(options)],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={
                "container": {"padding": "5!important", "background-color": "#002A48"},
                "icon": {"color": "#0078D4", "font-size": "18px"},
                "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#003A58"},
                "nav-link-selected": {"background-color": "#0078D4", "color": "white"},
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
                st.pyplot(fig)

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
                # Photo joueuse (Drive Photos) - affichage si disponible
                photos_index = st.session_state.get("photos_index", {})
                if photos_index:
                    try:
                        photo_path = find_best_photo_for_player(p, photos_index)
                        if photo_path and os.path.exists(photo_path):
                            st.image(photo_path, width=160)
                    except Exception:
                        pass


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
                    st.pyplot(fig)
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
                    st.pyplot(fig)
                else:
                    st.warning("Radar indisponible (données insuffisantes sur les métriques).")

        else:
            if player_name:
                p = player_name
                st.info(f"Joueuse : {p}")
            else:
                p = st.selectbox("Joueuse", sorted(pfc_kpi["Player"].dropna().unique().tolist()), key="edf_player")

            if edf_kpi is None or edf_kpi.empty or "Poste" not in edf_kpi.columns:
                st.warning("Aucune donnée EDF disponible pour la comparaison (EDF_Joueuses.xlsx / EDF_U19_Match*.csv).")
                return

            postes_display = sorted(edf_kpi["Poste"].dropna().astype(str).unique().tolist())
            poste = st.selectbox("Poste (référentiel EDF)", postes_display, key="edf_poste_ref")

            edf_line = edf_kpi[edf_kpi["Poste"] == poste].copy()
            edf_line = edf_line.rename(columns={"Poste": "Player"})
            edf_label = f"EDF {poste}"


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

        tab_raw, tab_week, tab_graph = st.tabs(
            ["🧾 Données brutes par joueuse", "📅 Moyennes 7 jours (glissant)", "📈 Graphique MD-6 → MD"]
        )

        # -----------------------
        # TAB 1 — RAW
        # -----------------------
        with tab_raw:
            st.subheader("Données brutes (par joueuse)")

            player_sel = player_name if player_name else st.selectbox("Joueuse", all_players, key="gps_raw_player_sel")
            d = gps_raw[gps_raw["Player"].astype(str) == nettoyer_nom_joueuse(player_sel)].copy()
            d = ensure_date_column(d)

            if d.empty:
                st.info("Aucune ligne GPS pour cette joueuse.")
            elif d["DATE"].notna().sum() == 0:
                st.info("Aucune date exploitable pour cette joueuse (colonne 'Activity Date' / 'DATE' / date dans le nom du fichier).")
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

        # -----------------------
        # TAB 2 — 7D rolling
        # -----------------------
        with tab_week:
            st.subheader("Moyennes sur une fenêtre glissante de 7 jours")

            player_sel = player_name if player_name else st.selectbox("Joueuse", all_players, key="gps_7d_player_sel")

            tmp = gps_raw[gps_raw["Player"].astype(str) == nettoyer_nom_joueuse(player_sel)].copy()
            tmp = ensure_date_column(tmp)
            tmp = tmp[tmp["DATE"].notna()].copy()

            if tmp.empty:
                st.info("Pas de dates exploitables pour cette joueuse (colonne 'Activity Date' / 'DATE' ou date JJ.MM.AAAA dans le nom du fichier).")
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

        # -----------------------
        # TAB 3 — Microcycle chart
        # -----------------------
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
                "Date de référence (MD) — le graphique prend les 7 jours précédents",
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

            # KPI selector (lines)
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

    elif page == "Joueuses Passerelles":
        st.header("🔄 Joueuses Passerelles")

        passerelle_data = load_passerelle_data()
        if not passerelle_data:
            st.warning("Aucune donnée passerelle.")
            return

        # Sources non filtrées (important si le profil est lié à une seule joueuse)
        pfc_source = pfc_kpi_all if 'pfc_kpi_all' in locals() and isinstance(pfc_kpi_all, pd.DataFrame) and not pfc_kpi_all.empty else pfc_kpi
        edf_source = edf_kpi_all if 'edf_kpi_all' in locals() and isinstance(edf_kpi_all, pd.DataFrame) and not edf_kpi_all.empty else edf_kpi

        # --- Sélection ---
        selected = st.selectbox("Sélectionnez une joueuse", list(passerelle_data.keys()), key="passerelle_player_sel")
        # Photo joueuse (Drive Photos) - affichage si disponible
        photos_index = st.session_state.get("photos_index", {})
        if photos_index:
            try:
                photo_path = find_best_photo_for_player(selected, photos_index)
                if photo_path and os.path.exists(photo_path):
                    st.image(photo_path, width=160)
            except Exception:
                pass

        selected_clean = nettoyer_nom_joueuse(selected)
        info = passerelle_data[selected]

        # --- Résolution du nom (Passerelle -> noms utilisés dans Stats/GPS) ---
        def _resolve_best_player_name(pass_key: str, pass_info: dict, candidates: list[str]) -> str:
            """Tente de retrouver le libellé 'Player' utilisé dans les données (stats/GPS) à partir de la passerelle."""
            if not candidates:
                return pass_key

            # Construire un libellé plus informatif si possible : "NOM Prénom"
            nom = str(pass_info.get("Nom", "") or pass_key).strip()
            prenom = str(pass_info.get("Prénom", "")).strip()
            full = f"{nom} {prenom}".strip()

            # Comparaison sur versions normalisées (accents, casse, ponctuation)
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

            # 1) match exact normalisé
            for c, cn in norm_map.items():
                if cn == base:
                    return c

            # 2) contient (ex: passerelle = NOM, candidates = "NOM Prénom")
            for c, cn in norm_map.items():
                if base and base in cn:
                    return c

            # 3) fuzzy
            from difflib import get_close_matches
            best_norm = get_close_matches(base, list(norm_map.values()), n=1, cutoff=0.55)
            if best_norm:
                inv = {v: k for k, v in norm_map.items()}
                return inv.get(best_norm[0], pass_key)

            return pass_key

        # candidates: joueurs présents dans les stats PFC + GPS (si dispo)
        stats_candidates = []
        if isinstance(pfc_source, pd.DataFrame) and not pfc_source.empty:
            # selon la structure, la colonne peut être "Joueuse" / "Player" / "Nom"
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

        # --- Identité ---
        st.subheader("Identité")
        cA, cB = st.columns([1, 2])
        with cA:
            if info.get("Photo"):
                st.image(info["Photo"], width=160)
        with cB:
            if info.get("Prénom"):
                st.write(f"**Prénom :** {info['Prénom']}")
            st.write(f"**Nom :** {info.get('Nom', selected)}")
            if info.get("Date de naissance"):
                st.write(f"**Date de naissance :** {info['Date de naissance']}")
            if info.get("Poste 1"):
                st.write(f"**Poste 1 :** {info['Poste 1']}")
            if info.get("Poste 2"):
                st.write(f"**Poste 2 :** {info['Poste 2']}")
            if info.get("Pied Fort"):
                st.write(f"**Pied Fort :** {info['Pied Fort']}")
            if info.get("Taille"):
                st.write(f"**Taille :** {info['Taille']}")
            if resolved_player:
                st.caption(f"Nom détecté dans les données : **{resolved_player}**")

        st.divider()

        # --- Onglets (Stats / EDF / GPS) ---
        tab_stats, tab_edf, tab_gps = st.tabs(["📈 Statistiques", "🆚 Comparaison EDF", "🏃 Données physiques (GPS)"])

        # =========================
        # TAB 1 — STATS (comme onglet Statistiques)
        # =========================
        with tab_stats:
            st.subheader("Statistiques joueuse")

            if not isinstance(pfc_source, pd.DataFrame) or pfc_source.empty:
                st.warning("Aucune donnée statistiques PFC disponible.")
            else:
                # Réutilise le pipeline d'agrégation déjà présent
                # -> on filtre les lignes de la joueuse puis on applique la même logique que l'onglet Statistiques
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
                        # fallback: match par normalisation
                        try:
                            base = normalize_str(str(resolved_player))
                            pfc_player_df = pfc_source[pfc_source[player_col].astype(str).map(lambda x: normalize_str(str(x)) == base)].copy()
                        except Exception:
                            pass

                    if pfc_player_df.empty:
                        st.info("Aucune ligne statistique trouvée pour cette joueuse.")
                    else:
                        # Fonction utilitaire existante dans le script: aggregate_player_stats(...)
                        try:
                            aggregated = aggregate_player_stats(pfc_player_df)
                        except Exception:
                            # fallback minimal: moyenne numérique
                            num_cols = pfc_player_df.select_dtypes(include="number").columns.tolist()
                            aggregated = pfc_player_df[num_cols].mean(numeric_only=True).to_frame().T
                            aggregated.insert(0, "Joueuse", resolved_player)

                        # Radar individuel (celui amélioré avec Forces/Faiblesses en dessous)
                        try:
                            fig = create_individual_radar(aggregated)
                            st.pyplot(fig, use_container_width=True)
                        except Exception as e:
                            st.warning(f"Radar indisponible : {e}")

                        # Tableau résumé (mêmes colonnes que d'habitude)
                        with st.expander("Voir les données agrégées"):
                            st.dataframe(aggregated, use_container_width=True)

        # =========================
        # TAB 2 — COMPARAISON EDF
        # =========================
        with tab_edf:
            st.subheader("Comparaison avec le référentiel EDF")

            if not isinstance(edf_source, pd.DataFrame) or edf_source.empty:
                st.warning("Aucune donnée EDF disponible (fichiers EDF_Joueuses / EDF_U19_Match*.csv).")
            else:
                # On essaie de déterminer un poste par défaut via la passerelle
                poste_default = str(info.get("Poste 1", "")).strip()
                postes = []
                for col in ["Poste", "POSTE", "Position", "POS"]:
                    if col in edf_source.columns:
                        postes = sorted(edf_source[col].dropna().astype(str).unique().tolist())
                        poste_col = col
                        break
                else:
                    poste_col = None

                if poste_col is None or not postes:
                    st.warning("Impossible d'identifier la colonne 'Poste' dans le référentiel EDF.")
                else:
                    # Default index if close match
                    idx_default = 0
                    if poste_default:
                        try:
                            from difflib import get_close_matches
                            m = get_close_matches(poste_default, postes, n=1, cutoff=0.4)
                            if m:
                                idx_default = postes.index(m[0])
                        except Exception:
                            pass

                    poste_sel = st.selectbox("Poste EDF de référence", postes, index=idx_default, key="passerelle_poste_edf_sel")

                    # Données pour comparaison EDF (profil joueuse vs référentiel)
                    player_df = prepare_comparison_data(pfc_source, resolved_player, selected_matches=None)

                    edf_line = edf_source[edf_source["Poste"] == poste_sel].copy()
                    if player_df is None or player_df.empty:
                        st.info("Pas assez de données match pour cette joueuse pour calculer un profil.")
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
                        else:
                            st.info("Impossible de générer le radar de comparaison (données insuffisantes).")

        with tab_gps:
            st.subheader("Données physiques (GPS)")

            gps_raw = st.session_state.get("gps_raw_df", pd.DataFrame())
            gps_weekly = st.session_state.get("gps_weekly_df", pd.DataFrame())

            if gps_raw is None or gps_raw.empty:
                st.warning("Aucune donnée GPS brute trouvée.")
            else:
                gps_raw = ensure_date_column(gps_raw)

                # Filtre joueuse
                dgps = gps_raw[gps_raw.get("Player", pd.Series(dtype=str)).astype(str) == str(resolved_player)].copy()
                if dgps.empty:
                    # fallback: normalisation
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
                        st.caption("Filtrage par période / fichier source")
                        d = ensure_date_column(dgps.copy())

                        c1, c2 = st.columns(2)
                        with c1:
                            if d["DATE"].notna().sum() == 0:
                                st.info("Aucune date exploitable (colonne 'Activity Date' / 'DATE' / date dans le nom du fichier).")
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
                                    st.caption("Vue hebdomadaire (somme par semaine ISO) — optionnelle")
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
                                "Date de référence (MD) — le graphique prend les 7 jours précédents",
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
                                    # On conserve l'amélioration "très visuelle" + possibilité de choisir les courbes
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
    .stApp { background: linear-gradient(135deg, #002B5C 0%, #002B5C 100%); color: white; }
    .main .block-container { background: linear-gradient(135deg, #003A58 0%, #0047AB 100%);
    border-radius: 10px; padding: 20px; color: white; }
    .stButton>button { background-color: #0078D4; color: white; border-radius: 5px; border: none; padding: 8px 16px; }
    .stSelectbox>div>div, .stMultiselect>div>div { background-color: #003A58; color: white; border-radius: 5px; border: 1px solid #0078D4; }
    .stMetric { background-color: rgba(0, 71, 171, 0.4); border-radius: 5px; padding: 10px; color: white; }
    .stDataFrame table { color: white !important; }
    </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <div style="background: linear-gradient(135deg, #002B5C 0%, #0047AB 100%);
            color: white; padding: 2rem; border-radius: 10px; margin-bottom: 2rem;
            text-align: center; position: relative;">
    <img src="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png" alt="Paris FC Logo"
         style="position:absolute; left:1rem; top:50%; transform:translateY(-50%); width:120px; opacity:0.9;">
    <h1 style="margin:0; font-size:3rem; font-weight:bold;">Paris FC - Centre de Formation Féminin</h1>
    <p style="margin-top:.5rem; font-size:1.2rem;">Data Center</p>
    </div>
    """,
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

    pfc_kpi, edf_kpi = collect_data()
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)


if __name__ == "__main__":
    main()





