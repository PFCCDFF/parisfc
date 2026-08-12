"""
drive_utils.py

Accès Google Drive pour sync_drive_to_supabase.py — équivalent hors
Streamlit de la logique déjà présente dans paris_football_club.py
(authenticate_google_drive, list_files_in_folder_paged, download_drive_csv_to_local).

Différence clé : l'app lit les identifiants via st.secrets ; ce module
les lit directement depuis un fichier credentials.json local (déjà
présent sur le VPS, à côté de l'app).
"""

import json
import logging
import os
import time
from typing import List, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

logger = logging.getLogger("sync_drive_to_supabase.drive")

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "credentials.json")
SYNC_STATE_PATH = os.path.join(os.path.dirname(__file__), "data", "supabase_sync_state.json")

# Mêmes dossiers Drive que l'app (voir constantes DRIVE_* dans paris_football_club.py)
DRIVE_MAIN_FOLDER_ID = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
DRIVE_GPS_FOLDER_ID = "1v4Iit4JlEDNACp2QWQVrP89j66zBqMFH"
DRIVE_GPS_MATCH_FOLDER_ID = "1jzLW_jR5sMtsP4lOb4mN9mJlthw3pvbu"

LOCAL_TMP_FOLDER = os.path.join(os.path.dirname(__file__), "data", "sync_tmp")


def authenticate_google_drive():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    return build("drive", "v3", credentials=creds)


def _is_retryable_http_error(e: Exception) -> bool:
    if not isinstance(e, HttpError):
        return False
    status = getattr(e.resp, "status", None)
    return status in (429, 500, 502, 503, 504)


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
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
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


def walk_drive_folders(service, root_folder_id: str, failed: dict):
    stack = [root_folder_id]
    seen = set()
    now = time.time()

    while stack:
        fid = stack.pop()
        if fid in seen:
            continue
        seen.add(fid)

        last_fail = failed.get(fid)
        if last_fail and (now - float(last_fail)) < 600:
            continue

        yield fid

        try:
            subfolders = list_files_in_folder_paged(
                service, fid,
                q_extra="mimeType='application/vnd.google-apps.folder'",
                page_size=200,
            )
            for sf in subfolders:
                stack.append(sf["id"])
        except Exception:
            failed[fid] = time.time()
            continue


def download_drive_csv_to_local(service, file_id: str, file_name: str, dest_folder: str) -> str:
    request = service.files().get_media(fileId=file_id)
    if not str(file_name).lower().endswith(".csv"):
        file_name = os.path.splitext(str(file_name))[0] + ".csv"

    os.makedirs(dest_folder, exist_ok=True)
    final_path = os.path.join(dest_folder, file_name)

    fh = open(final_path, "wb")
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()

    return final_path


# ------------------------------------------------------------
# État de sync (pour ne retraiter que les fichiers modifiés depuis
# le dernier passage) — équivalent de _load_gps_state/_save_gps_state
# ------------------------------------------------------------

def load_sync_state() -> dict:
    if os.path.exists(SYNC_STATE_PATH):
        try:
            with open(SYNC_STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_modifiedTime": None, "folders_failed": {}}


def save_sync_state(state: dict) -> None:
    os.makedirs(os.path.dirname(SYNC_STATE_PATH), exist_ok=True)
    with open(SYNC_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------
# Point d'entrée : récupère tous les nouveaux CSV (GPS + match +
# tactique) depuis Drive et les télécharge dans un dossier local
# temporaire. Retourne la liste des chemins locaux téléchargés.
# ------------------------------------------------------------

def fetch_new_csv_files() -> List[str]:
    from parsing_utils import is_gps_match_file, is_tactical_file, normalize_str

    service = authenticate_google_drive()
    state = load_sync_state()
    last_m = state.get("last_modifiedTime")
    newest_modified = last_m
    downloaded: List[str] = []

    def is_candidate(f: dict) -> bool:
        name = (f.get("name") or "")
        if f.get("mimeType") == "application/vnd.google-apps.folder":
            return False
        if not name.lower().endswith(".csv"):
            return False
        nname = normalize_str(name)
        if "gf1" in nname or "seance" in nname or "séance" in nname or "gps" in nname:
            return True
        if is_gps_match_file(name) or is_tactical_file(name):
            return True
        return False

    # Dossier GPS (séances + matchs GPS)
    for folder_id in walk_drive_folders(service, DRIVE_GPS_FOLDER_ID, state.setdefault("folders_failed", {})):
        try:
            q_extra = f"modifiedTime > '{last_m}'" if last_m else ""
            items = list_files_in_folder_paged(service, folder_id, q_extra=q_extra)
            for f in items:
                if not is_candidate(f):
                    continue
                path = download_drive_csv_to_local(service, f["id"], f["name"], LOCAL_TMP_FOLDER)
                downloaded.append(path)
                mtime = f.get("modifiedTime")
                if mtime and (newest_modified is None or mtime > newest_modified):
                    newest_modified = mtime
        except Exception:
            logger.exception("Échec lecture dossier GPS Drive %s", folder_id)
            state["folders_failed"][folder_id] = time.time()

    # Dossier GPS Match dédié
    if DRIVE_GPS_MATCH_FOLDER_ID:
        try:
            q_extra = f"modifiedTime > '{last_m}'" if last_m else ""
            items = list_files_in_folder_paged(service, DRIVE_GPS_MATCH_FOLDER_ID, q_extra=q_extra)
            for f in items:
                if not f.get("name", "").lower().endswith(".csv"):
                    continue
                path = download_drive_csv_to_local(service, f["id"], f["name"], LOCAL_TMP_FOLDER)
                downloaded.append(path)
                mtime = f.get("modifiedTime")
                if mtime and (newest_modified is None or mtime > newest_modified):
                    newest_modified = mtime
        except Exception:
            logger.exception("Échec lecture dossier GPS Match Drive")

    # Dossier principal (fichiers tactiques PFC_VS_...)
    try:
        q_extra = f"modifiedTime > '{last_m}'" if last_m else ""
        items = list_files_in_folder_paged(service, DRIVE_MAIN_FOLDER_ID, q_extra=q_extra)
        for f in items:
            if not is_candidate(f):
                continue
            path = download_drive_csv_to_local(service, f["id"], f["name"], LOCAL_TMP_FOLDER)
            downloaded.append(path)
            mtime = f.get("modifiedTime")
            if mtime and (newest_modified is None or mtime > newest_modified):
                newest_modified = mtime
    except Exception:
        logger.exception("Échec lecture dossier principal Drive")

    state["last_modifiedTime"] = newest_modified
    state["folders_failed"] = {
        k: v for k, v in state.get("folders_failed", {}).items()
        if (time.time() - float(v)) < 86400
    }
    save_sync_state(state)

    return downloaded
