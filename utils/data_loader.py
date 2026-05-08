# utils/data_loader.py
import os
import io
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dash_extensions.cache import cache
from config import *
from utils.helpers import normalize_str

# Chemin vers le fichier de credentials Google Drive
CREDENTIALS_PATH = "credentials.json"

def get_drive_service():
    """Retourne un service Google Drive authentifié."""
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return build("drive", "v3", credentials=creds)

@cache.memoize(timeout=3600)  # Cache pour 1 heure
def load_passerelle_data():
    """Charge les données des joueuses Passerelles depuis Google Drive ou localement."""
    file_path = os.path.join(PASSERELLE_FOLDER, PASSERELLE_FILENAME)
    if os.path.exists(file_path):
        return pd.read_excel(file_path, engine='openpyxl')

    # Télécharger depuis Google Drive
    service = get_drive_service()
    request = service.files().export(
        fileId=DRIVE_PASSERELLE_FOLDER_ID,
        mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    df = pd.read_excel(fh, engine='openpyxl')

    # Sauvegarder localement
    os.makedirs(PASSERELLE_FOLDER, exist_ok=True)
    df.to_excel(file_path, index=False, engine='openpyxl')

    return df

@cache.memoize(timeout=3600)
def load_photo_mapping():
    """Charge le mapping des photos depuis un fichier JSON local."""
    if os.path.exists(PHOTO_MAPPING_PATH):
        with open(PHOTO_MAPPING_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

@cache.memoize(timeout=3600)
def load_gps_name_map():
    """Charge le mapping des noms GPS."""
    if os.path.exists(GPS_NAME_MAP_PATH):
        with open(GPS_NAME_MAP_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def download_file_from_drive(file_id: str, destination_folder: str, filename: str) -> str:
    """Télécharge un fichier depuis Google Drive et le sauvegarde localement."""
    service = get_drive_service()
    request = service.files().get(fileId=file_id, alt="media")
    filepath = os.path.join(destination_folder, filename)
    os.makedirs(destination_folder, exist_ok=True)

    fh = io.FileIO(filepath, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return filepath
