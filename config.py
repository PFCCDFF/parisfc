# config.py
# Configuration globale pour l'application Paris FC Dash

# =========================
# DOSSIERS LOCAUX
# =========================
DATA_FOLDER = "data"
PASSERELLE_FOLDER = f"{DATA_FOLDER}/passerelle"
GPS_FOLDER = f"{DATA_FOLDER}/gps"
PHOTOS_FOLDER = f"{DATA_FOLDER}/photos"
LOGOS_FOLDER = f"{DATA_FOLDER}/logos"
OBJECTIFS_FOLDER = f"{DATA_FOLDER}/objectifs"

# =========================
# GOOGLE DRIVE
# =========================
# IDs des dossiers Google Drive (à remplacer par les tiens)
DRIVE_MAIN_FOLDER_ID = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
DRIVE_PASSERELLE_FOLDER_ID = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"
DRIVE_GPS_FOLDER_ID = "1v4Iit4JlEDNACp2QWQVrP89j66zBqMFH"
DRIVE_PHOTOS_FOLDER_ID = "1h-BwepZc96K7VpidPiy8FEqNiE10GLdE"
DRIVE_GPS_MATCH_FOLDER_ID = "1jzLW_jR5sMtsP4lOb4mN9mJlthw3pvbu"
DRIVE_LOGOS_FOLDER_ID = "1TCKyVOHzKynm6Z1fhKnNUKYDcN7NhMCj"

# =========================
# FICHIERS
# =========================
PASSERELLE_FILENAME = "Liste Joueuses Passerelles.xlsx"
EDF_JOUEUSES_FILENAME = "EDF_Joueuses.xlsx"
PERMISSIONS_FILENAME = "Classeurs permissions streamlit.xlsx"
EVAL_FILENAME = "Auto-évaluation de votre match (post-match).xlsx"
OBJECTIFS_EVAL_FILENAME = "Evaluations Objectifs.csv"
PHOTO_MAPPING_PATH = f"{DATA_FOLDER}/photo_mapping.json"
GPS_NAME_MAP_PATH = f"{DATA_FOLDER}/gps_name_map.json"

# =========================
# COLONNES
# =========================
POST_COLS = ["ATT", "DCD", "DCG", "DD", "DG", "GB", "MCD", "MCG", "MD", "MDef", "MG"]
BAD_TOKENS = {"CORNER", "COUP-FRANC", "COUP FRANC", "PENALTY", "CARTON", "CARTONS"}
GPS_GF1_PREFIX = "GF1"
