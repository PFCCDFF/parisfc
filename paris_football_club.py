# ============================================================
# PARIS FC - DATA CENTER (Streamlit)
# Version corrigée : gestion des dates, erreurs, et types
# ============================================================
import os
import io
import re
import unicodedata
import warnings
import logging
from typing import Dict, List, Optional, Set, Tuple, Union
from difflib import get_close_matches, SequenceMatcher
from datetime import datetime, timedelta
import time
import json
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from mplsoccer import PyPizza, Radar, FontManager, grid
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# --- Configuration ---
warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("parisfc.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Dossiers et fichiers
DATA_FOLDER = "data"
PASSERELLE_FOLDER = os.path.join(DATA_FOLDER, "passerelle")
GPS_FOLDER = os.path.join(DATA_FOLDER, "gps")
GPS_SYNC_STATE_PATH = os.path.join(DATA_FOLDER, "gps_sync_state.json")

# Constantes Drive (à remplacer)
DRIVE_MAIN_FOLDER_ID = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
DRIVE_PASSERELLE_FOLDER_ID = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"
DRIVE_GPS_FOLDER_ID = "1v4Iit4JlEDNACp2QWQVrP89j66zBqMFH"

# Fichiers attendus
PERMISSIONS_FILENAME = "Classeurs permissions streamlit.xlsx"
EDF_JOUEUSES_FILENAME = "EDF_Joueuses.xlsx"
PASSERELLE_FILENAME = "Liste Joueuses Passerelles.xlsx"
REFERENTIEL_FILENAME = "Noms Prénoms Paris FC.xlsx"

# --- Exceptions ---
class ParisFCError(Exception):
    """Base class for custom exceptions."""
    pass

class GPSsyncError(ParisFCError):
    """Erreur lors de la synchronisation GPS."""
    pass

class DataNormalizationError(ParisFCError):
    """Erreur lors de la normalisation des données."""
    pass

# --- 1. UTILS (fonctions génériques) ---
def normalize_str(s: str) -> str:
    """Normalise une chaîne : minuscules, sans accents, espaces normalisés."""
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.split()).lower()

def safe_float(x, default=np.nan) -> float:
    """Convertit une valeur en float, avec gestion des erreurs."""
    try:
        return float(x) if not pd.isna(x) else default
    except Exception:
        return default

def safe_datetime(d) -> Union[pd.Timestamp, None]:
    """Convertit une valeur en datetime, avec gestion des erreurs."""
    try:
        if pd.isna(d) or d is None:
            return None
        if isinstance(d, (pd.Timestamp, datetime)):
            return d
        return pd.to_datetime(d, errors="coerce")
    except Exception:
        return None

def build_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """Construit un fichier Excel en mémoire (bytes)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            sheet_name = str(name)[:31]
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output.getvalue()

# --- 2. NORMALISATION DES NOMS ---
PARTICLES = {"DE", "DU", "DES", "D", "DA", "DI", "DEL", "DELA", "DELLA", "LE", "LA", "LES"}

def normalize_name_raw(s: str) -> str:
    """Normalise un nom de joueuse (accents, virgules, tirets)."""
    if not s:
        return ""
    s = strip_accents_upper(s)
    s = s.replace(",", " ").replace("’", "'")
    s = re.sub(r"[^A-Z' -]", " ", s).replace("-", " ")
    return " ".join(s.split())

def strip_accents_upper(s: str) -> str:
    """Supprime les accents et met en majuscules."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper()

def tokens_name(s: str) -> List[str]:
    """Découpe un nom en tokens (ex: 'DUPONT ALICE' -> ['DUPONT', 'ALICE'])."""
    s = normalize_name_raw(s)
    if not s:
        return []
    toks = s.split()
    out, i = [], 0
    while i < len(toks):
        if toks[i] == "D" and i + 1 < len(toks):
            out.append("D" + toks[i + 1])
            i += 2
        else:
            out.append(toks[i])
            i += 1
    return out

def compact_name(s: str) -> str:
    """Forme compacte sans espaces (ex: 'DUPONTALICE')."""
    return re.sub(r"[^A-Z]", "", strip_accents_upper(s))

def map_player_name(
    raw_name: str,
    ref_set: Set[str],
    alias_to_canon: Dict[str, str],
    tokenkey_to_canon: Dict[str, str],
    compact_to_canon: Dict[str, str],
    first_to_canons: Dict[str, Set[str]],
    last_to_canons: Dict[str, Set[str]],
    cutoff_fuzzy: float = 0.90,
) -> Tuple[str, str, str]:
    """Mappe un nom brut vers sa forme canonique."""
    if not raw_name or str(raw_name).upper() in {"NAN", "NONE", "NULL"}:
        return "", "unmatched", str(raw_name) if raw_name else ""

    cleaned = normalize_name_raw(str(raw_name))
    if not cleaned:
        return "", "unmatched", str(raw_name)

    if cleaned in ref_set:
        return cleaned, "exact", raw_name
    if cleaned in alias_to_canon:
        return alias_to_canon[cleaned], "alias", raw_name

    toks = tokens_name(cleaned)
    if toks:
        key = " ".join(sorted(toks))
        if key in tokenkey_to_canon:
            return tokenkey_to_canon[key], "token_set", raw_name

    comp = compact_name(cleaned)
    if comp in compact_to_canon:
        return compact_to_canon[comp], "compact", raw_name

    if toks and len(toks) == 1:
        t = toks[0]
        cand = first_to_canons.get(t, set()) | last_to_canons.get(t, set())
        if cand:
            best = get_close_matches(cleaned, list(cand), n=1, cutoff=cutoff_fuzzy)
            if best:
                return best[0], "single_token", raw_name

    best = get_close_matches(cleaned, list(ref_set), n=1, cutoff=cutoff_fuzzy)
    if best:
        return best[0], "fuzzy", raw_name

    return cleaned, "unmatched", raw_name

# --- 3. GOOGLE DRIVE (synchronisation) ---
def authenticate_google_drive():
    """Authentification auprès de Google Drive."""
    scopes = ["https://www.googleapis.com/auth/drive"]
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return build("drive", "v3", credentials=creds)

def _load_gps_state() -> dict:
    """Charge l'état de la synchronisation GPS."""
    if os.path.exists(GPS_SYNC_STATE_PATH):
        try:
            with open(GPS_SYNC_STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erreur chargement état GPS: {e}")
    return {"last_modifiedTime": None, "folders_failed": {}}

def _save_gps_state(state: dict):
    """Sauvegarde l'état de la synchronisation GPS."""
    os.makedirs(DATA_FOLDER, exist_ok=True)
    with open(GPS_SYNC_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _execute_with_retry(call, max_tries: int = 3):
    """Exécute une requête Drive avec retry en cas d'erreur."""
    for attempt in range(max_tries):
        try:
            return call.execute()
        except HttpError as e:
            if e.resp.status in {429, 500, 502, 503, 504} and attempt < max_tries - 1:
                time.sleep(2 ** attempt)
                continue
            raise

def list_files_in_folder_paged(service, folder_id: str, q_extra: str = "", page_size: int = 200) -> List[dict]:
    """Liste les fichiers d'un dossier Drive (avec pagination)."""
    q = f"'{folder_id}' in parents and trashed=false"
    if q_extra:
        q += f" and ({q_extra})"

    out, page_token = [], None
    while True:
        req = service.files().list(
            q=q, fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
            pageSize=page_size, pageToken=page_token, supportsAllDrives=True, includeItemsFromAllDrives=True,
        )
        resp = _execute_with_retry(req)
        out.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out

def sync_gps_from_drive_autonomous():
    """Synchronise les fichiers GPS depuis Drive (incrémental)."""
    try:
        service = authenticate_google_drive()
        state = _load_gps_state()
        last_m = state.get("last_modifiedTime")
        newest_modified = last_m

        def is_gps_candidate(f: dict) -> bool:
            name = (f.get("name") or "").lower()
            mt = f.get("mimeType", "")
            return (
                mt != "application/vnd.google-apps.folder"
                and (name.endswith(".csv") or mt == "application/vnd.google-apps.spreadsheet")
                and any(token in name for token in ["gf1", "seance", "gps"])
            )

        for folder_id in [DRIVE_GPS_FOLDER_ID]:
            try:
                q_extra = f"modifiedTime > '{last_m}'" if last_m else ""
                items = list_files_in_folder_paged(service, folder_id, q_extra=q_extra)

                for f in items:
                    if not is_gps_candidate(f):
                        continue
                    fid, name, mt = f["id"], f.get("name", ""), f.get("mimeType", "")
                    try:
                        if mt == "application/vnd.google-apps.spreadsheet":
                            export_sheet_to_csv_local(service, fid, name)
                        elif name.lower().endswith(".csv"):
                            download_drive_csv_to_local(service, fid, name)
                    except Exception as e:
                        logger.warning(f"Échec téléchargement {name}: {e}")

                    mtime = f.get("modifiedTime")
                    if mtime and (newest_modified is None or mtime > newest_modified):
                        newest_modified = mtime

            except Exception as e:
                logger.error(f"Échec dossier {folder_id}: {e}")
                state.setdefault("folders_failed", {})[folder_id] = time.time()

        state["last_modifiedTime"] = newest_modified
        _save_gps_state(state)
        logger.info("Synchronisation GPS terminée.")

    except Exception as e:
        logger.error(f"Échec synchronisation GPS: {e}")
        raise GPSsyncError(f"Échec synchronisation GPS: {e}")

def download_drive_csv_to_local(service, file_id: str, file_name: str) -> str:
    """Télécharge un CSV depuis Drive."""
    request = service.files().get_media(fileId=file_id)
    final_path = os.path.join(GPS_FOLDER, file_name)
    os.makedirs(GPS_FOLDER, exist_ok=True)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    with open(final_path, "wb") as f:
        fh.seek(0)
        f.write(fh.read())
    return final_path

def export_sheet_to_csv_local(service, file_id: str, file_name: str) -> str:
    """Exporte un Google Sheet en CSV."""
    request = service.files().export_media(fileId=file_id, mimeType="text/csv")
    final_path = os.path.join(GPS_FOLDER, os.path.splitext(file_name)[0] + ".csv")
    os.makedirs(GPS_FOLDER, exist_ok=True)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    with open(final_path, "wb") as f:
        fh.seek(0)
        f.write(fh.read())
    return final_path

# --- 4. CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=3600, show_spinner="Chargement des permissions...")
def load_permissions() -> Dict:
    """Charge les permissions depuis Drive."""
    try:
        service = authenticate_google_drive()
        files = list_files_in_folder_paged(service, DRIVE_MAIN_FOLDER_ID)
        permissions_path = next(
            (f for f in files if normalize_str(f["name"]) == normalize_str(PERMISSIONS_FILENAME)),
            None,
        )
        if not permissions_path:
            raise FileNotFoundError("Fichier permissions introuvable.")

        path = download_drive_csv_to_local(service, permissions_path["id"], permissions_path["name"])
        permissions_df = pd.read_excel(path)

        permissions = {}
        for _, row in permissions_df.iterrows():
            profile = str(row.get("Profil", "")).strip()
            if not profile:
                continue
            permissions[profile] = {
                "password": str(row.get("Mot de passe", "")).strip(),
                "permissions": [p.strip() for p in str(row.get("Permissions", "")).split(",") if p.strip()],
                "player": str(row.get("Joueuse", "")),
            }
        return permissions
    except Exception as e:
        logger.error(f"Erreur chargement permissions: {e}")
        return {}

@st.cache_data(ttl=3600)
def load_passerelle_data() -> Dict:
    """Charge les données des joueuses passerelles."""
    passerelle_file = os.path.join(PASSERELLE_FOLDER, PASSERELLE_FILENAME)
    if not os.path.exists(passerelle_file):
        return {}
    try:
        df = pd.read_excel(passerelle_file)
        return {
            row["Nom"]: {
                "Prénom": row.get("Prénom", ""),
                "Photo": row.get("Photo", ""),
                "Date de naissance": row.get("Date de naissance", ""),
                "Poste 1": row.get("Poste 1", ""),
                "Poste 2": row.get("Poste 2", ""),
                "Pied Fort": row.get("Pied Fort", ""),
                "Taille": row.get("Taille", ""),
            }
            for _, row in df.iterrows()
        }
    except Exception as e:
        logger.error(f"Erreur chargement passerelles: {e}")
        return {}

# --- 5. GESTION DES DATES (corrigée) ---
def ensure_date_column(df: pd.DataFrame, date_col: str = "DATE") -> pd.DataFrame:
    """Garantit une colonne de dates valide (avec fallback)."""
    if df is None or df.empty:
        return df

    d = df.copy()

    # 1) Si la colonne existe déjà et est de type datetime, on la garde
    if date_col in d.columns:
        d[date_col] = d[date_col].apply(safe_datetime)
        if d[date_col].notna().any():
            return d

    # 2) Sinon, on cherche une colonne candidate (Activity Date, Date, etc.)
    for col in ["Activity Date", "activity date", "Date", "date"]:
        if col in d.columns:
            d[date_col] = d[col].apply(safe_datetime)
            if d[date_col].notna().any():
                return d

    # 3) Fallback: extraire la date du nom de fichier (si __source_file existe)
    if "__source_file" in d.columns:
        def extract_date_from_filename(fn: str) -> Optional[pd.Timestamp]:
            if not fn or not isinstance(fn, str):
                return None
            match = re.search(r"(\d{2}\.\d{2}\.\d{4})", fn)
            if match:
                try:
                    return pd.to_datetime(match.group(1), format="%d.%m.%Y", errors="coerce")
                except Exception:
                    return None
            return None

        d[date_col] = d["__source_file"].apply(extract_date_from_filename)

    # 4) Si toujours NaT, on met une date par défaut (aujourd'hui)
    if date_col in d.columns and d[date_col].isna().all():
        d[date_col] = pd.Timestamp(datetime.now().date())

    return d

def gps_last_7_days_summary(df_raw: pd.DataFrame, player_canon: str, end_date: Optional[pd.Timestamp] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Retourne un résumé des données GPS sur 7 jours."""
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    d = ensure_date_column(df_raw)
    d = d[d["Player"].astype(str) == player_canon].copy()
    d = d.dropna(subset=["DATE"]).copy()

    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    if end_date is None:
        end_dt = pd.to_datetime(d["DATE"].max()).normalize()
    else:
        end_dt = pd.to_datetime(end_date).normalize()

    start_dt = end_dt - timedelta(days=6)
    end_inclusive = end_dt + timedelta(days=1) - timedelta(seconds=1)

    df_7j = d[(d["DATE"] >= start_dt) & (d["DATE"] <= end_inclusive)].copy()
    if df_7j.empty:
        return pd.DataFrame(), pd.DataFrame()

    metric_cols = [c for c in [
        "Durée", "Durée_min", "Distance (m)", "Distance HID (>13 km/h)",
        "Distance HID (>19 km/h)", "CHARGE", "RPE"
    ] if c in df_7j.columns]

    means = df_7j[metric_cols].apply(pd.to_numeric, errors="coerce").mean(numeric_only=True)
    sums = df_7j[metric_cols].apply(pd.to_numeric, errors="coerce").sum(numeric_only=True)

    summary = pd.DataFrame([{
        "Player": player_canon,
        "Période": f"{start_dt.date()} → {end_dt.date()}",
        **{f"Moyenne 7j - {k}": float(v) for k, v in means.items()},
        **{f"Total 7j - {k}": float(v) for k, v in sums.items()},
        "Nb jours avec données (7j)": int(df_7j["DATE"].dt.date.nunique()),
        "Nb lignes": int(len(df_7j)),
    }])

    return df_7j, summary

# --- 6. STATISTIQUES ET KPIs ---
def create_individual_radar(df: pd.DataFrame):
    """Crée un radar individuel pour une joueuse."""
    if df is None or df.empty:
        return None

    metrics = [
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
        "Créativité 1", "Créativité 2",
    ]
    available = [m for m in metrics if m in df.columns]
    if not available:
        return None

    player = df.iloc[0]
    colors = ["#6A7CD9", "#00BFFE", "#FF9470", "#F27979", "#BFBFBF"] * 3
    pizza = PyPizza(
        params=available, background_color="#002B5C",
        straight_line_color="#FFFFFF", last_circle_color="#FFFFFF",
    )
    fig, _ = pizza.make_pizza(
        figsize=(3, 3),
        values=[player[m] for m in available],
        slice_colors=colors[:len(available)],
        kwargs_values={"color": "#FFFFFF", "fontsize": 3.5},
        kwargs_params={"color": "#FFFFFF", "fontsize": 3.5},
    )
    fig.set_facecolor("#002B5C")
    return fig

# --- 7. UI STREAMLIT ---
def script_streamlit(pfc_kpi: pd.DataFrame, edf_kpi: pd.DataFrame, permissions: Dict, user_profile: str):
    st.sidebar.markdown(
        "<div style='display:flex;justify-content:center;'><img src='https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png' width='100'></div>",
        unsafe_allow_html=True,
    )

    player_name = permissions.get(user_profile, {}).get("player", "")
    st.sidebar.title(f"Connecté: {user_profile}")
    if player_name:
        st.sidebar.write(f"Joueuse: {player_name}")

    # --- Export Excel ---
    with st.sidebar.expander("📤 Export Excel", expanded=False):
        if st.button("Générer le fichier Excel"):
            sheets = {
                "PFC_Detail": pfc_kpi if not pfc_kpi.empty else pd.DataFrame(),
                "EDF_Referentiel": edf_kpi if not edf_kpi.empty else pd.DataFrame(),
            }
            st.session_state["export_xlsx_bytes"] = build_excel_bytes(sheets)
            st.success("Fichier Excel généré avec succès !")

    # --- Pages ---
    options = ["Statistiques", "Comparaison", "Données Physiques", "Joueuses Passerelles"]
    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=options,
            icons=["graph-up-arrow", "people", "activity", "people-fill"],
            default_index=0,
        )

    # --- Statistiques ---
    if page == "Statistiques":
        st.header("Statistiques")
        if pfc_kpi is None or pfc_kpi.empty:
            st.warning("Aucune donnée disponible.")
            return

        if player_name:
            df_player = pfc_kpi[pfc_kpi["Player"] == player_name]
        else:
            player_sel = st.selectbox("Choisissez une joueuse", pfc_kpi["Player"].unique())
            df_player = pfc_kpi[pfc_kpi["Player"] == player_sel]

        if df_player.empty:
            st.warning("Aucune donnée pour cette joueuse.")
            return

        fig = create_individual_radar(df_player)
        if fig:
            st.pyplot(fig)

        kpis = ["Timing", "Force physique", "Intelligence tactique", "Technique 1", "Technique 2"]
        cols = st.columns(len(kpis))
        for col, kpi in zip(cols, kpis):
            with col:
                if kpi in df_player.columns:
                    st.metric(kpi, f"{int(df_player[kpi].iloc[0])}/100")

    # --- Données Physiques ---
    elif page == "Données Physiques":
        st.header("Données Physiques (GPS)")
        gps_raw = pd.DataFrame()  # À remplacer par tes données GPS réelles
        if gps_raw.empty:
            st.warning("Aucune donnée GPS disponible.")
            return

        gps_raw = ensure_date_column(gps_raw)
        all_players = sorted(gps_raw["Player"].dropna().unique())
        if not all_players:
            st.warning("Aucune joueuse dans les données GPS.")
            return

        player_sel = st.selectbox("Joueuse", all_players)
        end_date_ui = st.date_input(
            "Date de fin (fenêtre = 7 jours précédents)",
            value=datetime.now().date(),
        )

        df_7j, summary = gps_last_7_days_summary(gps_raw, player_sel, end_date=pd.Timestamp(end_date_ui))
        if not summary.empty:
            st.dataframe(summary, use_container_width=True)
        else:
            st.info("Aucune donnée sur cette période.")

    # --- Joueuses Passerelles ---
    elif page == "Joueuses Passerelles":
        st.header("Joueuses Passerelles")
        passerelle_data = load_passerelle_data()
        if not passerelle_data:
            st.warning("Aucune donnée passerelle.")
            return

        selected = st.selectbox("Sélectionnez une joueuse", list(passerelle_data.keys()))
        info = passerelle_data[selected]
        st.subheader("Identité")
        if info.get("Prénom"):
            st.write(f"**Prénom:** {info['Prénom']}")
        if info.get("Photo"):
            st.image(info["Photo"], width=150)

# --- 8. MAIN ---
def main():
    st.set_page_config(page_title="Paris FC - Data Center", layout="wide")
    st.markdown(
        """
        <style>
        .stApp { background: linear-gradient(135deg, #002B5C 0%, #0047AB 100%); color: white; }
        .main .block-container { background: #003A58; border-radius: 10px; padding: 20px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Authentification
    permissions = load_permissions()
    if not permissions:
        st.error("Impossible de charger les permissions.")
        return

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        with st.form("login_form"):
            username = st.text_input("Nom d'utilisateur")
            password = st.text_input("Mot de passe", type="password")
            if st.form_submit_button("Valider"):
                if username in permissions and password == permissions[username]["password"]:
                    st.session_state.authenticated = True
                    st.session_state.user_profile = username
                    st.rerun()
                else:
                    st.error("Identifiants incorrects.")
        return

    # Synchronisation GPS
    try:
        sync_gps_from_drive_autonomous()
    except GPSsyncError as e:
        st.warning(f"Synchronisation GPS échouée: {e}")

    # Chargement des données (exemple simplifié)
    pfc_kpi = pd.DataFrame({
        "Player": ["DUPONT Alice", "MARTIN Béatrice"],
        "Timing": [85, 78],
        "Force physique": [90, 88],
    })
    edf_kpi = pd.DataFrame({
        "Poste": ["Défenseur", "Milieu"],
        "Timing": [80, 85],
    })

    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)

if __name__ == "__main__":
    main()
