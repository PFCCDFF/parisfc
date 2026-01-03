# ============================================================
# PARIS FC - DATA CENTER (Streamlit)
# - PFC Matchs (CSV): stats + temps de jeu via segments Duration
# - EDF U19: comparaison vs référentiel EDF (moyenne par poste)
# - Référentiel noms: "Noms Prénoms Paris FC.xlsx"
# - GPS Entraînement: fichiers "GF1 ... .xls/.xlsx" (lecture simple)
# ============================================================

import os
import io
import re
import unicodedata
import warnings
from typing import Dict, List, Optional, Set, Tuple
from difflib import get_close_matches
from datetime import datetime

import numpy as np
import pandas as pd

import streamlit as st
from streamlit_option_menu import option_menu

from mplsoccer import PyPizza, Radar, FontManager, grid

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

warnings.filterwarnings("ignore")

# =========================
# CONFIG
# =========================
DATA_FOLDER = "data"
PASSERELLE_FOLDER = "data/passerelle"

# Dossiers Drive
DRIVE_MAIN_FOLDER_ID = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
DRIVE_PASSERELLE_FOLDER_ID = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"
DRIVE_GPS_FOLDER_ID = "1v4Iit4JlEDNACp2QWQVrP89j66zBqMFH"

# Fichiers attendus
PERMISSIONS_FILENAME = "Classeurs permissions streamlit.xlsx"
EDF_JOUEUSES_FILENAME = "EDF_Joueuses.xlsx"
PASSERELLE_FILENAME = "Liste Joueuses Passerelles.xlsx"
REFERENTIEL_FILENAME = "Noms Prénoms Paris FC.xlsx"

# Colonnes "poste" dans les lignes match (lineups)
POST_COLS = ['ATT', 'DCD', 'DCG', 'DD', 'DG', 'GB', 'MCD', 'MCG', 'MD', 'MDef', 'MG']

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

def _first_existing_col(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def add_creativite_kpis(pfc_kpi: pd.DataFrame) -> pd.DataFrame:
    """Ajoute uniquement les KPI:
    - Créativité 1 = (Passe dans dernier 1/3 + 2*Passe Décisive) / Passes totales
    - Créativité 2 = (Création de Deséquilibre joueuse) / (Création de Deséquilibre équipe sur le match)
    Sans modifier l'affichage ou les autres calculs.
    """
    if pfc_kpi is None or pfc_kpi.empty:
        return pfc_kpi

    df = pfc_kpi.copy()

    col_last_third = _first_existing_col(df, [
        "Passe dans dernier 1/3", "Passe dans le dernier 1/3", "Passe dernier 1/3",
        "Entrée dernier 1/3", "Entree dernier 1/3",
    ])
    col_assist = _first_existing_col(df, [
        "Passe Décisive", "Passe décisive", "Passes décisives", "Passes decisives", "Assists",
    ])
    col_pass_total = _first_existing_col(df, [
        "Passes totales", "Passe", "Passes tentées", "Passes tentees", "Passes",
    ])
    col_imbalance = _first_existing_col(df, [
        "Création de Deséquilibre", "Création de Déséquilibre", "Creation de Desequilibre",
        "Déséquilibres créés", "Desequilibres crees",
    ])

    def _as_count(series):
        if series is None:
            return pd.Series(0.0, index=df.index)
        if pd.api.types.is_numeric_dtype(series):
            return pd.to_numeric(series, errors="coerce").fillna(0).astype(float)
        s = series.astype(str).str.strip()
        return ((s != "") & (s.str.lower() != "nan")).astype(float)

    last_third = _as_count(df[col_last_third]) if col_last_third else pd.Series(0.0, index=df.index)
    assists = _as_count(df[col_assist]) if col_assist else pd.Series(0.0, index=df.index)
    passes_total = _as_count(df[col_pass_total]) if col_pass_total else pd.Series(0.0, index=df.index)

    denom = passes_total.replace(0, np.nan)
    df["Créativité 1"] = (((last_third + 2.0 * assists) / denom).fillna(0) * 100).clip(0, 100)

    if col_imbalance:
        imbalance = _as_count(df[col_imbalance])
        match_key = _first_existing_col(df, ["Timeline", "Match", "Match_ID", "ID Match", "Adversaire", "Date"])
        if match_key:
            team_total = df.groupby(match_key)[col_imbalance].transform(lambda x: _as_count(x).sum())
            df["Créativité 2"] = ((imbalance / team_total.replace(0, np.nan)).fillna(0) * 100).clip(0, 100)
        else:
            df["Créativité 2"] = 0.0
    else:
        df["Créativité 2"] = 0.0

    return df

def nettoyer_nom_joueuse(nom):
    if not isinstance(nom, str):
        nom = str(nom) if nom is not None else ""
    s = nom.strip().upper()
    s = (s.replace("É", "E").replace("È", "E").replace("Ê", "E")
           .replace("À", "A").replace("Ù", "U")
           .replace("Î", "I").replace("Ï", "I")
           .replace("Ô", "O").replace("Ö", "O")
           .replace("Â", "A").replace("Ä", "A")
           .replace("Ç", "C"))
    s = " ".join(s.split())
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    if len(parts) > 1 and parts[0] == parts[1]:
        return parts[0]
    return s

def nettoyer_nom_equipe(nom: str) -> str:
    if nom is None:
        return ""
    s = str(nom).strip().upper()

    # Supprimer accents
    s = (s.replace("É","E").replace("È","E").replace("Ê","E")
           .replace("À","A").replace("Ù","U")
           .replace("Î","I").replace("Ï","I")
           .replace("Ô","O").replace("Ö","O")
           .replace("Â","A").replace("Ä","A")
           .replace("Ç","C"))

    # Cas "LOSC, LOSC" (ou "XXXX, YYYY") : on prend le 1er token et on dédoublonne si répété
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        if len(parts) >= 2 and parts[0] == parts[1]:
            s = parts[0]
        else:
            s = parts[0]

    s = " ".join(s.split())
    return s


def looks_like_player(name: str) -> bool:
    n = nettoyer_nom_joueuse(str(name)) if name is not None else ""
    if not n or n in {"NAN", "NONE", "NULL"}:
        return False
    if any(tok in n for tok in BAD_TOKENS):
        return False
    # Evite "PFC" / "LE MANS" etc si présent
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


# =========================
# EXCEL READER
# =========================
def read_excel_auto(path: str, sheet_name=0) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xls":
        return pd.read_excel(path, sheet_name=sheet_name, engine="xlrd")
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


# =========================
# GOOGLE DRIVE
# =========================
def authenticate_google_drive():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType, modifiedTime, size)"
    ).execute()
    return results.get("files", [])

def download_file(service, file_id, file_name, output_folder, mime_type=None):
    os.makedirs(output_folder, exist_ok=True)
    final_path = os.path.join(output_folder, file_name)
    tmp_path = final_path + ".tmp"

    # Google Sheet -> export xlsx
    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
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

        path = download_file(service, candidate["id"], candidate["name"], DATA_FOLDER, mime_type=candidate.get("mimeType"))

        # retry once if corrupted
        try:
            _ = read_excel_auto(path)
        except Exception:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
            path = download_file(service, candidate["id"], candidate["name"], DATA_FOLDER, mime_type=candidate.get("mimeType"))

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

        # sécurité si dict (plusieurs feuilles)
        if isinstance(permissions_df, dict):
            if len(permissions_df) == 0:
                return {}
            permissions_df = list(permissions_df.values())[0]

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
                "player": player
            }
        return permissions

    except Exception as e:
        st.error(f"Erreur chargement permissions: {e}")
        return {}

def download_google_drive():
    service = authenticate_google_drive()
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(PASSERELLE_FOLDER, exist_ok=True)

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

    # GPS folder
    try:
        gps_files = list_files_in_folder(service, DRIVE_GPS_FOLDER_ID)
        for f in gps_files:
            is_sheet = f.get("mimeType") == "application/vnd.google-apps.spreadsheet"
            if f["name"].endswith((".xlsx", ".xls")) or is_sheet:
                download_file(service, f["id"], f["name"], DATA_FOLDER, mime_type=f.get("mimeType"))
    except Exception as e:
        st.warning(f"Impossible de télécharger les fichiers GPS: {e}")


# =========================
# REFERENTIEL NOMS
# =========================
def build_referentiel_players(ref_path: str) -> Tuple[Set[str], Dict[str, str]]:
    ref = read_excel_auto(ref_path)

    # sécurité si dict
    if isinstance(ref, dict):
        if len(ref) == 0:
            raise ValueError("Référentiel vide (aucune feuille lisible).")
        ref = list(ref.values())[0]

    if not isinstance(ref, pd.DataFrame) or ref.empty:
        raise ValueError("Référentiel illisible ou vide.")

    cols = {str(c).strip().upper(): c for c in ref.columns}
    col_nom = cols.get("NOM")
    col_pre = cols.get("PRÉNOM") or cols.get("PRENOM")

    if not col_nom or not col_pre:
        cols_norm = {normalize_str(c): c for c in ref.columns}
        col_nom = col_nom or cols_norm.get("nom")
        col_pre = col_pre or cols_norm.get("prenom") or cols_norm.get("prénom")

    if not col_nom or not col_pre:
        raise ValueError(f"Référentiel: colonnes NOM/Prénom introuvables: {ref.columns.tolist()}")

    ref = ref.copy()
    ref["CANON"] = (ref[col_nom].astype(str) + " " + ref[col_pre].astype(str)).apply(nettoyer_nom_joueuse)
    ref_set = set(ref["CANON"].dropna().unique().tolist())

    alias_to_canon: Dict[str, str] = {}
    for canon in ref_set:
        alias_to_canon[canon] = canon
        parts = canon.split()
        if len(parts) >= 2:
            prenom = parts[-1]
            nom = " ".join(parts[:-1])
            alias_to_canon[nettoyer_nom_joueuse(f"{prenom} {nom}")] = canon
            alias_to_canon[nettoyer_nom_joueuse(f"{nom}, {prenom}")] = canon
            alias_to_canon[nettoyer_nom_joueuse(f"{nom} {prenom[0]}.")] = canon
            alias_to_canon[nettoyer_nom_joueuse(f"{nom} {prenom[0]}")] = canon

    return ref_set, alias_to_canon

def map_player_name(raw_name: str,
                    ref_set: Set[str],
                    alias_to_canon: Dict[str, str],
                    fuzzy_cutoff: float = 0.93) -> Tuple[str, str, str]:
    if raw_name is None:
        return "", "unmatched", "empty"

    candidates = split_if_comma(raw_name)
    cleaned = [nettoyer_nom_joueuse(c) for c in candidates if c]

    for c in cleaned:
        if c in ref_set:
            return c, "exact", str(raw_name)
        if c in alias_to_canon:
            return alias_to_canon[c], "alias", str(raw_name)

    for c in cleaned:
        best = get_close_matches(c, list(ref_set), n=1, cutoff=fuzzy_cutoff)
        if best:
            return best[0], "fuzzy", str(raw_name)

    fallback = cleaned[0] if cleaned else nettoyer_nom_joueuse(str(raw_name))
    return fallback, "unmatched", str(raw_name)

def normalize_players_in_df(df: pd.DataFrame,
                            cols: List[str],
                            ref_set: Set[str],
                            alias_to_canon: Dict[str, str],
                            filename: str,
                            report: List[dict],
                            fuzzy_cutoff: float = 0.93) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        new_vals = []
        for v in out[col].tolist():
            mapped, status, raw = map_player_name(v, ref_set, alias_to_canon, fuzzy_cutoff=fuzzy_cutoff)
            if status in {"fuzzy", "unmatched"} and str(v).strip():
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
    # Heuristique:
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
    """
    Calcule le temps de jeu des joueuses à partir des segments Duration.
    Correction clé : prise en compte correcte des lignes adversaires
    (LOSC / Transition def LOSC / LOSC, LOSC).
    """
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

    # 🔑 garder lignes PFC + adversaire
    m = m[m["Row_team"].isin({home_clean, away_clean})].copy()
    if m.empty:
        return pd.DataFrame()

    # Détection unité Duration
    unit = infer_duration_unit(m["Duration"])

    def to_seconds(x):
        try:
            x = float(x)
        except Exception:
            return 0.0
        if x <= 0:
            return 0.0
        return x * 60.0 if unit == "minutes" else x

    played_seconds = {}

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

    df = pd.DataFrame({
        "Player": list(played_seconds.keys()),
        "Temps de jeu (en minutes)": [v / 60.0 for v in played_seconds.values()]
    })

    return df.sort_values("Temps de jeu (en minutes)", ascending=False).reset_index(drop=True)


# =========================
# STATS ACTIONS
# =========================
def players_shots(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()
    ps, p_on, p_goals = {}, {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Tir" in action:
            player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
            ps[player] = ps.get(player, 0) + action.count("Tir")
            if "Tir" in joueurs.columns:
                status = joueurs.iloc[i].get("Tir", None)
                if isinstance(status, str):
                    if "Tir Cadré" in status or "But" in status:
                        p_on[player] = p_on.get(player, 0) + status.count("Tir Cadré") + status.count("But")
                    if "But" in status:
                        p_goals[player] = p_goals.get(player, 0) + 1
    if not ps:
        return pd.DataFrame()
    return pd.DataFrame({
        "Player": list(ps.keys()),
        "Tirs": list(ps.values()),
        "Tirs cadrés": [p_on.get(p, 0) for p in ps],
        "Buts": [p_goals.get(p, 0) for p in ps],
    }).sort_values(by="Tirs", ascending=False)

def players_passes(joueurs):
    if joueurs is None or joueurs.empty or "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        return pd.DataFrame()
    short_, long_ = {}, {}
    ok_s, ok_l = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Passe" in action:
            player = nettoyer_nom_joueuse(str(joueurs.iloc[i].get("Row", "")))
            passe = joueurs.iloc[i].get("Passe", None) if "Passe" in joueurs.columns else None
            if isinstance(passe, str):
                if "Courte" in passe:
                    short_[player] = short_.get(player, 0) + passe.count("Courte")
                    if "Réussie" in passe:
                        ok_s[player] = ok_s.get(player, 0) + passe.count("Réussie")
                if "Longue" in passe:
                    long_[player] = long_.get(player, 0) + passe.count("Longue")
                    if "Réussie" in passe:
                        ok_l[player] = ok_l.get(player, 0) + passe.count("Réussie")
    if not short_:
        return pd.DataFrame()
    df = pd.DataFrame({
        "Player": list(short_.keys()),
        "Passes courtes": [short_.get(p, 0) for p in short_],
        "Passes longues": [long_.get(p, 0) for p in short_],
        "Passes réussies (courtes)": [ok_s.get(p, 0) for p in short_],
        "Passes réussies (longues)": [ok_l.get(p, 0) for p in short_],
    })
    df["Passes"] = df["Passes courtes"] + df["Passes longues"]
    df["Passes réussies"] = df["Passes réussies (courtes)"] + df["Passes réussies (longues)"]
    df["Pourcentage de passes réussies"] = (df["Passes réussies"] / df["Passes"] * 100).fillna(0)
    return df.sort_values(by="Passes courtes", ascending=False)

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
    df = pd.DataFrame({
        "Player": list(drb.keys()),
        "Dribbles": list(drb.values()),
        "Dribbles réussis": [drb_ok.get(p, 0) for p in drb],
    })
    df["Pourcentage de dribbles réussis"] = (df["Dribbles réussis"] / df["Dribbles"] * 100).fillna(0)
    return df.sort_values(by="Dribbles", ascending=False)

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
    df = pd.DataFrame({
        "Player": list(duels.keys()),
        "Duels défensifs": list(duels.values()),
        "Duels défensifs gagnés": [ok.get(p, 0) for p in duels],
        "Fautes": [faults.get(p, 0) for p in duels],
    })
    df["Pourcentage de duels défensifs gagnés"] = (df["Duels défensifs gagnés"] / df["Duels défensifs"] * 100).fillna(0)
    return df.sort_values(by="Duels défensifs", ascending=False)

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
    return pd.DataFrame({"Player": list(inter.keys()), "Interceptions": list(inter.values())}).sort_values(by="Interceptions", ascending=False)

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
    return pd.DataFrame({"Player": list(losses.keys()), "Pertes de balle": list(losses.values())}).sort_values(by="Pertes de balle", ascending=False)


# =========================
# METRICS / KPI / POSTES
# =========================
def create_metrics(df):
    if df.empty:
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
            mmax = df[cols[0]].max()
            df[metric] = np.where(df[cols[0]] > 0, df[cols[0]] / mmax, 0) if mmax > 0 else 0
        else:
            df[metric] = np.where(df[cols[0]] > 0, df.get(cols[1], 0) / df[cols[0]], 0)

    for metric in required_cols.keys():
        if metric in df.columns:
            df[metric] = (df[metric].rank(pct=True) * 100).fillna(0)
    return df

def create_kpis(df):
    if df.empty:
        return df
    if "Timing" in df.columns and "Force physique" in df.columns:
        df["Rigueur"] = (df["Timing"] + df["Force physique"]) / 2
    if "Intelligence tactique" in df.columns:
        df["Récupération"] = df["Intelligence tactique"]
    tech = [m for m in ["Technique 1", "Technique 2", "Technique 3"] if m in df.columns]
    if tech:
        df["Distribution"] = df[tech].mean(axis=1)
    if "Explosivité" in df.columns and "Prise de risque" in df.columns:
        df["Percussion"] = (df["Explosivité"] + df["Prise de risque"]) / 2
    if "Précision" in df.columns and "Sang-froid" in df.columns:
        df["Finition"] = (df["Précision"] + df["Sang-froid"]) / 2
    return df

def create_poste(df):
    if df.empty:
        return df
    required = ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition"]
    if not all(k in df.columns for k in required):
        return df
    df["Défenseur central"] = (df["Rigueur"] * 5 + df["Récupération"] * 5 + df["Distribution"] * 5 + df["Percussion"] * 1 + df["Finition"] * 1) / 17
    df["Défenseur latéral"] = (df["Rigueur"] * 3 + df["Récupération"] * 3 + df["Distribution"] * 3 + df["Percussion"] * 3 + df["Finition"] * 3) / 15
    df["Milieu défensif"] = (df["Rigueur"] * 4 + df["Récupération"] * 4 + df["Distribution"] * 4 + df["Percussion"] * 2 + df["Finition"] * 2) / 16
    df["Milieu relayeur"] = (df["Rigueur"] * 3 + df["Récupération"] * 3 + df["Distribution"] * 3 + df["Percussion"] * 3 + df["Finition"] * 3) / 15
    df["Milieu offensif"] = (df["Rigueur"] * 2 + df["Récupération"] * 2 + df["Distribution"] * 2 + df["Percussion"] * 4 + df["Finition"] * 4) / 14
    df["Attaquant"] = (df["Rigueur"] * 1 + df["Récupération"] * 1 + df["Distribution"] * 1 + df["Percussion"] * 5 + df["Finition"] * 5) / 13
    return df


# =========================
# CREATE DATA (PFC/EDF)
# =========================
def create_data(match, joueurs, is_edf, home_team=None, away_team=None):
    if is_edf:
        if "Player" not in joueurs.columns or "Temps de jeu" not in joueurs.columns or "Poste" not in joueurs.columns:
            return pd.DataFrame()
        df_duration = pd.DataFrame({
            "Player": joueurs["Player"].apply(nettoyer_nom_joueuse),
            "Temps de jeu (en minutes)": pd.to_numeric(joueurs["Temps de jeu"], errors="coerce").fillna(0),
            "Poste": joueurs["Poste"]
        })
        dfs = [df_duration]
    else:
        if not home_team or not away_team:
            return pd.DataFrame()
        df_duration = players_duration(match, home_team=home_team, away_team=away_team)
        dfs = [df_duration]

    for func in [players_shots, players_passes, players_dribbles, players_defensive_duels, players_interceptions, players_ball_losses]:
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
    df = df[(df.iloc[:, 1:] != 0).any(axis=1)]

    if "Temps de jeu (en minutes)" in df.columns:
        df = df[df["Temps de jeu (en minutes)"] >= 10]

    df = create_metrics(df)
    df = create_kpis(df)
    df = create_poste(df)
    return df

def filter_data_by_player(df, player_name):
    if not player_name or df.empty or "Player" not in df.columns:
        return df
    pn = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    out = tmp[tmp["Player_clean"] == pn].copy()
    out.drop(columns=["Player_clean"], inplace=True, errors="ignore")
    return out

def prepare_comparison_data(df, player_name, selected_matches=None):
    if df.empty or "Player" not in df.columns:
        return pd.DataFrame()
    pn = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    filtered = tmp[tmp["Player_clean"] == pn].copy()
    if selected_matches and "Adversaire" in filtered.columns:
        filtered = filtered[filtered["Adversaire"].isin(selected_matches)]
    if filtered.empty:
        return pd.DataFrame()

    aggregated = filtered.groupby("Player").agg({
        "Temps de jeu (en minutes)": "sum",
        "Buts": "sum",
    }).join(
        filtered.groupby("Player").mean(numeric_only=True).drop(
            columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
        )
    ).reset_index()

    return safe_int_numeric_only(aggregated)


# =========================
# GPS (lecture simple)
# =========================
def list_excel_files_local() -> List[str]:
    if not os.path.exists(DATA_FOLDER):
        return []
    return [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if f.lower().endswith((".xlsx", ".xls"))]

def standardize_gps_columns(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

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

    return out

def load_gps_raw(ref_set: Set[str], alias_to_canon: Dict[str, str]) -> pd.DataFrame:
    files = list_excel_files_local()
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
            dfp = read_excel_auto(p)
            dfp = standardize_gps_columns(dfp, os.path.basename(p))
            dfp["__source_file"] = os.path.basename(p)
            frames.append(dfp)
        except Exception:
            continue

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty or "NOM" not in df.columns:
        return pd.DataFrame()

    mapped = []
    for v in df["NOM"].astype(str).tolist():
        m, _, _ = map_player_name(v, ref_set, alias_to_canon, fuzzy_cutoff=0.93)
        mapped.append(m)
    df["Player"] = mapped

    for c in ["Durée", "Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)", "CHARGE", "RPE"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["DATE"] = pd.to_datetime(df.get("DATE", pd.NaT), errors="coerce")
    return df

def compute_gps_weekly_metrics(df_gps: pd.DataFrame) -> pd.DataFrame:
    if df_gps.empty:
        return pd.DataFrame()

    d = df_gps.copy()
    if "SEMAINE" not in d.columns:
        d["SEMAINE"] = d["DATE"].dt.isocalendar().week.astype("Int64")

    if "Durée" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée"], errors="coerce")
    else:
        d["Durée_min"] = np.nan

    if "CHARGE" not in d.columns and "RPE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["RPE"], errors="coerce").fillna(0) * d["Durée_min"].fillna(0)

    agg_map = {}
    for col in ["Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)", "CHARGE"]:
        if col in d.columns:
            agg_map[col] = "sum"

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
# COLLECT DATA
# =========================
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

    ref_set, alias_to_canon = build_referentiel_players(ref_path)
    name_report: List[dict] = []

    pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

    fichiers = [f for f in os.listdir(DATA_FOLDER)
                if f.endswith((".csv", ".xlsx", ".xls")) and normalize_str(f) != normalize_str(PERMISSIONS_FILENAME)]

    if selected_season and selected_season != "Toutes les saisons":
        fichiers = [f for f in fichiers if f"{selected_season}" in f]

    # GPS
    gps_raw = load_gps_raw(ref_set, alias_to_canon)
    gps_week = compute_gps_weekly_metrics(gps_raw)
    st.session_state["gps_weekly_df"] = gps_week
    st.session_state["gps_raw_df"] = gps_raw

    # ======================================================
    # EDF (référentiel par poste) ✅ ROBUSTE via référentiel
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
                # 1) Canoniser les joueuses EDF via référentiel
                edf_j = edf_joueuses.copy()
                edf_j["Player_raw"] = edf_j["Player"].astype(str)

                canon_list = []
                for v in edf_j["Player_raw"].tolist():
                    canon, _, _ = map_player_name(v, ref_set, alias_to_canon, fuzzy_cutoff=0.93)
                    canon_list.append(canon)
                edf_j["PlayerCanon"] = canon_list

                # Temps de jeu en minutes (sécurise)
                edf_j["Temps de jeu"] = pd.to_numeric(edf_j["Temps de jeu"], errors="coerce").fillna(0)

                # 2) Charger tous les EDF_U19_Match*.csv
                matchs_csv = [f for f in fichiers if f.startswith("EDF_U19_Match") and f.endswith(".csv")]
                all_edf_rows = []

                for csv_file in matchs_csv:
                    d = pd.read_csv(os.path.join(DATA_FOLDER, csv_file))
                    if "Row" not in d.columns:
                        continue

                    # Canoniser les noms venant du match EDF (Row)
                    d = d.copy()
                    d["Player_raw"] = d["Row"].astype(str)

                    canon_d = []
                    for v in d["Player_raw"].tolist():
                        canon, _, _ = map_player_name(v, ref_set, alias_to_canon, fuzzy_cutoff=0.93)
                        canon_d.append(canon)
                    d["PlayerCanon"] = canon_d

                    # Merge sur PlayerCanon (robuste)
                    d = d.merge(
                        edf_j[["PlayerCanon", "Poste", "Temps de jeu"]],
                        on="PlayerCanon",
                        how="left"
                    )

                    # Si Poste est très vide => diagnostic (mismatch référentiel)
                    if "Poste" not in d.columns or d["Poste"].isna().mean() > 0.9:
                        # On skip ce fichier mais on garde un warning utile
                        st.warning(f"EDF: merge faible sur {csv_file} (Poste NaN {d['Poste'].isna().mean():.0%}). Vérifie les noms EDF vs référentiel.")
                        continue

                    # 3) Construire un DF EDF "propre" :
                    # - df_duration depuis edf_j (unique par joueuse)
                    df_duration = edf_j[["PlayerCanon", "Poste", "Temps de jeu"]].copy()
                    df_duration = df_duration.rename(columns={"PlayerCanon": "Player"})
                    df_duration["Temps de jeu (en minutes)"] = df_duration["Temps de jeu"]
                    df_duration = df_duration.drop(columns=["Temps de jeu"])

                    # - stats actions depuis d (mais en forçant Row à être Player canon)
                    joueurs_edf = d.copy()
                    joueurs_edf["Row"] = joueurs_edf["PlayerCanon"]
                    joueurs_edf["Player"] = joueurs_edf["PlayerCanon"]

                    dfs = [df_duration]

                    for func in [players_shots, players_passes, players_dribbles,
                                players_defensive_duels, players_interceptions, players_ball_losses]:
                        try:
                            res = func(joueurs_edf)
                            if res is not None and not res.empty:
                                dfs.append(res)
                        except Exception:
                            pass

                    # Merge final
                    df_edf = dfs[0]
                    for other in dfs[1:]:
                        df_edf = df_edf.merge(other, on="Player", how="outer")

                    df_edf.fillna(0, inplace=True)

                    # Filtre temps de jeu >= 10
                    df_edf = df_edf[df_edf["Temps de jeu (en minutes)"] >= 10].copy()

                    # Metrics/KPIs/Postes
                    df_edf = create_metrics(df_edf)
                    df_edf = create_kpis(df_edf)
                    df_edf = create_poste(df_edf)

                    if not df_edf.empty and "Poste" in df_edf.columns:
                        all_edf_rows.append(df_edf)

                # 4) Référentiel = moyenne par poste
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
                data, cols=cols_to_fix, ref_set=ref_set, alias_to_canon=alias_to_canon,
                filename=filename, report=name_report
            )

            # Détection équipe PFC / ADV
            row_vals = data["Row"].astype(str).str.strip()
            unique_rows = set(row_vals.dropna().unique().tolist())
            equipe_pfc = "PFC" if "PFC" in unique_rows else str(parts[0]).strip()

            # ADV par valeur la plus fréquente autre que PFC
            counts = row_vals.value_counts()
            candidates = [k for k in counts.index.tolist() if k and k != equipe_pfc]
            if not candidates:
                continue
            equipe_adv = candidates[0]

            # Match = lignes équipes (filtre robuste via Row_clean)
            d2 = data.copy()
            d2["Row_clean"] = d2["Row"].astype(str).apply(nettoyer_nom_equipe)
            home_clean = nettoyer_nom_equipe(equipe_pfc)
            away_clean = nettoyer_nom_equipe(equipe_adv)

            match = d2[d2["Row_clean"].isin({home_clean, away_clean})].copy()
            if match.empty:
                continue

            # Joueurs = reste (hors events)
            mask_joueurs = ~d2["Row_clean"].str.contains("CORNER|COUP-FRANC|COUP FRANC|PENALTY|CARTON", na=False)
            mask_joueurs &= ~d2.index.isin(match.index)
            joueurs = d2[mask_joueurs].copy()
            if joueurs.empty:
                joueurs = pd.DataFrame(columns=["Row", "Action"])

            df = create_data(match, joueurs, False, home_team=equipe_pfc, away_team=equipe_adv)
            if df.empty:
                continue

            # Normalisation per-90 (sauf temps de jeu / pourcentages)
            if "Temps de jeu (en minutes)" in df.columns:
                num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != "Temps de jeu (en minutes)"]
                for idx, r in df.iterrows():
                    tp = safe_float(r.get("Temps de jeu (en minutes)", np.nan), default=np.nan)
                    if np.isnan(tp) or tp <= 0:
                        continue
                    scale = 90.0 / tp
                    for col in num_cols:
                        if "Pourcentage" in col:
                            continue
                        df.loc[idx, col] = r[col] * scale

            df = create_metrics(df)
            df = create_kpis(df)
            df = create_poste(df)

            adversaire = equipe_adv if equipe_pfc == "PFC" else equipe_pfc
            df.insert(1, "Adversaire", f"{adversaire} - {journee}")
            df.insert(2, "Journée", journee)
            df.insert(3, "Catégorie", categorie)
            df.insert(4, "Date", date)

            pfc_kpi = pd.concat([pfc_kpi, df], ignore_index=True)

        except Exception:
            continue

    st.session_state["name_report_df"] = pd.DataFrame(name_report).drop_duplicates() if name_report else pd.DataFrame()
    pfc_kpi = add_creativite_kpis(pfc_kpi)
    return pfc_kpi, edf_kpi
# =========================
# RADARS
# =========================
def create_individual_radar(df):
    if df.empty or "Player" not in df.columns:
        return None
    columns_to_plot = [
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
        "Créativité 1",
        "Créativité 2",
    ]
    available = [c for c in columns_to_plot if c in df.columns]
    if not available:
        return None

    colors = ["#6A7CD9", "#00BFFE", "#FF9470", "#F27979", "#BFBFBF"] * 2
    player = df.iloc[0]

    pizza = PyPizza(
        params=available,
        background_color="#002B5C",
        straight_line_color="#FFFFFF",
        last_circle_color="#FFFFFF",
    )
    fig, _ = pizza.make_pizza(
        figsize=(3, 3),
        values=[player[c] for c in available],
        slice_colors=colors[:len(available)],
        kwargs_values=dict(
            color="#FFFFFF",
            fontsize=3.5,
            bbox=dict(edgecolor="#FFFFFF", facecolor="#002B5C", boxstyle="round, pad=0.5", lw=1),
        ),
        kwargs_params=dict(color="#FFFFFF", fontsize=3.5, fontproperties="monospace"),
    )
    fig.set_facecolor("#002B5C")
    return fig

def create_comparison_radar(df, player1_name=None, player2_name=None):
    if df.empty or len(df) < 2:
        return None

    metrics = [
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
        "Créativité 1",
        "Créativité 2",
    ]
    available = [m for m in metrics if m in df.columns]
    if len(available) < 2:
        return None

    low, high = (0,) * len(available), (100,) * len(available)
    radar = Radar(available, low, high, num_rings=4, ring_width=1, center_circle_radius=1)

    URL1 = "https://raw.githubusercontent.com/googlefonts/roboto/main/src/hinted/Roboto-Thin.ttf"
    URL2 = "https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/RobotoSlab%5Bwght%5D.ttf"
    robotto_thin, robotto_bold = FontManager(URL1), FontManager(URL2)

    fig, axs = grid(
        figheight=14,
        grid_height=0.915,
        title_height=0.06,
        endnote_height=0.025,
        title_space=0,
        endnote_space=0,
        grid_key="radar",
    )

    radar.setup_axis(ax=axs["radar"], facecolor="None")
    radar.draw_circles(ax=axs["radar"], facecolor="#0c4281", edgecolor="#0c4281", lw=1.5)

    v1 = df.iloc[0][available].values
    v2 = df.iloc[1][available].values

    radar.draw_radar_compare(
        v1, v2,
        ax=axs["radar"],
        kwargs_radar={"facecolor": "#00f2c1", "alpha": 0.6},
        kwargs_compare={"facecolor": "#d80499", "alpha": 0.6},
    )

    radar.draw_range_labels(ax=axs["radar"], fontsize=18, color="#fcfcfc", fontproperties=robotto_thin.prop)
    radar.draw_param_labels(ax=axs["radar"], fontsize=18, color="#fcfcfc", fontproperties=robotto_thin.prop)

    p1 = player1_name if player1_name else df.iloc[0]["Player"]
    p2 = player2_name if player2_name else df.iloc[1]["Player"]

    axs["title"].text(0.01, 0.65, p1, fontsize=18, color="#01c49d", fontproperties=robotto_bold.prop, ha="left", va="center")
    axs["title"].text(0.99, 0.65, p2, fontsize=18, color="#d80499", fontproperties=robotto_bold.prop, ha="right", va="center")

    fig.set_facecolor("#002B5C")
    return fig


# =========================
# UI
# =========================
def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
    st.sidebar.markdown(
        "<div style='display:flex;justify-content:center;'><img src='https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png' width='100'></div>",
        unsafe_allow_html=True
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

    # Filtre par joueuse si profil associé
    if player_name and not pfc_kpi.empty and "Player" in pfc_kpi.columns:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)

    options = ["Statistiques", "Comparaison", "Données Physiques", "Joueuses Passerelles"]
    if check_permission(user_profile, "all", permissions):
        options.insert(2, "Gestion")

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=options,
            icons=["graph-up-arrow", "people", "gear", "activity", "people-fill"][:len(options)],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={
                "container": {"padding": "5!important", "background-color": "#002A48"},
                "icon": {"color": "#0078D4", "font-size": "18px"},
                "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#003A58"},
                "nav-link-selected": {"background-color": "#0078D4", "color": "white"}
            }
        )

    # =====================
    # STATISTIQUES
    # =====================
    if page == "Statistiques":
        st.header("Statistiques")

        if pfc_kpi.empty:
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

        aggregated = filtered.groupby("Player").agg({
            "Temps de jeu (en minutes)": "sum",
            "Buts": "sum",
        }).join(
            filtered.groupby("Player").mean(numeric_only=True).drop(
                columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
            )
        ).reset_index()

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
            needed = ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition"]
            if all(k in aggregated.columns for k in needed):
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1: st.metric("Rigueur", f"{int(aggregated['Rigueur'].iloc[0])}/100")
                with col2: st.metric("Récupération", f"{int(aggregated['Récupération'].iloc[0])}/100")
                with col3: st.metric("Distribution", f"{int(aggregated['Distribution'].iloc[0])}/100")
                with col4: st.metric("Percussion", f"{int(aggregated['Percussion'].iloc[0])}/100")
                with col5: st.metric("Finition", f"{int(aggregated['Finition'].iloc[0])}/100")
            else:
                st.info("KPIs non disponibles sur cette sélection.")

        with tab3:
            poste_cols = ["Défenseur central", "Défenseur latéral", "Milieu défensif", "Milieu relayeur", "Milieu offensif", "Attaquant"]
            if all(c in aggregated.columns for c in poste_cols):
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                with col1: st.metric("DC", f"{int(aggregated['Défenseur central'].iloc[0])}/100")
                with col2: st.metric("DL", f"{int(aggregated['Défenseur latéral'].iloc[0])}/100")
                with col3: st.metric("MD", f"{int(aggregated['Milieu défensif'].iloc[0])}/100")
                with col4: st.metric("MR", f"{int(aggregated['Milieu relayeur'].iloc[0])}/100")
                with col5: st.metric("MO", f"{int(aggregated['Milieu offensif'].iloc[0])}/100")
                with col6: st.metric("ATT", f"{int(aggregated['Attaquant'].iloc[0])}/100")
            else:
                st.info("Notes de poste non disponibles sur cette sélection.")

    # =====================
    # COMPARAISON ✅ EDF RESTAURÉ
    # =====================
    elif page == "Comparaison":
        st.header("Comparaison")

        if pfc_kpi.empty:
            st.warning("Aucune donnée PFC.")
            return

        # --- Helpers locaux (petits outils UI)
        def _player_selector(label: str, key: str):
            return st.selectbox(label, sorted(pfc_kpi["Player"].dropna().unique().tolist()), key=key)

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
            key="compare_mode_select"
        )

        st.divider()

        if mode == "Joueuse vs elle-même (matchs)":
            if player_name:
                p = player_name
                st.info(f"Joueuse : {p}")
            else:
                p = _player_selector("Joueuse", key="self_player")

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
                    (pfc_kpi["Player"].apply(nettoyer_nom_joueuse) == nettoyer_nom_joueuse(p)) &
                    (pfc_kpi["Adversaire"] == mlabel)
                ].copy()
                if md.empty:
                    continue

                agg = md.groupby("Player").agg({
                    "Temps de jeu (en minutes)": "sum",
                    "Buts": "sum",
                }).join(
                    md.groupby("Player").mean(numeric_only=True).drop(
                        columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
                    )
                ).reset_index()

                agg = safe_int_numeric_only(agg)
                if not agg.empty:
                    agg["Player"] = f"{p} ({mlabel})"
                    comp_rows.append(agg)

            if not comp_rows or len(comp_rows) < 2:
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
                    key="p2_other_player"
                )
            else:
                p1 = _player_selector("Joueuse A", key="p1_other_player")
                p2 = st.selectbox(
                    "Joueuse B",
                    [p for p in sorted(pfc_kpi["Player"].dropna().unique().tolist()) if nettoyer_nom_joueuse(p) != nettoyer_nom_joueuse(p1)],
                    key="p2_other_player"
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
                    st.warning("Pas assez de données pour afficher la comparaison (vérifie les filtres matchs / temps de jeu).")
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
                p = _player_selector("Joueuse", key="edf_player")

            if edf_kpi is None or edf_kpi.empty or "Poste" not in edf_kpi.columns:
                st.warning("Aucune donnée EDF disponible pour la comparaison (EDF_Joueuses.xlsx / EDF_U19_Match*.csv).")
                return

            postes_display = sorted(edf_kpi["Poste"].dropna().astype(str).unique().tolist())
            poste = st.selectbox("Poste (référentiel EDF)", postes_display, key="edf_poste_ref")

            edf_line = edf_kpi[edf_kpi["Poste"] == poste].copy()
            edf_line = edf_line.rename(columns={"Poste": "Player"})
            edf_label = f"EDF {poste}"

            if "Adversaire" in pfc_kpi.columns:
                matches = _matches_for_player(p)
                sel = st.multiselect("Limiter à certains matchs (optionnel)", matches, default=[], key="edf_player_matches")
            else:
                sel = None

            if st.button("Comparer avec le référentiel EDF", key="btn_compare_edf"):
                player_data = _aggregate_player(p, selected_matches=sel if sel else None)

                if player_data.empty or edf_line.empty:
                    st.warning("Pas assez de données pour afficher la comparaison.")
                    return

                players_data = pd.concat([player_data, edf_line], ignore_index=True)
                fig = create_comparison_radar(players_data, player1_name=p, player2_name=edf_label)
                if fig:
                    st.pyplot(fig)
                else:
                    st.warning("Radar indisponible (données insuffisantes sur les métriques).")
    elif page == "Gestion":
        st.header("Gestion des utilisateurs")
        if not check_permission(user_profile, "all", permissions):
            st.error("Accès refusé.")
            return
        users_data = []
        for profile, info in permissions.items():
            users_data.append({
                "Profil": profile,
                "Permissions": ", ".join(info["permissions"]),
                "Joueuse associée": info.get("player", "Aucune"),
            })
        st.dataframe(pd.DataFrame(users_data))

    # =====================
    # DONNEES PHYSIQUES
    # =====================
    elif page == "Données Physiques":
        st.header("📊 Données Physiques")
        gps_weekly = st.session_state.get("gps_weekly_df", pd.DataFrame())

        if gps_weekly.empty:
            st.warning("Aucune donnée GPS hebdo trouvée.")
            return

        all_players = sorted(set(gps_weekly["Player"].dropna().unique().tolist()))
        player_sel = player_name if player_name else st.selectbox("Sélectionnez une joueuse", all_players)
        dfp = gps_weekly[gps_weekly["Player"] == nettoyer_nom_joueuse(player_sel)].copy()

        st.subheader("GPS - Hebdomadaire")
        st.dataframe(dfp.sort_values("SEMAINE"))

    # =====================
    # PASSERELLES
    # =====================
    elif page == "Joueuses Passerelles":
        st.header("🔄 Joueuses Passerelles")
        passerelle_data = load_passerelle_data()
        if not passerelle_data:
            st.warning("Aucune donnée passerelle.")
            return

        selected = st.selectbox("Sélectionnez une joueuse", list(passerelle_data.keys()))
        info = passerelle_data[selected]

        st.subheader("Identité")
        if info.get("Prénom"): st.write(f"**Prénom :** {info['Prénom']}")
        if info.get("Photo"): st.image(info["Photo"], width=150)
        if info.get("Date de naissance"): st.write(f"**Date de naissance :** {info['Date de naissance']}")
        if info.get("Poste 1"): st.write(f"**Poste 1 :** {info['Poste 1']}")
        if info.get("Poste 2"): st.write(f"**Poste 2 :** {info['Poste 2']}")
        if info.get("Pied Fort"): st.write(f"**Pied Fort :** {info['Pied Fort']}")
        if info.get("Taille"): st.write(f"**Taille :** {info['Taille']}")


# =========================
# MAIN
# =========================
def main():
    st.set_page_config(page_title="Paris FC - Centre de Formation Féminin", layout="wide")

    st.markdown("""
    <style>
      .stApp { background: linear-gradient(135deg, #002B5C 0%, #002B5C 100%); color: white; }
      .main .block-container { background: linear-gradient(135deg, #003A58 0%, #0047AB 100%);
        border-radius: 10px; padding: 20px; color: white; }
      .stButton>button { background-color: #0078D4; color: white; border-radius: 5px; border: none; padding: 8px 16px; }
      .stSelectbox>div>div, .stMultiselect>div>div { background-color: #003A58; color: white; border-radius: 5px; border: 1px solid #0078D4; }
      .stMetric { background-color: rgba(0, 71, 171, 0.4); border-radius: 5px; padding: 10px; color: white; }
      .stDataFrame table { color: white !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="background: linear-gradient(135deg, #002B5C 0%, #0047AB 100%);
                color: white; padding: 2rem; border-radius: 10px; margin-bottom: 2rem;
                text-align: center; position: relative;">
        <img src="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png" alt="Paris FC Logo"
             style="position:absolute; left:1rem; top:50%; transform:translateY(-50%); width:120px; opacity:0.9;">
        <h1 style="margin:0; font-size:3rem; font-weight:bold;">Paris FC - Centre de Formation Féminin</h1>
        <p style="margin-top:.5rem; font-size:1.2rem;">Data Center</p>
    </div>
    """, unsafe_allow_html=True)

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
