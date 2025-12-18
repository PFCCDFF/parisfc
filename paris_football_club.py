# ============================================================
# PARIS FC - DATA CENTER (MATCH + EDF U19 + REFERENTIEL NOMS + GPS)
# Intègre :
# - Téléchargement Drive (CSV/XLS/XLSX + permissions + EDF + référentiel noms + passerelles)
# - Robustesse Unicode (ex: "Prénoms" avec accent combiné)
# - Référentiel "Noms Prénoms Paris FC.xlsx" = source de vérité pour les noms
# - Normalisation noms dans Row + colonnes postes (+ reporting fuzzy/unmatched)
# - Temps de jeu via segments Duration (XI PFC-only sur toutes les lignes match)
# - Fix ValueError astype(int) : conversion uniquement colonnes numériques
# - Onglet Comparaison conservé (matchs + référentiel EDF U19)
# - GPS : lecture "Data GPS", synthèse hebdo + daily, affichage tableaux + graphiques filtrables
# ============================================================

import os
import io
import re
import unicodedata
import warnings
from typing import Dict, List, Optional, Set, Tuple
from difflib import get_close_matches

import numpy as np
import pandas as pd

import streamlit as st
from streamlit_option_menu import option_menu

import matplotlib.pyplot as plt

from mplsoccer import PyPizza, Radar, FontManager, grid

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

warnings.filterwarnings("ignore")

# =========================
# CONFIG / CONSTANTES
# =========================
DATA_FOLDER = "data"
PASSERELLE_FOLDER = "data/passerelle"

DRIVE_MAIN_FOLDER_ID = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
DRIVE_PASSERELLE_FOLDER_ID = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"

PERMISSIONS_FILENAME = "Classeurs permissions streamlit.xlsx"
EDF_JOUEUSES_FILENAME = "EDF_Joueuses.xlsx"
PASSERELLE_FILENAME = "Liste Joueuses Passerelles.xlsx"

# Recherche du référentiel sans dépendre du nom exact (accents combinés)
REFERENTIEL_FILENAME = "Noms Prénoms Paris FC.xlsx"

POST_COLS = ['ATT', 'DCD', 'DCG', 'DD', 'DG', 'GB', 'MCD', 'MCG', 'MD', 'MDef', 'MG']

BAD_TOKENS = {"CORNER", "COUP-FRANC", "COUP FRANC", "PENALTY", "CARTON", "CARTONS", "PFC", "GB", "GARDIENNE", "GARDIEN"}

# GPS
GPS_KEYWORDS = ["gps", "dashboard"]  # on cherche un xlsx qui contient ces mots (normalisés)
GPS_SHEET_RAW = "Data GPS"


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

def nettoyer_nom_joueuse(nom):
    if not isinstance(nom, str):
        return nom
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
    s = (s.replace("É", "E").replace("È", "E").replace("Ê", "E")
           .replace("À", "A").replace("Ù", "U")
           .replace("Î", "I").replace("Ï", "I")
           .replace("Ô", "O").replace("Ö", "O")
           .replace("Â", "A").replace("Ä", "A")
           .replace("Ç", "C"))
    s = " ".join(s.split())
    return s

def looks_like_player(name: str) -> bool:
    n = nettoyer_nom_joueuse(str(name)) if name is not None else ""
    if not n or n in {"NAN", "NONE", "NULL"}:
        return False
    if any(tok in n for tok in BAD_TOKENS):
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
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get("files", [])

def download_file(service, file_id, file_name, output_folder):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    os.makedirs(output_folder, exist_ok=True)
    path = os.path.join(output_folder, file_name)
    with open(path, "wb") as f:
        f.write(fh.getbuffer())

def download_from_folder_by_names(service, folder_id: str, output_folder: str, filenames: Set[str]):
    files = list_files_in_folder(service, folder_id)
    found = set()
    for f in files:
        if f["name"] in filenames:
            download_file(service, f["id"], f["name"], output_folder)
            found.add(f["name"])
    return found

def download_google_drive():
    """
    Télécharge :
    - Tous les fichiers .csv/.xlsx/.xls du dossier principal (inclut le dashboard GPS)
    - Passerelles depuis son dossier
    - Référentiel noms (recherche normalisée)
    """
    service = authenticate_google_drive()
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(PASSERELLE_FOLDER, exist_ok=True)

    files = list_files_in_folder(service, DRIVE_MAIN_FOLDER_ID)
    for f in files:
        if f["name"].endswith((".csv", ".xlsx", ".xls")):
            download_file(service, f["id"], f["name"], DATA_FOLDER)

    files_pass = list_files_in_folder(service, DRIVE_PASSERELLE_FOLDER_ID)
    for f in files_pass:
        if f["name"] == PASSERELLE_FILENAME:
            download_file(service, f["id"], f["name"], PASSERELLE_FOLDER)
            break

    # Référentiel noms : match unicode/accents
    target_norm = normalize_str(REFERENTIEL_FILENAME)
    for f in files:
        if f["name"].endswith((".xlsx", ".xls")) and normalize_str(f["name"]) == target_norm:
            download_file(service, f["id"], f["name"], DATA_FOLDER)
            break

def download_permissions_file():
    try:
        service = authenticate_google_drive()
        found = download_from_folder_by_names(
            service, DRIVE_MAIN_FOLDER_ID, DATA_FOLDER, filenames={PERMISSIONS_FILENAME}
        )
        if PERMISSIONS_FILENAME in found:
            return os.path.join(DATA_FOLDER, PERMISSIONS_FILENAME)
        return find_local_file_by_normalized_name(DATA_FOLDER, PERMISSIONS_FILENAME)
    except Exception as e:
        st.error(f"Erreur téléchargement permissions: {e}")
        return None


# =========================
# REFERENTIEL NOMS
# =========================
def build_referentiel_players(ref_path: str) -> Tuple[Set[str], Dict[str, str]]:
    ref = pd.read_excel(ref_path)
    cols = {c.strip().upper(): c for c in ref.columns}
    col_nom = cols.get("NOM", None)
    col_pre = cols.get("PRÉNOM", cols.get("PRENOM", None))
    if not col_nom or not col_pre:
        raise ValueError(f"Référentiel: colonnes NOM/Prénom introuvables: {ref.columns.tolist()}")

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
            if looks_like_player(mapped):
                new_vals.append(mapped)
            else:
                new_vals.append(v)
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
        df = pd.read_excel(passerelle_file)
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
# PERMISSIONS
# =========================
def load_permissions():
    try:
        permissions_path = download_permissions_file()
        if not permissions_path or not os.path.exists(permissions_path):
            return {}
        permissions_df = pd.read_excel(permissions_path)
        permissions = {}
        for _, row in permissions_df.iterrows():
            profile = str(row.get("Profil", "")).strip()
            if not profile:
                continue
            permissions[profile] = {
                "password": str(row.get("Mot de passe", "")).strip(),
                "permissions": [p.strip() for p in str(row.get("Permissions", "")).split(",")]
                if pd.notna(row.get("Permissions", np.nan)) else [],
                "player": nettoyer_nom_joueuse(str(row.get("Joueuse", "")).strip())
                if pd.notna(row.get("Joueuse", np.nan)) else None
            }
        return permissions
    except Exception as e:
        st.error(f"Erreur chargement permissions: {e}")
        return {}

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
    """
    Colonnes postes = XI PFC même quand Row == adversaire.
    => On crédite le XI lu sur la ligne sur TOUTES les lignes match (PFC + ADV).
    """
    if match is None or match.empty or "Duration" not in match.columns or "Row" not in match.columns:
        return pd.DataFrame()

    available_posts = [p for p in POST_COLS if p in match.columns]
    if not available_posts:
        return pd.DataFrame()

    m = match.copy()
    m["Row_clean"] = m["Row"].astype(str).str.strip()
    m = m[m["Row_clean"].isin({str(home_team).strip(), str(away_team).strip()})].copy()
    if m.empty:
        return pd.DataFrame()

    unit = infer_duration_unit(m["Duration"])

    def to_seconds(d):
        d = safe_float(d, default=np.nan)
        if np.isnan(d) or d <= 0:
            return 0.0
        return d * 60.0 if unit == "minutes" else d

    played_seconds: Dict[str, float] = {}

    for c in ["Start time", "StartTime", "Start", "Time", "Timestamp"]:
        if c in m.columns:
            m = m.sort_values(by=c, ascending=True)
            break

    for _, row in m.iterrows():
        dur = to_seconds(row.get("Duration", 0))
        if dur <= 0:
            continue
        lineup = extract_lineup_from_row(row, available_posts)
        if not lineup:
            continue
        for p in lineup:
            played_seconds[p] = played_seconds.get(p, 0.0) + dur

    if not played_seconds:
        return pd.DataFrame()

    return (pd.DataFrame({
        "Player": list(played_seconds.keys()),
        "Temps de jeu (en minutes)": [v / 60.0 for v in played_seconds.values()],
    }).sort_values("Temps de jeu (en minutes)", ascending=False).reset_index(drop=True))


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
# CREATE DATA (PFC / EDF)
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

def generate_synthesis_excel(pfc_kpi):
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if not pfc_kpi.empty:
                tmp = pfc_kpi.copy()
                tmp.insert(0, "Joueuse", tmp["Player"])
                tmp.to_excel(writer, sheet_name="Synthèse", index=False)
        return output.getvalue()
    except Exception:
        return None


# =========================
# GPS : chargement + synthèses
# =========================
def find_gps_dashboard_file_local() -> Optional[str]:
    if not os.path.exists(DATA_FOLDER):
        return None
    candidates = [f for f in os.listdir(DATA_FOLDER) if f.lower().endswith((".xlsx", ".xls"))]
    for fn in candidates:
        n = normalize_str(fn)
        if all(k in n for k in GPS_KEYWORDS):
            return os.path.join(DATA_FOLDER, fn)
    return None

def load_gps_raw(ref_set: Set[str], alias_to_canon: Dict[str, str]) -> pd.DataFrame:
    gps_path = find_gps_dashboard_file_local()
    if not gps_path or not os.path.exists(gps_path):
        return pd.DataFrame()
    try:
        df = pd.read_excel(gps_path, sheet_name=GPS_SHEET_RAW)
    except Exception:
        return pd.DataFrame()

    required = {"DATE", "SEMAINE", "Type Session", "NOM"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame()

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    mapped = []
    for v in df["NOM"].astype(str).tolist():
        m, _, _ = map_player_name(v, ref_set, alias_to_canon, fuzzy_cutoff=0.93)
        mapped.append(m)
    df["Player"] = mapped

    # cast numériques usuels
    for c in df.columns:
        if isinstance(c, str) and any(k in normalize_str(c) for k in ["distance", "duree", "durée", "charge", "rpe", "acc", "dec", "vitesse", "sprint"]):
            df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

def compute_gps_weekly_metrics(df_gps: pd.DataFrame) -> pd.DataFrame:
    if df_gps.empty:
        return pd.DataFrame()

    d = df_gps[df_gps["Type Session"].astype(str).str.contains("Entrain", case=False, na=False)].copy()
    if d.empty:
        return pd.DataFrame()

    if "Durée" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée"], errors="coerce")
    else:
        d["Durée_min"] = np.nan

    # CHARGE fallback
    if "CHARGE" not in d.columns and "RPE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["RPE"], errors="coerce").fillna(0) * d["Durée_min"].fillna(0)

    agg_map = {}
    # colonnes possibles (selon ton dashboard)
    candidates = [
        "Distance (m)",
        "Distance HID (>13 km/h)",
        "Distance HID (>19 km/h)",
        "Distance par plage de vitesse (>25 km/h)",
        "# Acc/Dec",
        "CHARGE",
    ]
    for col in candidates:
        if col in d.columns:
            agg_map[col] = "sum"

    out = d.groupby(["Player", "SEMAINE"], as_index=False).agg(agg_map)

    # Distance relative
    if "Distance (m)" in d.columns:
        dur_week = d.groupby(["Player", "SEMAINE"], as_index=False)["Durée_min"].sum().rename(columns={"Durée_min": "_dur"})
        out = out.merge(dur_week, on=["Player", "SEMAINE"], how="left")
        out["Distance relative (m/min)"] = np.where(out["_dur"] > 0, out["Distance (m)"] / out["_dur"], 0.0)
        out.rename(columns={"_dur": "Durée hebdo (min)"}, inplace=True)
    else:
        out["Durée hebdo (min)"] = 0.0
        out["Distance relative (m/min)"] = 0.0

    # Monotonie / Contrainte + ACWR (hebdo)
    if "CHARGE" in d.columns and "CHARGE" in out.columns:
        d_day = d.groupby(["Player", "SEMAINE", "DATE"], as_index=False)["CHARGE"].sum()
        daily_stats = d_day.groupby(["Player", "SEMAINE"])["CHARGE"].agg(["mean", "std"]).reset_index()
        daily_stats["Monotonie"] = np.where(daily_stats["std"] > 0, daily_stats["mean"] / daily_stats["std"], np.nan)
        out = out.merge(daily_stats[["Player", "SEMAINE", "Monotonie"]], on=["Player", "SEMAINE"], how="left")
        out["Contrainte"] = out["CHARGE"] * out["Monotonie"]

        out = out.sort_values(["Player", "SEMAINE"])
        out["Aigue"] = out["CHARGE"]
        out["Chronique"] = out.groupby("Player")["Aigue"].transform(lambda s: s.rolling(4, min_periods=1).mean())
        out["ACWR"] = np.where(out["Chronique"] > 0, out["Aigue"] / out["Chronique"], np.nan)
    else:
        out["Monotonie"] = np.nan
        out["Contrainte"] = np.nan
        out["Aigue"] = np.nan
        out["Chronique"] = np.nan
        out["ACWR"] = np.nan

    # renommages HID
    out.rename(columns={
        "Distance HID (>13 km/h)": "Distance HID >13 (m)",
        "Distance HID (>19 km/h)": "Distance HID >19 (m)",
        "Distance par plage de vitesse (>25 km/h)": "Distance >25 (m)",
    }, inplace=True)

    return out

def compute_gps_daily_metrics(df_gps: pd.DataFrame) -> pd.DataFrame:
    if df_gps.empty:
        return pd.DataFrame()

    d = df_gps[df_gps["Type Session"].astype(str).str.contains("Entrain", case=False, na=False)].copy()
    if d.empty:
        return pd.DataFrame()

    d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    d = d.dropna(subset=["DATE"])

    if "Durée" in d.columns:
        d["Durée_min"] = pd.to_numeric(d["Durée"], errors="coerce")
    else:
        d["Durée_min"] = np.nan

    if "CHARGE" not in d.columns and "RPE" in d.columns:
        d["CHARGE"] = pd.to_numeric(d["RPE"], errors="coerce").fillna(0) * d["Durée_min"].fillna(0)

    agg = {}
    for col in ["Distance (m)", "Distance HID (>13 km/h)", "Distance HID (>19 km/h)",
                "Distance par plage de vitesse (>25 km/h)", "# Acc/Dec", "CHARGE"]:
        if col in d.columns:
            agg[col] = "sum"

    out = d.groupby(["Player", "DATE"], as_index=False).agg(agg)

    # Distance relative
    if "Distance (m)" in d.columns:
        mins = d.groupby(["Player", "DATE"], as_index=False)["Durée_min"].sum().rename(columns={"Durée_min": "_dur"})
        out = out.merge(mins, on=["Player", "DATE"], how="left")
        out["Distance relative (m/min)"] = np.where(out["_dur"] > 0, out["Distance (m)"] / out["_dur"], 0.0)
        out.rename(columns={"_dur": "Durée (min)"}, inplace=True)
    else:
        out["Durée (min)"] = 0.0
        out["Distance relative (m/min)"] = 0.0

    # ACWR daily (28 jours)
    if "CHARGE" in out.columns:
        out = out.sort_values(["Player", "DATE"])
        out["Aigue"] = out["CHARGE"]
        out["Chronique"] = out.groupby("Player")["Aigue"].transform(lambda s: s.rolling(28, min_periods=1).mean())
        out["ACWR"] = np.where(out["Chronique"] > 0, out["Aigue"] / out["Chronique"], np.nan)
    else:
        out["Aigue"] = np.nan
        out["Chronique"] = np.nan
        out["ACWR"] = np.nan

    out.rename(columns={
        "Distance HID (>13 km/h)": "Distance HID >13 (m)",
        "Distance HID (>19 km/h)": "Distance HID >19 (m)",
        "Distance par plage de vitesse (>25 km/h)": "Distance >25 (m)",
    }, inplace=True)

    return out


# =========================
# COLLECT DATA
# =========================
@st.cache_data
def collect_data(selected_season=None):
    download_google_drive()

    # ---- Référentiel (robuste Unicode) ----
    ref_path = os.path.join(DATA_FOLDER, REFERENTIEL_FILENAME)
    if not os.path.exists(ref_path):
        ref_path = find_local_file_by_normalized_name(DATA_FOLDER, REFERENTIEL_FILENAME)

    if not ref_path or not os.path.exists(ref_path):
        st.error(f"Référentiel introuvable dans '{DATA_FOLDER}'.")
        try:
            st.write("Fichiers présents:", os.listdir(DATA_FOLDER))
        except Exception:
            pass
        return pd.DataFrame(), pd.DataFrame()

    ref_set, alias_to_canon = build_referentiel_players(ref_path)
    name_report: List[dict] = []

    pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

    if not os.path.exists(DATA_FOLDER):
        return pfc_kpi, edf_kpi

    fichiers = [f for f in os.listdir(DATA_FOLDER)
                if f.endswith((".csv", ".xlsx", ".xls")) and normalize_str(f) != normalize_str(PERMISSIONS_FILENAME)]

    if selected_season and selected_season != "Toutes les saisons":
        fichiers = [f for f in fichiers if f"{selected_season}" in f]

    # ---- GPS (stocké en session) ----
    try:
        gps_raw = load_gps_raw(ref_set, alias_to_canon)
        gps_week = compute_gps_weekly_metrics(gps_raw)
        gps_day = compute_gps_daily_metrics(gps_raw)
        st.session_state["gps_weekly_df"] = gps_week
        st.session_state["gps_daily_df"] = gps_day
    except Exception:
        st.session_state["gps_weekly_df"] = pd.DataFrame()
        st.session_state["gps_daily_df"] = pd.DataFrame()

    # ---- EDF ----
    edf_path = os.path.join(DATA_FOLDER, EDF_JOUEUSES_FILENAME)
    if os.path.exists(edf_path):
        try:
            edf_joueuses = pd.read_excel(edf_path)
            needed = {"Player", "Poste", "Temps de jeu"}
            if needed.issubset(set(edf_joueuses.columns)):
                edf_joueuses["Player"] = edf_joueuses["Player"].apply(nettoyer_nom_joueuse)
                edf_joueuses = normalize_players_in_df(
                    edf_joueuses, cols=["Player"], ref_set=ref_set, alias_to_canon=alias_to_canon,
                    filename=EDF_JOUEUSES_FILENAME, report=name_report
                )

                matchs_csv = [f for f in fichiers if f.startswith("EDF_U19_Match") and f.endswith(".csv")]
                all_edf = []
                for csv_file in matchs_csv:
                    d = pd.read_csv(os.path.join(DATA_FOLDER, csv_file))
                    if "Row" not in d.columns:
                        continue
                    d["Player"] = d["Row"].apply(nettoyer_nom_joueuse)
                    d = normalize_players_in_df(
                        d, cols=["Player"], ref_set=ref_set, alias_to_canon=alias_to_canon,
                        filename=csv_file, report=name_report
                    )
                    d = d.merge(edf_joueuses, on="Player", how="left")
                    if d.empty:
                        continue
                    df_edf = create_data(d, d, True)
                    if not df_edf.empty:
                        all_edf.append(df_edf)

                if all_edf:
                    edf_kpi = pd.concat(all_edf, ignore_index=True)
                    if "Poste" in edf_kpi.columns:
                        edf_kpi = edf_kpi.groupby("Poste").mean(numeric_only=True).reset_index()
                        edf_kpi["Poste"] = edf_kpi["Poste"] + " moyenne (EDF)"
        except Exception:
            pass

    # ---- PFC MATCHS ----
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

            # normalisation noms (Row + postes)
            cols_to_fix = ["Row"] + [c for c in POST_COLS if c in data.columns]
            data = normalize_players_in_df(
                data, cols=cols_to_fix, ref_set=ref_set, alias_to_canon=alias_to_canon,
                filename=filename, report=name_report
            )

            row_vals = data["Row"].astype(str).str.strip()
            unique_rows = set(row_vals.dropna().unique().tolist())

            equipe_pfc = "PFC" if "PFC" in unique_rows else str(parts[0]).strip()

            equipe_adv = None
            if "Teamersaire" in data.columns:
                adv_series = data.loc[row_vals.eq(equipe_pfc), "Teamersaire"].dropna().astype(str).str.strip()
                if not adv_series.empty:
                    equipe_adv = adv_series.mode().iloc[0]
            if not equipe_adv:
                counts = row_vals.value_counts()
                candidates = [k for k in counts.index.tolist() if k and k != equipe_pfc]
                if candidates:
                    equipe_adv = candidates[0]
            if not equipe_adv:
                continue

            home_clean = nettoyer_nom_equipe(equipe_pfc)
            away_clean = nettoyer_nom_equipe(equipe_adv)

            d2 = data.copy()
            d2["Row_clean"] = d2["Row"].astype(str).str.strip().apply(nettoyer_nom_equipe)

            match = d2[d2["Row_clean"].isin({home_clean, away_clean})].copy()
            if match.empty:
                mask = d2["Row_clean"].str.contains(home_clean, na=False) | d2["Row_clean"].str.contains(away_clean, na=False)
                match = d2[mask].copy()
            if match.empty:
                continue

            mask_joueurs = ~d2["Row_clean"].str.contains("CORNER|COUP-FRANC|COUP FRANC|PENALTY|CARTON", na=False)
            mask_joueurs &= ~d2.index.isin(match.index)
            joueurs = d2[mask_joueurs].copy()
            if joueurs.empty:
                joueurs = pd.DataFrame(columns=["Row", "Action"])

            df = create_data(match, joueurs, False, home_team=equipe_pfc, away_team=equipe_adv)
            if df.empty:
                continue

            # normalisation /90 (numériques uniquement, hors pourcentages)
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

    try:
        st.session_state["name_report_df"] = pd.DataFrame(name_report).drop_duplicates() if name_report else pd.DataFrame()
    except Exception:
        pass

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
# UI STREAMLIT
# =========================
def plot_gps_evolution(base_df: pd.DataFrame, player_canon: str, granularity: str):
    if base_df is None or base_df.empty:
        st.warning("Aucune donnée GPS disponible.")
        return

    dfp = base_df[base_df["Player"] == player_canon].copy()
    if dfp.empty:
        st.info("Aucune donnée GPS trouvée pour cette joueuse.")
        return

    if granularity == "Semaine":
        xcol = "SEMAINE"
        dfp[xcol] = pd.to_numeric(dfp[xcol], errors="coerce")
        dfp = dfp.dropna(subset=[xcol]).sort_values(xcol)
        if dfp.empty:
            st.warning("Aucune semaine exploitable.")
            return
        xmin, xmax = int(dfp[xcol].min()), int(dfp[xcol].max())
        week_range = st.slider("Semaines", min_value=xmin, max_value=xmax, value=(xmin, xmax))
        dfp = dfp[(dfp[xcol] >= week_range[0]) & (dfp[xcol] <= week_range[1])]
    else:
        xcol = "DATE"
        dfp[xcol] = pd.to_datetime(dfp[xcol], errors="coerce")
        dfp = dfp.dropna(subset=[xcol]).sort_values(xcol)
        if dfp.empty:
            st.warning("Aucune date exploitable.")
            return
        dmin, dmax = dfp[xcol].min().date(), dfp[xcol].max().date()
        date_range = st.date_input("Dates", value=(dmin, dmax))
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            dfp = dfp[(dfp[xcol] >= start) & (dfp[xcol] <= end)]

    if dfp.empty:
        st.warning("Aucune donnée dans la plage sélectionnée.")
        return

    dist_col = "Distance (m)" if "Distance (m)" in dfp.columns else None
    hid_col = "Distance HID >13 (m)" if "Distance HID >13 (m)" in dfp.columns else (
        "Distance HID (>13 km/h)" if "Distance HID (>13 km/h)" in dfp.columns else None
    )
    acwr_col = "ACWR" if "ACWR" in dfp.columns else None

    choices = []
    if dist_col: choices.append("Distance")
    if hid_col: choices.append("HID")
    if acwr_col: choices.append("ACWR")

    if not choices:
        st.warning("Colonnes GPS attendues introuvables (Distance/HID/ACWR).")
        return

    selected = st.multiselect("Courbes", choices, default=choices)
    use_secondary = st.checkbox("ACWR sur axe secondaire", value=True) if "ACWR" in selected else False

    fig, ax = plt.subplots()

    if "Distance" in selected and dist_col:
        ax.plot(dfp[xcol], dfp[dist_col], marker="o", linestyle="-", label="Distance (m)")
    if "HID" in selected and hid_col:
        ax.plot(dfp[xcol], dfp[hid_col], marker="o", linestyle="-", label="HID >13 (m)")

    ax.set_xlabel("Semaine" if granularity == "Semaine" else "Date")
    ax.set_ylabel("mètres")

    if "ACWR" in selected and acwr_col:
        if use_secondary:
            ax2 = ax.twinx()
            ax2.plot(dfp[xcol], dfp[acwr_col], marker="o", linestyle="--", label="ACWR")
            ax2.set_ylabel("ACWR")
            lines, labels = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax2.legend(lines + lines2, labels + labels2, loc="upper left")
        else:
            ax.plot(dfp[xcol], dfp[acwr_col], marker="o", linestyle="--", label="ACWR")
            ax.legend(loc="upper left")
    else:
        ax.legend(loc="upper left")

    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    st.pyplot(fig)

    with st.expander("Voir la table source"):
        st.dataframe(dfp)


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

    rep_df = st.session_state.get("name_report_df", pd.DataFrame())
    if isinstance(rep_df, pd.DataFrame) and not rep_df.empty:
        st.sidebar.download_button(
            "⬇️ Rapport mapping noms",
            data=rep_df.to_csv(index=False).encode("utf-8"),
            file_name="rapport_mapping_noms.csv",
            mime="text/csv"
        )

    if check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Télécharger synthèse"):
            with st.spinner("Génération..."):
                p_all, _ = collect_data()
                excel_bytes = generate_synthesis_excel(p_all)
            if excel_bytes:
                st.sidebar.download_button(
                    label="⬇️ Télécharger le fichier Excel",
                    data=excel_bytes,
                    file_name="synthese_statistiques_joueuses.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    if selected_saison != "Toutes les saisons":
        pfc_kpi, edf_kpi = collect_data(selected_saison)
    else:
        pfc_kpi, edf_kpi = collect_data()

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

    elif page == "Comparaison":
        st.header("Comparaison")

        if pfc_kpi.empty:
            st.warning("Aucune donnée PFC.")
            return

        if player_name:
            st.subheader(f"Comparaison pour {player_name}")

            st.write("### 1) Comparer des matchs")
            if "Adversaire" in pfc_kpi.columns:
                unique_matches = pfc_kpi["Adversaire"].unique()
                selected_matches = st.multiselect("Sélectionnez au moins 2 matchs", unique_matches)
                if len(selected_matches) >= 2 and st.button("Comparer les matchs sélectionnés"):
                    comp = []
                    for mlabel in selected_matches:
                        md = pfc_kpi[pfc_kpi["Adversaire"] == mlabel]
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
                            agg["Player"] = f"{player_name} ({mlabel})"
                            comp.append(agg)
                    if len(comp) >= 2:
                        players_data = pd.concat(comp, ignore_index=True)
                        fig = create_comparison_radar(players_data)
                        if fig:
                            st.pyplot(fig)
                    else:
                        st.warning("Pas assez de données pour comparer ces matchs.")
            else:
                st.warning("Colonne 'Adversaire' manquante.")

            st.write("### 2) Comparer au référentiel EDF U19")
            if not edf_kpi.empty and "Poste" in edf_kpi.columns:
                poste = st.selectbox("Poste EDF", edf_kpi["Poste"].unique())
                edf_data = edf_kpi[edf_kpi["Poste"] == poste].rename(columns={"Poste": "Player"})
                player_data = prepare_comparison_data(pfc_kpi, player_name)
                if not player_data.empty and not edf_data.empty and st.button("Comparer avec EDF"):
                    players_data = pd.concat([player_data, edf_data], ignore_index=True)
                    fig = create_comparison_radar(players_data, player1_name=player_name, player2_name=f"EDF {poste}")
                    if fig:
                        st.pyplot(fig)
            else:
                st.warning("Aucune donnée EDF disponible.")
        else:
            st.subheader("Comparaison PFC vs PFC")
            p1 = st.selectbox("Joueuse 1", pfc_kpi["Player"].unique(), key="p1")
            p2 = st.selectbox("Joueuse 2", pfc_kpi["Player"].unique(), key="p2")

            agg1 = pfc_kpi[pfc_kpi["Player"] == p1].groupby("Player").mean(numeric_only=True).reset_index()
            agg2 = pfc_kpi[pfc_kpi["Player"] == p2].groupby("Player").mean(numeric_only=True).reset_index()
            agg1 = safe_int_numeric_only(agg1)
            agg2 = safe_int_numeric_only(agg2)

            if st.button("Afficher le radar"):
                players_data = pd.concat([agg1, agg2], ignore_index=True)
                fig = create_comparison_radar(players_data, player1_name=p1, player2_name=p2)
                if fig:
                    st.pyplot(fig)

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

    elif page == "Données Physiques":
        st.header("📊 Données Physiques")

        # Sélection joueuse (si non connectée à un profil)
        gps_weekly = st.session_state.get("gps_weekly_df", pd.DataFrame())
        gps_daily = st.session_state.get("gps_daily_df", pd.DataFrame())

        if (gps_weekly is None or gps_weekly.empty) and (gps_daily is None or gps_daily.empty):
            st.warning("Aucune donnée GPS trouvée (fichier dashboard GPS absent ou feuille 'Data GPS' manquante).")
            return

        all_players = []
        if isinstance(gps_weekly, pd.DataFrame) and not gps_weekly.empty:
            all_players += gps_weekly["Player"].dropna().unique().tolist()
        if isinstance(gps_daily, pd.DataFrame) and not gps_daily.empty:
            all_players += gps_daily["Player"].dropna().unique().tolist()
        all_players = sorted(set(all_players))

        if not player_name:
            player_name = st.selectbox("Sélectionnez une joueuse", all_players)

        player_canon = nettoyer_nom_joueuse(player_name)

        tab1, tab2 = st.tabs(["🏋️ Entraînements", "⚽ Matchs"])

        with tab1:
            st.subheader("Suivi GPS - Entraînement")

            granularity = st.radio("Granularité", ["Semaine", "Jour"], horizontal=True)
            base_df = gps_weekly if granularity == "Semaine" else gps_daily

            st.markdown("### 📈 Évolution charge de travail (Distance / HID / ACWR)")
            plot_gps_evolution(base_df, player_canon=player_canon, granularity=granularity)

            st.markdown("### Tableau synthèse")
            if granularity == "Semaine":
                dfp = gps_weekly[gps_weekly["Player"] == player_canon].copy()
                cols_show = [
                    "SEMAINE", "Durée hebdo (min)", "Distance (m)",
                    "Distance HID >13 (m)", "Distance HID >19 (m)", "Distance >25 (m)",
                    "Distance relative (m/min)", "# Acc/Dec",
                    "Aigue", "Chronique", "ACWR", "Monotonie", "Contrainte"
                ]
                cols_show = [c for c in cols_show if c in dfp.columns]
                st.dataframe(dfp[cols_show].sort_values("SEMAINE") if not dfp.empty else dfp)
            else:
                dfp = gps_daily[gps_daily["Player"] == player_canon].copy()
                cols_show = [
                    "DATE", "Durée (min)", "Distance (m)",
                    "Distance HID >13 (m)", "Distance HID >19 (m)", "Distance >25 (m)",
                    "Distance relative (m/min)", "# Acc/Dec",
                    "Aigue", "Chronique", "ACWR"
                ]
                cols_show = [c for c in cols_show if c in dfp.columns]
                st.dataframe(dfp[cols_show].sort_values("DATE") if not dfp.empty else dfp)

        with tab2:
            st.info("GPS Matchs : à brancher quand tu me donnes la structure de l’export GPS match (colonnes / feuille).")

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

        # GPS passerelles
        gps_weekly = st.session_state.get("gps_weekly_df", pd.DataFrame())
        gps_daily = st.session_state.get("gps_daily_df", pd.DataFrame())

        if isinstance(gps_weekly, pd.DataFrame) and not gps_weekly.empty:
            prenom = str(info.get("Prénom", "")).strip()
            nom = str(selected).strip()
            candidate = nettoyer_nom_joueuse(f"{nom} {prenom}") if prenom else nettoyer_nom_joueuse(nom)

            st.subheader("GPS - Entraînement (Metrics prioritaires)")
            dfp = gps_weekly[gps_weekly["Player"] == candidate].copy()
            if dfp.empty:
                st.info("Aucune donnée GPS trouvée pour cette joueuse (ou joueuse non reconnue).")
            else:
                cols_show = [
                    "SEMAINE", "Durée hebdo (min)", "Distance (m)",
                    "Distance HID >13 (m)", "Distance HID >19 (m)", "Distance >25 (m)",
                    "Distance relative (m/min)", "# Acc/Dec",
                    "Aigue", "Chronique", "ACWR"
                ]
                cols_show = [c for c in cols_show if c in dfp.columns]
                st.dataframe(dfp[cols_show].sort_values("SEMAINE"))

                with st.expander("📈 Graphique (hebdo)"):
                    plot_gps_evolution(gps_weekly, player_canon=candidate, granularity="Semaine")


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
        st.error("Impossible de charger les permissions (vérifie le fichier de permissions sur Drive).")
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
