# -*- coding: utf-8 -*-
import os
import io
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

from mplsoccer import PyPizza, Radar, FontManager, grid

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

warnings.filterwarnings("ignore")

# =========================================================
# CONFIG
# =========================================================
DATA_FOLDER = "data"
PASSERELLE_FOLDER = os.path.join(DATA_FOLDER, "passerelle")

FOLDER_ID_MAIN = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
FOLDER_ID_PASSERELLE = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"

PASSERELLE_FILE_NAME = "Liste Joueuses Passerelles.xlsx"
PERMISSIONS_FILE_NAME = "Classeurs permissions streamlit.xlsx"
EDF_JOUEUSES_FILE_NAME = "EDF_Joueuses.xlsx"

SCOPES = ["https://www.googleapis.com/auth/drive"]

POST_COLS = ["ATT", "DCD", "DCG", "DD", "DG", "GB", "MCD", "MCG", "MD", "MDef", "MG"]
EXCLUDED_ROW_TOKENS = ["CORNER", "COUP-FRANC", "COUP FRANC", "PENALTY", "CARTON"]


# =========================================================
# UTILS (ROBUSTES)
# =========================================================
def norm_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and np.isnan(x):
        return ""
    return str(x).strip().upper()


def nettoyer_nom_joueuse(nom) -> str:
    s = norm_str(nom)
    if not s:
        return ""
    s = (
        s.replace("É", "E").replace("È", "E").replace("Ê", "E")
        .replace("À", "A").replace("Â", "A")
        .replace("Ù", "U")
        .replace("Î", "I").replace("Ï", "I")
        .replace("Ô", "O")
        .replace("Ç", "C")
    )
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    if len(parts) >= 2 and parts[0] == parts[1]:
        return parts[0]
    return s


def safe_read_csv(path: str) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    try:
        return pd.read_csv(path, engine="python")
    except Exception as e:
        st.warning(f"Impossible de lire le CSV {os.path.basename(path)} ({e})")
        return pd.DataFrame()


def safe_read_excel(path: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path)
    except Exception as e:
        st.warning(f"Impossible de lire l'Excel {os.path.basename(path)} ({e})")
        return pd.DataFrame()


def require_cols(df: pd.DataFrame, cols: List[str], context: str = "") -> bool:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        st.warning(f"{context} — colonnes manquantes : {missing}" if context else f"Colonnes manquantes : {missing}")
        return False
    return True


def cast_numeric_only(df: pd.DataFrame, decimals: int = 0, to_int: bool = True) -> pd.DataFrame:
    """
    Applique round/fillna/astype UNIQUEMENT sur les colonnes numériques.
    Évite l'erreur ValueError sur .astype(int).
    """
    if df.empty:
        return df
    out = df.copy()
    num_cols = out.select_dtypes(include=[np.number]).columns
    if len(num_cols) == 0:
        return out
    if decimals is not None:
        out[num_cols] = out[num_cols].round(decimals)
    out[num_cols] = out[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0)
    if to_int:
        # conversion safe: int
        out[num_cols] = out[num_cols].astype(int)
    return out


# =========================================================
# GOOGLE DRIVE
# =========================================================
def authenticate_google_drive():
    service_account_info = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not service_account_info:
        st.error("Secret GOOGLE_SERVICE_ACCOUNT_JSON manquant dans st.secrets.")
        st.stop()
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def list_files_in_folder(service, folder_id: str) -> List[Dict]:
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get("files", [])


def download_file(service, file_id: str, file_name: str, output_folder: str) -> str:
    os.makedirs(output_folder, exist_ok=True)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    file_path = os.path.join(output_folder, file_name)
    with open(file_path, "wb") as f:
        f.write(fh.getbuffer())
    return file_path


def download_google_drive_data():
    service = authenticate_google_drive()

    os.makedirs(DATA_FOLDER, exist_ok=True)
    files = list_files_in_folder(service, FOLDER_ID_MAIN)
    for f in files:
        name = f.get("name", "")
        if name.endswith((".csv", ".xlsx")):
            download_file(service, f["id"], name, DATA_FOLDER)

    os.makedirs(PASSERELLE_FOLDER, exist_ok=True)
    files_p = list_files_in_folder(service, FOLDER_ID_PASSERELLE)
    for f in files_p:
        if f.get("name") == PASSERELLE_FILE_NAME:
            download_file(service, f["id"], f["name"], PASSERELLE_FOLDER)


def download_permissions_file() -> Optional[str]:
    service = authenticate_google_drive()
    files = list_files_in_folder(service, FOLDER_ID_MAIN)
    for f in files:
        if f.get("name") == PERMISSIONS_FILE_NAME:
            os.makedirs(DATA_FOLDER, exist_ok=True)
            return download_file(service, f["id"], f["name"], DATA_FOLDER)
    return None


# =========================================================
# PERMISSIONS / PASSERELLE
# =========================================================
def load_permissions() -> Dict:
    permissions_path = download_permissions_file()
    if not permissions_path or not os.path.exists(permissions_path):
        st.error(f"Fichier permissions introuvable: {PERMISSIONS_FILE_NAME}")
        return {}

    df = safe_read_excel(permissions_path)
    if df.empty:
        st.error("Fichier permissions vide ou illisible.")
        return {}

    if not require_cols(df, ["Profil", "Mot de passe", "Permissions"], "Permissions"):
        return {}

    permissions = {}
    for _, row in df.iterrows():
        profile = norm_str(row.get("Profil"))
        if not profile:
            continue

        perms_raw = row.get("Permissions")
        perms = []
        if pd.notna(perms_raw):
            perms = [p.strip() for p in str(perms_raw).split(",") if p.strip()]

        player = row.get("Joueuse")
        player_clean = nettoyer_nom_joueuse(player) if pd.notna(player) else None

        permissions[profile] = {
            "password": str(row.get("Mot de passe", "")).strip(),
            "permissions": perms,
            "player": player_clean,
        }
    return permissions


def load_passerelle_data() -> Dict:
    passerelle_file = os.path.join(PASSERELLE_FOLDER, PASSERELLE_FILE_NAME)
    if not os.path.exists(passerelle_file):
        return {}

    df = safe_read_excel(passerelle_file)
    if df.empty or "Nom" not in df.columns:
        return {}

    out = {}
    for _, row in df.iterrows():
        nom = row.get("Nom")
        if pd.isna(nom):
            continue
        key = str(nom).strip()
        if not key:
            continue
        out[key] = {
            "Prénom": row.get("Prénom", ""),
            "Photo": row.get("Photo", ""),
            "Date de naissance": row.get("Date de naissance", ""),
            "Poste 1": row.get("Poste 1", ""),
            "Poste 2": row.get("Poste 2", ""),
            "Pied Fort": row.get("Pied Fort", ""),
            "Taille": row.get("Taille", ""),
        }
    return out


# =========================================================
# DATA SPLIT (match/joueurs) + TEMPS DE JEU
# =========================================================
def build_match_and_joueurs(data: pd.DataFrame, equipe_dom: str, equipe_ext: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if data.empty or "Row" not in data.columns:
        return pd.DataFrame(), pd.DataFrame()

    dom = norm_str(equipe_dom)
    ext = norm_str(equipe_ext)

    match_rows = []
    joueurs_rows = []

    for _, r in data.iterrows():
        row_val = norm_str(r.get("Row"))

        # Lignes match: contient le nom d'équipe (tolère "PFC 2MT", "PFC (2)", etc.)
        if (dom and dom in row_val) or (ext and ext in row_val):
            match_rows.append(r)
            continue

        # Exclusions
        if any(tok in row_val for tok in EXCLUDED_ROW_TOKENS):
            continue

        joueurs_rows.append(r)

    match = pd.DataFrame(match_rows).reset_index(drop=True) if match_rows else pd.DataFrame()
    joueurs = pd.DataFrame(joueurs_rows).reset_index(drop=True) if joueurs_rows else pd.DataFrame()
    return match, joueurs


def players_duration(match: pd.DataFrame) -> pd.DataFrame:
    if match.empty:
        return pd.DataFrame()
    if "Duration" not in match.columns:
        st.warning("Colonne 'Duration' manquante pour calculer le temps de jeu.")
        return pd.DataFrame()

    available_posts = [c for c in POST_COLS if c in match.columns]
    if not available_posts:
        st.warning("Aucune colonne de postes (ATT/DCD/...) pour calculer le temps de jeu.")
        return pd.DataFrame()

    dur = pd.to_numeric(match["Duration"], errors="coerce")
    if dur.dropna().empty:
        return pd.DataFrame()

    duration_is_seconds = dur.dropna().max() > 300  # heuristique
    minutes_by_player: Dict[str, float] = {}

    for _, row in match.iterrows():
        d = pd.to_numeric(row.get("Duration", np.nan), errors="coerce")
        if pd.isna(d) or d <= 0:
            continue
        minutes = float(d / 60.0) if duration_is_seconds else float(d)

        players_in_line = set()
        for poste in available_posts:
            p = nettoyer_nom_joueuse(row.get(poste, ""))
            if not p or p in {"NAN", "NONE"}:
                continue
            if p in players_in_line:
                continue
            players_in_line.add(p)
            minutes_by_player[p] = minutes_by_player.get(p, 0.0) + minutes

    if not minutes_by_player:
        return pd.DataFrame()

    df = pd.DataFrame({"Player": list(minutes_by_player.keys()),
                       "Temps de jeu (en minutes)": list(minutes_by_player.values())})
    return df.sort_values("Temps de jeu (en minutes)", ascending=False).reset_index(drop=True)


# =========================================================
# STATS (joueurs)
# =========================================================
def players_shots(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs.empty or not require_cols(joueurs, ["Action", "Row"], "Tirs"):
        return pd.DataFrame()
    shots, sot, goals = {}, {}, {}
    for _, r in joueurs.iterrows():
        action = r.get("Action")
        if not isinstance(action, str) or "Tir" not in action:
            continue
        player = nettoyer_nom_joueuse(r.get("Row"))
        shots[player] = shots.get(player, 0) + action.count("Tir")

        if "Tir" in joueurs.columns:
            detail = r.get("Tir")
            if isinstance(detail, str):
                if ("Tir Cadré" in detail) or ("But" in detail):
                    sot[player] = sot.get(player, 0) + detail.count("Tir Cadré") + detail.count("But")
                if "But" in detail:
                    goals[player] = goals.get(player, 0) + 1

    if not shots:
        return pd.DataFrame()
    return (pd.DataFrame({
        "Player": list(shots.keys()),
        "Tirs": list(shots.values()),
        "Tirs cadrés": [sot.get(p, 0) for p in shots.keys()],
        "Buts": [goals.get(p, 0) for p in shots.keys()],
    }).sort_values("Tirs", ascending=False).reset_index(drop=True))


def players_passes(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs.empty or not require_cols(joueurs, ["Action", "Row"], "Passes"):
        return pd.DataFrame()

    sp, lp, sp_ok, lp_ok = {}, {}, {}, {}
    for _, r in joueurs.iterrows():
        action = r.get("Action")
        if not isinstance(action, str) or "Passe" not in action:
            continue
        player = nettoyer_nom_joueuse(r.get("Row"))

        passe = r.get("Passe") if "Passe" in joueurs.columns else None
        if not isinstance(passe, str):
            continue

        if "Courte" in passe:
            sp[player] = sp.get(player, 0) + passe.count("Courte")
            if "Réussie" in passe:
                sp_ok[player] = sp_ok.get(player, 0) + passe.count("Réussie")

        if "Longue" in passe:
            lp[player] = lp.get(player, 0) + passe.count("Longue")
            if "Réussie" in passe:
                lp_ok[player] = lp_ok.get(player, 0) + passe.count("Réussie")

    if not sp and not lp:
        return pd.DataFrame()

    players = sorted(set(list(sp.keys()) + list(lp.keys())))
    df = pd.DataFrame({
        "Player": players,
        "Passes courtes": [sp.get(p, 0) for p in players],
        "Passes longues": [lp.get(p, 0) for p in players],
        "Passes réussies (courtes)": [sp_ok.get(p, 0) for p in players],
        "Passes réussies (longues)": [lp_ok.get(p, 0) for p in players],
    })
    df["Passes"] = df["Passes courtes"] + df["Passes longues"]
    df["Passes réussies"] = df["Passes réussies (courtes)"] + df["Passes réussies (longues)"]
    df["Pourcentage de passes réussies"] = (df["Passes réussies"] / df["Passes"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0)
    return df.sort_values("Passes courtes", ascending=False).reset_index(drop=True)


def players_dribbles(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs.empty or not require_cols(joueurs, ["Action", "Row"], "Dribbles"):
        return pd.DataFrame()
    d, d_ok = {}, {}
    for _, r in joueurs.iterrows():
        action = r.get("Action")
        if not isinstance(action, str) or "Dribble" not in action:
            continue
        player = nettoyer_nom_joueuse(r.get("Row"))
        d[player] = d.get(player, 0) + action.count("Dribble")

        if "Dribble" in joueurs.columns:
            detail = r.get("Dribble")
            if isinstance(detail, str) and "Réussi" in detail:
                d_ok[player] = d_ok.get(player, 0) + detail.count("Réussi")
    if not d:
        return pd.DataFrame()
    df = pd.DataFrame({
        "Player": list(d.keys()),
        "Dribbles": list(d.values()),
        "Dribbles réussis": [d_ok.get(p, 0) for p in d.keys()],
    })
    df["Pourcentage de dribbles réussis"] = (df["Dribbles réussis"] / df["Dribbles"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0)
    return df.sort_values("Dribbles", ascending=False).reset_index(drop=True)


def players_defensive_duels(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs.empty or not require_cols(joueurs, ["Action", "Row"], "Duels"):
        return pd.DataFrame()

    duels, won, faults = {}, {}, {}

    duels_col = "Duel défensifs" if "Duel défensifs" in joueurs.columns else ("Duel défensif" if "Duel défensif" in joueurs.columns else None)

    for _, r in joueurs.iterrows():
        action = r.get("Action")
        if not isinstance(action, str) or "Duel défensif" not in action:
            continue
        player = nettoyer_nom_joueuse(r.get("Row"))
        duels[player] = duels.get(player, 0) + action.count("Duel défensif")

        if duels_col:
            detail = r.get(duels_col)
            if isinstance(detail, str):
                if "Gagné" in detail:
                    won[player] = won.get(player, 0) + detail.count("Gagné")
                if "Faute" in detail:
                    faults[player] = faults.get(player, 0) + detail.count("Faute")

    if not duels:
        return pd.DataFrame()

    df = pd.DataFrame({
        "Player": list(duels.keys()),
        "Duels défensifs": list(duels.values()),
        "Duels défensifs gagnés": [won.get(p, 0) for p in duels.keys()],
        "Fautes": [faults.get(p, 0) for p in duels.keys()],
    })
    df["Pourcentage de duels défensifs gagnés"] = (df["Duels défensifs gagnés"] / df["Duels défensifs"] * 100).replace([np.inf, -np.inf], np.nan).fillna(0)
    return df.sort_values("Duels défensifs", ascending=False).reset_index(drop=True)


def players_interceptions(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs.empty or not require_cols(joueurs, ["Action", "Row"], "Interceptions"):
        return pd.DataFrame()
    inter = {}
    for _, r in joueurs.iterrows():
        action = r.get("Action")
        if not isinstance(action, str) or "Interception" not in action:
            continue
        player = nettoyer_nom_joueuse(r.get("Row"))
        inter[player] = inter.get(player, 0) + action.count("Interception")
    if not inter:
        return pd.DataFrame()
    return pd.DataFrame({"Player": list(inter.keys()), "Interceptions": list(inter.values())}).sort_values("Interceptions", ascending=False).reset_index(drop=True)


def players_ball_losses(joueurs: pd.DataFrame) -> pd.DataFrame:
    if joueurs.empty or not require_cols(joueurs, ["Action", "Row"], "Pertes"):
        return pd.DataFrame()
    losses = {}
    for _, r in joueurs.iterrows():
        action = r.get("Action")
        if not isinstance(action, str) or "Perte de balle" not in action:
            continue
        player = nettoyer_nom_joueuse(r.get("Row"))
        losses[player] = losses.get(player, 0) + action.count("Perte de balle")
    if not losses:
        return pd.DataFrame()
    return pd.DataFrame({"Player": list(losses.keys()), "Pertes de balle": list(losses.values())}).sort_values("Pertes de balle", ascending=False).reset_index(drop=True)


# =========================================================
# METRICS / KPIs / POSTE
# =========================================================
def create_metrics(df: pd.DataFrame) -> pd.DataFrame:
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
            base, malus = cols[0], cols[1]
            df[metric] = np.where(df[base] > 0, (df[base] - df.get(malus, 0)) / df[base], 0)

        elif metric == "Force physique":
            base, win = cols[0], cols[1]
            df[metric] = np.where(df[base] > 0, df.get(win, 0) / df[base], 0)

        elif metric in ["Intelligence tactique", "Technique 1", "Prise de risque", "Sang-froid"]:
            base = cols[0]
            m = df[base].max()
            df[metric] = np.where(df[base] > 0, df[base] / m, 0) if m and m > 0 else 0

        elif metric in ["Technique 2", "Technique 3", "Explosivité", "Précision"]:
            base, ok = cols[0], cols[1]
            df[metric] = np.where(df[base] > 0, df.get(ok, 0) / df[base], 0)

    for metric in required_cols.keys():
        if metric in df.columns:
            df[metric] = (df[metric].rank(pct=True) * 100).replace([np.inf, -np.inf], np.nan).fillna(0)

    return df


def create_kpis(df: pd.DataFrame) -> pd.DataFrame:
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


def create_poste(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    required = ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition"]
    if not all(c in df.columns for c in required):
        return df

    df["Défenseur central"] = (df["Rigueur"] * 5 + df["Récupération"] * 5 + df["Distribution"] * 5 + df["Percussion"] * 1 + df["Finition"] * 1) / 17
    df["Défenseur latéral"] = (df["Rigueur"] * 3 + df["Récupération"] * 3 + df["Distribution"] * 3 + df["Percussion"] * 3 + df["Finition"] * 3) / 15
    df["Milieu défensif"] = (df["Rigueur"] * 4 + df["Récupération"] * 4 + df["Distribution"] * 4 + df["Percussion"] * 2 + df["Finition"] * 2) / 16
    df["Milieu relayeur"] = (df["Rigueur"] * 3 + df["Récupération"] * 3 + df["Distribution"] * 3 + df["Percussion"] * 3 + df["Finition"] * 3) / 15
    df["Milieu offensif"] = (df["Rigueur"] * 2 + df["Récupération"] * 2 + df["Distribution"] * 2 + df["Percussion"] * 4 + df["Finition"] * 4) / 14
    df["Attaquant"] = (df["Rigueur"] * 1 + df["Récupération"] * 1 + df["Distribution"] * 1 + df["Percussion"] * 5 + df["Finition"] * 5) / 13

    return df


# =========================================================
# CREATE DATA
# =========================================================
def create_data(match: pd.DataFrame, joueurs: pd.DataFrame, is_edf: bool) -> pd.DataFrame:
    try:
        if is_edf:
            if joueurs.empty:
                return pd.DataFrame()
            if not require_cols(joueurs, ["Player", "Poste", "Temps de jeu"], "EDF"):
                return pd.DataFrame()
            j = joueurs.copy()
            j["Player"] = j["Player"].apply(nettoyer_nom_joueuse)
            df_duration = j[["Player", "Temps de jeu", "Poste"]].rename(columns={"Temps de jeu": "Temps de jeu (en minutes)"})
        else:
            df_duration = players_duration(match)

        dfs = []
        if not df_duration.empty:
            dfs.append(df_duration)

        for fn in [players_shots, players_passes, players_dribbles, players_defensive_duels, players_interceptions, players_ball_losses]:
            res = fn(joueurs)
            if not res.empty:
                dfs.append(res)

        if not dfs:
            return pd.DataFrame()

        for d in dfs:
            if "Player" in d.columns:
                d["Player"] = d["Player"].apply(nettoyer_nom_joueuse)

        df = dfs[0]
        for other in dfs[1:]:
            df = df.merge(other, on="Player", how="outer")

        df = df.fillna(0)

        # retirer lignes totalement vides (hors Player)
        if df.shape[1] > 1:
            df = df[(df.iloc[:, 1:] != 0).any(axis=1)]

        # filtre temps de jeu minimum
        if "Temps de jeu (en minutes)" in df.columns:
            df = df[df["Temps de jeu (en minutes)"] >= 10]

        df = create_metrics(df)
        df = create_kpis(df)
        df = create_poste(df)

        return df.reset_index(drop=True)

    except Exception as e:
        st.warning(f"Erreur create_data: {e}")
        return pd.DataFrame()


def filter_data_by_player(df: pd.DataFrame, player_name: str) -> pd.DataFrame:
    if df.empty or "Player" not in df.columns:
        return df
    target = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    out = tmp[tmp["Player_clean"] == target].drop(columns=["Player_clean"], errors="ignore")
    return out


def prepare_comparison_data(df: pd.DataFrame, player_name: str, selected_matches: Optional[List[str]] = None) -> pd.DataFrame:
    if df.empty or "Player" not in df.columns:
        return pd.DataFrame()

    target = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    tmp = tmp[tmp["Player_clean"] == target]

    if selected_matches and "Adversaire" in tmp.columns:
        tmp = tmp[tmp["Adversaire"].isin(selected_matches)]

    if tmp.empty:
        return pd.DataFrame()

    agg_sum = tmp.groupby("Player", as_index=False).agg({"Temps de jeu (en minutes)": "sum", "Buts": "sum"})
    agg_mean = tmp.groupby("Player").mean(numeric_only=True).drop(columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore").reset_index()

    out = agg_sum.merge(agg_mean, on="Player", how="left")
    # ✅ cast safe : uniquement colonnes numériques
    out = cast_numeric_only(out, decimals=0, to_int=True)
    return out


def generate_synthesis_excel(pfc_kpi: pd.DataFrame) -> Optional[bytes]:
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if not pfc_kpi.empty:
                out = pfc_kpi.copy()
                out.insert(0, "Joueuse", out["Player"])
                out.to_excel(writer, sheet_name="Synthèse", index=False)
        return output.getvalue()
    except Exception as e:
        st.warning(f"Erreur synthèse Excel: {e}")
        return None


# =========================================================
# COLLECT DATA
# =========================================================
@st.cache_data
def collect_data(selected_season: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        download_google_drive_data()

        pfc_kpi = pd.DataFrame()
        edf_kpi = pd.DataFrame()

        if not os.path.exists(DATA_FOLDER):
            return pfc_kpi, edf_kpi

        fichiers = [f for f in os.listdir(DATA_FOLDER)
                    if f.endswith((".csv", ".xlsx")) and f != PERMISSIONS_FILE_NAME]

        if selected_season and selected_season != "Toutes les saisons":
            fichiers = [f for f in fichiers if selected_season in f]

        # ---------- EDF
        edf_path = os.path.join(DATA_FOLDER, EDF_JOUEUSES_FILE_NAME)
        if os.path.exists(edf_path):
            edf_joueuses = safe_read_excel(edf_path)
            if not edf_joueuses.empty and require_cols(edf_joueuses, ["Player", "Poste", "Temps de jeu"], "EDF_Joueuses"):
                edf_joueuses = edf_joueuses.copy()
                edf_joueuses["Player"] = edf_joueuses["Player"].apply(nettoyer_nom_joueuse)

                matchs_csv = [f for f in fichiers if f.startswith("EDF_U19_Match") and f.endswith(".csv")]
                all_edf = []
                for csv_file in matchs_csv:
                    df_raw = safe_read_csv(os.path.join(DATA_FOLDER, csv_file))
                    if df_raw.empty or "Row" not in df_raw.columns:
                        continue
                    df_raw = df_raw.copy()
                    df_raw["Player"] = df_raw["Row"].apply(nettoyer_nom_joueuse)
                    df_raw = df_raw.merge(edf_joueuses, on="Player", how="left")
                    df = create_data(df_raw, df_raw, True)
                    if not df.empty:
                        all_edf.append(df)

                if all_edf:
                    edf_kpi = pd.concat(all_edf, ignore_index=True)
                    if "Poste" in edf_kpi.columns:
                        edf_kpi = edf_kpi.groupby("Poste", as_index=False).mean(numeric_only=True)
                        edf_kpi["Poste"] = edf_kpi["Poste"] + " moyenne (EDF)"

        # ---------- PFC
        for filename in fichiers:
            if not (filename.endswith(".csv") and "PFC" in filename):
                continue

            path = os.path.join(DATA_FOLDER, filename)
            data = safe_read_csv(path)
            if data.empty or "Row" not in data.columns:
                continue

            parts = filename.split(".")[0].split("_")
            if len(parts) < 6:
                continue

            equipe_dom = parts[0]
            equipe_ext = parts[2]
            journee = parts[3]
            categorie = parts[4]
            date = parts[5]

            match, joueurs = build_match_and_joueurs(data, equipe_dom, equipe_ext)
            if joueurs.empty:
                continue

            joueurs = joueurs.copy()
            joueurs["Player"] = joueurs["Row"].apply(nettoyer_nom_joueuse)

            df = create_data(match, joueurs, False)
            if df.empty:
                continue

            # Normalisation par 90 minutes (uniquement sur colonnes numériques pertinentes)
            if "Temps de jeu (en minutes)" in df.columns:
                for idx, row in df.iterrows():
                    time_played = float(row.get("Temps de jeu (en minutes)", 0) or 0)
                    if time_played <= 0:
                        continue
                    factor = 90.0 / time_played
                    for col in df.columns:
                        if col in ["Player", "Temps de jeu (en minutes)", "Buts"]:
                            continue
                        if "Pourcentage" in col:
                            continue
                        if pd.api.types.is_numeric_dtype(df[col]):
                            df.loc[idx, col] = row[col] * factor

            df = create_metrics(df)
            df = create_kpis(df)
            df = create_poste(df)

            adversaire = equipe_ext if norm_str(equipe_dom) == "PFC" else equipe_dom
            df.insert(1, "Adversaire", f"{adversaire} - {journee}")
            df.insert(2, "Journée", journee)
            df.insert(3, "Catégorie", categorie)
            df.insert(4, "Date", date)

            pfc_kpi = pd.concat([pfc_kpi, df], ignore_index=True)

        return pfc_kpi, edf_kpi

    except Exception as e:
        st.warning(f"Erreur collect_data: {e}")
        return pd.DataFrame(), pd.DataFrame()


# =========================================================
# RADARS
# =========================================================
def create_individual_radar(df: pd.DataFrame):
    if df.empty or "Player" not in df.columns:
        st.warning("Aucune donnée radar.")
        return None

    cols = ["Timing", "Force physique", "Intelligence tactique",
            "Technique 1", "Technique 2", "Technique 3",
            "Explosivité", "Prise de risque", "Précision", "Sang-froid"]
    cols = [c for c in cols if c in df.columns]
    if not cols:
        st.warning("Pas de métriques radar.")
        return None

    colors = ["#6A7CD9", "#00BFFE", "#FF9470", "#F27979", "#BFBFBF"] * 2
    player = df.iloc[0]

    pizza = PyPizza(params=cols, background_color="#002B5C",
                    straight_line_color="#FFFFFF", last_circle_color="#FFFFFF")
    fig, _ = pizza.make_pizza(
        figsize=(3, 3),
        values=[player[c] for c in cols],
        slice_colors=colors[:len(cols)],
        kwargs_values=dict(color="#FFFFFF", fontsize=3.5,
                           bbox=dict(edgecolor="#FFFFFF", facecolor="#002B5C", boxstyle="round,pad=0.5", lw=1)),
        kwargs_params=dict(color="#FFFFFF", fontsize=3.5, fontproperties="monospace"),
    )
    fig.set_facecolor("#002B5C")
    return fig


def create_comparison_radar(df: pd.DataFrame, player1_name=None, player2_name=None):
    if df.empty or len(df) < 2:
        st.warning("Données insuffisantes comparaison.")
        return None

    metrics = ["Timing", "Force physique", "Intelligence tactique",
               "Technique 1", "Technique 2", "Technique 3",
               "Explosivité", "Prise de risque", "Précision", "Sang-froid"]
    metrics = [m for m in metrics if m in df.columns]
    if len(metrics) < 2:
        st.warning("Pas assez de métriques.")
        return None

    low, high = (0,) * len(metrics), (100,) * len(metrics)
    radar = Radar(metrics, low, high, num_rings=4, ring_width=1, center_circle_radius=1)

    URL1 = "https://raw.githubusercontent.com/googlefonts/roboto/main/src/hinted/Roboto-Thin.ttf"
    URL2 = "https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/RobotoSlab%5Bwght%5D.ttf"
    robotto_thin, robotto_bold = FontManager(URL1), FontManager(URL2)

    fig, axs = grid(figheight=14, grid_height=0.915, title_height=0.06,
                    endnote_height=0.025, title_space=0, endnote_space=0, grid_key="radar")

    radar.setup_axis(ax=axs["radar"], facecolor="None")
    radar.draw_circles(ax=axs["radar"], facecolor="#0c4281", edgecolor="#0c4281", lw=1.5)

    v1 = df.iloc[0][metrics].values
    v2 = df.iloc[1][metrics].values

    radar.draw_radar_compare(
        v1, v2, ax=axs["radar"],
        kwargs_radar={"facecolor": "#00f2c1", "alpha": 0.6},
        kwargs_compare={"facecolor": "#d80499", "alpha": 0.6},
    )

    radar.draw_range_labels(ax=axs["radar"], fontsize=18, color="#fcfcfc", fontproperties=robotto_thin.prop)
    radar.draw_param_labels(ax=axs["radar"], fontsize=18, color="#fcfcfc", fontproperties=robotto_thin.prop)

    label1 = player1_name if player1_name else df.iloc[0].get("Player", "Joueur 1")
    label2 = player2_name if player2_name else df.iloc[1].get("Player", "Joueur 2")

    axs["title"].text(0.01, 0.65, label1, fontsize=18, color="#01c49d",
                      fontproperties=robotto_bold.prop, ha="left", va="center")
    axs["title"].text(0.99, 0.65, label2, fontsize=18, color="#d80499",
                      fontproperties=robotto_bold.prop, ha="right", va="center")

    fig.set_facecolor("#002B5C")
    return fig


# =========================================================
# PERMISSIONS HELPERS
# =========================================================
def check_permission(user_profile: str, required_permission: str, permissions: Dict) -> bool:
    if user_profile not in permissions:
        return False
    if "all" in permissions[user_profile]["permissions"]:
        return True
    return required_permission in permissions[user_profile]["permissions"]


def get_player_for_profile(profile: str, permissions: Dict) -> Optional[str]:
    if profile in permissions:
        return permissions[profile].get("player")
    return None


# =========================================================
# UI STREAMLIT
# =========================================================
def script_streamlit(pfc_kpi: pd.DataFrame, edf_kpi: pd.DataFrame, permissions: Dict, user_profile: str):
    logo_pfc = "https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"
    st.sidebar.markdown(f"<div style='display:flex;justify-content:center;'><img src='{logo_pfc}' width='100'></div>", unsafe_allow_html=True)

    player_name = get_player_for_profile(user_profile, permissions)
    st.sidebar.title(f"Connecté en tant que: {user_profile}")
    if player_name:
        st.sidebar.write(f"Joueuse associée: {player_name}")

    saison_options = ["Toutes les saisons", "2425", "2526"]
    selected_saison = st.sidebar.selectbox("Sélectionnez une saison", saison_options)

    if st.sidebar.button("🔒 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user_profile = None
        st.rerun()

    if check_permission(user_profile, "update_data", permissions) or check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Mettre à jour la base de données"):
            with st.spinner("Mise à jour en cours..."):
                download_google_drive_data()
                st.cache_data.clear()
            st.success("✅ Mise à jour terminée")
            st.rerun()

    if check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Télécharger la synthèse des statistiques"):
            with st.spinner("Génération du fichier..."):
                pfc_all, _ = collect_data("Toutes les saisons")
                excel_bytes = generate_synthesis_excel(pfc_all)
            if excel_bytes:
                st.sidebar.download_button(
                    label="⬇️ Télécharger le fichier Excel",
                    data=excel_bytes,
                    file_name="synthese_statistiques_joueuses.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

    # Load data saison
    if selected_saison != "Toutes les saisons":
        pfc_kpi, edf_kpi = collect_data(selected_saison)
    else:
        pfc_kpi, edf_kpi = collect_data("Toutes les saisons")

    if player_name and not pfc_kpi.empty and "Player" in pfc_kpi.columns:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)

    available_options = ["Statistiques"]
    if check_permission(user_profile, "compare_players", permissions) or check_permission(user_profile, "all", permissions) or player_name:
        available_options.append("Comparaison")
    if check_permission(user_profile, "all", permissions):
        available_options.append("Gestion")
    available_options.append("Données Physiques")
    available_options.append("Joueuses Passerelles")

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=available_options,
            icons=["graph-up-arrow", "people", "gear", "activity", "people-fill"],
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

    # =========================
    # PAGE: STATISTIQUES
    # =========================
    if page == "Statistiques":
        st.header("Statistiques")
        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible.")
            return

        if player_name:
            st.subheader(f"Statistiques pour {player_name}")
            if "Adversaire" not in pfc_kpi.columns:
                st.warning("Colonne 'Adversaire' manquante.")
                return

            unique_matches = sorted(pfc_kpi["Adversaire"].dropna().unique())
            game = st.multiselect("Choisissez un ou plusieurs matchs", unique_matches)
            filtered = pfc_kpi[pfc_kpi["Adversaire"].isin(game)] if game else pfc_kpi

            if filtered.empty:
                st.warning("Aucune donnée pour les matchs sélectionnés.")
                return

            aggregated = prepare_comparison_data(filtered, player_name)
            if aggregated.empty:
                st.warning("Aucune donnée agrégée.")
                return

            c1, c2 = st.columns(2)
            c1.metric("Temps de jeu", f"{int(aggregated['Temps de jeu (en minutes)'].iloc[0])} minutes")
            c2.metric("Buts", f"{int(aggregated.get('Buts', pd.Series([0])).iloc[0])}")

            tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
            with tab1:
                fig = create_individual_radar(aggregated)
                if fig:
                    st.pyplot(fig)

            with tab2:
                if "Rigueur" in aggregated.columns:
                    cols = st.columns(5)
                    cols[0].metric("Rigueur", f"{int(aggregated['Rigueur'].iloc[0])}/100")
                    cols[1].metric("Récupération", f"{int(aggregated.get('Récupération', pd.Series([0])).iloc[0])}/100")
                    cols[2].metric("Distribution", f"{int(aggregated.get('Distribution', pd.Series([0])).iloc[0])}/100")
                    cols[3].metric("Percussion", f"{int(aggregated.get('Percussion', pd.Series([0])).iloc[0])}/100")
                    cols[4].metric("Finition", f"{int(aggregated.get('Finition', pd.Series([0])).iloc[0])}/100")

            with tab3:
                if "Défenseur central" in aggregated.columns:
                    cols = st.columns(6)
                    cols[0].metric("Défenseur central", f"{int(aggregated['Défenseur central'].iloc[0])}/100")
                    cols[1].metric("Défenseur latéral", f"{int(aggregated['Défenseur latéral'].iloc[0])}/100")
                    cols[2].metric("Milieu défensif", f"{int(aggregated['Milieu défensif'].iloc[0])}/100")
                    cols[3].metric("Milieu relayeur", f"{int(aggregated['Milieu relayeur'].iloc[0])}/100")
                    cols[4].metric("Milieu offensif", f"{int(aggregated['Milieu offensif'].iloc[0])}/100")
                    cols[5].metric("Attaquant", f"{int(aggregated['Attaquant'].iloc[0])}/100")

        else:
            st.subheader("Sélectionnez une joueuse du Paris FC")
            if "Player" not in pfc_kpi.columns:
                st.warning("Colonne 'Player' manquante.")
                return

            player = st.selectbox("Choisissez une joueuse", sorted(pfc_kpi["Player"].dropna().unique()))
            player_data = pfc_kpi[pfc_kpi["Player"] == player]
            if player_data.empty:
                st.warning("Aucune donnée pour cette joueuse.")
                return

            if "Adversaire" in player_data.columns:
                game = st.multiselect("Choisissez un ou plusieurs matchs", sorted(player_data["Adversaire"].dropna().unique()))
                filtered = player_data[player_data["Adversaire"].isin(game)] if game else player_data
            else:
                filtered = player_data

            aggregated = filtered.groupby("Player", as_index=False).agg({"Temps de jeu (en minutes)": "sum", "Buts": "sum"}).merge(
                filtered.groupby("Player").mean(numeric_only=True).drop(columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore").reset_index(),
                on="Player",
                how="left"
            )
            aggregated = cast_numeric_only(aggregated, decimals=0, to_int=True)

            c1, c2 = st.columns(2)
            c1.metric("Temps de jeu", f"{int(aggregated['Temps de jeu (en minutes)'].iloc[0])} minutes")
            c2.metric("Buts", f"{int(aggregated.get('Buts', pd.Series([0])).iloc[0])}")

            tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
            with tab1:
                fig = create_individual_radar(aggregated)
                if fig:
                    st.pyplot(fig)

            with tab2:
                if "Rigueur" in aggregated.columns:
                    cols = st.columns(5)
                    cols[0].metric("Rigueur", f"{int(aggregated['Rigueur'].iloc[0])}/100")
                    cols[1].metric("Récupération", f"{int(aggregated.get('Récupération', pd.Series([0])).iloc[0])}/100")
                    cols[2].metric("Distribution", f"{int(aggregated.get('Distribution', pd.Series([0])).iloc[0])}/100")
                    cols[3].metric("Percussion", f"{int(aggregated.get('Percussion', pd.Series([0])).iloc[0])}/100")
                    cols[4].metric("Finition", f"{int(aggregated.get('Finition', pd.Series([0])).iloc[0])}/100")

            with tab3:
                if "Défenseur central" in aggregated.columns:
                    cols = st.columns(6)
                    cols[0].metric("Défenseur central", f"{int(aggregated['Défenseur central'].iloc[0])}/100")
                    cols[1].metric("Défenseur latéral", f"{int(aggregated['Défenseur latéral'].iloc[0])}/100")
                    cols[2].metric("Milieu défensif", f"{int(aggregated['Milieu défensif'].iloc[0])}/100")
                    cols[3].metric("Milieu relayeur", f"{int(aggregated['Milieu relayeur'].iloc[0])}/100")
                    cols[4].metric("Milieu offensif", f"{int(aggregated['Milieu offensif'].iloc[0])}/100")
                    cols[5].metric("Attaquant", f"{int(aggregated['Attaquant'].iloc[0])}/100")

    # =========================
    # PAGE: COMPARAISON
    # =========================
    elif page == "Comparaison":
        st.header("Comparaison")
        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible.")
            return

        if player_name:
            st.subheader(f"Comparaison pour {player_name}")

            st.write("### 1) Comparer sur différents matchs")
            if "Adversaire" in pfc_kpi.columns:
                matches = sorted(pfc_kpi["Adversaire"].dropna().unique())
                selected = st.multiselect("Sélectionnez 2 matchs ou plus", matches, key="cmp_matches")
                if len(selected) >= 2 and st.button("Comparer les matchs sélectionnés"):
                    blocks = []
                    for m in selected:
                        d = pfc_kpi[pfc_kpi["Adversaire"] == m]
                        agg = prepare_comparison_data(d, player_name)
                        if not agg.empty:
                            agg = agg.copy()
                            agg["Player"] = f"{player_name} ({m})"
                            blocks.append(agg)
                    if len(blocks) >= 2:
                        fig = create_comparison_radar(pd.concat(blocks, ignore_index=True))
                        if fig:
                            st.pyplot(fig)
                    else:
                        st.warning("Pas assez de données valides pour comparer.")

            st.write("### 2) Comparer avec l'EDF")
            if not edf_kpi.empty and "Poste" in edf_kpi.columns:
                poste = st.selectbox("Sélectionnez un poste EDF", edf_kpi["Poste"].unique(), key="edf_poste")
                edf_data = edf_kpi[edf_kpi["Poste"] == poste].rename(columns={"Poste": "Player"})
                player_data = prepare_comparison_data(pfc_kpi, player_name)

                if not edf_data.empty and not player_data.empty and st.button("Comparer avec EDF"):
                    fig = create_comparison_radar(pd.concat([player_data, edf_data], ignore_index=True),
                                                  player1_name=player_name, player2_name=f"EDF {poste}")
                    if fig:
                        st.pyplot(fig)
            else:
                st.warning("Aucune donnée EDF disponible.")

        else:
            st.subheader("Comparaison PFC (admin)")
            if "Player" not in pfc_kpi.columns:
                st.warning("Colonne 'Player' manquante.")
                return

            p1 = st.selectbox("Joueuse 1", sorted(pfc_kpi["Player"].dropna().unique()), key="p1")
            p2 = st.selectbox("Joueuse 2", sorted(pfc_kpi["Player"].dropna().unique()), key="p2")

            a1 = pfc_kpi[pfc_kpi["Player"] == p1].groupby("Player", as_index=False).mean(numeric_only=True)
            a2 = pfc_kpi[pfc_kpi["Player"] == p2].groupby("Player", as_index=False).mean(numeric_only=True)

            a1 = cast_numeric_only(a1, decimals=0, to_int=True)
            a2 = cast_numeric_only(a2, decimals=0, to_int=True)

            if st.button("Afficher le radar", key="radar_admin"):
                if not a1.empty and not a2.empty:
                    fig = create_comparison_radar(pd.concat([a1, a2], ignore_index=True))
                    if fig:
                        st.pyplot(fig)
                else:
                    st.warning("Données insuffisantes.")

    # =========================
    # PAGE: GESTION
    # =========================
    elif page == "Gestion":
        st.header("Gestion des utilisateurs")
        if not check_permission(user_profile, "all", permissions):
            st.error("Accès réservé.")
            return

        users_data = [{
            "Profil": prof,
            "Permissions": ", ".join(info.get("permissions", [])),
            "Joueuse associée": info.get("player") or "Aucune"
        } for prof, info in permissions.items()]
        st.dataframe(pd.DataFrame(users_data))

    # =========================
    # PAGE: DONNÉES PHYSIQUES
    # =========================
    elif page == "Données Physiques":
        st.header("📊 Données Physiques")
        st.info("En construction.")

    # =========================
    # PAGE: PASSERELLES
    # =========================
    elif page == "Joueuses Passerelles":
        st.header("🔄 Joueuses Passerelles")
        passerelle = load_passerelle_data()
        if not passerelle:
            st.warning("Aucune donnée passerelle.")
            return

        selected = st.selectbox("Sélectionnez une joueuse", list(passerelle.keys()))
        info = passerelle[selected]

        st.subheader("Identité")
        if info.get("Prénom"):
            st.write(f"**Prénom :** {info['Prénom']}")
        if info.get("Photo"):
            st.image(info["Photo"], width=150, caption="Photo")
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


# =========================================================
# THEME / MAIN
# =========================================================
def apply_theme_css():
    st.markdown(
        """
        <style>
            .stApp { background: linear-gradient(135deg, #002B5C 0%, #002B5C 100%); color: white; }
            .main .block-container { background: linear-gradient(135deg, #003A58 0%, #0047AB 100%); border-radius: 10px; padding: 20px; color: white; }
            .stButton>button { background-color: #0078D4; color: white; border-radius: 5px; border: none; padding: 8px 16px; }
            .stSelectbox>div>div, .stMultiselect>div>div { background-color: #003A58; color: white; border-radius: 5px; border: 1px solid #0078D4; }
            .stTextInput>div>div>input { background-color: #003A58; color: white; border-radius: 5px; border: 1px solid #0078D4; }
            .stMetric { background-color: rgba(0, 71, 171, 0.4); border-radius: 5px; padding: 10px; color: white; }
        </style>
        """,
        unsafe_allow_html=True
    )


def main():
    st.set_page_config(page_title="Paris FC - Centre de Formation Féminin", layout="wide")
    apply_theme_css()

    st.markdown(
        """
        <div style="background:linear-gradient(135deg,#002B5C 0%,#0047AB 100%);
                    color:white;padding:2rem;border-radius:10px;margin-bottom:2rem;position:relative;">
            <div style="position:absolute;left:1rem;top:50%;transform:translateY(-50%);">
                <img src="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png" width="120" style="opacity:0.9;">
            </div>
            <h1 style="text-align:center;margin:0;font-size:3rem;font-weight:bold;">Paris FC - Centre de Formation Féminin</h1>
            <p style="text-align:center;margin-top:0.5rem;font-size:1.2rem;">Data Center</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    permissions = load_permissions()
    if not permissions:
        st.error("Impossible de charger les permissions.")
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
                u = norm_str(username)
                if u in permissions and password == permissions[u]["password"]:
                    st.session_state.authenticated = True
                    st.session_state.user_profile = u
                    st.rerun()
                else:
                    st.error("Nom d'utilisateur ou mot de passe incorrect")
        st.stop()

    pfc_kpi, edf_kpi = collect_data("Toutes les saisons")
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)


if __name__ == "__main__":
    main()
