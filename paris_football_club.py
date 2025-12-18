import pandas as pd
import numpy as np
import os
import io
from typing import Dict, List, Optional, Set, Tuple

from mplsoccer import PyPizza, Radar, FontManager, grid
import streamlit as st
from streamlit_option_menu import option_menu

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import warnings
warnings.filterwarnings("ignore")

# =========================
# CONSTANTES / HELPERS
# =========================
POST_COLS = ['ATT', 'DCD', 'DCG', 'DD', 'DG', 'GB', 'MCD', 'MCG', 'MD', 'MDef', 'MG']

def safe_float(x, default=np.nan) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

def safe_int_numeric_only(df: pd.DataFrame, round_first: bool = True) -> pd.DataFrame:
    """Convertit en int uniquement les colonnes numériques (évite ValueError sur string)."""
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
    """Nettoie le nom d'une joueuse en supprimant les doublons et standardisant le format."""
    if isinstance(nom, str):
        nom = nom.strip().upper()
        nom = nom.replace("É", "E").replace("È", "E").replace("Ê", "E").replace("À", "A").replace("Ù", "U")
        parts = [part.strip().upper() for part in nom.split(",")]
        if len(parts) > 1 and parts[0] == parts[1]:
            return parts[0]
        return nom
    return nom

def is_valid_player_name(x: str) -> bool:
    x = nettoyer_nom_joueuse(str(x)) if x is not None else ""
    return x not in {"", "NAN", "NONE", "NULL"}

def infer_duration_unit(series: pd.Series) -> str:
    """
    Détecte si Duration est en secondes ou minutes.
    - total ~90 => minutes
    - total ~5400 => secondes
    """
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
        p = nettoyer_nom_joueuse(str(row.get(poste, "")))
        if is_valid_player_name(p):
            players.add(p)
    return players

# =========================
# GOOGLE DRIVE
# =========================
def authenticate_google_drive():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

def download_file(service, file_id, file_name, output_folder):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    os.makedirs(output_folder, exist_ok=True)
    file_path = os.path.join(output_folder, file_name)
    with open(file_path, "wb") as f:
        f.write(fh.getbuffer())

def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get("files", [])

def download_passerelle_files(service):
    folder_id = "19_ZU-FsAiNKxCfTw_WKzhTcuPDsGoVhL"
    output_folder = "data/passerelle"
    files = list_files_in_folder(service, folder_id)
    for file in files:
        if file["name"] == "Liste Joueuses Passerelles.xlsx":
            download_file(service, file["id"], file["name"], output_folder)
            return

def download_google_drive():
    service = authenticate_google_drive()
    folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
    output_folder = "data"
    files = list_files_in_folder(service, folder_id)
    for file in files:
        if file["name"].endswith((".csv", ".xlsx")):
            download_file(service, file["id"], file["name"], output_folder)
    download_passerelle_files(service)

def download_permissions_file():
    try:
        service = authenticate_google_drive()
        folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
        files = list_files_in_folder(service, folder_id)
        for file in files:
            if file["name"] == "Classeurs permissions streamlit.xlsx":
                output_folder = "data"
                download_file(service, file["id"], file["name"], output_folder)
                return os.path.join(output_folder, file["name"])
        return None
    except Exception as e:
        st.error(f"Erreur lors du téléchargement du fichier de permissions: {e}")
        return None

def load_permissions():
    try:
        permissions_path = download_permissions_file()
        if not permissions_path:
            return {}
        permissions_df = pd.read_excel(permissions_path)
        permissions = {}
        for _, row in permissions_df.iterrows():
            profile = str(row.get("Profil", "")).strip()
            if not profile:
                continue
            permissions[profile] = {
                "password": str(row.get("Mot de passe", "")).strip(),
                "permissions": [p.strip() for p in str(row.get("Permissions", "")).split(",")] if pd.notna(row.get("Permissions", np.nan)) else [],
                "player": nettoyer_nom_joueuse(row.get("Joueuse", "")) if pd.notna(row.get("Joueuse", np.nan)) else None,
            }
        return permissions
    except Exception as e:
        st.error(f"Erreur lors du chargement des permissions: {e}")
        return {}

# =========================
# PASSERELLES
# =========================
def load_passerelle_data():
    passerelle_data = {}
    passerelle_file = "data/passerelle/Liste Joueuses Passerelles.xlsx"
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
    except Exception as e:
        st.warning(f"Erreur lecture passerelles: {e}")
    return passerelle_data

# =========================
# TEMPS DE JEU
# =========================
def players_duration(match: pd.DataFrame, home_team: str, away_team: str) -> pd.DataFrame:
    """
    Calcule le temps de jeu PFC à partir des segments (Duration).
    IMPORTANT : dans ces CSV, les colonnes de postes (ATT, DG, etc.) décrivent le XI PFC,
    y compris sur les lignes où Row == adversaire. On ne tente donc PAS de reconstruire l'XI adverse.
    """
    if match is None or match.empty or "Duration" not in match.columns or "Row" not in match.columns:
        st.warning("Match vide ou colonnes manquantes ('Duration'/'Row') pour calculer la durée de jeu")
        return pd.DataFrame()

    available_posts = [p for p in POST_COLS if p in match.columns]
    if not available_posts:
        st.warning("Aucune colonne de poste disponible pour calculer la durée de jeu")
        return pd.DataFrame()

    # On ne garde que les lignes 'match' (Row = une des deux équipes)
    m = match.copy()
    m["Row_clean"] = m["Row"].astype(str).str.strip()
    m = m[m["Row_clean"].isin({str(home_team).strip(), str(away_team).strip()})].copy()
    if m.empty:
        st.warning("Aucune ligne match trouvée (Row != équipes) pour calculer la durée de jeu")
        return pd.DataFrame()

    # Détecter l'unité de Duration (secondes vs minutes)
    unit = infer_duration_unit(m["Duration"])

    def to_seconds(d):
        d = safe_float(d, default=np.nan)
        if np.isnan(d) or d <= 0:
            return 0.0
        return d * 60.0 if unit == "minutes" else d

    # Trier par temps si dispo
    if "Start time" in m.columns:
        m = m.sort_values(by="Start time", ascending=True)

    played_seconds: Dict[str, float] = {}
    current_pfc_lineup: Set[str] = set()

    for _, row in m.iterrows():
        dur_sec = to_seconds(row.get("Duration", 0))
        if dur_sec <= 0:
            continue

        # ⚠️ Les colonnes de postes décrivent le PFC même sur Row == adversaire
        current_pfc_lineup = extract_lineup_from_row(row, available_posts)

        if not current_pfc_lineup:
            continue

        for p in current_pfc_lineup:
            played_seconds[p] = played_seconds.get(p, 0.0) + dur_sec

    if not played_seconds:
        return pd.DataFrame()

    df = pd.DataFrame({
        "Player": list(played_seconds.keys()),
        "Temps de jeu (en minutes)": [v / 60.0 for v in played_seconds.values()],
    }).sort_values("Temps de jeu (en minutes)", ascending=False).reset_index(drop=True)

    return df

# =========================
# STATS ACTIONS
# =========================
def players_shots(joueurs):
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de tirs")
        return pd.DataFrame()
    players_shots, players_shots_on_target, players_goals = {}, {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Tir" in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i].get("Row", ""))
            players_shots[player] = players_shots.get(player, 0) + action.count("Tir")
            if "Tir" in joueurs.columns:
                is_successful = joueurs.iloc[i].get("Tir", None)
                if isinstance(is_successful, str):
                    if "Tir Cadré" in is_successful or "But" in is_successful:
                        players_shots_on_target[player] = players_shots_on_target.get(player, 0) + is_successful.count("Tir Cadré") + is_successful.count("But")
                    if "But" in is_successful:
                        players_goals[player] = players_goals.get(player, 0) + 1
    if not players_shots:
        return pd.DataFrame()
    return pd.DataFrame({
        "Player": list(players_shots.keys()),
        "Tirs": list(players_shots.values()),
        "Tirs cadrés": [players_shots_on_target.get(player, 0) for player in players_shots],
        "Buts": [players_goals.get(player, 0) for player in players_shots],
    }).sort_values(by="Tirs", ascending=False)

def players_passes(joueurs):
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de passes")
        return pd.DataFrame()
    player_short_passes, player_long_passes = {}, {}
    players_successful_short_passes, players_successful_long_passes = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Passe" in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i].get("Row", ""))
            if "Passe" in joueurs.columns:
                passe = joueurs.iloc[i].get("Passe", None)
                if isinstance(passe, str):
                    if "Courte" in passe:
                        player_short_passes[player] = player_short_passes.get(player, 0) + passe.count("Courte")
                        if "Réussie" in passe:
                            players_successful_short_passes[player] = players_successful_short_passes.get(player, 0) + passe.count("Réussie")
                    if "Longue" in passe:
                        player_long_passes[player] = player_long_passes.get(player, 0) + passe.count("Longue")
                        if "Réussie" in passe:
                            players_successful_long_passes[player] = players_successful_long_passes.get(player, 0) + passe.count("Réussie")
    if not player_short_passes:
        return pd.DataFrame()
    df_passes = pd.DataFrame({
        "Player": list(player_short_passes.keys()),
        "Passes courtes": [player_short_passes.get(player, 0) for player in player_short_passes],
        "Passes longues": [player_long_passes.get(player, 0) for player in player_short_passes],
        "Passes réussies (courtes)": [players_successful_short_passes.get(player, 0) for player in player_short_passes],
        "Passes réussies (longues)": [players_successful_long_passes.get(player, 0) for player in player_short_passes],
    })
    df_passes["Passes"] = df_passes["Passes courtes"] + df_passes["Passes longues"]
    df_passes["Passes réussies"] = df_passes["Passes réussies (courtes)"] + df_passes["Passes réussies (longues)"]
    df_passes["Pourcentage de passes réussies"] = (df_passes["Passes réussies"] / df_passes["Passes"] * 100).fillna(0)
    return df_passes.sort_values(by="Passes courtes", ascending=False)

def players_dribbles(joueurs):
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de dribbles")
        return pd.DataFrame()
    players_dribbles, players_successful_dribbles = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Dribble" in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i].get("Row", ""))
            players_dribbles[player] = players_dribbles.get(player, 0) + action.count("Dribble")
            if "Dribble" in joueurs.columns:
                is_successful = joueurs.iloc[i].get("Dribble", None)
                if isinstance(is_successful, str) and "Réussi" in is_successful:
                    players_successful_dribbles[player] = players_successful_dribbles.get(player, 0) + is_successful.count("Réussi")
    if not players_dribbles:
        return pd.DataFrame()
    df_dribbles = pd.DataFrame({
        "Player": list(players_dribbles.keys()),
        "Dribbles": list(players_dribbles.values()),
        "Dribbles réussis": [players_successful_dribbles.get(player, 0) for player in players_dribbles],
    })
    df_dribbles["Pourcentage de dribbles réussis"] = (df_dribbles["Dribbles réussis"] / df_dribbles["Dribbles"] * 100).fillna(0)
    return df_dribbles.sort_values(by="Dribbles", ascending=False)

def players_defensive_duels(joueurs):
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de duels défensifs")
        return pd.DataFrame()
    players_defensive_duels, players_successful_defensive_duels, players_faults = {}, {}, {}
    duels_col = "Duel défensifs" if "Duel défensifs" in joueurs.columns else ("Duel défensif" if "Duel défensif" in joueurs.columns else None)
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Duel défensif" in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i].get("Row", ""))
            players_defensive_duels[player] = players_defensive_duels.get(player, 0) + action.count("Duel défensif")
            if duels_col and duels_col in joueurs.columns:
                is_successful = joueurs.iloc[i].get(duels_col, None)
                if isinstance(is_successful, str):
                    if "Gagné" in is_successful:
                        players_successful_defensive_duels[player] = players_successful_defensive_duels.get(player, 0) + is_successful.count("Gagné")
                    if "Faute" in is_successful:
                        players_faults[player] = players_faults.get(player, 0) + is_successful.count("Faute")
    if not players_defensive_duels:
        return pd.DataFrame()
    df_duels_defensifs = pd.DataFrame({
        "Player": list(players_defensive_duels.keys()),
        "Duels défensifs": list(players_defensive_duels.values()),
        "Duels défensifs gagnés": [players_successful_defensive_duels.get(player, 0) for player in players_defensive_duels],
        "Fautes": [players_faults.get(player, 0) for player in players_defensive_duels],
    })
    df_duels_defensifs["Pourcentage de duels défensifs gagnés"] = (
        df_duels_defensifs["Duels défensifs gagnés"] / df_duels_defensifs["Duels défensifs"] * 100
    ).fillna(0)
    return df_duels_defensifs.sort_values(by="Duels défensifs", ascending=False)

def players_interceptions(joueurs):
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques d'interceptions")
        return pd.DataFrame()
    players_interceptions = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Interception" in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i].get("Row", ""))
            players_interceptions[player] = players_interceptions.get(player, 0) + action.count("Interception")
    if not players_interceptions:
        return pd.DataFrame()
    return pd.DataFrame({
        "Player": list(players_interceptions.keys()),
        "Interceptions": list(players_interceptions.values()),
    }).sort_values(by="Interceptions", ascending=False)

def players_ball_losses(joueurs):
    if "Action" not in joueurs.columns or "Row" not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de pertes de balle")
        return pd.DataFrame()
    players_ball_losses = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i].get("Action", None)
        if isinstance(action, str) and "Perte de balle" in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i].get("Row", ""))
            players_ball_losses[player] = players_ball_losses.get(player, 0) + action.count("Perte de balle")
    if not players_ball_losses:
        return pd.DataFrame()
    return pd.DataFrame({
        "Player": list(players_ball_losses.keys()),
        "Pertes de balle": list(players_ball_losses.values()),
    }).sort_values(by="Pertes de balle", ascending=False)

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
        if not all(col in df.columns for col in cols):
            continue
        if metric == "Timing":
            df[metric] = np.where(df[cols[0]] > 0, (df[cols[0]] - df.get(cols[1], 0)) / df[cols[0]], 0)
        elif metric == "Force physique":
            df[metric] = np.where(df[cols[0]] > 0, df.get(cols[1], 0) / df[cols[0]], 0)
        elif metric in ["Intelligence tactique", "Technique 1", "Prise de risque", "Sang-froid"]:
            mmax = df[cols[0]].max()
            df[metric] = np.where(df[cols[0]] > 0, df[cols[0]] / mmax, 0) if mmax > 0 else 0
        elif metric in ["Technique 2", "Technique 3", "Explosivité", "Précision"]:
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
    tech_metrics = [m for m in ["Technique 1", "Technique 2", "Technique 3"] if m in df.columns]
    if tech_metrics:
        df["Distribution"] = df[tech_metrics].mean(axis=1)
    if "Explosivité" in df.columns and "Prise de risque" in df.columns:
        df["Percussion"] = (df["Explosivité"] + df["Prise de risque"]) / 2
    if "Précision" in df.columns and "Sang-froid" in df.columns:
        df["Finition"] = (df["Précision"] + df["Sang-froid"]) / 2
    return df

def create_poste(df):
    if df.empty:
        return df
    required_kpis = ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition"]
    if not all(k in df.columns for k in required_kpis):
        return df
    df["Défenseur central"] = (df["Rigueur"] * 5 + df["Récupération"] * 5 + df["Distribution"] * 5 + df["Percussion"] * 1 + df["Finition"] * 1) / 17
    df["Défenseur latéral"] = (df["Rigueur"] * 3 + df["Récupération"] * 3 + df["Distribution"] * 3 + df["Percussion"] * 3 + df["Finition"] * 3) / 15
    df["Milieu défensif"] = (df["Rigueur"] * 4 + df["Récupération"] * 4 + df["Distribution"] * 4 + df["Percussion"] * 2 + df["Finition"] * 2) / 16
    df["Milieu relayeur"] = (df["Rigueur"] * 3 + df["Récupération"] * 3 + df["Distribution"] * 3 + df["Percussion"] * 3 + df["Finition"] * 3) / 15
    df["Milieu offensif"] = (df["Rigueur"] * 2 + df["Récupération"] * 2 + df["Distribution"] * 2 + df["Percussion"] * 4 + df["Finition"] * 4) / 14
    df["Attaquant"] = (df["Rigueur"] * 1 + df["Récupération"] * 1 + df["Distribution"] * 1 + df["Percussion"] * 5 + df["Finition"] * 5) / 13
    return df

# =========================
# DATAFRAME MATCH COMPLET
# =========================
def create_data(match, joueurs, is_edf, home_team=None, away_team=None):
    try:
        if is_edf:
            if "Player" not in joueurs.columns:
                st.error("La colonne 'Player' est manquante dans les données EDF.")
                return pd.DataFrame()
            joueurs = joueurs.copy()
            joueurs["Player"] = joueurs["Player"].apply(nettoyer_nom_joueuse)
            if "Poste" not in joueurs.columns or "Temps de jeu" not in joueurs.columns:
                st.error("Les colonnes 'Poste' ou 'Temps de jeu' sont manquantes dans les données EDF.")
                return pd.DataFrame()
            df_duration = pd.DataFrame({
                "Player": joueurs["Player"],
                "Temps de jeu (en minutes)": pd.to_numeric(joueurs["Temps de jeu"], errors="coerce").fillna(0),
                "Poste": joueurs["Poste"],
            })
        else:
            if not home_team or not away_team:
                st.warning("home_team / away_team manquants pour calculer le temps de jeu.")
                return pd.DataFrame()
            df_duration = players_duration(match, home_team=home_team, away_team=away_team)

        dfs = [df_duration]
        calc_functions = [
            ("tirs", players_shots),
            ("passes", players_passes),
            ("dribbles", players_dribbles),
            ("duels", players_defensive_duels),
            ("interceptions", players_interceptions),
            ("pertes", players_ball_losses),
        ]
        for name, func in calc_functions:
            try:
                result = func(joueurs)
                if result is not None and not result.empty:
                    dfs.append(result)
            except Exception as e:
                st.warning(f"Erreur lors du calcul des {name}: {e}")

        valid_dfs = []
        for d in dfs:
            if d is not None and not d.empty and "Player" in d.columns:
                d = d.copy()
                d["Player"] = d["Player"].apply(nettoyer_nom_joueuse)
                valid_dfs.append(d)

        if not valid_dfs:
            return pd.DataFrame()

        df = valid_dfs[0]
        for other_df in valid_dfs[1:]:
            df = df.merge(other_df, on="Player", how="outer")

        if not df.empty:
            df.fillna(0, inplace=True)
            df = df[(df.iloc[:, 1:] != 0).any(axis=1)]
            if "Temps de jeu (en minutes)" in df.columns:
                df = df[df["Temps de jeu (en minutes)"] >= 10]
            df = create_metrics(df)
            df = create_kpis(df)
            df = create_poste(df)
        return df
    except Exception as e:
        st.error(f"Erreur lors de la création des données: {e}")
        return pd.DataFrame()

def filter_data_by_player(df, player_name):
    if not player_name or df.empty or "Player" not in df.columns:
        return df
    player_name_clean = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)
    filtered_df = tmp[tmp["Player_clean"] == player_name_clean].copy()
    filtered_df.drop(columns=["Player_clean"], inplace=True, errors="ignore")
    return filtered_df

def prepare_comparison_data(df, player_name, selected_matches=None):
    if df.empty or "Player" not in df.columns:
        return pd.DataFrame()
    player_name_clean = nettoyer_nom_joueuse(player_name)
    tmp = df.copy()
    tmp["Player_clean"] = tmp["Player"].apply(nettoyer_nom_joueuse)

    if selected_matches:
        filtered_df = tmp[tmp["Player_clean"] == player_name_clean]
        if "Adversaire" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["Adversaire"].isin(selected_matches)]
    else:
        filtered_df = tmp[tmp["Player_clean"] == player_name_clean]

    if filtered_df.empty:
        return pd.DataFrame()

    aggregated_data = filtered_df.groupby("Player").agg({
        "Temps de jeu (en minutes)": "sum",
        "Buts": "sum",
    }).join(
        filtered_df.groupby("Player").mean(numeric_only=True).drop(
            columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
        )
    ).reset_index()

    aggregated_data = safe_int_numeric_only(aggregated_data, round_first=True)
    return aggregated_data

def generate_synthesis_excel(pfc_kpi):
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if not pfc_kpi.empty:
                tmp = pfc_kpi.copy()
                tmp.insert(0, "Joueuse", tmp["Player"])
                tmp.to_excel(writer, sheet_name="Synthèse", index=False)
        return output.getvalue()
    except Exception as e:
        st.error(f"Erreur lors de la génération du fichier Excel de synthèse : {e}")
        return None

# =========================
# COLLECT DATA
# =========================
@st.cache_data
def collect_data(selected_season=None):
    try:
        download_google_drive()
        pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()
        data_folder = "data"
        if not os.path.exists(data_folder):
            return pfc_kpi, edf_kpi

        fichiers = [f for f in os.listdir(data_folder) if f.endswith((".csv", ".xlsx")) and f != "Classeurs permissions streamlit.xlsx"]
        if not fichiers:
            return pfc_kpi, edf_kpi

        if selected_season and selected_season != "Toutes les saisons":
            fichiers = [f for f in fichiers if f"{selected_season}" in f]

        # ---- EDF ----
        edf_joueuses_path = os.path.join(data_folder, "EDF_Joueuses.xlsx")
        if os.path.exists(edf_joueuses_path):
            edf_joueuses = pd.read_excel(edf_joueuses_path)
            needed = {"Player", "Poste", "Temps de jeu"}
            if needed.issubset(set(edf_joueuses.columns)):
                edf_joueuses["Player"] = edf_joueuses["Player"].apply(nettoyer_nom_joueuse)
                matchs_csv = [f for f in fichiers if f.startswith("EDF_U19_Match") and f.endswith(".csv")]
                if matchs_csv:
                    all_edf_data = []
                    for csv_file in matchs_csv:
                        match_data = pd.read_csv(os.path.join(data_folder, csv_file))
                        if "Row" not in match_data.columns:
                            continue
                        match_data["Player"] = match_data["Row"].apply(nettoyer_nom_joueuse)
                        match_data = match_data.merge(edf_joueuses, on="Player", how="left")
                        if match_data.empty:
                            continue
                        df = create_data(match_data, match_data, True)
                        if not df.empty:
                            all_edf_data.append(df)
                    if all_edf_data:
                        edf_kpi = pd.concat(all_edf_data, ignore_index=True)
                        if "Poste" in edf_kpi.columns:
                            edf_kpi = edf_kpi.groupby("Poste").mean(numeric_only=True).reset_index()
                            edf_kpi["Poste"] = edf_kpi["Poste"] + " moyenne (EDF)"

        # ---- PFC ----
        for filename in fichiers:
            if not (filename.endswith(".csv") and "PFC" in filename):
                continue

            path = os.path.join(data_folder, filename)
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

                # ======= MODIFICATION CLE : équipes depuis le CSV (Teamersaire/Row) =======
                row_vals = data["Row"].astype(str).str.strip()
                unique_rows = set(row_vals.dropna().unique().tolist())

                equipe_pfc = "PFC" if "PFC" in unique_rows else str(parts[0]).strip()

                equipe_adv = None
                if "Teamersaire" in data.columns:
                    adv_series = data.loc[row_vals.eq(equipe_pfc), "Teamersaire"]
                    adv_series = adv_series.dropna().astype(str).str.strip()
                    if not adv_series.empty:
                        equipe_adv = adv_series.mode().iloc[0]

                if not equipe_adv:
                    counts = row_vals.value_counts()
                    candidates = [k for k in counts.index.tolist() if k and k != equipe_pfc]
                    if candidates:
                        equipe_adv = candidates[0]

                if not equipe_adv:
                    st.warning(f"Adversaire introuvable dans {filename} (Teamersaire/Row).")
                    continue
                # ======================================================================

                match, joueurs = pd.DataFrame(), pd.DataFrame()
                for i in range(len(data)):
                    r = str(data["Row"].iloc[i]).strip()
                    if r in {equipe_pfc, equipe_adv}:
                        match = pd.concat([match, data.iloc[i:i+1]], ignore_index=True)
                    elif not any(x in r for x in ["Corner", "Coup-franc", "Penalty", "Carton"]):
                        joueurs = pd.concat([joueurs, data.iloc[i:i+1]], ignore_index=True)

                if match.empty or joueurs.empty:
                    st.warning(f"Match/joueurs vides pour {filename} (PFC={equipe_pfc}, ADV={equipe_adv}).")
                    continue

                joueurs = joueurs.copy()
                joueurs["Player"] = joueurs["Row"].apply(nettoyer_nom_joueuse)

                df = create_data(match, joueurs, False, home_team=equipe_pfc, away_team=equipe_adv)
                if df.empty:
                    continue

                # Normalisation par 90 minutes (seulement sur numériques)
                for idx, row in df.iterrows():
                    time_played = safe_float(row.get("Temps de jeu (en minutes)", np.nan), default=np.nan)
                    if np.isnan(time_played) or time_played <= 0:
                        continue
                    scale = 90.0 / time_played
                    for col in df.columns:
                        if col in ["Player", "Temps de jeu (en minutes)", "Buts"] or "Pourcentage" in col:
                            continue
                        if pd.api.types.is_numeric_dtype(df[col]):
                            df.loc[idx, col] = row[col] * scale

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

        return pfc_kpi, edf_kpi

    except Exception:
        return pd.DataFrame(), pd.DataFrame()

# =========================
# RADARS
# =========================
def create_individual_radar(df):
    if df.empty or "Player" not in df.columns:
        st.warning("Aucune donnée disponible pour créer le radar.")
        return None

    columns_to_plot = [
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
    ]
    available_columns = [col for col in columns_to_plot if col in df.columns]
    if not available_columns:
        st.warning("Aucune colonne de métrique disponible pour le radar")
        return None

    colors = ["#6A7CD9", "#00BFFE", "#FF9470", "#F27979", "#BFBFBF"] * 2
    player = df.iloc[0]

    pizza = PyPizza(
        params=available_columns,
        background_color="#002B5C",
        straight_line_color="#FFFFFF",
        last_circle_color="#FFFFFF",
    )

    fig, _ = pizza.make_pizza(
        figsize=(3, 3),
        values=[player[col] for col in available_columns],
        slice_colors=colors[:len(available_columns)],
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
        st.warning("Données insuffisantes pour créer une comparaison.")
        return None

    metrics = [
        "Timing", "Force physique", "Intelligence tactique",
        "Technique 1", "Technique 2", "Technique 3",
        "Explosivité", "Prise de risque", "Précision", "Sang-froid",
    ]
    available_metrics = [m for m in metrics if m in df.columns]
    if len(available_metrics) < 2:
        st.warning("Pas assez de métriques disponibles pour la comparaison")
        return None

    low, high = (0,) * len(available_metrics), (100,) * len(available_metrics)
    radar = Radar(available_metrics, low, high, num_rings=4, ring_width=1, center_circle_radius=1)

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

    player_values_1 = df.iloc[0][available_metrics].values
    player_values_2 = df.iloc[1][available_metrics].values

    radar.draw_radar_compare(
        player_values_1,
        player_values_2,
        ax=axs["radar"],
        kwargs_radar={"facecolor": "#00f2c1", "alpha": 0.6},
        kwargs_compare={"facecolor": "#d80499", "alpha": 0.6},
    )

    radar.draw_range_labels(ax=axs["radar"], fontsize=18, color="#fcfcfc", fontproperties=robotto_thin.prop)
    radar.draw_param_labels(ax=axs["radar"], fontsize=18, color="#fcfcfc", fontproperties=robotto_thin.prop)

    player1_label = player1_name if player1_name else df.iloc[0]["Player"]
    player2_label = player2_name if player2_name else df.iloc[1]["Player"]

    axs["title"].text(0.01, 0.65, player1_label, fontsize=18, color="#01c49d",
                      fontproperties=robotto_bold.prop, ha="left", va="center")
    axs["title"].text(0.99, 0.65, player2_label, fontsize=18, color="#d80499",
                      fontproperties=robotto_bold.prop, ha="right", va="center")

    fig.set_facecolor("#002B5C")
    return fig

# =========================
# PERMISSIONS
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
# UI
# =========================
def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
    logo_pfc = "https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"
    st.sidebar.markdown(f"<div style='display: flex; justify-content: center;'><img src='{logo_pfc}' width='100'></div>", unsafe_allow_html=True)

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
            with st.spinner("Mise à jour des données en cours..."):
                download_google_drive()
                pfc_kpi, edf_kpi = collect_data(selected_saison)
            st.success("✅ Mise à jour terminée")
            st.cache_data.clear()

    if check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Télécharger la synthèse des statistiques"):
            with st.spinner("Génération du fichier de synthèse en cours..."):
                pfc_kpi_all, _ = collect_data()
                excel_bytes = generate_synthesis_excel(pfc_kpi_all)
                if excel_bytes:
                    st.sidebar.download_button(
                        label="⬇️ Télécharger le fichier Excel",
                        data=excel_bytes,
                        file_name="synthese_statistiques_joueuses.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                    st.success("✅ Fichier Excel prêt à être téléchargé !")

    if selected_saison != "Toutes les saisons":
        pfc_kpi, edf_kpi = collect_data(selected_saison)
    else:
        pfc_kpi, edf_kpi = collect_data()

    if player_name and not pfc_kpi.empty and "Player" in pfc_kpi.columns:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)
        if pfc_kpi.empty:
            st.warning(f"Aucune donnée disponible pour la joueuse {player_name}")

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

    # ---- STATISTIQUES ----
    if page == "Statistiques":
        st.header("Statistiques")
        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible pour votre profil.")
            return

        if player_name:
            st.subheader(f"Statistiques pour {player_name}")
            if "Adversaire" not in pfc_kpi.columns:
                st.warning("Colonne 'Adversaire' manquante.")
                return

            unique_matches = pfc_kpi["Adversaire"].unique()
            game = st.multiselect("Choisissez un ou plusieurs matchs", unique_matches)
            filtered_data = pfc_kpi[pfc_kpi["Adversaire"].isin(game)] if game else pfc_kpi
            if filtered_data.empty:
                st.warning("Aucune donnée disponible pour les matchs sélectionnés.")
                return

            aggregated_data = filtered_data.groupby("Player").agg({
                "Temps de jeu (en minutes)": "sum",
                "Buts": "sum",
            }).join(
                filtered_data.groupby("Player").mean(numeric_only=True).drop(
                    columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
                )
            ).reset_index()

            aggregated_data = safe_int_numeric_only(aggregated_data)

            c1, c2 = st.columns(2)
            with c1:
                st.metric("Temps de jeu", f"{int(aggregated_data['Temps de jeu (en minutes)'].iloc[0])} minutes")
            with c2:
                st.metric("Buts", f"{int(aggregated_data['Buts'].iloc[0])}")

            tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
            with tab1:
                fig = create_individual_radar(aggregated_data)
                if fig:
                    st.pyplot(fig)

            with tab2:
                for k in ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition"]:
                    if k not in aggregated_data.columns:
                        continue
                if "Rigueur" in aggregated_data.columns:
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1: st.metric("Rigueur", f"{int(aggregated_data['Rigueur'].iloc[0])}/100")
                    with col2: st.metric("Récupération", f"{int(aggregated_data['Récupération'].iloc[0])}/100")
                    with col3: st.metric("Distribution", f"{int(aggregated_data['Distribution'].iloc[0])}/100")
                    with col4: st.metric("Percussion", f"{int(aggregated_data['Percussion'].iloc[0])}/100")
                    with col5: st.metric("Finition", f"{int(aggregated_data['Finition'].iloc[0])}/100")

            with tab3:
                if "Défenseur central" in aggregated_data.columns:
                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                    with col1: st.metric("Défenseur central", f"{int(aggregated_data['Défenseur central'].iloc[0])}/100")
                    with col2: st.metric("Défenseur latéral", f"{int(aggregated_data['Défenseur latéral'].iloc[0])}/100")
                    with col3: st.metric("Milieu défensif", f"{int(aggregated_data['Milieu défensif'].iloc[0])}/100")
                    with col4: st.metric("Milieu relayeur", f"{int(aggregated_data['Milieu relayeur'].iloc[0])}/100")
                    with col5: st.metric("Milieu offensif", f"{int(aggregated_data['Milieu offensif'].iloc[0])}/100")
                    with col6: st.metric("Attaquant", f"{int(aggregated_data['Attaquant'].iloc[0])}/100")
        else:
            st.subheader("Sélectionnez une joueuse du Paris FC")
            player = st.selectbox("Choisissez une joueuse", pfc_kpi["Player"].unique())
            player_data = pfc_kpi[pfc_kpi["Player"] == player]
            if player_data.empty:
                st.error("Aucune donnée disponible pour cette joueuse.")
                return
            if "Adversaire" not in player_data.columns:
                st.warning("Colonne 'Adversaire' manquante.")
                return
            game = st.multiselect("Choisissez un ou plusieurs matchs", player_data["Adversaire"].unique())
            filtered_data = player_data[player_data["Adversaire"].isin(game)] if game else player_data
            if filtered_data.empty:
                st.warning("Aucune donnée disponible pour les matchs sélectionnés.")
                return

            aggregated_data = filtered_data.groupby("Player").agg({
                "Temps de jeu (en minutes)": "sum",
                "Buts": "sum",
            }).join(
                filtered_data.groupby("Player").mean(numeric_only=True).drop(
                    columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
                )
            ).reset_index()

            aggregated_data = safe_int_numeric_only(aggregated_data)

            c1, c2 = st.columns(2)
            with c1:
                st.metric("Temps de jeu", f"{int(aggregated_data['Temps de jeu (en minutes)'].iloc[0])} minutes")
            with c2:
                st.metric("Buts", f"{int(aggregated_data['Buts'].iloc[0])}")

            tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
            with tab1:
                fig = create_individual_radar(aggregated_data)
                if fig:
                    st.pyplot(fig)

    # ---- COMPARAISON (incluant EDF) ----
    elif page == "Comparaison":
        st.header("Comparaison")
        if pfc_kpi.empty:
            st.warning("Aucune donnée PFC disponible.")
            return

        if player_name:
            st.subheader(f"Comparaison pour {player_name}")

            st.write("### 1) Comparez vos performances sur différents matchs")
            if "Adversaire" in pfc_kpi.columns:
                unique_matches = pfc_kpi["Adversaire"].unique()
                selected_matches = st.multiselect("Sélectionnez les matchs (2 ou +)", unique_matches, key="selected_matches")
                if len(selected_matches) >= 2:
                    comparison_data = []
                    for match_label in selected_matches:
                        match_data = pfc_kpi[pfc_kpi["Adversaire"] == match_label]
                        if match_data.empty:
                            continue
                        aggregated = match_data.groupby("Player").agg({
                            "Temps de jeu (en minutes)": "sum",
                            "Buts": "sum",
                        }).join(
                            match_data.groupby("Player").mean(numeric_only=True).drop(
                                columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
                            )
                        ).reset_index()
                        aggregated = safe_int_numeric_only(aggregated)
                        if not aggregated.empty:
                            aggregated["Player"] = f"{player_name} ({match_label})"
                            comparison_data.append(aggregated)
                    if len(comparison_data) >= 2 and st.button("Comparer les matchs sélectionnés"):
                        players_data = pd.concat(comparison_data, ignore_index=True)
                        fig = create_comparison_radar(players_data)
                        if fig:
                            st.pyplot(fig)
                else:
                    st.info("Sélectionne au moins 2 matchs.")

            st.write("### 2) Comparez-vous au référentiel EDF U19")
            if not edf_kpi.empty and "Poste" in edf_kpi.columns:
                poste = st.selectbox("Sélectionnez un poste EDF", edf_kpi["Poste"].unique(), key="edf_poste")
                edf_data = edf_kpi[edf_kpi["Poste"] == poste].rename(columns={"Poste": "Player"})
                if not edf_data.empty:
                    player_data = prepare_comparison_data(pfc_kpi, player_name)
                    if not player_data.empty and st.button("Comparer avec le poste EDF"):
                        players_data = pd.concat([player_data, edf_data], ignore_index=True)
                        fig = create_comparison_radar(players_data, player1_name=player_name, player2_name=f"EDF {poste}")
                        if fig:
                            st.pyplot(fig)
            else:
                st.warning("Aucune donnée EDF disponible.")

            st.write("### 3) Comparez-vous à vos moyennes globales")
            player_global_data = prepare_comparison_data(pfc_kpi, player_name)
            if not player_global_data.empty and "Adversaire" in pfc_kpi.columns:
                selected_match = st.selectbox("Sélectionnez un match", pfc_kpi["Adversaire"].unique(), key="specific_match")
                match_data = pfc_kpi[pfc_kpi["Adversaire"] == selected_match]
                if not match_data.empty and st.button("Comparer avec mes moyennes"):
                    match_aggregated = match_data.groupby("Player").agg({
                        "Temps de jeu (en minutes)": "sum",
                        "Buts": "sum",
                    }).join(
                        match_data.groupby("Player").mean(numeric_only=True).drop(
                            columns=["Temps de jeu (en minutes)", "Buts"], errors="ignore"
                        )
                    ).reset_index()
                    match_aggregated = safe_int_numeric_only(match_aggregated)
                    match_aggregated["Player"] = f"{player_name} ({selected_match})"
                    player_global_data["Player"] = f"{player_name} (Moyenne globale)"
                    players_data = pd.concat([match_aggregated, player_global_data], ignore_index=True)
                    fig = create_comparison_radar(players_data)
                    if fig:
                        st.pyplot(fig)

        else:
            st.subheader("Sélectionnez une joueuse du Paris FC")
            player1 = st.selectbox("Choisissez une joueuse", pfc_kpi["Player"].unique(), key="player_1")
            player1_data = pfc_kpi[pfc_kpi["Player"] == player1]
            if player1_data.empty:
                st.error("Aucune donnée disponible.")
                return

            game1 = st.multiselect("Matchs (joueuse 1)", player1_data["Adversaire"].unique(), key="games_1") if "Adversaire" in player1_data.columns else []
            filtered_player1_data = player1_data[player1_data["Adversaire"].isin(game1)] if game1 else player1_data
            aggregated_player1_data = filtered_player1_data.groupby("Player").mean(numeric_only=True).reset_index()
            aggregated_player1_data = safe_int_numeric_only(aggregated_player1_data)

            tab1, tab2 = st.tabs(["Comparaison (PFC)", "Comparaison (EDF)"])
            with tab1:
                player2 = st.selectbox("Joueuse 2 (PFC)", pfc_kpi["Player"].unique(), key="player_2_pfc")
                player2_data = pfc_kpi[pfc_kpi["Player"] == player2]
                game2 = st.multiselect("Matchs (joueuse 2)", player2_data["Adversaire"].unique(), key="games_2_pfc") if "Adversaire" in player2_data.columns else []
                filtered_player2_data = player2_data[player2_data["Adversaire"].isin(game2)] if game2 else player2_data
                aggregated_player2_data = filtered_player2_data.groupby("Player").mean(numeric_only=True).reset_index()
                aggregated_player2_data = safe_int_numeric_only(aggregated_player2_data)

                if st.button("Afficher le radar", key="button_pfc"):
                    players_data = pd.concat([aggregated_player1_data, aggregated_player2_data], ignore_index=True)
                    fig = create_comparison_radar(players_data, player1_name=player1, player2_name=player2)
                    if fig:
                        st.pyplot(fig)

            with tab2:
                if not edf_kpi.empty and "Poste" in edf_kpi.columns:
                    poste = st.selectbox("Poste EDF", edf_kpi["Poste"].unique(), key="player_2_edf")
                    player2_data = edf_kpi[edf_kpi["Poste"] == poste].rename(columns={"Poste": "Player"})
                    if st.button("Afficher le radar", key="button_edf"):
                        players_data = pd.concat([aggregated_player1_data, player2_data], ignore_index=True)
                        fig = create_comparison_radar(players_data, player1_name=player1, player2_name=f"EDF {poste}")
                        if fig:
                            st.pyplot(fig)
                else:
                    st.warning("Aucune donnée EDF disponible.")

    # ---- GESTION ----
    elif page == "Gestion":
        st.header("Gestion des utilisateurs")
        if not check_permission(user_profile, "all", permissions):
            st.error("Vous n'avez pas la permission d'accéder à cette page.")
            return
        users_data = []
        for profile, info in permissions.items():
            users_data.append({
                "Profil": profile,
                "Permissions": ", ".join(info["permissions"]),
                "Joueuse associée": info.get("player", "Aucune"),
            })
        st.dataframe(pd.DataFrame(users_data))

    # ---- DONNEES PHYSIQUES ----
    elif page == "Données Physiques":
        st.header("📊 Données Physiques")
        st.info("Section en construction (placeholder).")

    # ---- PASSERELLES ----
    elif page == "Joueuses Passerelles":
        st.header("🔄 Joueuses Passerelles")
        passerelle_data = load_passerelle_data()
        if not passerelle_data:
            st.warning("Aucune donnée de joueuse passerelle disponible.")
            return
        selected_joueuse = st.selectbox("Sélectionnez une joueuse", list(passerelle_data.keys()))
        info = passerelle_data[selected_joueuse]
        st.subheader("Identité")
        if info.get("Prénom"): st.write(f"**Prénom :** {info['Prénom']}")
        if info.get("Photo"): st.image(info["Photo"], width=150, caption="Photo")
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
        .main .block-container { background: linear-gradient(135deg, #003A58 0%, #0047AB 100%); border-radius: 10px; padding: 20px; color: white; }
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
        st.error("Impossible de charger les permissions. Vérifiez le fichier 'Classeurs permissions streamlit.xlsx'.")
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

