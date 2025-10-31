import pandas as pd
import numpy as np
import os
from mplsoccer import PyPizza, Radar, FontManager, grid
import streamlit as st
from streamlit_option_menu import option_menu
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import warnings

warnings.filterwarnings('ignore')

# =============================================
# FONCTIONS D'AUTHENTIFICATION ET GESTION DRIVE
# =============================================
def authenticate_google_drive():
    """Authentification avec Google Drive."""
    SCOPES = ['https://www.googleapis.com/auth/drive']
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

def download_file(service, file_id, file_name, output_folder):
    """Télécharge un fichier depuis Google Drive."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_path = os.path.join(output_folder, file_name)
    with open(file_path, 'wb') as f:
        f.write(fh.getbuffer())
    print(f"Fichier téléchargé : {file_path}")

def list_files_in_folder(service, folder_id):
    """Liste les fichiers dans un dossier Google Drive."""
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def download_google_drive():
    """Télécharge les données depuis Google Drive."""
    try:
        service = authenticate_google_drive()
        folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
        output_folder = "data"
        os.makedirs(output_folder, exist_ok=True)
        files = list_files_in_folder(service, folder_id)
        if not files:
            print("Aucun fichier trouvé dans le dossier.")
        else:
            for file in files:
                if file['name'].endswith(('.csv', '.xlsx')) and file['name'] != "Classeurs permissions streamlit.xlsx":
                    print(f"Téléchargement de : {file['name']}...")
                    download_file(service, file['id'], file['name'], output_folder)
    except Exception as e:
        st.error(f"Erreur lors du téléchargement des fichiers: {e}")
        raise e

def download_permissions_file():
    """Télécharge le fichier des permissions depuis Google Drive."""
    try:
        service = authenticate_google_drive()
        folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
        files = list_files_in_folder(service, folder_id)
        for file in files:
            if file['name'] == "Classeurs permissions streamlit.xlsx":
                output_folder = "data"
                os.makedirs(output_folder, exist_ok=True)
                download_file(service, file['id'], file['name'], output_folder)
                permissions_path = os.path.join(output_folder, file['name'])
                return permissions_path
        return None
    except Exception as e:
        st.error(f"Erreur lors du téléchargement du fichier de permissions: {e}")
        return None

def load_permissions():
    """Charge les permissions depuis le fichier Excel."""
    try:
        permissions_path = download_permissions_file()
        if permissions_path:
            permissions_df = pd.read_excel(permissions_path)
            permissions = {}
            for _, row in permissions_df.iterrows():
                profile = str(row['Profil']).strip()
                permissions[profile] = {
                    "password": str(row['Mot de passe']).strip(),
                    "permissions": [p.strip() for p in str(row['Permissions']).split(',')] if pd.notna(row['Permissions']) else [],
                    "player": nettoyer_nom_joueuse(row['Joueuse']) if pd.notna(row['Joueuse']) else None
                }
            return permissions
        return {}
    except Exception as e:
        st.error(f"Erreur lors du chargement des permissions: {e}")
        return {}

# =============================================
# FONCTIONS UTILITAIRES
# =============================================
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

# =============================================
# FONCTIONS DE TRAITEMENT DES DONNÉES
# =============================================
def players_edf_duration(match):
    """Calcule la durée de jeu pour les joueuses EDF."""
    if 'Poste' not in match.columns or 'Temps de jeu' not in match.columns:
        st.warning("Colonnes manquantes pour calculer la durée de jeu EDF")
        return pd.DataFrame()
    df_filtered = match.loc[match['Poste'] != 'Gardienne']
    if df_filtered.empty:
        return pd.DataFrame()
    df_duration = pd.DataFrame({
        'Player': df_filtered['Player'].apply(nettoyer_nom_joueuse),
        'Temps de jeu (en minutes)': df_filtered['Temps de jeu']
    })
    return df_duration

def players_duration(match):
    """Calcule la durée de jeu pour les joueuses PFC."""
    if 'Duration' not in match.columns:
        st.warning("Colonne 'Duration' manquante pour calculer la durée de jeu")
        return pd.DataFrame()
    players_duration = {}
    list_of_players = ['ATT', 'DCD', 'DCG', 'DD', 'DG', 'GB', 'MCD', 'MCG', 'MD', 'MDef', 'MG']
    available_posts = [poste for poste in list_of_players if poste in match.columns]
    if not available_posts:
        st.warning("Aucune colonne de poste disponible pour calculer la durée de jeu")
        return pd.DataFrame()
    for i in range(len(match)):
        duration = match.iloc[i]['Duration']
        for poste in available_posts:
            if poste in match.columns:
                player = nettoyer_nom_joueuse(str(match.iloc[i][poste]))
                if player:
                    if player in players_duration:
                        players_duration[player] += duration
                    else:
                        players_duration[player] = duration
    if not players_duration:
        return pd.DataFrame()
    for player in players_duration:
        players_duration[player] /= 60
    df_duration = pd.DataFrame({
        'Player': list(players_duration.keys()),
        'Temps de jeu (en minutes)': list(players_duration.values())
    })
    df_duration = df_duration.sort_values(by='Temps de jeu (en minutes)', ascending=False)
    return df_duration

def players_shots(joueurs):
    """Calcule les statistiques de tirs."""
    if 'Action' not in joueurs.columns or 'Row' not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de tirs")
        return pd.DataFrame()
    players_shots, players_shots_on_target, players_goals = {}, {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Tir' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_shots[player] = players_shots.get(player, 0) + action.count('Tir')
            if 'Tir' in joueurs.columns:
                is_successful = joueurs.iloc[i]['Tir']
                if isinstance(is_successful, str):
                    if 'Tir Cadré' in is_successful or 'But' in is_successful:
                        players_shots_on_target[player] = players_shots_on_target.get(player, 0) + is_successful.count('Tir Cadré') + is_successful.count('But')
                    if 'But' in is_successful:
                        players_goals[player] = players_goals.get(player, 0) + 1
    if not players_shots:
        return pd.DataFrame()
    return pd.DataFrame({
        'Player': list(players_shots.keys()),
        'Tirs': list(players_shots.values()),
        'Tirs cadrés': [players_shots_on_target.get(player, 0) for player in players_shots],
        'Buts': [players_goals.get(player, 0) for player in players_shots]
    }).sort_values(by='Tirs', ascending=False)

def players_passes(joueurs):
    """Calcule les statistiques de passes."""
    if 'Action' not in joueurs.columns or 'Row' not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de passes")
        return pd.DataFrame()
    player_short_passes, player_long_passes = {}, {}
    players_successful_short_passes, players_successful_long_passes = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Passe' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            if 'Passe' in joueurs.columns:
                passe = joueurs.iloc[i]['Passe']
                if isinstance(passe, str):
                    if 'Courte' in passe:
                        player_short_passes[player] = player_short_passes.get(player, 0) + passe.count('Courte')
                        if 'Réussie' in passe:
                            players_successful_short_passes[player] = players_successful_short_passes.get(player, 0) + passe.count('Réussie')
                    if 'Longue' in passe:
                        player_long_passes[player] = player_long_passes.get(player, 0) + passe.count('Longue')
                        if 'Réussie' in passe:
                            players_successful_long_passes[player] = players_successful_long_passes.get(player, 0) + passe.count('Réussie')
    if not player_short_passes:
        return pd.DataFrame()
    df_passes = pd.DataFrame({
        'Player': list(player_short_passes.keys()),
        'Passes courtes': [player_short_passes.get(player, 0) for player in player_short_passes],
        'Passes longues': [player_long_passes.get(player, 0) for player in player_short_passes],
        'Passes réussies (courtes)': [players_successful_short_passes.get(player, 0) for player in player_short_passes],
        'Passes réussies (longues)': [players_successful_long_passes.get(player, 0) for player in player_short_passes]
    })
    if not df_passes.empty:
        df_passes['Passes'] = df_passes['Passes courtes'] + df_passes['Passes longues']
        df_passes['Passes réussies'] = df_passes['Passes réussies (courtes)'] + df_passes['Passes réussies (longues)']
        df_passes['Pourcentage de passes réussies'] = (df_passes['Passes réussies'] / df_passes['Passes'] * 100).fillna(0)
    return df_passes.sort_values(by='Passes courtes', ascending=False)

def players_dribbles(joueurs):
    """Calcule les statistiques de dribbles."""
    if 'Action' not in joueurs.columns or 'Row' not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de dribbles")
        return pd.DataFrame()
    players_dribbles, players_successful_dribbles = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Dribble' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_dribbles[player] = players_dribbles.get(player, 0) + action.count('Dribble')
            if 'Dribble' in joueurs.columns:
                is_successful = joueurs.iloc[i]['Dribble']
                if isinstance(is_successful, str) and 'Réussi' in is_successful:
                    players_successful_dribbles[player] = players_successful_dribbles.get(player, 0) + is_successful.count('Réussi')
    if not players_dribbles:
        return pd.DataFrame()
    df_dribbles = pd.DataFrame({
        'Player': list(players_dribbles.keys()),
        'Dribbles': list(players_dribbles.values()),
        'Dribbles réussis': [players_successful_dribbles.get(player, 0) for player in players_dribbles]
    })
    if not df_dribbles.empty:
        df_dribbles['Pourcentage de dribbles réussis'] = (df_dribbles['Dribbles réussis'] / df_dribbles['Dribbles'] * 100).fillna(0)
    return df_dribbles.sort_values(by='Dribbles', ascending=False)

def players_defensive_duels(joueurs):
    """Calcule les statistiques de duels défensifs."""
    if 'Action' not in joueurs.columns or 'Row' not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de duels défensifs")
        return pd.DataFrame()
    players_defensive_duels, players_successful_defensive_duels, players_faults = {}, {}, {}
    duels_col = 'Duel défensifs' if 'Duel défensifs' in joueurs.columns else ('Duel défensif' if 'Duel défensif' in joueurs.columns else None)
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Duel défensif' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_defensive_duels[player] = players_defensive_duels.get(player, 0) + action.count('Duel défensif')
            if duels_col and duels_col in joueurs.columns:
                is_successful = joueurs.iloc[i][duels_col]
                if isinstance(is_successful, str):
                    if 'Gagné' in is_successful:
                        players_successful_defensive_duels[player] = players_successful_defensive_duels.get(player, 0) + is_successful.count('Gagné')
                    if 'Faute' in is_successful:
                        players_faults[player] = players_faults.get(player, 0) + is_successful.count('Faute')
    if not players_defensive_duels:
        return pd.DataFrame()
    df_duels_defensifs = pd.DataFrame({
        'Player': list(players_defensive_duels.keys()),
        'Duels défensifs': list(players_defensive_duels.values()),
        'Duels défensifs gagnés': [players_successful_defensive_duels.get(player, 0) for player in players_defensive_duels],
        'Fautes': [players_faults.get(player, 0) for player in players_defensive_duels]
    })
    if not df_duels_defensifs.empty and 'Duels défensifs' in df_duels_defensifs.columns:
        df_duels_defensifs['Pourcentage de duels défensifs gagnés'] = (
            df_duels_defensifs['Duels défensifs gagnés'] / df_duels_defensifs['Duels défensifs'] * 100
        ).fillna(0)
    return df_duels_defensifs.sort_values(by='Duels défensifs', ascending=False)

def players_interceptions(joueurs):
    """Calcule les statistiques d'interceptions."""
    if 'Action' not in joueurs.columns or 'Row' not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques d'interceptions")
        return pd.DataFrame()
    players_interceptions = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Interception' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_interceptions[player] = players_interceptions.get(player, 0) + action.count('Interception')
    if not players_interceptions:
        return pd.DataFrame()
    return pd.DataFrame({
        'Player': list(players_interceptions.keys()),
        'Interceptions': list(players_interceptions.values())
    }).sort_values(by='Interceptions', ascending=False)

def players_ball_losses(joueurs):
    """Calcule les statistiques de pertes de balle."""
    if 'Action' not in joueurs.columns or 'Row' not in joueurs.columns:
        st.warning("Colonnes manquantes pour calculer les statistiques de pertes de balle")
        return pd.DataFrame()
    players_ball_losses = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Perte de balle' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_ball_losses[player] = players_ball_losses.get(player, 0) + action.count('Perte de balle')
    if not players_ball_losses:
        return pd.DataFrame()
    return pd.DataFrame({
        'Player': list(players_ball_losses.keys()),
        'Pertes de balle': list(players_ball_losses.values())
    }).sort_values(by='Pertes de balle', ascending=False)

def create_metrics(df):
    """Crée les métriques à partir des données brutes."""
    if df.empty:
        return df
    required_cols = {
        'Timing': ['Duels défensifs', 'Fautes'],
        'Force physique': ['Duels défensifs', 'Duels défensifs gagnés'],
        'Intelligence tactique': ['Interceptions'],
        'Technique 1': ['Passes'],
        'Technique 2': ['Passes courtes', 'Passes réussies (courtes)'],
        'Technique 3': ['Passes longues', 'Passes réussies (longues)'],
        'Explosivité': ['Dribbles', 'Dribbles réussis'],
        'Prise de risque': ['Dribbles'],
        'Précision': ['Tirs', 'Tirs cadrés'],
        'Sang-froid': ['Tirs']
    }
    for metric, cols in required_cols.items():
        if all(col in df.columns for col in cols):
            if metric == 'Timing':
                df[metric] = np.where(df[cols[0]] > 0,
                                    (df[cols[0]] - df.get(cols[1], 0)) / df[cols[0]], 0)
            elif metric == 'Force physique':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df.get(cols[1], 0) / df[cols[0]], 0)
            elif metric == 'Intelligence tactique':
                if df[cols[0]].max() > 0:
                    df[metric] = np.where(df[cols[0]] > 0,
                                        df[cols[0]] / df[cols[0]].max(), 0)
            elif metric == 'Technique 1':
                if df[cols[0]].max() > 0:
                    df[metric] = np.where(df[cols[0]] > 0,
                                        df[cols[0]] / df[cols[0]].max(), 0)
            elif metric == 'Technique 2':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df.get(cols[1], 0) / df[cols[0]], 0)
            elif metric == 'Technique 3':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df.get(cols[1], 0) / df[cols[0]], 0)
            elif metric == 'Explosivité':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df.get(cols[1], 0) / df[cols[0]], 0)
            elif metric == 'Prise de risque':
                if df[cols[0]].max() > 0:
                    df[metric] = np.where(df[cols[0]] > 0,
                                        df[cols[0]] / df[cols[0]].max(), 0)
            elif metric == 'Précision':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df.get(cols[1], 0) / df[cols[0]], 0)
            elif metric == 'Sang-froid':
                if df[cols[0]].max() > 0:
                    df[metric] = np.where(df[cols[0]] > 0,
                                        df[cols[0]] / df[cols[0]].max(), 0)
    for metric in required_cols.keys():
        if metric in df.columns:
            df[metric] = (df[metric].rank(pct=True) * 100).fillna(0)
    return df

def create_kpis(df):
    """Crée les KPIs à partir des métriques."""
    if df.empty:
        return df
    if 'Timing' in df.columns and 'Force physique' in df.columns:
        df['Rigueur'] = (df['Timing'] + df['Force physique']) / 2
    if 'Intelligence tactique' in df.columns:
        df['Récupération'] = df['Intelligence tactique']
    tech_metrics = [m for m in ['Technique 1', 'Technique 2', 'Technique 3'] if m in df.columns]
    if len(tech_metrics) > 0:
        df['Distribution'] = df[tech_metrics].mean(axis=1)
    if 'Explosivité' in df.columns and 'Prise de risque' in df.columns:
        df['Percussion'] = (df['Explosivité'] + df['Prise de risque']) / 2
    if 'Précision' in df.columns and 'Sang-froid' in df.columns:
        df['Finition'] = (df['Précision'] + df['Sang-froid']) / 2
    return df

def create_poste(df):
    """Crée les notes par poste."""
    if df.empty:
        return df
    required_kpis = ['Rigueur', 'Récupération', 'Distribution', 'Percussion', 'Finition']
    available_kpis = [kpi for kpi in required_kpis if kpi in df.columns]
    if len(available_kpis) < 5:
        return df
    df['Défenseur central'] = (df['Rigueur'] * 5 + df['Récupération'] * 5 +
                              df['Distribution'] * 5 + df['Percussion'] * 1 +
                              df['Finition'] * 1) / 17
    df['Défenseur latéral'] = (df['Rigueur'] * 3 + df['Récupération'] * 3 +
                              df['Distribution'] * 3 + df['Percussion'] * 3 +
                              df['Finition'] * 3) / 15
    df['Milieu défensif'] = (df['Rigueur'] * 4 + df['Récupération'] * 4 +
                            df['Distribution'] * 4 + df['Percussion'] * 2 +
                            df['Finition'] * 2) / 16
    df['Milieu relayeur'] = (df['Rigueur'] * 3 + df['Récupération'] * 3 +
                            df['Distribution'] * 3 + df['Percussion'] * 3 +
                            df['Finition'] * 3) / 15
    df['Milieu offensif'] = (df['Rigueur'] * 2 + df['Récupération'] * 2 +
                            df['Distribution'] * 2 + df['Percussion'] * 4 +
                            df['Finition'] * 4) / 14
    df['Attaquant'] = (df['Rigueur'] * 1 + df['Récupération'] * 1 +
                      df['Distribution'] * 1 + df['Percussion'] * 5 +
                      df['Finition'] * 5) / 13
    return df


def create_data(match, joueurs, is_edf):
    """Crée un dataframe complet à partir des données brutes."""
    try:
        if is_edf:
            if 'Player' not in joueurs.columns:
                st.error("La colonne 'Player' est manquante dans les données EDF.")
                return pd.DataFrame()
            joueurs['Player'] = joueurs['Player'].apply(nettoyer_nom_joueuse)

            # Vérifier que les colonnes 'Poste' et 'Temps de jeu' sont présentes
            if 'Poste' not in joueurs.columns or 'Temps de jeu' not in joueurs.columns:
                st.error("Les colonnes 'Poste' ou 'Temps de jeu' sont manquantes dans les données EDF.")
                return pd.DataFrame()

            df_duration = pd.DataFrame({
                'Player': joueurs['Player'],
                'Temps de jeu (en minutes)': joueurs['Temps de jeu'],
                'Poste': joueurs['Poste']
            })
        else:
            df_duration = players_duration(match)

        dfs = [df_duration]
        calc_functions = [
            ('tirs', players_shots),
            ('passes', players_passes),
            ('dribbles', players_dribbles),
            ('duels', players_defensive_duels),
            ('interceptions', players_interceptions),
            ('pertes', players_ball_losses)
        ]
        for name, func in calc_functions:
            try:
                result = func(joueurs)
                if not result.empty:
                    dfs.append(result)
            except Exception as e:
                st.warning(f"Erreur lors du calcul des {name}: {e}")

        valid_dfs = []
        for df in dfs:
            if not df.empty and 'Player' in df.columns:
                df['Player'] = df['Player'].apply(nettoyer_nom_joueuse)
                valid_dfs.append(df)

        if not valid_dfs:
            return pd.DataFrame()

        df = valid_dfs[0]
        for other_df in valid_dfs[1:]:
            df = df.merge(other_df, on='Player', how='outer')

        if not df.empty:
            df.fillna(0, inplace=True)
            df = df[(df.iloc[:, 1:] != 0).any(axis=1)]
            if 'Temps de jeu (en minutes)' in df.columns:
                df = df[df['Temps de jeu (en minutes)'] >= 10]
            try:
                df = create_metrics(df)
                df = create_kpis(df)
                df = create_poste(df)
            except Exception as e:
                st.warning(f"Erreur lors du calcul des métriques: {e}")
        return df
    except Exception as e:
        st.error(f"Erreur lors de la création des données: {e}")
        return pd.DataFrame()


def filter_data_by_player(df, player_name):
    """Filtre les données pour une joueuse spécifique."""
    if not player_name or df.empty or 'Player' not in df.columns:
        return df
    player_name_clean = nettoyer_nom_joueuse(player_name)
    df['Player_clean'] = df['Player'].apply(nettoyer_nom_joueuse)
    filtered_df = df[df['Player_clean'] == player_name_clean].copy()
    filtered_df.drop(columns=['Player_clean'], inplace=True, errors='ignore')
    return filtered_df

def prepare_comparison_data(df, player_name, selected_matches=None):
    """Prépare les données pour la comparaison."""
    if df.empty or 'Player' not in df.columns:
        return pd.DataFrame()
    player_name_clean = nettoyer_nom_joueuse(player_name)
    df['Player_clean'] = df['Player'].apply(nettoyer_nom_joueuse)
    if selected_matches:
        filtered_df = df[df['Player_clean'] == player_name_clean]
        if 'Adversaire' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Adversaire'].isin(selected_matches)]
    else:
        filtered_df = df[df['Player_clean'] == player_name_clean]
    if filtered_df.empty:
        return pd.DataFrame()
    aggregated_data = filtered_df.groupby('Player').agg({
        'Temps de jeu (en minutes)': 'sum',
        'Buts': 'sum',
    }).join(
        filtered_df.groupby('Player').mean(numeric_only=True).drop(
            columns=['Temps de jeu (en minutes)', 'Buts'], errors='ignore'
        )
    ).round().astype(int).reset_index()
    return aggregated_data

# =============================================
# FONCTIONS DE COLLECTE DES DONNÉES
# =============================================
@st.cache_data
def collect_data():
    """Collecte et traite les données depuis Google Drive."""
    try:
        download_google_drive()
        pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()
        data_folder = "data"
        if not os.path.exists(data_folder):
            # st.error(f"Le dossier '{data_folder}' n'existe pas.")
            return pfc_kpi, edf_kpi

        fichiers = [f for f in os.listdir(data_folder) if f.endswith(('.csv', '.xlsx')) and f != "Classeurs permissions streamlit.xlsx"]
        if not fichiers:
            # st.warning(f"Aucun fichier de données trouvé dans '{data_folder}'.")
            return pfc_kpi, edf_kpi

        edf_joueuses_path = os.path.join(data_folder, "EDF_Joueuses.xlsx")
        if os.path.exists(edf_joueuses_path):
            edf_joueuses = pd.read_excel(edf_joueuses_path)
            if 'Player' not in edf_joueuses.columns or 'Poste' not in edf_joueuses.columns or 'Temps de jeu' not in edf_joueuses.columns:
                # st.error("Les colonnes 'Player', 'Poste' ou 'Temps de jeu' sont manquantes dans le fichier EDF_Joueuses.xlsx.")
                return pfc_kpi, edf_kpi
            edf_joueuses['Player'] = edf_joueuses['Player'].apply(nettoyer_nom_joueuse)

            matchs_csv = [f for f in fichiers if f.startswith('EDF_U19_Match') and f.endswith('.csv')]
            if matchs_csv:
                all_edf_data = []
                for csv_file in matchs_csv:
                    match_data = pd.read_csv(os.path.join(data_folder, csv_file))
                    if 'Row' not in match_data.columns:
                        # st.error(f"La colonne 'Row' est manquante dans le fichier {csv_file}.")
                        continue
                    match_data['Player'] = match_data['Row'].apply(nettoyer_nom_joueuse)

                    match_data = match_data.merge(edf_joueuses, on='Player', how='left')
                    if match_data.empty:
                        # st.warning(f"Aucune donnée valide trouvée dans le fichier {csv_file} après fusion.")
                        continue

                    df = create_data(match_data, match_data, True)
                    if not df.empty:
                        all_edf_data.append(df)

                if all_edf_data:
                    edf_kpi = pd.concat(all_edf_data)
                    if 'Poste' in edf_kpi.columns:
                        edf_kpi = edf_kpi.groupby('Poste').mean(numeric_only=True).reset_index()
                        edf_kpi['Poste'] = edf_kpi['Poste'] + ' moyenne (EDF)'
                    # else:
                    #     st.warning("Colonne 'Poste' manquante dans les données EDF.")
                # else:
                #     st.warning("Aucune donnée EDF valide trouvée.")
            # else:
            #     st.warning("Aucun fichier CSV EDF trouvé.")
        # else:
        #     st.warning("Fichier Excel EDF_Joueuses.xlsx introuvable.")

        # Traitement des données PFC
        for filename in fichiers:
            path = os.path.join(data_folder, filename)
            try:
                if filename.endswith('.csv') and 'PFC' in filename:
                    # print(f"Traitement du fichier CSV PFC: {filename}")
                    parts = filename.split('.')[0].split('_')
                    if len(parts) < 6:
                        # st.warning(f"Le nom du fichier {filename} ne suit pas le format attendu.")
                        continue
                    try:
                        equipe_domicile = parts[0]
                        equipe_exterieur = parts[2]
                        journee = parts[3]
                        categorie = parts[4]
                        date = parts[5]
                        data = pd.read_csv(path)
                        if 'Row' not in data.columns:
                            # st.error(f"La colonne 'Row' est manquante dans le fichier {filename}.")
                            continue
                        match, joueurs = pd.DataFrame(), pd.DataFrame()
                        for i in range(len(data)):
                            if data['Row'].iloc[i] in [equipe_domicile, equipe_exterieur]:
                                match = pd.concat([match, data.iloc[i:i+1]], ignore_index=True)
                            elif not any(str(x) in str(data['Row'].iloc[i]) for x in ['Corner', 'Coup-franc', 'Penalty', 'Carton']):
                                joueurs = pd.concat([joueurs, data.iloc[i:i+1]], ignore_index=True)
                        if not joueurs.empty:
                            joueurs['Player'] = joueurs['Row'].apply(nettoyer_nom_joueuse)
                            df = create_data(match, joueurs, False)
                            if not df.empty:
                                for index, row in df.iterrows():
                                    time_played = row['Temps de jeu (en minutes)']
                                    for col in df.columns:
                                        if col not in ['Player', 'Temps de jeu (en minutes)', 'Buts'] and 'Pourcentage' not in col:
                                            df.loc[index, col] = row[col] * (90 / time_played)
                                df = create_metrics(df)
                                df = create_kpis(df)
                                df = create_poste(df)
                                adversaire = equipe_exterieur if equipe_domicile == 'PFC' else equipe_domicile
                                df.insert(1, 'Adversaire', f'{adversaire} - {journee}')
                                df.insert(2, 'Journée', journee)
                                df.insert(3, 'Catégorie', categorie)
                                df.insert(4, 'Date', date)
                                pfc_kpi = pd.concat([pfc_kpi, df])
                    except Exception as e:
                        # st.error(f"Erreur lors du traitement du fichier {filename}: {e}")
                        pass
            except Exception as e:
                # st.error(f"Erreur lors du traitement du fichier {filename}: {e}")
                pass

        return pfc_kpi, edf_kpi
    except Exception as e:
        # st.error(f"Erreur lors de la collecte des données: {e}")
        return pd.DataFrame(), pd.DataFrame()


# =============================================
# FONCTIONS DE VISUALISATION
# =============================================
def create_individual_radar(df):
    """Crée un radar individuel pour une joueuse."""
    if df.empty or 'Player' not in df.columns:
        st.warning("Aucune donnée disponible pour créer le radar.")
        return None
    try:
        columns_to_plot = [
            'Timing', 'Force physique', 'Intelligence tactique',
            'Technique 1', 'Technique 2', 'Technique 3',
            'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid'
        ]
        available_columns = [col for col in columns_to_plot if col in df.columns]
        if not available_columns:
            st.warning("Aucune colonne de métrique disponible pour le radar")
            return None
        colors = ['#6A7CD9', '#00BFFE', '#FF9470', '#F27979', '#BFBFBF'] * 2
        player = df.iloc[0]
        pizza = PyPizza(
            params=available_columns,
            background_color='#0e1117',
            straight_line_color='#FFFFFF',
            last_circle_color='#FFFFFF'
        )
        fig, _ = pizza.make_pizza(
            figsize=(3, 3),
            values=[player[col] for col in available_columns],
            slice_colors=colors[:len(available_columns)],
            kwargs_values=dict(
                color='#FFFFFF',
                fontsize=3.5,
                bbox=dict(edgecolor='#FFFFFF', facecolor='#0e1117', boxstyle='round, pad=0.2', lw=1)
            ),
            kwargs_params=dict(color='#FFFFFF', fontsize=3.5, fontproperties='monospace')
        )
        fig.set_facecolor('#0e1117')
        return fig
    except Exception as e:
        st.error(f"Erreur lors de la création du radar: {e}")
        return None

def create_comparison_radar(df, player1_name=None, player2_name=None):
    """Crée un radar de comparaison entre deux joueurs."""
    if df.empty or len(df) < 2:
        st.warning("Données insuffisantes pour créer une comparaison.")
        return None
    try:
        metrics = [
            'Timing', 'Force physique', 'Intelligence tactique',
            'Technique 1', 'Technique 2', 'Technique 3',
            'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid'
        ]
        available_metrics = [m for m in metrics if m in df.columns]
        if len(available_metrics) < 2:
            st.warning("Pas assez de métriques disponibles pour la comparaison")
            return None
        low, high = (0,) * len(available_metrics), (100,) * len(available_metrics)
        radar = Radar(
            available_metrics,
            low,
            high,
            num_rings=4,
            ring_width=1,
            center_circle_radius=1
        )
        URL1 = 'https://raw.githubusercontent.com/googlefonts/roboto/main/src/hinted/Roboto-Thin.ttf'
        URL2 = 'https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/RobotoSlab%5Bwght%5D.ttf'
        robotto_thin, robotto_bold = FontManager(URL1), FontManager(URL2)
        fig, axs = grid(
            figheight=14,
            grid_height=0.915,
            title_height=0.06,
            endnote_height=0.025,
            title_space=0,
            endnote_space=0,
            grid_key='radar'
        )
        radar.setup_axis(ax=axs['radar'], facecolor='None')
        radar.draw_circles(
            ax=axs['radar'],
            facecolor='#28252c',
            edgecolor='#39353f',
            lw=1.5
        )
        player_values_1 = df.iloc[0][available_metrics].values
        player_values_2 = df.iloc[1][available_metrics].values
        radar.draw_radar_compare(
            player_values_1,
            player_values_2,
            ax=axs['radar'],
            kwargs_radar={'facecolor': '#00f2c1', 'alpha': 0.6},
            kwargs_compare={'facecolor': '#d80499', 'alpha': 0.6}
        )
        radar.draw_range_labels(
            ax=axs['radar'],
            fontsize=18,
            color='#fcfcfc',
            fontproperties=robotto_thin.prop
        )
        radar.draw_param_labels(
            ax=axs['radar'],
            fontsize=18,
            color='#fcfcfc',
            fontproperties=robotto_thin.prop
        )
        player1_label = player1_name if player1_name else df.iloc[0]['Player']
        player2_label = player2_name if player2_name else df.iloc[1]['Player']
        axs['title'].text(
            0.01, 0.65,
            player1_label,
            fontsize=18,
            color='#01c49d',
            fontproperties=robotto_bold.prop,
            ha='left',
            va='center'
        )
        axs['title'].text(
            0.99, 0.65,
            player2_label,
            fontsize=18,
            fontproperties=robotto_bold.prop,
            ha='right',
            va='center',
            color='#d80499'
        )
        fig.set_facecolor('#0e1117')
        return fig
    except Exception as e:
        st.error(f"Erreur lors de la création du radar de comparaison: {e}")
        return None

# =============================================
# GESTION DES PROFILS ET PERMISSIONS
# =============================================
def check_permission(user_profile, required_permission, permissions):
    """Vérifie si un profil a une permission spécifique."""
    if user_profile not in permissions:
        return False
    if "all" in permissions[user_profile]["permissions"]:
        return True
    return required_permission in permissions[user_profile]["permissions"]

def get_player_for_profile(profile, permissions):
    """Récupère le nom de la joueuse associée à un profil."""
    if profile in permissions:
        return permissions[profile].get("player", None)
    return None

# =============================================
# INTERFACE STREAMLIT
# =============================================
def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
    """Interface principale adaptée aux permissions et filtrée par joueuse."""
    logo_pfc = "https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"
    st.sidebar.markdown(f"<div style='display: flex; justify-content: center;'><img src='{logo_pfc}' width='100'></div>", unsafe_allow_html=True)

    player_name = get_player_for_profile(user_profile, permissions)
    st.sidebar.title(f"Connecté en tant que: {user_profile}")
    if player_name:
        st.sidebar.write(f"Joueuse associée: {player_name}")

    if st.sidebar.button("🔒 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user_profile = None
        st.rerun()

    if check_permission(user_profile, "update_data", permissions) or check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Mettre à jour la base de données"):
            with st.spinner("Mise à jour des données en cours..."):
                download_google_drive()
            st.success("✅ Mise à jour terminée")
            st.cache_data.clear()

    if player_name and not pfc_kpi.empty and 'Player' in pfc_kpi.columns:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)
        if pfc_kpi.empty:
            st.warning(f"Aucune donnée disponible pour la joueuse {player_name}")

    available_options = ["Statistiques"]
    if check_permission(user_profile, "compare_players", permissions) or check_permission(user_profile, "all", permissions) or player_name:
        available_options.append("Comparaison")
    if check_permission(user_profile, "all", permissions):
        available_options.append("Gestion")

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=available_options,
            icons=["graph-up-arrow", "people", "gear"][:len(available_options)],
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

    logo_certifie_paris = "https://i.postimg.cc/2SZj5JdZ/Certifie-Paris-Blanc.png"
    st.sidebar.markdown(
        f"""
        <div style='display: flex; flex-direction: column; height: 100vh; justify-content: space-between;'>
            <div></div>
            <div style='text-align: center; margin-bottom: 300px;'>
                <img src='{logo_certifie_paris}' width='200'>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    if page == "Statistiques":
        st.header("Statistiques")
        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible pour votre profil.")
        else:
            if player_name:
                st.subheader(f"Statistiques pour {player_name}")
                if 'Adversaire' in pfc_kpi.columns:
                    unique_matches = pfc_kpi['Adversaire'].unique()
                    if len(unique_matches) > 0:
                        game = st.multiselect("Choisissez un ou plusieurs matchs", unique_matches)
                        if game:
                            filtered_data = pfc_kpi[pfc_kpi['Adversaire'].isin(game)]
                        else:
                            filtered_data = pfc_kpi
                        if not filtered_data.empty:
                            aggregated_data = filtered_data.groupby('Player').agg({
                                'Temps de jeu (en minutes)': 'sum',
                                'Buts': 'sum',
                            }).join(
                                filtered_data.groupby('Player').mean(numeric_only=True).drop(
                                    columns=['Temps de jeu (en minutes)', 'Buts'], errors='ignore'
                                )
                            ).round().astype(int).reset_index()
                            time_played, goals = st.columns(2)
                            with time_played:
                                st.metric("Temps de jeu", f"{aggregated_data['Temps de jeu (en minutes)'].iloc[0]} minutes")
                            with goals:
                                st.metric("Buts", f"{aggregated_data['Buts'].iloc[0]}")
                            tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
                            with tab1:
                                fig = create_individual_radar(aggregated_data)
                                if fig:
                                    st.pyplot(fig)
                            with tab2:
                                if 'Rigueur' in aggregated_data.columns:
                                    col1, col2, col3, col4, col5 = st.columns(5)
                                    with col1: st.metric("Rigueur", f"{aggregated_data['Rigueur'].iloc[0]}/100")
                                    with col2: st.metric("Récupération", f"{aggregated_data['Récupération'].iloc[0]}/100")
                                    with col3: st.metric("Distribution", f"{aggregated_data['Distribution'].iloc[0]}/100")
                                    with col4: st.metric("Percussion", f"{aggregated_data['Percussion'].iloc[0]}/100")
                                    with col5: st.metric("Finition", f"{aggregated_data['Finition'].iloc[0]}/100")
                            with tab3:
                                if 'Défenseur central' in aggregated_data.columns:
                                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                                    with col1: st.metric("Défenseur central", f"{aggregated_data['Défenseur central'].iloc[0]}/100")
                                    with col2: st.metric("Défenseur latéral", f"{aggregated_data['Défenseur latéral'].iloc[0]}/100")
                                    with col3: st.metric("Milieu défensif", f"{aggregated_data['Milieu défensif'].iloc[0]}/100")
                                    with col4: st.metric("Milieu relayeur", f"{aggregated_data['Milieu relayeur'].iloc[0]}/100")
                                    with col5: st.metric("Milieu offensif", f"{aggregated_data['Milieu offensif'].iloc[0]}/100")
                                    with col6: st.metric("Attaquant", f"{aggregated_data['Attaquant'].iloc[0]}/100")
                        else:
                            st.warning("Aucune donnée disponible pour les matchs sélectionnés.")
                    else:
                        st.warning("Aucun match disponible pour cette joueuse.")
                else:
                    st.warning("Colonne 'Adversaire' manquante dans les données.")
            else:
                st.subheader("Sélectionnez une joueuse du Paris FC")
                if not pfc_kpi.empty and 'Player' in pfc_kpi.columns:
                    player = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique())
                    player_data = pfc_kpi[pfc_kpi['Player'] == player]
                    if player_data.empty:
                        st.error("Aucune donnée disponible pour cette joueuse.")
                    else:
                        if 'Adversaire' in player_data.columns:
                            game = st.multiselect("Choisissez un ou plusieurs matchs", player_data['Adversaire'].unique())
                            filtered_data = player_data[player_data['Adversaire'].isin(game)] if game else player_data
                            if not filtered_data.empty:
                                aggregated_data = filtered_data.groupby('Player').agg({
                                    'Temps de jeu (en minutes)': 'sum',
                                    'Buts': 'sum',
                                }).join(
                                    filtered_data.groupby('Player').mean(numeric_only=True).drop(
                                        columns=['Temps de jeu (en minutes)', 'Buts'], errors='ignore'
                                    )
                                ).round().astype(int).reset_index()
                                time_played, goals = st.columns(2)
                                with time_played:
                                    st.metric("Temps de jeu", f"{aggregated_data['Temps de jeu (en minutes)'].iloc[0]} minutes")
                                with goals:
                                    st.metric("Buts", f"{aggregated_data['Buts'].iloc[0]}")
                                tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])
                                with tab1:
                                    fig = create_individual_radar(aggregated_data)
                                    if fig:
                                        st.pyplot(fig)
                                with tab2:
                                    if 'Rigueur' in aggregated_data.columns:
                                        col1, col2, col3, col4, col5 = st.columns(5)
                                        with col1: st.metric("Rigueur", f"{aggregated_data['Rigueur'].iloc[0]}/100")
                                        with col2: st.metric("Récupération", f"{aggregated_data['Récupération'].iloc[0]}/100")
                                        with col3: st.metric("Distribution", f"{aggregated_data['Distribution'].iloc[0]}/100")
                                        with col4: st.metric("Percussion", f"{aggregated_data['Percussion'].iloc[0]}/100")
                                        with col5: st.metric("Finition", f"{aggregated_data['Finition'].iloc[0]}/100")
                                with tab3:
                                    if 'Défenseur central' in aggregated_data.columns:
                                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                                        with col1: st.metric("Défenseur central", f"{aggregated_data['Défenseur central'].iloc[0]}/100")
                                        with col2: st.metric("Défenseur latéral", f"{aggregated_data['Défenseur latéral'].iloc[0]}/100")
                                        with col3: st.metric("Milieu défensif", f"{aggregated_data['Milieu défensif'].iloc[0]}/100")
                                        with col4: st.metric("Milieu relayeur", f"{aggregated_data['Milieu relayeur'].iloc[0]}/100")
                                        with col5: st.metric("Milieu offensif", f"{aggregated_data['Milieu offensif'].iloc[0]}/100")
                                        with col6: st.metric("Attaquant", f"{aggregated_data['Attaquant'].iloc[0]}/100")
    elif page == "Comparaison":
        st.header("Comparaison")
        if player_name:
            st.subheader(f"Comparaison pour {player_name}")
            if pfc_kpi.empty:
                st.warning(f"Aucune donnée disponible pour {player_name}.")
            else:
                st.write("### 1. Comparez vos performances sur différents matchs")
                if 'Adversaire' in pfc_kpi.columns:
                    unique_matches = pfc_kpi['Adversaire'].unique()
                    if len(unique_matches) >= 1:
                        selected_matches = st.multiselect(
                            "Sélectionnez les matchs à comparer (2 ou plus)",
                            unique_matches,
                            key='selected_matches'
                        )
                        if len(selected_matches) >= 2:
                            comparison_data = []
                            for match in selected_matches:
                                match_data = pfc_kpi[pfc_kpi['Adversaire'] == match]
                                if not match_data.empty:
                                    aggregated = match_data.groupby('Player').agg({
                                        'Temps de jeu (en minutes)': 'sum',
                                        'Buts': 'sum',
                                    }).join(
                                        match_data.groupby('Player').mean(numeric_only=True).drop(
                                            columns=['Temps de jeu (en minutes)', 'Buts'], errors='ignore'
                                        )
                                    ).round().astype(int).reset_index()
                                    if not aggregated.empty:
                                        aggregated['Player'] = f"{player_name} ({match})"
                                        comparison_data.append(aggregated)
                            if len(comparison_data) >= 2:
                                players_data = pd.concat(comparison_data)
                                if st.button("Comparer les matchs sélectionnés"):
                                    if len(players_data) >= 2:
                                        fig = create_comparison_radar(players_data)
                                        if fig:
                                            st.pyplot(fig)
                                    else:
                                        st.warning("Pas assez de données pour la comparaison.")
                            else:
                                st.warning("Pas assez de matchs sélectionnés avec des données valides.")
                        else:
                            st.warning("Veuillez sélectionner au moins 2 matchs pour la comparaison.")

                st.write("### 2. Comparez-vous aux données EDF")
                if not edf_kpi.empty and 'Poste' in edf_kpi.columns:
                    poste = st.selectbox(
                        "Sélectionnez un poste EDF pour comparaison",
                        edf_kpi['Poste'].unique(),
                        key='edf_poste'
                    )
                    edf_data = edf_kpi[edf_kpi['Poste'] == poste].rename(columns={'Poste': 'Player'})
                    if not edf_data.empty:
                        player_data = prepare_comparison_data(pfc_kpi, player_name)
                        if not player_data.empty:
                            if st.button("Comparer avec le poste EDF"):
                                players_data = pd.concat([player_data, edf_data])
                                fig = create_comparison_radar(
                                    players_data,
                                    player1_name=player_name,
                                    player2_name=f"EDF {poste}"
                                )
                                if fig:
                                    st.pyplot(fig)
                        else:
                            st.warning("Aucune donnée disponible pour cette joueuse.")
                    else:
                        st.warning("Aucune donnée EDF disponible pour ce poste.")
                else:
                    st.warning("Aucune donnée EDF disponible pour la comparaison.")

                st.write("### 3. Comparez-vous à vos moyennes globales")
                if not pfc_kpi.empty:
                    player_global_data = prepare_comparison_data(pfc_kpi, player_name)
                    if not player_global_data.empty:
                        if 'Adversaire' in pfc_kpi.columns:
                            selected_match = st.selectbox(
                                "Sélectionnez un match spécifique à comparer",
                                pfc_kpi['Adversaire'].unique(),
                                key='specific_match'
                            )
                            match_data = pfc_kpi[pfc_kpi['Adversaire'] == selected_match]
                            if not match_data.empty:
                                match_aggregated = match_data.groupby('Player').agg({
                                    'Temps de jeu (en minutes)': 'sum',
                                    'Buts': 'sum',
                                }).join(
                                    match_data.groupby('Player').mean(numeric_only=True).drop(
                                        columns=['Temps de jeu (en minutes)', 'Buts'], errors='ignore'
                                    )
                                ).round().astype(int).reset_index()
                                match_aggregated['Player'] = f"{player_name} ({selected_match})"
                                player_global_data['Player'] = f"{player_name} (Moyenne globale)"
                                if st.button("Comparer avec mes moyennes"):
                                    players_data = pd.concat([match_aggregated, player_global_data])
                                    fig = create_comparison_radar(players_data)
                                    if fig:
                                        st.pyplot(fig)
                            else:
                                st.warning("Aucune donnée disponible pour ce match.")
                        else:
                            st.warning("Colonne 'Adversaire' manquante dans les données.")
                    else:
                        st.warning("Aucune donnée disponible pour cette joueuse.")
        else:
            st.subheader("Sélectionnez une joueuse du Paris FC")
            if not pfc_kpi.empty and 'Player' in pfc_kpi.columns:
                player1 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_1')
                player1_data = pfc_kpi[pfc_kpi['Player'] == player1]
                if player1_data.empty:
                    st.error("Aucune donnée disponible pour cette joueuse.")
                else:
                    if 'Adversaire' in player1_data.columns:
                        game1 = st.multiselect("Choisissez un ou plusieurs matchs", player1_data['Adversaire'].unique(), key='games_1')
                        filtered_player1_data = player1_data[player1_data['Adversaire'].isin(game1)] if game1 else player1_data
                        aggregated_player1_data = filtered_player1_data.groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()
                        tab1, tab2 = st.tabs(["Comparaison (PFC)", "Comparaison (EDF)"])
                        with tab1:
                            st.subheader("Sélectionnez une autre joueuse du Paris FC")
                            player2 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_2_pfc')
                            player2_data = pfc_kpi[pfc_kpi['Player'] == player2]
                            if player2_data.empty:
                                st.error("Aucune donnée disponible pour cette joueuse.")
                            else:
                                if 'Adversaire' in player2_data.columns:
                                    game2 = st.multiselect("Choisissez un ou plusieurs matchs", player2_data['Adversaire'].unique(), key='games_2_pfc')
                                    filtered_player2_data = player2_data[player2_data['Adversaire'].isin(game2)] if game2 else player2_data
                                    aggregated_player2_data = filtered_player2_data.groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()
                                    if st.button("Afficher le radar", key='button_pfc'):
                                        if aggregated_player1_data.empty or aggregated_player2_data.empty:
                                            st.error("Veuillez sélectionner au moins un match pour chaque joueur.")
                                        else:
                                            players_data = pd.concat([aggregated_player1_data, aggregated_player2_data])
                                            fig = create_comparison_radar(players_data)
                                            if fig:
                                                st.pyplot(fig)
                        with tab2:
                            if not edf_kpi.empty and 'Poste' in edf_kpi.columns:
                                st.subheader("Sélectionnez un poste de l'Équipe de France")
                                player2 = st.selectbox("Choisissez un poste de comparaison", edf_kpi['Poste'].unique(), key='player_2_edf')
                                player2_data = edf_kpi[edf_kpi['Poste'] == player2].rename(columns={'Poste': 'Player'})
                                if st.button("Afficher le radar", key='button_edf'):
                                    if aggregated_player1_data.empty:
                                        st.error("Veuillez sélectionner au moins un match pour la joueuse PFC.")
                                    else:
                                        players_data = pd.concat([aggregated_player1_data, player2_data])
                                        fig = create_comparison_radar(players_data)
                                        if fig:
                                            st.pyplot(fig)
                            else:
                                st.warning("Aucune donnée EDF disponible.")
    elif page == "Gestion":
        st.header("Gestion des utilisateurs")
        if check_permission(user_profile, "all", permissions):
            st.write("Cette page est réservée à la gestion des utilisateurs.")
            st.subheader("Liste des utilisateurs")
            users_data = []
            for profile, info in permissions.items():
                users_data.append({
                    "Profil": profile,
                    "Permissions": ", ".join(info["permissions"]),
                    "Joueuse associée": info.get("player", "Aucune")
                })
            users_df = pd.DataFrame(users_data)
            st.dataframe(users_df)
            with st.expander("Ajouter un utilisateur"):
                with st.form("add_user_form"):
                    new_profile = st.text_input("Nouveau profil")
                    new_password = st.text_input("Mot de passe", type="password")
                    new_permissions = st.multiselect(
                        "Permissions",
                        ["view_stats", "compare_players", "update_data", "all"],
                        default=["view_stats"]
                    )
                    new_player = st.text_input("Joueuse associée (optionnel)")
                    submitted = st.form_submit_button("Créer le profil")
                    if submitted:
                        if new_profile in permissions:
                            st.error("Ce profil existe déjà!")
                        else:
                            permissions[new_profile] = {
                                "password": new_password,
                                "permissions": new_permissions,
                                "player": nettoyer_nom_joueuse(new_player) if new_player else None
                            }
                            st.success(f"Profil {new_profile} créé avec succès!")
        else:
            st.error("Vous n'avez pas la permission d'accéder à cette page.")

# =============================================
# POINT D'ENTRÉE PRINCIPAL
# =============================================
if __name__ == '__main__':
    st.set_page_config(
        page_title="Paris FC - Pôle vidéo/data CDFF",
        page_icon="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png",
        layout="wide"
    )

    # CSS personnalisé pour le style
   st.markdown("""
<style>
    /* Fond de l'application */
    .stApp {
        background: linear-gradient(135deg, #002B5C 0%, #0047AB 100%);
        color: white;
    }

    /* Fond du conteneur principal */
    .main .block-container {
        background: linear-gradient(135deg, #0047AB 0%, #0078D4 100%);
        border-radius: 10px;
        padding: 20px;
        color: white;
    }

    /* Sidebar */
    .sidebar .sidebar-content {
        background: #002B5C;
        color: white;
        border-right: 1px solid #0078D4;
    }

    /* En-tête de la sidebar */
    .sidebar .sidebar-content h1,
    .sidebar .sidebar-content p {
        color: white;
    }

    /* Boutons */
    .stButton>button {
        background-color: #0078D4;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 8px 16px;
    }

    /* Sélecteurs */
    .stSelectbox>div>div, .stMultiselect>div>div {
        background-color: #003A58;
        color: white;
        border-radius: 5px;
        border: 1px solid #0078D4;
    }

    /* Champs de texte */
    .stTextInput>div>div>input {
        background-color: #003A58;
        color: white;
        border-radius: 5px;
        border: 1px solid #0078D4;
    }

    /* Onglets */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #003A58;
        gap: 0;
        border-radius: 5px;
    }

    /* Onglet actif */
    .stTabs [aria-selected="true"] {
        background-color: #0078D4;
        color: white;
    }

    /* Métriques */
    .stMetric {
        background-color: #003A58;
        border-radius: 5px;
        padding: 10px;
        color: white;
    }

    /* DataFrames */
    .stDataFrame {
        background-color: white;
        color: black;
        border-radius: 5px;
    }

    /* Messages d'erreur */
    .stAlert {
        background-color: #d32f2f;
        color: white;
        border-radius: 5px;
    }

    /* Messages de succès */
    [data-baseweb="notification"] .stAlert {
        background-color: #388e3c;
        color: white;
        border-radius: 5px;
    }

    /* Conteneurs de colonnes */
    [data-testid="column"] {
        background-color: rgba(0, 71, 171, 0.3);
        border-radius: 5px;
        padding: 10px;
        margin: 5px;
    }

    /* Espacement des éléments */
    [data-testid="stVerticalBlock"] {
        gap: 1rem;
    }

    /* Logo dans la sidebar */
    .sidebar-logo {
        display: flex;
        justify-content: center;
        margin-bottom: 20px;
    }

    /* Logo dans la sidebar (image) */
    .sidebar-logo img {
        width: 80px;
    }
</style>
""", unsafe_allow_html=True)


    # En-tête personnalisé
    st.markdown("""
    <div class="main-header">
        <div class="logo-container">
            <img src="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png" alt="Paris FC Logo">
        </div>
        <h1>Pôle vidéo/data CDFF</h1>
        <p>Saison 2025-26</p>
    </div>
    """, unsafe_allow_html=True)

    # Chargement des permissions et des données
    permissions = load_permissions()
    if not permissions:
        st.error("Impossible de charger les permissions. Vérifiez que le fichier 'Classeurs permissions streamlit.xlsx' est présent dans le dossier Google Drive.")
        st.stop()

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user_profile" not in st.session_state:
        st.session_state.user_profile = None

    # Logique d'authentification (inchangée)
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

    # Chargement des données
    try:
        pfc_kpi, edf_kpi = collect_data()
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {e}")
        pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

    # Appel de la fonction principale de l'interface
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)

