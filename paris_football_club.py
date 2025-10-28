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

# --- Fonction de nettoyage des noms de joueuses ---
def nettoyer_nom_joueuse(nom):
    """Nettoie le nom d'une joueuse en supprimant les doublons séparés par une virgule."""
    if isinstance(nom, str):
        nom = nom.strip().upper()  # Convertir en majuscules pour la comparaison
        parts = [part.strip().upper() for part in nom.split(",")]
        if len(parts) > 1 and parts[0] == parts[1]:
            return parts[0]
        return nom
    return nom

# --- Fonctions d'authentification et téléchargement Google Drive ---
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

def download_permissions_file(service, folder_id):
    """Télécharge le fichier des permissions depuis Google Drive."""
    files = list_files_in_folder(service, folder_id)
    for file in files:
        if file['name'] == "Classeurs permissions streamlit.xlsx":
            output_folder = "data"
            os.makedirs(output_folder, exist_ok=True)
            download_file(service, file['id'], file['name'], output_folder)
            permissions_path = os.path.join(output_folder, file['name'])
            return permissions_path
    return None

def load_permissions():
    """Charge les permissions depuis le fichier Excel."""
    service = authenticate_google_drive()
    folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
    permissions_path = download_permissions_file(service, folder_id)
    if permissions_path:
        permissions_df = pd.read_excel(permissions_path)
        permissions = {}
        for _, row in permissions_df.iterrows():
            profile = row['Profil']
            permissions[profile] = {
                "password": row['Mot de passe'],
                "permissions": row['Permissions'].split(',') if isinstance(row['Permissions'], str) else [],
                "player": row.get('Joueuse', None)  # Récupère le nom de la joueuse associée
            }
        return permissions
    return {}

# --- Gestion des profils et permissions ---
def check_permission(user_profile, required_permission, permissions):
    """Vérifie si un profil a une permission spécifique."""
    if user_profile in permissions:
        return required_permission in permissions[user_profile]["permissions"]
    return False

def get_player_for_profile(profile, permissions):
    """Récupère le nom de la joueuse associée à un profil."""
    if profile in permissions:
        return permissions[profile].get("player", None)
    return None

# --- Fonctions de chargement et traitement des données ---
def download_google_drive():
    """Télécharge les données depuis Google Drive."""
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

@st.cache_data
def charger_donnees():
    """Charge les données locales."""
    data_folder = "data"
    if not os.path.exists(data_folder):
        st.error(f"Le dossier '{data_folder}' n'existe pas.")
        return {}
    fichiers = [f for f in os.listdir(data_folder) if f.endswith(('.csv', '.xlsx')) and f != "Classeurs permissions streamlit.xlsx"]
    if not fichiers:
        st.warning(f"Aucun fichier de données trouvé dans '{data_folder}'.")
        return {}
    df_dict = {}
    for f in fichiers:
        path = os.path.join(data_folder, f)
        try:
            if f.endswith(".csv"):
                df_dict[f] = pd.read_csv(path)
            else:
                df_dict[f] = pd.read_excel(path)
        except Exception as e:
            st.error(f"Erreur lors de la lecture du fichier {f}: {e}")
    return df_dict

# --- Fonctions de traitement des données (inchangées) ---
def players_edf_duration(match):
    df_filtered = match.loc[match['Poste'] != 'Gardienne']
    df_duration = pd.DataFrame({
        'Player': df_filtered['Player'].apply(nettoyer_nom_joueuse),
        'Temps de jeu (en minutes)': df_filtered['Temps de jeu']
    })
    return df_duration

def players_duration(match):
    players_duration = {}
    list_of_players = ['ATT', 'DCD', 'DCG', 'DD', 'DG', 'GB', 'MCD', 'MCG', 'MD', 'MDef', 'MG']
    for i in range(len(match)):
        duration = match.iloc[i]['Duration']
        for poste in list_of_players:
            player = nettoyer_nom_joueuse(match.iloc[i][poste])
            if player in players_duration:
                players_duration[player] += duration
            else:
                players_duration[player] = duration
    for player in players_duration:
        players_duration[player] /= 60
    df_duration = pd.DataFrame({
        'Player': list(players_duration.keys()),
        'Temps de jeu (en minutes)': list(players_duration.values())
    })
    df_duration = df_duration.sort_values(by='Temps de jeu (en minutes)', ascending=False)
    df_duration['Player'] = df_duration['Player'].replace('HAMINI ALYA', 'HAMICI ALYA')
    return df_duration

def players_shots(joueurs):
    players_shots, players_shots_on_target, players_goals = {}, {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Tir' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_shots[player] = players_shots.get(player, 0) + action.count('Tir')
            is_successful = joueurs.iloc[i]['Tir']
            if isinstance(is_successful, str) and ('Tir Cadré' in is_successful or 'But' in is_successful):
                players_shots_on_target[player] = players_shots_on_target.get(player, 0) + is_successful.count('Tir Cadré') + is_successful.count('But')
            if isinstance(is_successful, str) and 'But' in is_successful:
                players_goals[player] = players_goals.get(player, 0) + 1
    return pd.DataFrame({
        'Player': list(players_shots.keys()),
        'Tirs': list(players_shots.values()),
        'Tirs cadrés': [players_shots_on_target.get(player, 0) for player in players_shots],
        'Buts': [players_goals.get(player, 0) for player in players_shots]
    }).sort_values(by='Tirs', ascending=False)

def players_passes(joueurs):
    player_short_passes, player_long_passes = {}, {}
    players_successful_short_passes, players_successful_long_passes = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Passe' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            passe = joueurs.iloc[i]['Passe']
            if isinstance(passe, str) and 'Courte' in passe:
                player_short_passes[player] = player_short_passes.get(player, 0) + passe.count('Courte')
                if 'Réussie' in passe:
                    players_successful_short_passes[player] = players_successful_short_passes.get(player, 0) + passe.count('Réussie')
            if isinstance(passe, str) and 'Longue' in passe:
                player_long_passes[player] = player_long_passes.get(player, 0) + passe.count('Longue')
                if 'Réussie' in passe:
                    players_successful_long_passes[player] = players_successful_long_passes.get(player, 0) + passe.count('Réussie')
    df_passes = pd.DataFrame({
        'Player': list(player_short_passes.keys()),
        'Passes courtes': [player_short_passes.get(player, 0) for player in player_short_passes],
        'Passes longues': [player_long_passes.get(player, 0) for player in player_short_passes],
        'Passes réussies (courtes)': [players_successful_short_passes.get(player, 0) for player in player_short_passes],
        'Passes réussies (longues)': [players_successful_long_passes.get(player, 0) for player in player_short_passes]
    })
    df_passes['Passes'] = df_passes['Passes courtes'] + df_passes['Passes longues']
    df_passes['Passes réussies'] = df_passes['Passes réussies (courtes)'] + df_passes['Passes réussies (longues)']
    df_passes['Pourcentage de passes réussies'] = (df_passes['Passes réussies'] / df_passes['Passes'] * 100).fillna(0)
    return df_passes.sort_values(by='Passes courtes', ascending=False)

def players_dribbles(joueurs):
    players_dribbles, players_successful_dribbles = {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Dribble' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_dribbles[player] = players_dribbles.get(player, 0) + action.count('Dribble')
            is_successful = joueurs.iloc[i]['Dribble']
            if isinstance(is_successful, str) and 'Réussi' in is_successful:
                players_successful_dribbles[player] = players_successful_dribbles.get(player, 0) + is_successful.count('Réussi')
    df_dribbles = pd.DataFrame({
        'Player': list(players_dribbles.keys()),
        'Dribbles': list(players_dribbles.values()),
        'Dribbles réussis': [players_successful_dribbles.get(player, 0) for player in players_dribbles]
    })
    df_dribbles['Pourcentage de dribbles réussis'] = (df_dribbles['Dribbles réussis'] / df_dribbles['Dribbles'] * 100).fillna(0)
    return df_dribbles.sort_values(by='Dribbles', ascending=False)

def players_defensive_duels(joueurs):
    players_defensive_duels, players_successful_defensive_duels, players_faults = {}, {}, {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Duel défensif' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_defensive_duels[player] = players_defensive_duels.get(player, 0) + action.count('Duel défensif')
            is_successful = joueurs.iloc[i]['Duel défensifs']
            if isinstance(is_successful, str) and 'Gagné' in is_successful:
                players_successful_defensive_duels[player] = players_successful_defensive_duels.get(player, 0) + is_successful.count('Gagné')
            if isinstance(is_successful, str) and 'Faute' in is_successful:
                players_faults[player] = players_faults.get(player, 0) + is_successful.count('Faute')
    df_duels_defensifs = pd.DataFrame({
        'Player': list(players_defensive_duels.keys()),
        'Duels défensifs': list(players_defensive_duels.values()),
        'Duels défensifs gagnés': [players_successful_defensive_duels.get(player, 0) for player in players_defensive_duels],
        'Fautes': [players_faults.get(player, 0) for player in players_defensive_duels]
    })
    df_duels_defensifs['Pourcentage de duels défensifs gagnés'] = (df_duels_defensifs['Duels défensifs gagnés'] / df_duels_defensifs['Duels défensifs'] * 100).fillna(0)
    return df_duels_defensifs.sort_values(by='Duels défensifs', ascending=False)

def players_interceptions(joueurs):
    players_interceptions = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Interception' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_interceptions[player] = players_interceptions.get(player, 0) + action.count('Interception')
    return pd.DataFrame({
        'Player': list(players_interceptions.keys()),
        'Interceptions': list(players_interceptions.values())
    }).sort_values(by='Interceptions', ascending=False)

def players_ball_losses(joueurs):
    players_ball_losses = {}
    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Perte de balle' in action:
            player = nettoyer_nom_joueuse(joueurs.iloc[i]['Row'])
            players_ball_losses[player] = players_ball_losses.get(player, 0) + action.count('Perte de balle')
    return pd.DataFrame({
        'Player': list(players_ball_losses.keys()),
        'Pertes de balle': list(players_ball_losses.values())
    }).sort_values(by='Pertes de balle', ascending=False)

def create_data(match, joueurs, is_edf):
    if is_edf:
        df_duration = players_edf_duration(match)
    else:
        df_duration = players_duration(match)
    dfs = [
        df_duration,
        players_shots(joueurs),
        players_passes(joueurs),
        players_dribbles(joueurs),
        players_defensive_duels(joueurs),
        players_interceptions(joueurs),
        players_ball_losses(joueurs)
    ]
    for df in dfs:
        df['Player'] = df['Player'].apply(nettoyer_nom_joueuse)
    df = df_duration
    for other_df in dfs[1:]:
        df = df.merge(other_df, on='Player', how='outer')
    df.fillna(0, inplace=True)
    df = df[(df.iloc[:, 1:] != 0).any(axis=1)]
    df = df[df['Temps de jeu (en minutes)'] >= 10]
    return df

def create_metrics(df):
    metrics = ['Timing', 'Force physique', 'Intelligence tactique', 'Technique 1', 'Technique 2', 'Technique 3', 'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']
    df['Timing'] = np.where(df['Duels défensifs'] > 0, (df['Duels défensifs'] - df['Fautes']) / df['Duels défensifs'], 0)
    df['Force physique'] = np.where(df['Duels défensifs'] > 0, df['Duels défensifs gagnés'] / df['Duels défensifs'], 0)
    df['Intelligence tactique'] = np.where(df['Interceptions'] > 0, df['Interceptions'] / df['Interceptions'].max(), 0)
    df['Technique 1'] = np.where(df['Passes'] > 0, df['Passes'] / df['Passes'].max(), 0)
    df['Technique 2'] = np.where(df['Passes courtes'] > 0, df['Passes réussies (courtes)'] / df['Passes courtes'], 0)
    df['Technique 3'] = np.where(df['Passes longues'] > 0, df['Passes réussies (longues)'] / df['Passes longues'], 0)
    df['Explosivité'] = np.where(df['Dribbles'] > 0, df['Dribbles réussis'] / df['Dribbles'], 0)
    df['Prise de risque'] = np.where(df['Dribbles'] > 0, df['Dribbles'] / df['Dribbles'].max(), 0)
    df['Précision'] = np.where(df['Tirs'] > 0, df['Tirs cadrés'] / df['Tirs'], 0)
    df['Sang-froid'] = np.where(df['Tirs'] > 0, df['Tirs'] / df['Tirs'].max(), 0)
    for metric in metrics:
        df[metric] = (df[metric].rank(pct=True) * 100).fillna(0)
    return df

def create_kpis(df):
    df['Rigueur'] = (df['Timing'] + df['Force physique']) / 2
    df['Récupération'] = df['Intelligence tactique']
    df['Distribution'] = (df['Technique 1'] + df['Technique 2'] + df['Technique 3']) / 3
    df['Percussion'] = (df['Explosivité'] + df['Prise de risque']) / 2
    df['Finition'] = (df['Précision'] + df['Sang-froid']) / 2
    return df

def create_poste(df):
    df['Défenseur central'] = (df['Rigueur'] * 5 + df['Récupération'] * 5 + df['Distribution'] * 5 + df['Percussion'] * 1 + df['Finition'] * 1) / 17
    df['Défenseur latéral'] = (df['Rigueur'] * 3 + df['Récupération'] * 3 + df['Distribution'] * 3 + df['Percussion'] * 3 + df['Finition'] * 3) / 15
    df['Milieu défensif'] = (df['Rigueur'] * 4 + df['Récupération'] * 4 + df['Distribution'] * 4 + df['Percussion'] * 2 + df['Finition'] * 2) / 16
    df['Milieu relayeur'] = (df['Rigueur'] * 3 + df['Récupération'] * 3 + df['Distribution'] * 3 + df['Percussion'] * 3 + df['Finition'] * 3) / 15
    df['Milieu offensif'] = (df['Rigueur'] * 2 + df['Récupération'] * 2 + df['Distribution'] * 2 + df['Percussion'] * 4 + df['Finition'] * 4) / 14
    df['Attaquant'] = (df['Rigueur'] * 1 + df['Récupération'] * 1 + df['Distribution'] * 1 + df['Percussion'] * 5 + df['Finition'] * 5) / 13
    return df

# --- Fonctions de visualisation ---
def create_individual_radar(df):
    columns_to_plot = ['Timing', 'Force physique', 'Intelligence tactique', 'Technique 1', 'Technique 2', 'Technique 3', 'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']
    colors = ['#6A7CD9', '#00BFFE', '#FF9470', '#F27979', '#BFBFBF'] * 2
    player = df.iloc[0]
    pizza = PyPizza(params=columns_to_plot, background_color='#0e1117', straight_line_color='#FFFFFF', last_circle_color='#FFFFFF')
    fig, _ = pizza.make_pizza(
        figsize=(8, 8),
        values=[player[col] for col in columns_to_plot],
        slice_colors=colors,
        kwargs_values=dict(color='#FFFFFF', fontsize=9, bbox=dict(edgecolor='#FFFFFF', facecolor='#0e1117', boxstyle='round, pad=0.2', lw=1)),
        kwargs_params=dict(color='#FFFFFF', fontsize=10, fontproperties='monospace')
    )
    fig.set_facecolor('#0e1117')
    return fig

def create_comparison_radar(df):
    metrics = ['Timing', 'Force physique', 'Intelligence tactique', 'Technique 1', 'Technique 2', 'Technique 3', 'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']
    low, high = (0,) * 10, (100,) * 10
    radar = Radar(metrics, low, high, num_rings=4, ring_width=1, center_circle_radius=1)
    URL1 = 'https://raw.githubusercontent.com/googlefonts/roboto/main/src/hinted/Roboto-Thin.ttf'
    URL2 = 'https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/RobotoSlab%5Bwght%5D.ttf'
    robotto_thin, robotto_bold = FontManager(URL1), FontManager(URL2)
    fig, axs = grid(figheight=14, grid_height=0.915, title_height=0.06, endnote_height=0.025, title_space=0, endnote_space=0, grid_key='radar')
    radar.setup_axis(ax=axs['radar'], facecolor='None')
    radar.draw_circles(ax=axs['radar'], facecolor='#28252c', edgecolor='#39353f', lw=1.5)
    player_values_1, player_values_2 = df.iloc[0][metrics].values, df.iloc[1][metrics].values
    radar.draw_radar_compare(player_values_1, player_values_2, ax=axs['radar'],
                             kwargs_radar={'facecolor': '#00f2c1', 'alpha': 0.6},
                             kwargs_compare={'facecolor': '#d80499', 'alpha': 0.6})
    radar.draw_range_labels(ax=axs['radar'], fontsize=25, color='#fcfcfc', fontproperties=robotto_thin.prop)
    radar.draw_param_labels(ax=axs['radar'], fontsize=25, color='#fcfcfc', fontproperties=robotto_thin.prop)
    axs['title'].text(0.01, 0.65, df.iloc[0]['Player'], fontsize=25, color='#01c49d', fontproperties=robotto_bold.prop, ha='left', va='center')
    axs['title'].text(0.99, 0.65, df.iloc[1]['Player'], fontsize=25, fontproperties=robotto_bold.prop, ha='right', va='center', color='#d80499')
    fig.set_facecolor('#0e1117')
    return fig

# --- Fonction principale de collecte des données ---
@st.cache_data
def collect_data():
    """Collecte et traite les données."""
    download_google_drive()
    pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()
    data_folder = "data"
    if not os.path.exists(data_folder):
        st.error(f"Le dossier '{data_folder}' n'existe pas.")
        return pfc_kpi, edf_kpi
    fichiers = [f for f in os.listdir(data_folder) if f.endswith(('.csv', '.xlsx')) and f != "Classeurs permissions streamlit.xlsx"]
    if not fichiers:
        st.warning(f"Aucun fichier de données trouvé dans '{data_folder}'.")
        return pfc_kpi, edf_kpi
    for filename in fichiers:
        path = os.path.join(data_folder, filename)
        try:
            if filename.endswith('.xlsx') and 'EDF' in filename:
                print(f"Traitement du fichier Excel : {filename}")
                edf = pd.read_excel(path)
                matchs_csv = [f for f in fichiers if f.startswith('EDF_U19_Match') and f.endswith('.csv')]
                matchs = []
                for csv_file in matchs_csv:
                    matchs.append(pd.read_csv(os.path.join(data_folder, csv_file)))
                for i, match_data in enumerate(matchs):
                    try:
                        df = create_data(edf[edf['Match'] == f'Match {i + 1}'], match_data, True)
                        for index, row in df.iterrows():
                            time_played = row['Temps de jeu (en minutes)']
                            for col in df.columns:
                                if col not in ['Player', 'Temps de jeu (en minutes)', 'Buts'] and 'Pourcentage' not in col:
                                    df.loc[index, col] = row[col] * (90 / time_played)
                        df = create_metrics(df)
                        df = df.merge(edf[['Player', 'Poste']], on='Player', how='left')
                        cols = ['Player', 'Poste'] + [col for col in df.columns if col not in ['Player', 'Poste']]
                        df = df[cols]
                        edf_kpi = pd.concat([edf_kpi, df])
                    except Exception as e:
                        st.error(f"Erreur lors du traitement du Match {i + 1}: {e}")
                if not edf_kpi.empty:
                    edf_kpi = edf_kpi.groupby('Poste').mean(numeric_only=True).reset_index()
                    edf_kpi = edf_kpi.drop(columns='Temps de jeu (en minutes)', errors='ignore')
                    edf_kpi['Poste'] = edf_kpi['Poste'].replace({'Milieux axiale': 'Milieu axiale', 'Milieux offensive': 'Milieu offensive'})
                    edf_kpi['Poste'] = edf_kpi['Poste'] + ' moyenne (EDF)'
            elif filename.endswith('.csv') and 'PFC' in filename:
                print(f"Traitement du fichier CSV : {filename}")
                data = pd.read_csv(path)
                parts = filename.split('.')[0].split('_')
                if len(parts) < 6:
                    st.warning(f"Le nom du fichier {filename} ne suit pas le format attendu.")
                    continue
                equipe_domicile, _, equipe_exterieur, journee, categorie, date = parts[:6]
                match, joueurs = pd.DataFrame(), pd.DataFrame()
                for i in range(len(data)):
                    if data['Row'].iloc[i] in [equipe_domicile, equipe_exterieur]:
                        match = pd.concat([match, data.iloc[i:i+1]], ignore_index=True)
                    elif not any(x in data['Row'].iloc[i] for x in ['Corner', 'Coup-franc', 'Penalty', 'Carton']):
                        joueurs = pd.concat([joueurs, data.iloc[i:i+1]], ignore_index=True)
                if not joueurs.empty:
                    df = create_data(match, joueurs, False)
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
            st.error(f"Erreur lors du traitement du fichier {filename}: {e}")
    return pfc_kpi, edf_kpi

# --- Fonction pour filtrer les données par joueuse ---
def filter_data_by_player(df, player_name):
    """Filtre les données pour une joueuse spécifique."""
    if player_name:
        # Nettoyer le nom pour la comparaison
        player_name_clean = nettoyer_nom_joueuse(player_name)
        # Filtrer les données
        df['Player_clean'] = df['Player'].apply(nettoyer_nom_joueuse)
        filtered_df = df[df['Player_clean'] == player_name_clean].copy()
        filtered_df.drop(columns=['Player_clean'], inplace=True, errors='ignore')
        return filtered_df
    return df

# --- Interface Streamlit avec gestion des permissions et filtrage par joueuse ---
def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
    """Interface principale adaptée aux permissions et filtrée par joueuse."""
    logo_pfc = "https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"
    st.sidebar.markdown(f"<div style='display: flex; justify-content: center;'><img src='{logo_pfc}' width='100'></div>", unsafe_allow_html=True)

    # Récupérer le nom de la joueuse associée au profil
    player_name = get_player_for_profile(user_profile, permissions)
    st.sidebar.title(f"Connecté en tant que: {user_profile}")
    if player_name:
        st.sidebar.write(f"Joueuse associée: {player_name}")

    # Bouton de mise à jour des données (uniquement pour les profils autorisés)
    if check_permission(user_profile, "update_data", permissions):
        if st.sidebar.button("Mettre à jour la base de données"):
            with st.spinner("Mise à jour des données en cours..."):
                download_google_drive()
            st.success("✅ Mise à jour terminée")
            st.cache_data.clear()

    # Filtrer les données en fonction du profil
    if player_name:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)

    # Affichage des onglets en fonction des permissions
    available_options = ["Statistiques"]
    if check_permission(user_profile, "compare_players", permissions):
        available_options.append("Comparaison")

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=available_options,
            icons=["graph-up-arrow", "people"][:len(available_options)],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={
                "container": {"padding": "5!important", "background-color": "transparent"},
                "icon": {"font-size": "18px"},
                "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#0E1117"},
                "nav-link-selected": {"background-color": "#0E1117", "color": "#ecebe3", "font-weight": "bold"}
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

    # --- Pages ---
    if page == "Statistiques":
        st.header("Statistiques")

        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible pour votre profil.")
        else:
            if player_name:
                st.subheader(f"Statistiques pour {player_name}")

                # Pour une joueuse, on n'a qu'une seule ligne de données
                player_data = pfc_kpi

                if player_data.empty:
                    st.warning(f"Aucune donnée disponible pour {player_name}.")
                else:
                    time_played, goals = st.columns(2)
                    with time_played:
                        st.metric("Temps de jeu", f"{player_data['Temps de jeu (en minutes)'].iloc[0]} minutes")
                    with goals:
                        st.metric("Buts", f"{player_data['Buts'].iloc[0]}")

                    tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])

                    with tab1:
                        fig = create_individual_radar(player_data)
                        st.pyplot(fig)

                    with tab2:
                        col1, col2, col3, col4, col5 = st.columns(5)
                        with col1: st.metric("Rigueur", f"{player_data['Rigueur'].iloc[0]}/100")
                        with col2: st.metric("Récupération", f"{player_data['Récupération'].iloc[0]}/100")
                        with col3: st.metric("Distribution", f"{player_data['Distribution'].iloc[0]}/100")
                        with col4: st.metric("Percussion", f"{player_data['Percussion'].iloc[0]}/100")
                        with col5: st.metric("Finition", f"{player_data['Finition'].iloc[0]}/100")

                    with tab3:
                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                        with col1: st.metric("Défenseur central", f"{player_data['Défenseur central'].iloc[0]}/100")
                        with col2: st.metric("Défenseur latéral", f"{player_data['Défenseur latéral'].iloc[0]}/100")
                        with col3: st.metric("Milieu défensif", f"{player_data['Milieu défensif'].iloc[0]}/100")
                        with col4: st.metric("Milieu relayeur", f"{player_data['Milieu relayeur'].iloc[0]}/100")
                        with col5: st.metric("Milieu offensif", f"{player_data['Milieu offensif'].iloc[0]}/100")
                        with col6: st.metric("Attaquant", f"{player_data['Attaquant'].iloc[0]}/100")
            else:
                # Pour les profils admin/coach (accès à toutes les joueuses)
                st.subheader("Sélectionnez une joueuse du Paris FC")
                player = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique())
                player_data = pfc_kpi[pfc_kpi['Player'] == player]
                game = st.multiselect("Choisissez un ou plusieurs matchs", player_data['Adversaire'].unique())
                player_data = player_data[player_data['Adversaire'].isin(game)]
                player_data = player_data.groupby('Player').agg({
                    'Temps de jeu (en minutes)': 'sum',
                    'Buts': 'sum',
                }).join(
                    player_data.groupby('Player').mean(numeric_only=True).drop(columns=['Temps de jeu (en minutes)', 'Buts'])
                ).round().astype(int).reset_index()

                if not game:
                    st.error("Veuillez sélectionner au moins un match.")
                else:
                    time_played, goals = st.columns(2)
                    with time_played:
                        st.metric("Temps de jeu", f"{player_data['Temps de jeu (en minutes)'].iloc[0]} minutes")
                    with goals:
                        st.metric("Buts", f"{player_data['Buts'].iloc[0]}")

                    tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])

                    with tab1:
                        fig = create_individual_radar(player_data)
                        st.pyplot(fig)

                    with tab2:
                        col1, col2, col3, col4, col5 = st.columns(5)
                        with col1: st.metric("Rigueur", f"{player_data['Rigueur'].iloc[0]}/100")
                        with col2: st.metric("Récupération", f"{player_data['Récupération'].iloc[0]}/100")
                        with col3: st.metric("Distribution", f"{player_data['Distribution'].iloc[0]}/100")
                        with col4: st.metric("Percussion", f"{player_data['Percussion'].iloc[0]}/100")
                        with col5: st.metric("Finition", f"{player_data['Finition'].iloc[0]}/100")

                    with tab3:
                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                        with col1: st.metric("Défenseur central", f"{player_data['Défenseur central'].iloc[0]}/100")
                        with col2: st.metric("Défenseur latéral", f"{player_data['Défenseur latéral'].iloc[0]}/100")
                        with col3: st.metric("Milieu défensif", f"{player_data['Milieu défensif'].iloc[0]}/100")
                        with col4: st.metric("Milieu relayeur", f"{player_data['Milieu relayeur'].iloc[0]}/100")
                        with col5: st.metric("Milieu offensif", f"{player_data['Milieu offensif'].iloc[0]}/100")
                        with col6: st.metric("Attaquant", f"{player_data['Attaquant'].iloc[0]}/100")

    elif page == "Comparaison":
        st.header("Comparaison")

        # Seuls les profils admin/coach peuvent comparer
        if not player_name:
            st.subheader("Sélectionnez une joueuse du Paris FC")
            player1 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_1')
            player1_data = pfc_kpi[pfc_kpi['Player'] == player1]
            game1 = st.multiselect("Choisissez un ou plusieurs matchs", player1_data['Adversaire'].unique(), key='games_1')
            player1_data = player1_data[player1_data['Adversaire'].isin(game1)].groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()

            tab1, tab2 = st.tabs(["Comparaison (PFC)", "Comparaison (EDF)"])

            with tab1:
                st.subheader("Sélectionnez une autre joueuse du Paris FC")
                player2 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_2_pfc')
                player2_data = pfc_kpi[pfc_kpi['Player'] == player2]
                game2 = st.multiselect("Choisissez un ou plusieurs matchs", player2_data['Adversaire'].unique(), key='games_2_pfc')
                player2_data = player2_data[player2_data['Adversaire'].isin(game2)].groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()
                if st.button("Afficher le radar", key='button_pfc'):
                    if not game1 or not game2:
                        st.error("Veuillez sélectionner au moins un match pour chaque joueur.")
                    else:
                        players_data = pd.concat([player1_data, player2_data])
                        fig = create_comparison_radar(players_data)
                        st.pyplot(fig)

            with tab2:
                st.subheader("Sélectionnez un poste de l'Équipe de France")
                player2 = st.selectbox("Choisissez un poste de comparaison", edf_kpi['Poste'].unique(), key='player_2_edf')
                player2_data = edf_kpi[edf_kpi['Poste'] == player2].rename(columns={'Poste': 'Player'})
                if st.button("Afficher le radar", key='button_edf'):
                    if not game1:
                        st.error("Veuillez sélectionner au moins un match.")
                    else:
                        players_data = pd.concat([player1_data, player2_data])
                        fig = create_comparison_radar(players_data)
                        st.pyplot(fig)
        else:
            st.warning("Les joueuses ne peuvent pas accéder à la fonctionnalité de comparaison.")

# --- Point d'entrée principal ---
if __name__ == '__main__':
    st.set_page_config(page_title="Paris Football Club", page_icon="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png", layout="wide")
    st.title("Paris Football Club")

    # Chargement des permissions
    permissions = load_permissions()
    if not permissions:
        st.error("Impossible de charger les permissions. Vérifiez que le fichier 'Classeurs permissions streamlit.xlsx' est présent dans le dossier Google Drive.")
        st.stop()

    # Authentification
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

    # Téléchargement et traitement des données
    logo_monochrome = "https://i.postimg.cc/BQQ5K5tp/Monochrome.png"
    st.markdown(f"<style>.logo-container{{position:absolute;top:-100px;right:10px;}}.logo-container img{{width:90px;}}</style><div class='logo-container'><img src='{logo_monochrome}'></div>", unsafe_allow_html=True)

    # Chargement des données
    pfc_kpi, edf_kpi = collect_data()

    # Affichage de l'interface
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)
