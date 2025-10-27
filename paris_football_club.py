import pandas as pd 
import numpy as np
import os
from mplsoccer import PyPizza
import io
from mplsoccer import Radar, FontManager, grid
import streamlit as st
from streamlit_option_menu import option_menu

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import warnings
warnings.filterwarnings('ignore')

# Fonction d'authentification via un compte de service
def authenticate_google_drive():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    # Récupérer le JSON depuis les secrets Streamlit
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]

    # Authentification via le fichier de service
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    
    # Construire le service Google Drive
    service = build('drive', 'v3', credentials=creds)
    
    return service

# Télécharger un fichier
def download_file(service, file_id, file_name, output_folder):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False

    while not done:
        status, done = downloader.next_chunk()
        print(f"Téléchargement en cours : {int(status.progress() * 100)}%")

    # Chemin complet pour enregistrer le fichier
    file_path = os.path.join(output_folder, file_name)

    # Enregistrer le fichier localement
    with open(file_path, 'wb') as f:
        f.write(fh.getbuffer())
    print(f"Fichier téléchargé : {file_path}\n")

# Lister les fichiers dans un dossier
def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

# Script principal
def download_google_drive():
    # Authentification
    service = authenticate_google_drive()

    # ID du dossier Google Drive (remplace par le tien)
    folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"

    # Dossier de sortie pour les fichiers téléchargés
    output_folder = "data"
    os.makedirs(output_folder, exist_ok=True)

    # Récupérer les fichiers du dossier
    files = list_files_in_folder(service, folder_id)
    if not files:
        print("Aucun fichier trouvé dans le dossier.")
    else:
        print(f"Fichiers trouvés : {len(files)}\n")
        for file in files:
            if file['name'].endswith('.csv') or file['name'].endswith('.xlsx'):  # Vérifier si le fichier est un .csv ou .xlsx
                print(f"Téléchargement de : {file['name']}...")
                download_file(service, file['id'], file['name'], output_folder)
            else:
                print(f"Fichier ignoré (non .csv ou .xlsx) : {file['name']}\n")

def players_edf_duration(match):
    df_filtered = match.loc[match['Poste'] != 'Gardienne']

    df_duration = pd.DataFrame({
        'Player': df_filtered['Player'],
        'Temps de jeu (en minutes)': df_filtered['Temps de jeu']
    })

    return df_duration

def players_duration(match):
    players_duration = {}

    list_of_players = ['ATT', 'DCD', 'DCG', 'DD', 'DG', 'GB', 'MCD', 'MCG', 'MD', 'MDef', 'MG']

    for i in range (len(match)):
        duration = match.iloc[i]['Duration']
        for poste in list_of_players:
            player = match.iloc[i][poste]
            if player in players_duration:
                players_duration[player] += duration
            else:
                players_duration[player] = duration

    # Divise la durée par 60 pour obtenir le nombre de minutes
    for player in players_duration:
        players_duration[player] = players_duration[player] / 60

    df_duration = pd.DataFrame({
        'Player': list(players_duration.keys()),
        'Temps de jeu (en minutes)': list(players_duration.values())
    })

    df_duration = df_duration.sort_values(by='Temps de jeu (en minutes)', ascending=False)

    df_duration['Player'] = df_duration['Player'].replace('HAMINI Alya', 'HAMICI Alya') # Correction d'une erreur de saisie

    return df_duration

def players_shots(joueurs):
    players_shots = {}
    players_shots_on_target = {}
    players_goals = {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Tir' in action:
            shot_count = action.count('Tir')
            players_shots[joueurs.iloc[i]['Row']] = players_shots.get(joueurs.iloc[i]['Row'], 0) + shot_count

            is_successful = joueurs.iloc[i]['Tir']
            if isinstance(is_successful, str) and ('Tir Cadré' in is_successful or 'But' in is_successful):
                is_successful_count = is_successful.count('Tir Cadré') + is_successful.count('But')
                players_shots_on_target[joueurs.iloc[i]['Row']] = players_shots_on_target.get(joueurs.iloc[i]['Row'], 0) + is_successful_count

            if isinstance(is_successful, str) and 'But' in is_successful:
                players_goals[joueurs.iloc[i]['Row']] = players_goals.get(joueurs.iloc[i]['Row'], 0) + 1

    df_tirs = pd.DataFrame({
        'Player': list(players_shots.keys()),
        'Tirs': list(players_shots.values()),
        'Tirs cadrés': [players_shots_on_target.get(player, 0) for player in players_shots],
        'Buts': [players_goals.get(player, 0) for player in players_shots]
    })

    df_tirs = df_tirs.sort_values(by='Tirs', ascending=False)

    return df_tirs

def players_passes(joueurs):
    player_short_passes = {}
    player_long_passes = {}
    players_successful_short_passes = {}
    players_successful_long_passes = {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Passe' in action:
            passe = joueurs.iloc[i]['Passe']
            if isinstance(passe, str) and 'Courte' in passe:
                short_pass_count = passe.count('Courte')
                player_short_passes[joueurs.iloc[i]['Row']] = player_short_passes.get(joueurs.iloc[i]['Row'], 0) + short_pass_count

                is_successful = joueurs.iloc[i]['Passe']
                if isinstance(is_successful, str) and 'Réussie' in is_successful:
                    is_successful_count = is_successful.count('Réussie')
                    players_successful_short_passes[joueurs.iloc[i]['Row']] = players_successful_short_passes.get(joueurs.iloc[i]['Row'], 0) + is_successful_count

            if isinstance(passe, str) and 'Longue' in passe:
                long_pass_count = passe.count('Longue')
                player_long_passes[joueurs.iloc[i]['Row']] = player_long_passes.get(joueurs.iloc[i]['Row'], 0) + long_pass_count

                is_successful = joueurs.iloc[i]['Passe']
                if isinstance(is_successful, str) and 'Réussie' in is_successful:
                    is_successful_count = is_successful.count('Réussie')
                    players_successful_long_passes[joueurs.iloc[i]['Row']] = players_successful_long_passes.get(joueurs.iloc[i]['Row'], 0) + is_successful_count

    df_passes = pd.DataFrame({
        'Player': list(player_short_passes.keys()),
        'Passes courtes': [player_short_passes.get(player, 0) for player in player_short_passes],
        'Passes longues': [player_long_passes.get(player, 0) for player in player_short_passes],
        'Passes réussies (courtes)': [players_successful_short_passes.get(player, 0) for player in player_short_passes],
        'Passes réussies (longues)': [players_successful_long_passes.get(player, 0) for player in player_short_passes]
    })

    # Calcul de la précision
    df_passes['Passes'] = df_passes['Passes courtes'] + df_passes['Passes longues']
    df_passes['Passes réussies'] = df_passes['Passes réussies (courtes)'] + df_passes['Passes réussies (longues)']
    df_passes['Pourcentage de passes réussies'] = df_passes['Passes réussies'] / df_passes['Passes'] * 100

    # Tri du dataframe par le nombre de passes courtes
    df_passes = df_passes.sort_values(by='Passes courtes', ascending=False)

    return df_passes

def players_dribbles(joueurs):
    players_dribbles = {}
    players_successful_dribbles = {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Dribble' in action:
            dribble_count = action.count('Dribble')
            players_dribbles[joueurs.iloc[i]['Row']] = players_dribbles.get(joueurs.iloc[i]['Row'], 0) + dribble_count

            is_successful = joueurs.iloc[i]['Dribble']
            if isinstance(is_successful, str) and 'Réussi' in is_successful:
                is_successful_count = is_successful.count('Réussi')
                players_successful_dribbles[joueurs.iloc[i]['Row']] = players_successful_dribbles.get(joueurs.iloc[i]['Row'], 0) + is_successful_count

    df_dribbles = pd.DataFrame({
        'Player': list(players_dribbles.keys()),
        'Dribbles': list(players_dribbles.values()),
        'Dribbles réussis': [players_successful_dribbles.get(player, 0) for player in players_dribbles]
    })

    df_dribbles['Pourcentage de dribbles réussis'] = (df_dribbles['Dribbles réussis'] / df_dribbles['Dribbles']) * 100

    df_dribbles = df_dribbles.sort_values(by='Dribbles', ascending=False)

    return df_dribbles

def players_defensive_duels(joueurs):
    players_defensive_duels = {}
    players_successful_defensive_duels = {}
    players_faults = {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Duel défensif' in action:
            defensive_duels_count = action.count('Duel défensif')
            players_defensive_duels[joueurs.iloc[i]['Row']] = players_defensive_duels.get(joueurs.iloc[i]['Row'], 0) + defensive_duels_count

            is_successful = joueurs.iloc[i]['Duel défensifs']
            if isinstance(is_successful, str) and 'Gagné' in is_successful:
                is_successful_count = is_successful.count('Gagné')
                players_successful_defensive_duels[joueurs.iloc[i]['Row']] = players_successful_defensive_duels.get(joueurs.iloc[i]['Row'], 0) + is_successful_count
            
            is_fault = joueurs.iloc[i]['Duel défensifs']
            if isinstance(is_fault, str) and 'Faute' in is_fault:
                is_fault_count = is_fault.count('Faute')
                players_faults[joueurs.iloc[i]['Row']] = players_faults.get(joueurs.iloc[i]['Row'], 0) + is_fault_count

    df_duels_defensifs = pd.DataFrame({
        'Player': list(players_defensive_duels.keys()),
        'Duels défensifs': list(players_defensive_duels.values()),
        'Duels défensifs gagnés': [players_successful_defensive_duels.get(player, 0) for player in players_defensive_duels],
        'Fautes': [players_faults.get(player, 0) for player in players_defensive_duels]
    })

    df_duels_defensifs['Pourcentage de duels défensifs gagnés'] = (df_duels_defensifs['Duels défensifs gagnés'] / df_duels_defensifs['Duels défensifs']) * 100

    df_duels_defensifs = df_duels_defensifs.sort_values(by='Duels défensifs', ascending=False)

    return df_duels_defensifs

def players_interceptions(joueurs):
    players_interceptions = {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Interception' in action:
            interception_count = action.count('Interception')
            players_interceptions[joueurs.iloc[i]['Row']] = players_interceptions.get(joueurs.iloc[i]['Row'], 0) + interception_count

    df_interceptions = pd.DataFrame({
        'Player': list(players_interceptions.keys()),
        'Interceptions': list(players_interceptions.values())
    })

    df_interceptions = df_interceptions.sort_values(by='Interceptions', ascending=False)

    return df_interceptions

def players_ball_losses(joueurs):
    players_ball_losses = {}

    for i in range(len(joueurs)):
        action = joueurs.iloc[i]['Action']
        if isinstance(action, str) and 'Perte de balle' in action:
            ball_losses_count = action.count('Perte de balle')
            players_ball_losses[joueurs.iloc[i]['Row']] = players_ball_losses.get(joueurs.iloc[i]['Row'], 0) + ball_losses_count

    df_pertes_de_balle = pd.DataFrame({
        'Player': list(players_ball_losses.keys()),
        'Pertes de balle': list(players_ball_losses.values())
    })

    df_pertes_de_balle = df_pertes_de_balle.sort_values(by='Pertes de balle', ascending=False)

    return df_pertes_de_balle

def create_data(match, joueurs, is_edf):
    if is_edf:
        df_duration = players_edf_duration(match)
    else:   
        df_duration = players_duration(match)
    df_tirs = players_shots(joueurs)
    df_passes = players_passes(joueurs)
    df_dribbles = players_dribbles(joueurs)
    df_duels_defensifs = players_defensive_duels(joueurs)
    df_interceptions = players_interceptions(joueurs)
    df_pertes_de_balle = players_ball_losses(joueurs)

    # Nettoyage des noms de joueurs avant le merge
    dfs = [df_duration, df_tirs, df_passes, df_dribbles, df_duels_defensifs, df_interceptions, df_pertes_de_balle]
    for df in dfs:
        df['Player'] = df['Player'].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # Fusionner les dataframes
    df = df_duration.merge(df_tirs, on='Player', how='outer')
    df = df.merge(df_passes, on='Player', how='outer')
    df = df.merge(df_dribbles, on='Player', how='outer')
    df = df.merge(df_duels_defensifs, on='Player', how='outer')
    df = df.merge(df_interceptions, on='Player', how='outer')
    df = df.merge(df_pertes_de_balle, on='Player', how='outer')

    df.fillna(0, inplace=True)

    # Supprime les joueurs qui ont 0 dans toutes les colonnes (hormis celle du temps de jeu)
    df = df[(df.Tirs != 0) | (df['Tirs cadrés'] != 0) | (df.Buts != 0) | (df.Passes != 0) | (df['Passes réussies'] != 0) | (df['Pourcentage de passes réussies'] != 0) | (df.Dribbles != 0) | (df['Dribbles réussis'] != 0) | (df['Pourcentage de dribbles réussis'] != 0) | (df['Duels défensifs'] != 0) | (df['Duels défensifs gagnés'] != 0) | (df['Pourcentage de duels défensifs gagnés'] != 0) | (df.Interceptions != 0) | (df['Pertes de balle'] != 0)]

    # Supprime les joueurs qui ont joué moins de 10 minutes
    df = df[df['Temps de jeu (en minutes)'] >= 10]

    return df

def create_metrics(df):
    df['Timing'] = np.where((df['Duels défensifs'] > 0) & (df['Duels défensifs'].sum() - df['Fautes'].sum() > 0), (df['Duels défensifs'] - df['Fautes']) / (df['Duels défensifs'].sum() - df['Fautes'].sum()), 0)
    df['Force physique'] = np.where((df['Duels défensifs'] > 0) & (df['Duels défensifs gagnés'].sum() > 0), (df['Duels défensifs gagnés'] / df['Duels défensifs']) / (df['Duels défensifs gagnés'].sum() / df['Duels défensifs'].sum()), 0)
    df['Intelligence tactique'] = np.where(df['Interceptions'].sum() > 0, df['Interceptions'] / df['Interceptions'].sum(), 0)
    df['Technique 1'] = np.where(df['Passes'].sum() > 0, df['Passes'] / df['Passes'].sum(), 0)
    df['Technique 2'] = np.where((df['Passes courtes'] > 0) & (df['Passes réussies (courtes)'].sum() > 0), (df['Passes réussies (courtes)'] / df['Passes courtes']) / (df['Passes réussies (courtes)'].sum() / df['Passes courtes'].sum()), 0)
    df['Technique 3'] = np.where((df['Passes longues'] > 0) & (df['Passes réussies (longues)'].sum() > 0), (df['Passes réussies (longues)'] / df['Passes longues']) / (df['Passes réussies (longues)'].sum() / df['Passes longues'].sum()), 0)
    df['Explosivité'] = np.where((df['Dribbles'] > 0) & (df['Dribbles réussis'].sum() > 0), (df['Dribbles réussis'] / df['Dribbles']) / (df['Dribbles réussis'].sum() / df['Dribbles'].sum()), 0)
    df['Prise de risque'] = np.where(df['Dribbles'].sum() > 0, df['Dribbles'] / df['Dribbles'].sum(), 0)
    # df['Créativité 1'] = TODO
    # df['Créativité 2'] = TODO
    # df['Prise de décision'] = TODO
    df['Précision'] = np.where((df['Tirs'] > 0) & (df['Tirs cadrés'].sum() > 0), (df['Tirs cadrés'] / df['Tirs']) / (df['Tirs cadrés'].sum() / df['Tirs'].sum()), 0)
    df['Sang-froid'] = np.where(df['Tirs'].sum() > 0, df['Tirs'] / df['Tirs'].sum(), 0)

    # Convertir les métriques en percentiles (de 0 à 100)
    metrics = ['Timing', 'Force physique', 'Intelligence tactique', 'Technique 1', 'Technique 2', 'Technique 3', 'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']

    for metric in metrics:
        df[metric] = (df[metric].rank(pct=True, method='average') * 100).fillna(0)

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

def create_individual_radar(df):
    # Filtering and sorting which columns to use in radar
    columns_to_plot = ['Timing', 'Force physique', 'Intelligence tactique', 'Technique 1', 'Technique 2', 'Technique 3', 'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']

    # Liste des couleurs rendues plus vives
    colors = [
        '#6A7CD9',  # Plus vif que #D9DEF7
        '#6A7CD9',  # Même couleur pour maintenir la cohérence
        '#00BFFE',  # Plus vif que #C3F0FE
        '#FF9470',  # Plus vif que #FEE9DD
        '#FF9470',  # Même couleur pour maintenir la cohérence
        '#FF9470',  # Même couleur pour maintenir la cohérence
        '#F27979',  # Plus vif que #F5D6D6
        '#F27979',  # Même couleur pour maintenir la cohérence
        '#BFBFBF',  # Plus vif que #D9D9D9
        '#BFBFBF'   # Même couleur pour maintenir la cohérence
    ]

    player = df.iloc[0]

    # Setup the pizza plot
    pizza = PyPizza(
        params=columns_to_plot,
        background_color='#0e1117',
        straight_line_color='#FFFFFF',
        straight_line_lw=1,
        last_circle_color='#FFFFFF',
        last_circle_lw=2,
        other_circle_color='#FFFFFF',
        other_circle_ls='-',
        other_circle_lw=0
    )

    # Create the figure and plot
    fig, _ = pizza.make_pizza(
        figsize=(8, 8),  # Taille de la figure
        values=[player[col] for col in columns_to_plot],  # Utiliser les valeurs pour chaque colonne
        slice_colors=colors,  # Couleurs des tranches
        kwargs_values=dict(
            color='#FFFFFF', fontsize=9, fontproperties='serif', 
            bbox={
                'edgecolor': '#FFFFFF',
                'facecolor': '#0e1117',    
                "boxstyle": 'round, pad= .2',
                "lw": 1
            }
        ),

        kwargs_params=dict(
            color='#FFFFFF',  # Blanc pour les labels
            fontsize=10,  # Taille de la police des labels
            fontproperties='monospace'  # Police customisée
        )
    )

    # Mettre à jour la couleur de fond de la figure
    fig.set_facecolor('#0e1117')

    return fig

def create_comparison_radar(df):
    # Sélectionner les métriques en fonction de la position du joueur
    metrics = ['Timing', 'Force physique', 'Intelligence tactique', 'Technique 1', 'Technique 2', 'Technique 3', 'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']

    # Les limites inférieures et supérieures pour les statistiques
    low = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    high = (100, 100, 100, 100, 100, 100, 100, 100, 100, 100)

    radar = Radar(metrics, low, high,
                  round_int=[True] * len(metrics),
                  num_rings=4,
                  ring_width=1, center_circle_radius=1)

    # Charger les polices
    URL = 'https://raw.githubusercontent.com/googlefonts/roboto/main/src/hinted/Roboto-Thin.ttf'
    robotto_thin = FontManager(URL)

    URL2 = ('https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/'
            'RobotoSlab%5Bwght%5D.ttf')
    robotto_bold = FontManager(URL2)

    # Créer la figure
    fig, axs = grid(figheight=14, grid_height=0.915, title_height=0.06, endnote_height=0.025,
                    title_space=0, endnote_space=0, grid_key='radar', axis=False)

    # Préparer le radar
    radar.setup_axis(ax=axs['radar'], facecolor='None')
    radar.draw_circles(ax=axs['radar'], facecolor='#28252c', edgecolor='#39353f', lw=1.5)
    
    # Extraire les valeurs du joueur et les aplatir pour correspondre au radar
    player_values_1 = df.iloc[0][metrics].values.flatten()
    player_values_2 = df.iloc[1][metrics].values.flatten()

    # Dessiner le radar
    radar.draw_radar_compare(player_values_1, player_values_2, ax=axs['radar'],
                             kwargs_radar={'facecolor': '#00f2c1', 'alpha': 0.6},
                             kwargs_compare={'facecolor': '#d80499', 'alpha': 0.6})
    radar.draw_range_labels(ax=axs['radar'], fontsize=25, color='#fcfcfc',
                            fontproperties=robotto_thin.prop)
    radar.draw_param_labels(ax=axs['radar'], fontsize=25, color='#fcfcfc',
                            fontproperties=robotto_thin.prop)

    # Ajouter le titre et les détails du joueur
    axs['title'].text(0.01, 0.65, f"{df.iloc[0]['Player']}", fontsize=25, color='#01c49d',
                                fontproperties=robotto_bold.prop, ha='left', va='center')
    axs['title'].text(0.99, 0.65, f"{df.iloc[1]['Player']}", fontsize=25,
                                fontproperties=robotto_bold.prop, ha='right', va='center', color='#d80499')

    # Mettre à jour la couleur de fond de la figure
    fig.set_facecolor('#0e1117')

    return fig

@st.cache_data
def collect_data():
    download_google_drive()

    pfc_kpi = pd.DataFrame()

    for filename in os.listdir('data'):

        if filename.endswith('.xlsx'):
            print(f'\'{filename}\' : Récupération des statistiques en cours...')
            
            # Charger un fichier Excel
            edf = pd.read_excel(f'data/{filename}')

            unique_matches = edf['Match'].unique()

            for match in unique_matches:
                # Création d'un dataframe pour le match en cours
                globals()[match] = edf[edf['Match'] == match]

            match_1 = pd.read_csv("data/EDF_U19_Match1.csv")
            match_2 = pd.read_csv("data/EDF_U19_Match2.csv")
            match_3 = pd.read_csv("data/EDF_U19_Match3.csv")

            matchs = [match_1, match_2, match_3]

            edf_kpi = pd.DataFrame()

            for i in range (len(matchs)):
                df = create_data(globals()[f'Match {i + 1}'], matchs[i], True)

                for index, row in df.iterrows():
                    time_played = row['Temps de jeu (en minutes)']
                    for col in df.columns:
                            if col not in ['Player', 'Temps de jeu (en minutes)', 'Buts'] and 'Pourcentage' not in col:
                                df.loc[index, col] = row[col] * (90 / time_played)

                df = create_metrics(df)

                # Ajouter la colonne 'Poste' en associant chaque joueuse à son poste depuis le fichier EDF_Joueuses.xlsx
                df = df.merge(edf[['Player', 'Poste']], on='Player', how='left')

                # Réorganiser les colonnes pour que 'Poste' soit en deuxième position
                cols = ['Player', 'Poste'] + [col for col in df.columns if col not in ['Player', 'Poste']]
                df = df[cols]

                edf_kpi = pd.concat([edf_kpi, df])

            print(f'\'{filename}\' : Récupération des statistiques terminée.\n')
                
            edf_kpi = edf_kpi.groupby('Poste').mean(numeric_only=True).reset_index()

            # Remove the 'Temps de jeu (en minutes)' column
            edf_kpi = edf_kpi.drop(columns='Temps de jeu (en minutes)')

            # Rename mispelled positions
            edf_kpi['Poste'] = edf_kpi['Poste'].replace({
                'Milieux axiale': 'Milieu axiale',
                'Milieux offensive': 'Milieu offensive'
            })

            # Add "(EDF)" to the player names
            edf_kpi['Poste'] = edf_kpi['Poste'] + ' moyenne (EDF)'

            print('Collecte des données EDF terminée.\n')
        
        elif filename.endswith('.csv'):
            if 'PFC' not in filename:
                continue
            
            # Charger un fichier CSV
            data = pd.read_csv(f'data/{filename}')

            # On supprime l'extension du fichier
            base_filename = filename.split('.')[0]
            print(base_filename)
            
            # On découpe le nom du fichier en parties
            parts = base_filename.split('_')
            
            # Récupération des informations
            equipe_domicile = parts[0]

            equipe_exterieur = parts[2]
            journee = parts[3]
            categorie = parts[4]
            date = parts[5]

            match = pd.DataFrame()
            joueurs = pd.DataFrame()

            print(f'\'{filename}\' : Récupération des statistiques en cours...')

            for i in range (len(data)):
                if data['Row'].iloc[i] == equipe_domicile or data['Row'].iloc[i] == equipe_exterieur:
                    match = pd.concat([match, data.iloc[i:i+1]], ignore_index=True)
                elif not('Corner' in data['Row'].iloc[i] or 'Coup-franc' in data['Row'].iloc[i] or 'Penalty' in data['Row'].iloc[i] or 'Carton' in data['Row'].iloc[i]):
                    joueurs = pd.concat([joueurs, data.iloc[i:i+1]], ignore_index=True)

            if len(joueurs) > 0:
                df = create_data(match, joueurs, False)

                for index, row in df.iterrows():
                    time_played = row['Temps de jeu (en minutes)']
                    for col in df.columns:
                            if col not in ['Player', 'Temps de jeu (en minutes)', 'Buts'] and 'Pourcentage' not in col:
                                df.loc[index, col] = row[col] * (90 / time_played)

                df = create_metrics(df)

                df = create_kpis(df)

                df = create_poste(df) 

                if equipe_domicile == 'PFC':
                    adversaire = equipe_exterieur
                else:
                    adversaire = equipe_domicile

                df.insert(1, 'Adversaire', f'{adversaire} - {journee}')
                df.insert(2, 'Journée', journee)
                df.insert(3, 'Catégorie', categorie)
                df.insert(4, 'Date', date)

                pfc_kpi = pd.concat([pfc_kpi, df])

                print(f'\'{filename}\' : Récupération des statistiques terminée.\n')
            
            else:
                print(f'\'{filename}\' : Aucune statistique à récupérer.\n')

    print('Collecte des données PFC terminée.')

    return pfc_kpi, edf_kpi

def script_streamlit(pfc_kpi, edf_kpi):
    # Chemin du logo (peut être une URL ou un fichier local)
    logo_pfc = "https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"

    # Affichage du logo
    st.sidebar.markdown(
        f"""
        <div style="display: flex; justify-content: center;">
            <img src="{logo_pfc}" width="100">
        </div>
        """,
        unsafe_allow_html=True
    )

    with st.sidebar:
        page = option_menu(
            menu_title="",
            options=["Statistiques", "Comparaison"],
            icons=["graph-up-arrow", "people"],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={
                "container": {"padding": "5!important", "background-color": "transparent"},
                "icon": {"font-size": "18px"},
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin":"0px",
                    "--hover-color": "#0E1117"
                },
                "nav-link-selected": {
                    "background-color": "#0E1117",
                    "color": "#ecebe3",
                    "font-weight": "bold"
                }
            }
        )

    # URL du logo
    logo_certifié_paris = "https://i.postimg.cc/2SZj5JdZ/Certifie-Paris-Blanc.png"

    # Affichage du logo toujours en bas de la barre latérale
    st.sidebar.markdown(
        f"""
        <div style="display: flex; flex-direction: column; height: 100vh; justify-content: space-between;">
            <div></div>  <!-- Cette div pousse le logo vers le bas -->
            <div style="text-align: center; margin-bottom: 300px;">
                <img src="{logo_certifié_paris}" width="200">
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    if page == "Statistiques":
        st.header("Statistiques")

        st.subheader("Sélectionnez une joueuse du Paris FC")

        # Select a player
        player = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique())

        # Filter the player
        player_data = pfc_kpi[pfc_kpi['Player'] == player]

        # Select games
        game = st.multiselect("Choisissez un ou plusieurs matchs", player_data['Adversaire'].unique())

        # Filter the games
        player_data = player_data[player_data['Adversaire'].isin(game)]
        
        # Group by player
        player_data = player_data.groupby('Player').agg({
            'Temps de jeu (en minutes)': 'sum',  # Somme des temps de jeu
            'Buts': 'sum',                       # Somme des buts
        }).join(
            player_data.groupby('Player').mean(numeric_only=True).drop(columns=['Temps de jeu (en minutes)', 'Buts'])
        ).round().astype(int).reset_index()
        
        if (len(game) == 0):
            st.error("Veuillez sélectionner au moins un match.")
        else:
            time_played, goals = st.columns(2)

            with time_played:
                st.metric("Temps de jeu", f"{player_data['Temps de jeu (en minutes)'].iloc[0]} minutes")

            with goals:
                st.metric("Buts", f"{player_data['Buts'].iloc[0]}")

            # Crée des onglets
            tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])

            with tab1:
                fig = create_individual_radar(player_data)
                st.pyplot(fig)

            with tab2:
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Rigueur", f"{player_data['Rigueur'].iloc[0]}/100")
                
                with col2:
                    st.metric("Récupération", f"{player_data['Récupération'].iloc[0]}/100")

                with col3:
                    st.metric("Distribution", f"{player_data['Distribution'].iloc[0]}/100")

                with col4:
                    st.metric("Percussion", f"{player_data['Percussion'].iloc[0]}/100")

                with col5:    
                    st.metric("Finition", f"{player_data['Finition'].iloc[0]}/100")
            
            with tab3:
                col1, col2, col3, col4, col5, col6 = st.columns(6)

                with col1:
                    st.metric("Défenseur central", f"{player_data['Défenseur central'].iloc[0]}/100")

                with col2:
                    st.metric("Défenseur latéral", f"{player_data['Défenseur latéral'].iloc[0]}/100")

                with col3:
                    st.metric("Milieu défensif", f"{player_data['Milieu défensif'].iloc[0]}/100")

                with col4:
                    st.metric("Milieu relayeur", f"{player_data['Milieu relayeur'].iloc[0]}/100")

                with col5:
                    st.metric("Milieu offensif", f"{player_data['Milieu offensif'].iloc[0]}/100")

                with col6:
                    st.metric("Attaquant", f"{player_data['Attaquant'].iloc[0]}/100")

    elif page == "Comparaison":
        st.header("Comparaison")

        st.subheader("Sélectionnez une joueuse du Paris FC")

        # Select a player
        player1 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_1')

        # Filter the player
        player1_data = pfc_kpi[pfc_kpi['Player'] == player1]

        # Select games
        game1 = st.multiselect("Choisissez un ou plusieurs matchs", player1_data['Adversaire'].unique(), key='games_1')

        # Filter the games
        player1_data = player1_data[player1_data['Adversaire'].isin(game1)]

        # Group by player
        player1_data = player1_data.groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()

        tab1, tab2 = st.tabs(["Comparaison (PFC)", "Comparaison (EDF)"])
        
        with tab1:
            st.subheader("Sélectionnez une autre joueuse du Paris FC")

            # Select a player
            player2 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_2_pfc')

            # Filter the player
            player2_data = pfc_kpi[pfc_kpi['Player'] == player2]

            # Select games
            game2 = st.multiselect("Choisissez un ou plusieurs matchs", player2_data['Adversaire'].unique(), key='games_2_pfc')

            # Filter the games
            player2_data = player2_data[player2_data['Adversaire'].isin(game2)]

            # Group by player
            player2_data = player2_data.groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()

            if st.button("Afficher le radar", key='button_pfc'):
                if (len(game1) == 0 or len(game2) == 0):
                    st.error("Veuillez sélectionner au moins un match pour chaque joueur.")
                else:
                    players_data = pd.concat([player1_data, player2_data])
                    fig = create_comparison_radar(players_data)
                    st.pyplot(fig)

        with tab2:
            st.subheader("Sélectionnez un poste de l'Équipe de France")

            # Select a position to compare
            player2 = st.selectbox("Choisissez un poste de comparaison", edf_kpi['Poste'].unique(), key='player_2_edf')

            # Filter the player
            player2_data = edf_kpi[edf_kpi['Poste'] == player2]

            # Rename 'Poste' to 'Player' for consistency
            player2_data.rename(columns={'Poste': 'Player'}, inplace=True)

            if st.button("Afficher le radar", key='button_edf'):
                if (len(game1) == 0):
                    st.error("Veuillez sélectionner au moins un match.")
                else:
                    players_data = pd.concat([player1_data, player2_data])
                    fig = create_comparison_radar(players_data)
                    st.pyplot(fig)

if __name__ == '__main__':
    st.set_page_config(
        page_title="Paris Football Club",
        page_icon="https://i.postimg.cc/J4vyzjXG/Logo-Paris-FC.png"
    )

    # Title of the app
    st.title("Paris Football Club")

    logo_monochrome = "https://i.postimg.cc/BQQ5K5tp/Monochrome.png"

    # CSS pour placer le logo en haut à droite
    st.markdown(
        f"""
        <style>
            .logo-container {{
                position: absolute;
                top: -100px;
                right: 10px;
            }}
            .logo-container img {{
                width: 90px;
            }}
        </style>
        <div class="logo-container">
            <img src="{logo_monochrome}">
        </div>
        """,
        unsafe_allow_html=True
    )

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        with st.form("login_form"):
            password = st.text_input("Mot de passe", type="password")
            submitted = st.form_submit_button("Valider")

            if submitted:
                if password == st.secrets["password"]["password"]:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Mot de passe incorrect")

    if st.session_state.authenticated:
        pfc_kpi, edf_kpi = collect_data()
        script_streamlit(pfc_kpi, edf_kpi)



