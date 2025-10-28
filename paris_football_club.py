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
# FONCTIONS DE TRAITEMENT DES DONNÉES
# =============================================

def nettoyer_nom_joueuse(nom):
    """Nettoie le nom d'une joueuse en supprimant les doublons et standardisant le format."""
    if isinstance(nom, str):
        nom = nom.strip().upper()
        # Remplacer les caractères spéciaux
        nom = nom.replace("É", "E").replace("È", "E").replace("Ê", "E").replace("À", "A").replace("Ù", "U")
        parts = [part.strip().upper() for part in nom.split(",")]
        if len(parts) > 1 and parts[0] == parts[1]:
            return parts[0]
        return nom
    return nom

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

# [Les autres fonctions de traitement des données restent similaires]

def create_metrics(df):
    """Crée les métriques à partir des données brutes."""
    if df.empty:
        return df

    # Vérifier les colonnes nécessaires
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
                                    (df[cols[0]] - df[cols[1]]) / df[cols[0]], 0)
            elif metric == 'Force physique':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df.get(cols[1], 0) / df[cols[0]], 0)
            elif metric == 'Intelligence tactique':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[0]] / df[cols[0]].max(), 0)
            elif metric == 'Technique 1':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[0]] / df[cols[0]].max(), 0)
            elif metric == 'Technique 2':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[1]] / df[cols[0]], 0)
            elif metric == 'Technique 3':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[1]] / df[cols[0]], 0)
            elif metric == 'Explosivité':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[1]] / df[cols[0]], 0)
            elif metric == 'Prise de risque':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[0]] / df[cols[0]].max(), 0)
            elif metric == 'Précision':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[1]] / df[cols[0]], 0)
            elif metric == 'Sang-froid':
                df[metric] = np.where(df[cols[0]] > 0,
                                    df[cols[0]] / df[cols[0]].max(), 0)

    # Calculer les percentiles pour les métriques qui existent
    for metric in required_cols.keys():
        if metric in df.columns:
            df[metric] = (df[metric].rank(pct=True) * 100).fillna(0)

    return df

def create_kpis(df):
    """Crée les KPIs à partir des métriques."""
    if df.empty:
        return df

    # Calculer les KPIs si les métriques nécessaires existent
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
        st.warning("Données insuffisantes pour calculer les notes par poste")
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
            df_duration = players_edf_duration(match)
        else:
            df_duration = players_duration(match)

        # Appeler toutes les fonctions de calcul
        dfs = [df_duration]

        # Fonctions de calcul des statistiques
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

        # Nettoyage des noms de joueurs avant le merge
        valid_dfs = []
        for df in dfs:
            if not df.empty and 'Player' in df.columns:
                df['Player'] = df['Player'].apply(nettoyer_nom_joueuse)
                valid_dfs.append(df)

        if not valid_dfs:
            return pd.DataFrame()

        # Fusionner les dataframes
        df = valid_dfs[0]
        for other_df in valid_dfs[1:]:
            df = df.merge(other_df, on='Player', how='outer')

        if not df.empty:
            df.fillna(0, inplace=True)
            df = df[(df.iloc[:, 1:] != 0).any(axis=1)]
            if 'Temps de jeu (en minutes)' in df.columns:
                df = df[df['Temps de jeu (en minutes)'] >= 10]

            # Appliquer les transformations métriques
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

# =============================================
# FONCTIONS DE COLLECTE DES DONNÉES
# =============================================

@st.cache_data
def collect_data():
    """Collecte et traite les données depuis Google Drive."""
    try:
        # Télécharger d'abord les données
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

        # Traiter les fichiers
        for filename in fichiers:
            path = os.path.join(data_folder, filename)
            try:
                if filename.endswith('.csv'):
                    data = pd.read_csv(path)
                else:
                    data = pd.read_excel(path)

                if filename.endswith('.xlsx') and 'EDF' in filename:
                    print(f"Traitement du fichier Excel EDF: {filename}")
                    edf = data
                    matchs_csv = [f for f in fichiers if f.startswith('EDF_U19_Match') and f.endswith('.csv')]

                    for csv_file in matchs_csv:
                        try:
                            match_data = pd.read_csv(os.path.join(data_folder, csv_file))
                            df = create_data(edf[edf['Match'] == f'Match {1}'], match_data, True)
                            if not df.empty:
                                for index, row in df.iterrows():
                                    time_played = row['Temps de jeu (en minutes)']
                                    for col in df.columns:
                                        if col not in ['Player', 'Temps de jeu (en minutes)', 'Buts'] and 'Pourcentage' not in col:
                                            df.loc[index, col] = row[col] * (90 / time_played)
                                df = create_metrics(df)
                                if 'Poste' in edf.columns:
                                    df = df.merge(edf[['Player', 'Poste']], on='Player', how='left')
                                cols = ['Player', 'Poste'] + [col for col in df.columns if col not in ['Player', 'Poste']]
                                df = df[cols]
                                edf_kpi = pd.concat([edf_kpi, df])
                        except Exception as e:
                            st.error(f"Erreur lors du traitement du fichier {csv_file}: {e}")

                    if not edf_kpi.empty and 'Poste' in edf_kpi.columns:
                        edf_kpi = edf_kpi.groupby('Poste').mean(numeric_only=True).reset_index()
                        edf_kpi = edf_kpi.drop(columns='Temps de jeu (en minutes)', errors='ignore')
                        edf_kpi['Poste'] = edf_kpi['Poste'].replace({
                            'Milieux axiale': 'Milieu axiale',
                            'Milieux offensive': 'Milieu offensive'
                        })
                        edf_kpi['Poste'] = edf_kpi['Poste'] + ' moyenne (EDF)'

                elif filename.endswith('.csv') and 'PFC' in filename:
                    print(f"Traitement du fichier CSV PFC: {filename}")
                    parts = filename.split('.')[0].split('_')
                    if len(parts) < 6:
                        st.warning(f"Le nom du fichier {filename} ne suit pas le format attendu.")
                        continue

                    try:
                        equipe_domicile = parts[0]
                        equipe_exterieur = parts[2]
                        journee = parts[3]
                        categorie = parts[4]
                        date = parts[5]

                        match, joueurs = pd.DataFrame(), pd.DataFrame()

                        for i in range(len(data)):
                            if data['Row'].iloc[i] in [equipe_domicile, equipe_exterieur]:
                                match = pd.concat([match, data.iloc[i:i+1]], ignore_index=True)
                            elif not any(str(x) in str(data['Row'].iloc[i]) for x in ['Corner', 'Coup-franc', 'Penalty', 'Carton']):
                                joueurs = pd.concat([joueurs, data.iloc[i:i+1]], ignore_index=True)

                        if not joueurs.empty:
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
                        st.error(f"Erreur lors du traitement du fichier {filename}: {e}")

            except Exception as e:
                st.error(f"Erreur lors du traitement du fichier {filename}: {e}")

        return pfc_kpi, edf_kpi

    except Exception as e:
        st.error(f"Erreur lors de la collecte des données: {e}")
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
            figsize=(8, 8),
            values=[player[col] for col in available_columns],
            slice_colors=colors[:len(available_columns)],
            kwargs_values=dict(
                color='#FFFFFF',
                fontsize=9,
                bbox=dict(edgecolor='#FFFFFF', facecolor='#0e1117', boxstyle='round, pad=0.2', lw=1)
            ),
            kwargs_params=dict(color='#FFFFFF', fontsize=10, fontproperties='monospace')
        )
        fig.set_facecolor('#0e1117')
        return fig
    except Exception as e:
        st.error(f"Erreur lors de la création du radar: {e}")
        return None

def create_comparison_radar(df):
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
            fontsize=25,
            color='#fcfcfc',
            fontproperties=robotto_thin.prop
        )
        radar.draw_param_labels(
            ax=axs['radar'],
            fontsize=25,
            color='#fcfcfc',
            fontproperties=robotto_thin.prop
        )

        axs['title'].text(
            0.01, 0.65,
            df.iloc[0]['Player'],
            fontsize=25,
            color='#01c49d',
            fontproperties=robotto_bold.prop,
            ha='left',
            va='center'
        )
        axs['title'].text(
            0.99, 0.65,
            df.iloc[1]['Player'],
            fontsize=25,
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

    # Récupérer le nom de la joueuse associée au profil
    player_name = get_player_for_profile(user_profile, permissions)
    st.sidebar.title(f"Connecté en tant que: {user_profile}")
    if player_name:
        st.sidebar.write(f"Joueuse associée: {player_name}")

    # Bouton de déconnexion
    if st.sidebar.button("🔒 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user_profile = None
        st.rerun()

    # Bouton de mise à jour des données (uniquement pour les profils autorisés)
    if check_permission(user_profile, "update_data", permissions) or check_permission(user_profile, "all", permissions):
        if st.sidebar.button("Mettre à jour la base de données"):
            with st.spinner("Mise à jour des données en cours..."):
                download_google_drive()
            st.success("✅ Mise à jour terminée")
            st.cache_data.clear()

    # Filtrer les données en fonction du profil
    if player_name and not pfc_kpi.empty and 'Player' in pfc_kpi.columns:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)
        if pfc_kpi.empty:
            st.warning(f"Aucune donnée disponible pour la joueuse {player_name}")

    # Déterminer les options disponibles en fonction des permissions
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

    # Pages de l'application
    if page == "Statistiques":
        # [Code pour la page Statistiques]
        pass

    elif page == "Comparaison":
        # [Code pour la page Comparaison]
        pass

    elif page == "Gestion":
        # [Code pour la page Gestion]
        pass

# =============================================
# POINT D'ENTRÉE PRINCIPAL
# =============================================

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
    try:
        pfc_kpi, edf_kpi = collect_data()
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {e}")
        pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

    # Affichage de l'interface
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)
