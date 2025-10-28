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
    """Nettoie le nom d'une joueuse en supprimant les doublons et standardisant le format."""
    if isinstance(nom, str):
        # Supprimer les espaces en trop et mettre en majuscules
        nom = nom.strip().upper()
        # Supprimer les doublons (ex: "NOM, NOM" -> "NOM")
        parts = [part.strip().upper() for part in nom.split(",")]
        if len(parts) > 1 and parts[0] == parts[1]:
            return parts[0]
        # Supprimer les accents et caractères spéciaux
        nom = nom.replace("É", "E").replace("È", "E").replace("Ê", "E").replace("À", "A")
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
    try:
        service = authenticate_google_drive()
        folder_id = "1wXIqggriTHD9NIx8U89XmtlbZqNWniGD"
        permissions_path = download_permissions_file(service, folder_id)
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

# --- Gestion des profils et permissions ---
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

# --- Fonctions de traitement des données ---
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

    # Vérifier que toutes les colonnes de poste existent
    available_posts = [poste for poste in list_of_players if poste in match.columns]
    if not available_posts:
        st.warning("Aucune colonne de poste disponible pour calculer la durée de jeu")
        return pd.DataFrame()

    for i in range(len(match)):
        duration = match.iloc[i]['Duration']
        for poste in available_posts:
            if poste in match.columns:
                player = nettoyer_nom_joueuse(str(match.iloc[i][poste]))
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

# [Les autres fonctions de traitement des données restent similaires avec gestion des erreurs]

def create_metrics(df):
    """Crée les métriques à partir des données brutes."""
    if df.empty:
        return df

    metrics = ['Timing', 'Force physique', 'Intelligence tactique',
               'Technique 1', 'Technique 2', 'Technique 3',
               'Explosivité', 'Prise de risque', 'Précision', 'Sang-froid']

    # Calculer uniquement les métriques pour lesquelles on a les données
    if 'Duels défensifs' in df.columns and 'Fautes' in df.columns:
        df['Timing'] = np.where(df['Duels défensifs'] > 0,
                               (df['Duels défensifs'] - df['Fautes']) / df['Duels défensifs'], 0)
        df['Force physique'] = np.where(df['Duels défensifs'] > 0,
                                       df.get('Duels défensifs gagnés', 0) / df['Duels défensifs'], 0)

    if 'Interceptions' in df.columns:
        df['Intelligence tactique'] = np.where(df['Interceptions'] > 0,
                                             df['Interceptions'] / df['Interceptions'].max(), 0)

    if 'Passes' in df.columns:
        df['Technique 1'] = np.where(df['Passes'] > 0,
                                    df['Passes'] / df['Passes'].max(), 0)

    if 'Passes courtes' in df.columns and 'Passes réussies (courtes)' in df.columns:
        df['Technique 2'] = np.where(df['Passes courtes'] > 0,
                                    df['Passes réussies (courtes)'] / df['Passes courtes'], 0)

    if 'Passes longues' in df.columns and 'Passes réussies (longues)' in df.columns:
        df['Technique 3'] = np.where(df['Passes longues'] > 0,
                                    df['Passes réussies (longues)'] / df['Passes longues'], 0)

    if 'Dribbles' in df.columns:
        df['Explosivité'] = np.where(df['Dribbles'] > 0,
                                   df.get('Dribbles réussis', 0) / df['Dribbles'], 0)
        df['Prise de risque'] = np.where(df['Dribbles'] > 0,
                                       df['Dribbles'] / df['Dribbles'].max(), 0)

    if 'Tirs' in df.columns:
        df['Précision'] = np.where(df['Tirs'] > 0,
                                  df.get('Tirs cadrés', 0) / df['Tirs'], 0)
        df['Sang-froid'] = np.where(df['Tirs'] > 0,
                                   df['Tirs'] / df['Tirs'].max(), 0)

    # Calculer les percentiles uniquement pour les métriques qui existent
    for metric in metrics:
        if metric in df.columns:
            df[metric] = (df[metric].rank(pct=True) * 100).fillna(0)

    return df

def create_kpis(df):
    """Crée les KPIs à partir des métriques."""
    if df.empty:
        return df

    # Calculer uniquement les KPIs pour lesquels on a les métriques nécessaires
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

        # Appeler toutes les fonctions de calcul avec gestion des erreurs
        dfs = [df_duration]

        try:
            dfs.append(players_shots(joueurs))
        except Exception as e:
            st.warning(f"Erreur lors du calcul des tirs: {e}")

        try:
            dfs.append(players_passes(joueurs))
        except Exception as e:
            st.warning(f"Erreur lors du calcul des passes: {e}")

        try:
            dfs.append(players_dribbles(joueurs))
        except Exception as e:
            st.warning(f"Erreur lors du calcul des dribbles: {e}")

        try:
            dfs.append(players_defensive_duels(joueurs))
        except Exception as e:
            st.warning(f"Erreur lors du calcul des duels défensifs: {e}")

        try:
            dfs.append(players_interceptions(joueurs))
        except Exception as e:
            st.warning(f"Erreur lors du calcul des interceptions: {e}")

        try:
            dfs.append(players_ball_losses(joueurs))
        except Exception as e:
            st.warning(f"Erreur lors du calcul des pertes de balle: {e}")

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

# --- Fonction pour filtrer les données par joueuse ---
def filter_data_by_player(df, player_name):
    """Filtre les données pour une joueuse spécifique."""
    if not player_name or df.empty or 'Player' not in df.columns:
        return df

    player_name_clean = nettoyer_nom_joueuse(player_name)
    df['Player_clean'] = df['Player'].apply(nettoyer_nom_joueuse)
    filtered_df = df[df['Player_clean'] == player_name_clean].copy()
    filtered_df.drop(columns=['Player_clean'], inplace=True, errors='ignore')
    return filtered_df

# --- Fonction principale de collecte des données ---
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

        # Charger les fichiers de données
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

# --- Interface Streamlit avec gestion des permissions ---
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
        # Filtrer les données pour n'afficher que celles de la joueuse connectée
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)
        # Vérifier que des données existent après filtrage
        if pfc_kpi.empty:
            st.warning(f"Aucune donnée disponible pour la joueuse {player_name}")

    # Déterminer les options disponibles en fonction des permissions
    available_options = ["Statistiques"]

    # Les joueuses peuvent accéder à la comparaison (mais uniquement avec elles-mêmes ou EDF)
    if check_permission(user_profile, "compare_players", permissions) or check_permission(user_profile, "all", permissions) or player_name:
        available_options.append("Comparaison")

    # Option "Gestion" réservée aux admins
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

    # --- Pages ---
    if page == "Statistiques":
        st.header("Statistiques")

        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible pour votre profil.")
        else:
            if player_name:
                # Pour une joueuse, affichage direct de ses statistiques
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
                # Pour les profils admin/coach (accès à toutes les joueuses)
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
            # Pour une joueuse: comparaison avec elle-même ou avec EDF
            st.subheader(f"Comparaison pour {player_name}")

            if pfc_kpi.empty:
                st.warning(f"Aucune donnée disponible pour {player_name}.")
            else:
                st.write("### Comparaison de vos performances sur différents matchs")

                if 'Adversaire' in pfc_kpi.columns:
                    unique_matches = pfc_kpi['Adversaire'].unique()
                    if len(unique_matches) >= 2:
                        match1, match2 = st.columns(2)
                        with match1:
                            game1 = st.selectbox("Sélectionnez le premier match", unique_matches, key='game1')
                        with match2:
                            game2 = st.selectbox("Sélectionnez le deuxième match", unique_matches, key='game2')

                        if game1 and game2 and game1 != game2:
                            data1 = pfc_kpi[pfc_kpi['Adversaire'] == game1]
                            data2 = pfc_kpi[pfc_kpi['Adversaire'] == game2]

                            if not data1.empty and not data2.empty:
                                data1 = data1.copy()
                                data2 = data2.copy()
                                data1['Player'] = f"{player_name} ({game1})"
                                data2['Player'] = f"{player_name} ({game2})"

                                players_data = pd.concat([data1, data2])
                                fig = create_comparison_radar(players_data)
                                if fig:
                                    st.pyplot(fig)
                            else:
                                st.warning("Aucune donnée disponible pour les matchs sélectionnés.")
                        else:
                            st.warning("Veuillez sélectionner deux matchs différents.")
                    else:
                        st.warning("Pas assez de matchs disponibles pour la comparaison.")

                st.write("### Comparaison avec les données EDF")
                if not edf_kpi.empty and 'Poste' in edf_kpi.columns:
                    poste = st.selectbox("Sélectionnez un poste EDF pour comparaison",
                                       edf_kpi['Poste'].unique())
                    edf_data = edf_kpi[edf_kpi['Poste'] == poste].rename(columns={'Poste': 'Player'})

                    if not edf_data.empty:
                        player_data = pfc_kpi.copy()

                        if st.button("Comparer avec EDF"):
                            players_data = pd.concat([player_data, edf_data])
                            fig = create_comparison_radar(players_data)
                            if fig:
                                st.pyplot(fig)
                    else:
                        st.warning("Aucune donnée EDF disponible pour ce poste.")
                else:
                    st.warning("Aucune donnée EDF disponible.")
        else:
            # Pour les profils admin/coach: comparaison normale entre joueuses
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
    try:
        pfc_kpi, edf_kpi = collect_data()
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {e}")
        pfc_kpi, edf_kpi = pd.DataFrame(), pd.DataFrame()

    # Affichage de l'interface
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)
