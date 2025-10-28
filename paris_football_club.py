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
        nom = nom.strip().upper()
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
                profile = row['Profil']
                permissions[profile] = {
                    "password": row['Mot de passe'],
                    "permissions": row['Permissions'].split(',') if isinstance(row['Permissions'], str) else [],
                    "player": row.get('Joueuse', None)
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

# --- Fonctions de chargement et traitement des données ---
@st.cache_data
def charger_donnees():
    """Charge les données locales."""
    data_folder = "data"
    if not os.path.exists(data_folder):
        st.error(f"Le dossier '{data_folder}' n'existe pas.")
        return {}, {}
    fichiers = [f for f in os.listdir(data_folder) if f.endswith(('.csv', '.xlsx')) and f != "Classeurs permissions streamlit.xlsx"]
    if not fichiers:
        st.warning(f"Aucun fichier de données trouvé dans '{data_folder}'.")
        return {}, {}
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

# [Les autres fonctions de traitement des données restent identiques...]

# --- Fonction pour filtrer les données par joueuse ---
def filter_data_by_player(df, player_name):
    """Filtre les données pour une joueuse spécifique."""
    if not player_name or df.empty:
        return df
    player_name_clean = nettoyer_nom_joueuse(player_name)
    df['Player_clean'] = df['Player'].apply(nettoyer_nom_joueuse)
    filtered_df = df[df['Player_clean'] == player_name_clean].copy()
    filtered_df.drop(columns=['Player_clean'], inplace=True, errors='ignore')
    return filtered_df

# --- Fonction principale de collecte des données ---
@st.cache_data
def collect_data():
    """Collecte et traite les données."""
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
        df_dict = charger_donnees()

        # Traiter les fichiers EDF
        for filename, df in df_dict.items():
            if filename.endswith('.xlsx') and 'EDF' in filename:
                print(f"Traitement du fichier Excel : {filename}")
                edf = df
                matchs_csv = [f for f in fichiers if f.startswith('EDF_U19_Match') and f.endswith('.csv')]
                matchs = []
                for csv_file in matchs_csv:
                    try:
                        matchs.append(pd.read_csv(os.path.join(data_folder, csv_file)))
                    except Exception as e:
                        st.error(f"Erreur lors de la lecture du fichier {csv_file}: {e}")

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
                data = df
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

        return pfc_kpi, edf_kpi

    except Exception as e:
        st.error(f"Erreur lors de la collecte des données: {e}")
        return pd.DataFrame(), pd.DataFrame()

# [Les fonctions create_metrics, create_kpis, create_poste, create_individual_radar et create_comparison_radar restent identiques]

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
    if player_name:
        pfc_kpi = filter_data_by_player(pfc_kpi, player_name)

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
        # [Le code de la page Statistiques reste identique]
        pass

    elif page == "Comparaison":
        # [Le code de la page Comparaison reste identique]
        pass

    elif page == "Gestion":
        # [Le code de la page Gestion reste identique]
        pass

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
