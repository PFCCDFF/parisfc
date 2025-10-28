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
    SCOPES = ['https://www.googleapis.com/auth/drive']
    service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

def download_file(service, file_id, file_name, output_folder):
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
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def download_permissions_file(service, folder_id):
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

# --- Gestion des profils et permissions ---
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

# --- Fonctions de traitement des données ---
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

# [Les autres fonctions de traitement des données restent identiques...]

# --- Fonction pour filtrer les données par joueuse ---
def filter_data_by_player(df, player_name):
    if not player_name:
        return df
    player_name_clean = nettoyer_nom_joueuse(player_name)
    df['Player_clean'] = df['Player'].apply(nettoyer_nom_joueuse)
    filtered_df = df[df['Player_clean'] == player_name_clean].copy()
    filtered_df.drop(columns=['Player_clean'], inplace=True, errors='ignore')
    return filtered_df

# --- Fonction principale de collecte des données ---
@st.cache_data
def collect_data():
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
    # [Le reste de la fonction collect_data reste inchangé...]
    return pfc_kpi, edf_kpi

# --- Interface Streamlit avec gestion des permissions ---
def script_streamlit(pfc_kpi, edf_kpi, permissions, user_profile):
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
        st.header("Statistiques")

        if pfc_kpi.empty:
            st.warning("Aucune donnée disponible pour votre profil.")
        else:
            if player_name:
                # Pour une joueuse, affichage direct de ses statistiques
                st.subheader(f"Statistiques pour {player_name}")

                if pfc_kpi.empty:
                    st.warning(f"Aucune donnée disponible pour {player_name}.")
                else:
                    # Affichage des données de la joueuse
                    player_data = pfc_kpi

                    # Afficher les matchs disponibles pour cette joueuse
                    if 'Adversaire' in player_data.columns:
                        unique_matches = player_data['Adversaire'].unique()
                        if len(unique_matches) > 0:
                            game = st.multiselect("Choisissez un ou plusieurs matchs", unique_matches)

                            if game:
                                filtered_data = player_data[player_data['Adversaire'].isin(game)]
                            else:
                                filtered_data = player_data

                            aggregated_data = filtered_data.groupby('Player').agg({
                                'Temps de jeu (en minutes)': 'sum',
                                'Buts': 'sum',
                            }).join(
                                filtered_data.groupby('Player').mean(numeric_only=True).drop(columns=['Temps de jeu (en minutes)', 'Buts'])
                            ).round().astype(int).reset_index()
                        else:
                            aggregated_data = player_data
                            st.warning("Aucun match disponible pour cette joueuse.")
                    else:
                        aggregated_data = player_data

                    time_played, goals = st.columns(2)
                    with time_played:
                        st.metric("Temps de jeu", f"{aggregated_data['Temps de jeu (en minutes)'].iloc[0]} minutes")
                    with goals:
                        st.metric("Buts", f"{aggregated_data['Buts'].iloc[0]}")

                    tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])

                    with tab1:
                        fig = create_individual_radar(aggregated_data)
                        st.pyplot(fig)

                    with tab2:
                        col1, col2, col3, col4, col5 = st.columns(5)
                        with col1: st.metric("Rigueur", f"{aggregated_data['Rigueur'].iloc[0]}/100")
                        with col2: st.metric("Récupération", f"{aggregated_data['Récupération'].iloc[0]}/100")
                        with col3: st.metric("Distribution", f"{aggregated_data['Distribution'].iloc[0]}/100")
                        with col4: st.metric("Percussion", f"{aggregated_data['Percussion'].iloc[0]}/100")
                        with col5: st.metric("Finition", f"{aggregated_data['Finition'].iloc[0]}/100")

                    with tab3:
                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                        with col1: st.metric("Défenseur central", f"{aggregated_data['Défenseur central'].iloc[0]}/100")
                        with col2: st.metric("Défenseur latéral", f"{aggregated_data['Défenseur latéral'].iloc[0]}/100")
                        with col3: st.metric("Milieu défensif", f"{aggregated_data['Milieu défensif'].iloc[0]}/100")
                        with col4: st.metric("Milieu relayeur", f"{aggregated_data['Milieu relayeur'].iloc[0]}/100")
                        with col5: st.metric("Milieu offensif", f"{aggregated_data['Milieu offensif'].iloc[0]}/100")
                        with col6: st.metric("Attaquant", f"{aggregated_data['Attaquant'].iloc[0]}/100")
            else:
                # Pour les profils admin/coach (accès à toutes les joueuses)
                st.subheader("Sélectionnez une joueuse du Paris FC")
                player = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique())
                player_data = pfc_kpi[pfc_kpi['Player'] == player]

                if player_data.empty:
                    st.error("Aucune donnée disponible pour cette joueuse.")
                else:
                    game = st.multiselect("Choisissez un ou plusieurs matchs", player_data['Adversaire'].unique())
                    filtered_data = player_data[player_data['Adversaire'].isin(game)] if game else player_data

                    if filtered_data.empty:
                        st.warning("Aucun match sélectionné ou aucune donnée disponible.")
                    else:
                        aggregated_data = filtered_data.groupby('Player').agg({
                            'Temps de jeu (en minutes)': 'sum',
                            'Buts': 'sum',
                        }).join(
                            filtered_data.groupby('Player').mean(numeric_only=True).drop(columns=['Temps de jeu (en minutes)', 'Buts'])
                        ).round().astype(int).reset_index()

                        time_played, goals = st.columns(2)
                        with time_played:
                            st.metric("Temps de jeu", f"{aggregated_data['Temps de jeu (en minutes)'].iloc[0]} minutes")
                        with goals:
                            st.metric("Buts", f"{aggregated_data['Buts'].iloc[0]}")

                        tab1, tab2, tab3 = st.tabs(["Radar", "KPIs", "Postes"])

                        with tab1:
                            fig = create_individual_radar(aggregated_data)
                            st.pyplot(fig)

                        with tab2:
                            col1, col2, col3, col4, col5 = st.columns(5)
                            with col1: st.metric("Rigueur", f"{aggregated_data['Rigueur'].iloc[0]}/100")
                            with col2: st.metric("Récupération", f"{aggregated_data['Récupération'].iloc[0]}/100")
                            with col3: st.metric("Distribution", f"{aggregated_data['Distribution'].iloc[0]}/100")
                            with col4: st.metric("Percussion", f"{aggregated_data['Percussion'].iloc[0]}/100")
                            with col5: st.metric("Finition", f"{aggregated_data['Finition'].iloc[0]}/100")

                        with tab3:
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
            # Pour une joueuse: comparaison avec elle-même (différents matchs) ou avec EDF
            st.subheader(f"Comparaison pour {player_name}")

            if pfc_kpi.empty:
                st.warning(f"Aucune donnée disponible pour {player_name}.")
            else:
                # Option 1: Comparaison avec elle-même sur différents matchs
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
                                players_data = pd.concat([data1, data2])
                                fig = create_comparison_radar(players_data)
                                st.pyplot(fig)
                            else:
                                st.warning("Aucune donnée disponible pour les matchs sélectionnés.")
                        else:
                            st.warning("Veuillez sélectionner deux matchs différents.")
                    else:
                        st.warning("Pas assez de matchs disponibles pour la comparaison.")

                # Option 2: Comparaison avec les données EDF
                st.write("### Comparaison avec les données EDF")
                if not edf_kpi.empty:
                    poste = st.selectbox("Sélectionnez un poste EDF pour comparaison",
                                       edf_kpi['Poste'].unique())
                    edf_data = edf_kpi[edf_kpi['Poste'] == poste].rename(columns={'Poste': 'Player'})

                    if not edf_data.empty:
                        # Préparer les données de la joueuse pour la comparaison
                        player_data = pfc_kpi.copy()
                        player_data = player_data.rename(columns={'Player': 'Player'})

                        if st.button("Comparer avec EDF"):
                            players_data = pd.concat([player_data, edf_data])
                            fig = create_comparison_radar(players_data)
                            st.pyplot(fig)
                    else:
                        st.warning("Aucune donnée EDF disponible pour ce poste.")
                else:
                    st.warning("Aucune donnée EDF disponible.")
        else:
            # Pour les profils admin/coach: comparaison normale entre joueuses
            st.subheader("Sélectionnez une joueuse du Paris FC")
            player1 = st.selectbox("Choisissez un joueur", pfc_kpi['Player'].unique(), key='player_1')
            player1_data = pfc_kpi[pfc_kpi['Player'] == player1]

            if player1_data.empty:
                st.error("Aucune donnée disponible pour cette joueuse.")
            else:
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
                        game2 = st.multiselect("Choisissez un ou plusieurs matchs", player2_data['Adversaire'].unique(), key='games_2_pfc')
                        filtered_player2_data = player2_data[player2_data['Adversaire'].isin(game2)] if game2 else player2_data
                        aggregated_player2_data = filtered_player2_data.groupby('Player').mean(numeric_only=True).round().astype(int).reset_index()

                        if st.button("Afficher le radar", key='button_pfc'):
                            if aggregated_player1_data.empty or aggregated_player2_data.empty:
                                st.error("Veuillez sélectionner au moins un match pour chaque joueur.")
                            else:
                                players_data = pd.concat([aggregated_player1_data, aggregated_player2_data])
                                fig = create_comparison_radar(players_data)
                                st.pyplot(fig)

                with tab2:
                    st.subheader("Sélectionnez un poste de l'Équipe de France")
                    player2 = st.selectbox("Choisissez un poste de comparaison", edf_kpi['Poste'].unique(), key='player_2_edf')
                    player2_data = edf_kpi[edf_kpi['Poste'] == player2].rename(columns={'Poste': 'Player'})

                    if st.button("Afficher le radar", key='button_edf'):
                        if aggregated_player1_data.empty:
                            st.error("Veuillez sélectionner au moins un match pour la joueuse PFC.")
                        else:
                            players_data = pd.concat([aggregated_player1_data, player2_data])
                            fig = create_comparison_radar(players_data)
                            st.pyplot(fig)

    elif page == "Gestion":
        st.header("Gestion des utilisateurs")
        if check_permission(user_profile, "all", permissions):
            st.write("Cette page est réservée à la gestion des utilisateurs.")

            # Affichage des utilisateurs actuels
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

            # Formulaire pour ajouter un utilisateur
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
                                "player": new_player if new_player else None
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
    pfc_kpi, edf_kpi = collect_data()

    # Affichage de l'interface
    script_streamlit(pfc_kpi, edf_kpi, permissions, st.session_state.user_profile)
