# app.py
import os
import dash
from dash import dcc, html, Input, Output, callback, State, dash_table
import plotly.express as px
import pandas as pd
from config import *
from utils.data_loader import load_passerelle_data, load_photo_mapping, load_gps_name_map
from utils.visualizations import create_radar_chart, create_bar_chart
from utils.helpers import normalize_str

# Initialisation de l'app Dash
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    assets_folder="assets",  # Pour charger le CSS
)
server = app.server  # Nécessaire pour Gunicorn

# Charger les données une fois au démarrage (ou via cache)
def get_initial_data():
    df_passerelle = load_passerelle_data()
    photo_mapping = load_photo_mapping()
    gps_name_map = load_gps_name_map()
    return df_passerelle, photo_mapping, gps_name_map

# Layout principal
app.layout = html.Div([
    # Barre latérale
    html.Div([
        html.H2("Paris FC", style={"color": "white", "textAlign": "center"}),
        html.Hr(style={"borderColor": "white"}),
        html.H3("Menu", style={"color": "white"}),
        dcc.Link(
            "Joueuses Passerelles",
            href="/players",
            style={"color": "white", "display": "block", "padding": "10px", "textDecoration": "none"},
        ),
        dcc.Link(
            "Statistiques",
            href="/stats",
            style={"color": "white", "display": "block", "padding": "10px", "textDecoration": "none"},
        ),
        dcc.Link(
            "Données GPS",
            href="/gps",
            style={"color": "white", "display": "block", "padding": "10px", "textDecoration": "none"},
        ),
        html.Hr(style={"borderColor": "white"}),
        html.H3("Admin", style={"color": "white"}),
        html.Button(
            "Update Data",
            id="update-button",
            n_clicks=0,
            style={"width": "100%", "margin": "10px 0"},
        ),
    ], className="sidebar"),

    # Contenu principal
    html.Div([
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content", className="main-content"),
    ], style={"marginLeft": "220px"}),
])

# Callback pour la navigation entre les pages
@callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
)
def render_page(pathname):
    df_passerelle, photo_mapping, _ = get_initial_data()

    if pathname == "/players":
        # Préparer les options pour le dropdown
        player_options = [
            {"label": f"{row['Prénom']} {row['Nom']}", "value": idx}
            for idx, row in df_passerelle.iterrows()
        ]

        return html.Div([
            html.H1("Liste des Joueuses Passerelles"),
            html.Div([
                dcc.Dropdown(
                    id="player-dropdown",
                    options=player_options,
                    placeholder="Sélectionnez une joueuse",
                    style={"width": "50%"},
                ),
            ], className="dropdown-container"),
            html.Div(id="player-info"),
            dcc.Graph(id="player-radar"),
        ])

    elif pathname == "/stats":
        return html.Div([
            html.H1("Statistiques Générales"),
            dcc.Graph(
                id="stats-chart",
                figure=create_bar_chart(
                    df_passerelle,
                    x="Nom",
                    y="Âge",
                    title="Âge des Joueuses",
                ),
            ),
        ])

    elif pathname == "/gps":
        return html.Div([
            html.H1("Données GPS"),
            html.P("Cette section affichera les données GPS des joueuses."),
        ])

    else:
        return html.Div([
            html.H1("Bienvenue sur le Data Center Paris FC"),
            html.P("Utilisez le menu pour naviguer entre les sections."),
        ])

# Callback pour afficher les infos d'une joueuse
@callback(
    [Output("player-info", "children"), Output("player-radar", "figure")],
    Input("player-dropdown", "value"),
)
def update_player_info(selected_player_idx):
    if selected_player_idx is None:
        return "Sélectionnez une joueuse", {}

    df_passerelle, photo_mapping, _ = get_initial_data()
    player_data = df_passerelle.iloc[selected_player_idx]

    # Infos textuelles
    info = html.Div([
        html.H2(f"{player_data['Prénom']} {player_data['Nom']}", className="player-card"),
        html.Img(
            src=photo_mapping.get(normalize_str(f"{player_data['Prénom']} {player_data['Nom']}"), ""),
            className="player-photo",
        ),
        html.P(f"Date de naissance: {player_data.get('Date de naissance', 'N/A')}"),
        html.P(f"Poste 1: {player_data.get('Poste 1', 'N/A')}"),
        html.P(f"Poste 2: {player_data.get('Poste 2', 'N/A')}"),
        html.P(f"Pied fort: {player_data.get('Pied fort', 'N/A')}"),
        html.P(f"Taille: {player_data.get('Taille', 'N/A')} cm"),
    ], className="player-card")

    # Stats pour le radar (exemple avec des valeurs fictives)
    stats = {
        "Passes": player_data.get("Passes", 80),
        "Tirs": player_data.get("Tirs", 70),
        "Dribbles": player_data.get("Dribbles", 60),
        "Défense": player_data.get("Défense", 90),
        "Vitesse": player_data.get("Vitesse", 85),
    }
    fig = create_radar_chart(stats, f"{player_data['Prénom']} {player_data['Nom']}")

    return info, fig

# Callback pour le bouton Update (simule une mise à jour)
@callback(
    Output("update-button", "n_clicks"),
    Input("update-button", "n_clicks"),
    prevent_initial_call=True,
)
def update_data(n_clicks):
    if n_clicks > 0:
        # Ici, tu pourrais ajouter la logique pour re-télécharger les données
        # Exemple : vider le cache ou re-charger les fichiers
        print("Mise à jour des données déclenchée")
    return n_clicks

if __name__ == "__main__":
    app.run_server(debug=True)
