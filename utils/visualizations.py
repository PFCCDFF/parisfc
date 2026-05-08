# utils/visualizations.py
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict

def create_radar_chart(stats: Dict[str, float], player_name: str):
    """
    Crée un graphique radar pour les statistiques d'une joueuse.

    Args:
        stats: Dictionnaire des statistiques (ex: {"Passes": 80, "Tirs": 70})
        player_name: Nom de la joueuse

    Returns:
        Figure Plotly
    """
    categories = list(stats.keys())
    values = list(stats.values())

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill="toself",
        name=player_name,
        line_color="royalblue",
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
            ),
        ),
        showlegend=True,
        title=f"Statistiques de {player_name}",
        height=500,
    )
    return fig

def create_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str):
    """Crée un graphique en barres."""
    fig = px.bar(df, x=x_col, y=y_col, title=title)
    fig.update_layout(height=400)
    return fig

def create_pizza_chart(stats: Dict[str, float], player_name: str):
    """
    Crée un graphique "pizza" (style mplsoccer) avec Plotly.
    Note: Plotly ne supporte pas directement les pizzas, donc on utilise un radar personnalisé.
    """
    # Normaliser les valeurs entre 0 et 100
    max_val = max(stats.values()) if stats else 100
    normalized_stats = {k: (v / max_val) * 100 for k, v in stats.items()}

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=[100] * len(normalized_stats),  # Rayon fixe pour la pizza
        theta=list(normalized_stats.keys()),
        fill="toself",
        name=player_name,
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=False),  # Masquer l'axe radial
            angularaxis=dict(direction="clockwise"),  # Sens horaire
        ),
        showlegend=True,
        title=f"Pizza Chart - {player_name}",
    )
    return fig
