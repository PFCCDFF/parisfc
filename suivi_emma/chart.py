# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/chart.py, version du 14/10/2026).
# Seuls les imports ont été adaptés ; le template Plotly n'est plus posé par défaut pour toute l'app (appliqué figure par figure via appliquer_template). Garder la logique alignée sur l'original.
import math
import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go
import streamlit as st


# Fond de page du thème sombre de Streamlit (backgroundColor par défaut).
FOND_SOMBRE = "#0E1117"


def is_dark_theme():
    """True si le thème résolu (navigateur/app) est sombre. Fallback : clair."""
    try:
        t = st.context.theme.type          # Streamlit >= 1.46
        if t in ("dark", "light"):
            return t == "dark"
    except Exception:
        pass
    try:
        return st.get_option("theme.base") == "dark"
    except Exception:
        return False


def ink(alpha=0.85):
    """Couleur d'encre (texte d'annotation, lignes de repère) lisible sur le
    fond du thème courant : bleu nuit PFC en clair, gris très clair en sombre."""
    rgb = "233,236,245" if is_dark_theme() else "12,24,63"
    return f"rgba({rgb},{alpha})"


def nice_ymax(val):
    """Prochain nombre 'rond' strictement au-dessus de val."""
    if pd.isna(val) or val <= 0:
        return 10
    val = float(val)
    exp = 10 ** math.floor(math.log10(val))
    result = math.ceil(val / exp) * exp
    return result if result > val else result + exp


def nice_ymin(val):
    """Prochain nombre 'rond' strictement en dessous de val (valeurs négatives)."""
    if pd.isna(val) or val >= 0:
        return -10
    val = float(val)
    exp = 10 ** math.floor(math.log10(abs(val)))
    result = -math.ceil(abs(val) / exp) * exp
    return result if result < val else result - exp


def y_range(series):
    """Retourne [ymin, ymax] adapté pour un axe Plotly, gère les valeurs négatives (ex. accel_min)."""
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return [0, 10]
    vmax = clean.max()
    vmin = clean.min()
    if vmax <= 0:
        return [nice_ymin(vmin), 0]
    return [0, nice_ymax(vmax)]


pio.templates["pfc"] = go.layout.Template(
    layout=go.Layout(
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.08)"),
        xaxis=dict(showgrid=False),
    ),
    data=dict(scatter=[go.Scatter(cliponaxis=False)])
)
TEMPLATE_PFC = "simple_white+pfc"


def appliquer_template(fig):
    """L'original posait pio.templates.default à l'import, ce qui changeait le style de
    TOUS les graphiques de l'app hôte : le template est appliqué ici figure par figure."""
    fig.update_layout(template=TEMPLATE_PFC)
    return fig
