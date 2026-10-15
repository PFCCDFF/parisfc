# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/metrics.py, version du 14/10/2026).
# Seuls les imports ont été adaptés. Garder la logique alignée sur l'original.
from dataclasses import dataclass
from typing import Callable, Mapping

import pandas as pd
from suivi_emma.chart import is_dark_theme

METRICS = {
    "Faible intensité & Volume": {
        "distance_m":     "Distance (m)",
        "speed_avg_mmin": "Volume (m/min)",
    },
    "Haute intensité": {
        # dist_13_19 (= B3 + B4) reste en base pour l'historique, mais n'est
        # plus affichée en D1 : remplacée par ses deux bandes.
        "dist_13_15":      "Dist. 13-15 km/h (m)",   # B3 Catapult
        "dist_15_19":      "Dist. 15-19 km/h (m)",   # B4 Catapult
        "dist_19_23":      "Dist. 19-23 km/h (m)",   # HID2
        "sprint_distance": "Dist. sprint (m)",
        "sprint_count":    "Sprints (nb)",
    },
    "Intensité max": {
        "speed_max_kmh": "Vit. max (km/h)",
        "accel_max":     "Accél. max (m/s²)",
        "accel_min":     "Décél. max (m/s²)",
    },
    "Accélérations / Décélérations": {
        "ad2": "AD2 (nb)",
        "ad3": "AD3 (nb)",
    },
}

ALL_METRIC_COLS   = [col for cat in METRICS.values() for col in cat.keys()]
ALL_METRIC_LABELS = {col: label for cat in METRICS.values() for col, label in cat.items()}

# Rapports PDF : catalogue de l'app plus 0-13 km/h, retirée des pages mais
# gardée dans les PDF.
PDF_METRIC_COLS = ["distance_m", "speed_avg_mmin", "dist_below_13"] + [
    c for c in ALL_METRIC_COLS if c not in {"distance_m", "speed_avg_mmin"}
]
PDF_METRIC_LABELS = {**ALL_METRIC_LABELS, "dist_below_13": "Dist. <13 km/h (m)"}

CATEGORY_COLORS = {
    "Intensité max":                 "#BD3032",
    "Accélérations / Décélérations": "#F2A900",
    "Haute intensité":               "#0680BE",
    "Faible intensité & Volume":     "#007A7A",
}
# col_key → couleur de sa catégorie
METRIC_CATEGORY_COLOR = {
    col: CATEGORY_COLORS[cat]
    for cat, metrics in METRICS.items()
    for col in metrics
}

RADAR_METRICS = [
    ("distance_m", "Distance"),
    ("speed_avg_mmin", "Volume"),
    ("speed_max_kmh", "Vit. max"),
    ("dist_19_23", "19-23 km/h"),
    ("sprint_count", "Sprints"),
    ("ad2", "AD2"),
    ("ad3", "AD3"),
]

RADAR_METRIC_COLS = [col for col, _ in RADAR_METRICS]
RADAR_METRIC_LABELS = {col: label for col, label in RADAR_METRICS}

POSITION_COLORS = {
    "GOALKEEPER": "#6C757D",
    "CENTER_BACK": "#0C183F",
    "FULL_BACK": "#2457A6",
    "WING_BACK": "#2457A6",
    "DEFENSIVE_MIDFIELD": "#007A7A",
    "CENTRAL_MIDFIELD": "#00A676",
    "ATTACKING_MIDFIELD": "#F2A900",
    "WINGER": "#E66A2C",
    "STRIKER": "#BD3032",
    "FORWARD": "#BD3032",
}

DEFAULT_POSITION_COLOR = "#0680BE"


def metric_fmt(col):
    """Retourne le format Plotly/Python (.0f ou .1f) selon la métrique."""
    return ".1f" if col in {"speed_avg_mmin", "speed_max_kmh", "accel_min", "accel_max"} else ".0f"


@dataclass(frozen=True)
class MetricCatalog:
    """Décrit les métriques et leur rendu pour une source GPS donnée."""

    metrics: Mapping[str, Mapping[str, str]]
    metric_cols: list[str]
    labels: Mapping[str, str]
    category_colors: Mapping[str, str]
    category_color_by_metric: Mapping[str, str]
    format_for: Callable[[str], str]
    non_summable_cols: frozenset[str]


def daily_team_means(session_data, metric_col, player_aggregation="sum", player_col="player_id"):
    """Moyenne équipe par jour, après agrégation des séances de chaque joueuse.

    ``player_aggregation`` permet de cumuler les métriques de charge ou de
    conserver une agrégation adaptée aux métriques non cumulables.
    """
    columns = ["session_date", player_col, "session_id", metric_col]
    if session_data.empty or any(column not in session_data.columns for column in columns):
        return pd.DataFrame(columns=["session_date", "value", "session_count"])

    d = session_data[columns].dropna(subset=["session_date", player_col, metric_col]).copy()
    if d.empty:
        return pd.DataFrame(columns=["session_date", "value", "session_count"])

    player_day = (
        d.groupby(["session_date", player_col], as_index=False)[metric_col]
        .agg(player_aggregation)
    )
    team_day = (
        player_day.groupby("session_date", as_index=False)[metric_col]
        .mean()
        .rename(columns={metric_col: "value"})
    )
    session_counts = d.groupby("session_date")["session_id"].nunique().rename("session_count")
    return team_day.merge(session_counts, on="session_date").sort_values("session_date")


def rpe_session_attendance(rpe_data):
    """Assiduité RPE par joueuse sur les séances ayant reçu une réponse.

    Les doublons éventuels de formulaire ne comptent qu'une fois. Les réponses
    non rattachées à une séance sont exclues, car elles ne peuvent pas être
    comparées à une séance d'équipe.
    """
    columns = ["session_id", "Numero", "Joueuse"]
    empty = pd.DataFrame(columns=["Numero", "Joueuse", "Réponses", "% assiduité"])
    if rpe_data.empty or any(column not in rpe_data.columns for column in columns):
        return empty, 0

    d = rpe_data[columns].dropna(subset=["session_id", "Joueuse"]).drop_duplicates(["session_id", "Joueuse"])
    session_count = d["session_id"].nunique()
    if not session_count:
        return empty, 0

    attendance = (
        d.groupby(["Numero", "Joueuse"], dropna=False)["session_id"].nunique()
        .reset_index(name="Réponses")
        .sort_values("Numero", na_position="last")
    )
    attendance["% assiduité"] = (
        (attendance["Réponses"] / session_count * 100).round(0).astype(int).astype(str) + " %"
    )
    return attendance, session_count


def weekly_team_means(weekly_player_data, metric_col):
    """Moyenne équipe hebdomadaire, hors gardiennes.

    ``weekly_player_data`` contient déjà une ligne agrégée par joueuse et par
    semaine, afin de respecter la règle d'agrégation de chaque métrique.
    """
    d = weekly_player_data[weekly_player_data["position"] != "GOALKEEPER"].copy()
    if d.empty or metric_col not in d.columns:
        return pd.DataFrame(columns=["week", "week_start", "value"])
    return (
        d.groupby("week", as_index=False)
        .agg(week_start=("session_date", "min"), value=(metric_col, "mean"))
        .sort_values("week_start")
    )


# ── Heatmaps de charge : code couleur partagé par toute l'app ─────────────────
# Une couleur de base par métrique, intensité (transparence) selon la valeur.
_HEAT_STD = (6, 128, 190)                # bleu Paris FC (métriques "autres")
_HEAT_ZONE = {
    "distance_m":      (126, 87, 194),   # distance globale (violet, distinct)
    "dist_13_19":      (232, 181, 0),    # 13-19 km/h (jaune)
    "dist_13_15":      (232, 181, 0),
    "dist_15_19":      (232, 181, 0),
    "dist_19_23":      (234, 115, 23),   # 19-23 km/h (orange)
    "sprint_distance": (189, 48, 50),    # sprint (rouge)
    "sprint_count":    (189, 48, 50),
}
# label affiché → couleur de base RGB
_HEAT_RGB = {ALL_METRIC_LABELS[c]: _HEAT_ZONE.get(c, _HEAT_STD) for c in ALL_METRIC_COLS}
_HEAT_INVERT_LABEL = ALL_METRIC_LABELS.get("accel_min")


def heatmap_styles(column):
    """Styles Pandas d'une colonne de heatmap de charge : couleur de base selon
    la métrique, intensité = valeur (min→max), texte blanc/noir auto lisible.
    accel_min inversé (plus la décél. est forte, plus c'est intense)."""
    values = pd.to_numeric(column, errors="coerce")
    vmin, vmax = values.min(), values.max()
    invert = column.name == _HEAT_INVERT_LABEL
    r, g, b = _HEAT_RGB.get(column.name, _HEAT_STD)
    # La teinte est composée en opaque sur le fond réel du thème (blanc en clair,
    # gris foncé Streamlit en sombre) : sinon un fond semi-transparent se mélange
    # au fond sombre et le texte noir choisi devient illisible en thème sombre.
    br, bg, bb = (38, 39, 48) if is_dark_theme() else (255, 255, 255)
    out = []
    for v in values:
        if pd.isna(v) or pd.isna(vmin) or pd.isna(vmax) or vmin == vmax:
            out.append("")
            continue
        ratio = (v - vmin) / (vmax - vmin)
        if invert:
            ratio = 1 - ratio
        a = 0.12 + 0.80 * ratio
        cr = r * a + br * (1 - a)
        cg = g * a + bg * (1 - a)
        cb = b * a + bb * (1 - a)
        lum = 0.2126 * cr + 0.7152 * cg + 0.0722 * cb
        fg = "white" if lum < 140 else "black"
        out.append(f"background-color: rgb({cr:.0f},{cg:.0f},{cb:.0f}); color: {fg}")
    return out


HEATMAP_LEGEND = (
    "<div style='display:flex;align-items:center;gap:14px;flex-wrap:wrap;"
    "margin:2px 0 6px 0;font-size:12px;color:gray'>"
    "<span>Intensité couleur = valeur (clair → foncé)</span>"
    "<span><span style='color:#7E57C2'>■</span> distance</span>"
    "<span><span style='color:#E8B500'>■</span> 13-19</span>"
    "<span><span style='color:#EA7317'>■</span> 19-23</span>"
    "<span><span style='color:#BD3032'>■</span> sprint</span>"
    "<span><span style='color:#0680BE'>■</span> autres</span></div>"
)


def position_color(position):
    if position is None:
        return DEFAULT_POSITION_COLOR
    return POSITION_COLORS.get(str(position).upper(), DEFAULT_POSITION_COLOR)


AGG_MAX_COLS = {"speed_max_kmh", "accel_max"}
AGG_MIN_COLS = {"accel_min"}
AGG_MEAN_COLS = {"speed_avg_mmin"}
NON_SUMMABLE_COLS = AGG_MAX_COLS | AGG_MIN_COLS | AGG_MEAN_COLS


# Comptages propres à chaque capteur : FieldWiz compte environ 4 fois plus
# d'accélérations / décélérations que Catapult (médianes par km au
# 2026-10-05, séances > 3 km : AD2 70 contre 18, AD3 14 contre 3). Un cumul
# qui mêle séances D1 (Catapult) et U19 (FieldWiz) les laisse donc vides.
COLONNES_PAR_CAPTEUR = ("ad2", "ad3")


def groupes_mixtes(rows, by) -> set:
    """Valeurs de `by` (joueuse, semaine...) dont les lignes mêlent les deux
    sources (colonne `source`, D1 / U19)."""
    if rows is None or rows.empty or "source" not in rows.columns:
        return set()
    nb = rows.groupby(by)["source"].nunique()
    return set(nb[nb > 1].index)


def masquer_comptages_mixtes(agg, rows, by):
    """Copie de `agg` (une ligne par valeur de `by`, en colonne ou en index)
    avec COLONNES_PAR_CAPTEUR vidées pour les groupes mixtes de `rows`."""
    mixtes = groupes_mixtes(rows, by)
    cols = [c for c in COLONNES_PAR_CAPTEUR if c in agg.columns]
    if not mixtes or not cols:
        return agg
    agg = agg.copy()
    cles = agg[by] if by in agg.columns else agg.index.to_series()
    agg.loc[cles.isin(mixtes).to_numpy(), cols] = float("nan")
    return agg


# Nom de chaque capteur dans le détail « 20 Catapult + 120 FieldWiz » d'un
# cumul mixte.
NOM_CAPTEUR = {"D1": "Catapult", "U19": "FieldWiz"}
# Colonne du comptage FieldWiz gardé à part (``ad2__fieldwiz``) dans un
# tableau par joueuse ; la colonne d'origine garde alors le seul Catapult.
SUFFIXE_FIELDWIZ = "__fieldwiz"


def sommes_par_capteur(rows, cols=COLONNES_PAR_CAPTEUR) -> dict:
    """{col: {source: somme}} des comptages propres au capteur, sources
    présentes seulement (NaN si la source n'a pas la mesure)."""
    if rows is None or rows.empty or "source" not in rows.columns:
        return {}
    return {
        c: pd.to_numeric(rows[c], errors="coerce").groupby(rows["source"]).sum(min_count=1).to_dict()
        for c in cols if c in rows.columns
    }


def detail_par_capteur(rows, cols=COLONNES_PAR_CAPTEUR) -> dict:
    """{col: « 20 Catapult + 120 FieldWiz »} pour des lignes qui mêlent les
    deux sources, à afficher à la place du cumul ; {} sinon."""
    if rows is None or rows.empty or "source" not in rows.columns or rows["source"].nunique() < 2:
        return {}
    return {
        c: " + ".join(
            f"{'—' if pd.isna(sommes.get(src)) else f'{sommes.get(src):.0f}'} {nom}"
            for src, nom in NOM_CAPTEUR.items()
        )
        for c, sommes in sommes_par_capteur(rows, cols).items()
    }


def separer_par_capteur(agg, rows, by, cols=COLONNES_PAR_CAPTEUR):
    """Copie de `agg` (une ligne par `by`) où chaque comptage propre au
    capteur est coupé en deux : `<col>` = Catapult (lignes D1), `<col>__fieldwiz`
    = FieldWiz (lignes U19). Inchangé si `rows` n'a pas de ligne U19."""
    if rows is None or rows.empty or "source" not in rows.columns or not (rows["source"] == "U19").any():
        return agg
    agg = agg.copy()
    for c in [c for c in cols if c in rows.columns and c in agg.columns]:
        sommes = pd.to_numeric(rows[c], errors="coerce").groupby([rows[by], rows["source"]]).sum(min_count=1)
        par_source = sommes.unstack("source")
        for src, nom in (("D1", c), ("U19", f"{c}{SUFFIXE_FIELDWIZ}")):
            agg[nom] = agg[by].map(par_source[src]) if src in par_source else float("nan")
    return agg


def is_summable(col):
    return col not in NON_SUMMABLE_COLS


def player_aggregation(col):
    """Mode d'agrégation par joueuse pour une métrique de charge, sur une
    fenêtre de plusieurs séances (même convention que
    views/vue_equipe.py::_daily_player_aggregation)."""
    if col in AGG_MAX_COLS:
        return "max"
    if col in AGG_MIN_COLS:
        return "min"
    if col in AGG_MEAN_COLS:
        return "mean"
    return "sum"


CATALOG = MetricCatalog(
    metrics=METRICS,
    metric_cols=ALL_METRIC_COLS,
    labels=ALL_METRIC_LABELS,
    category_colors=CATEGORY_COLORS,
    category_color_by_metric=METRIC_CATEGORY_COLOR,
    format_for=metric_fmt,
    non_summable_cols=frozenset(NON_SUMMABLE_COLS),
)
