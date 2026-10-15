# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/session_charts.py, version du 14/10/2026).
# Repris : profils, barres équipe, carte de charge (rendu st.dataframe au lieu d'AG Grid),
# week_label. Non repris : graphiques D1 hebdo/références (onglets non portés).
"""
utils/session_charts.py

Fonctions de graphique génériques partagées entre les vues D1
(views/vue_individuelle.py, views/vue_equipe.py) et les vues U19
(views/vue_individuelle_u19.py, views/vue_equipe_u19.py). Extraites des
fichiers D1 : comportement strictement identique, aucune de ces fonctions
n'était couplée à player_id -- seules `bar_chart_team`/`render_heatmap`
géraient une colonne "position" optionnelle (D1 en a une, u19.joueuses
n'en a pas -- ces deux fonctions s'en passent proprement si absente).
"""

import re

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from suivi_emma.chart import ink
from suivi_emma.metrics import (
    ALL_METRIC_COLS,
    SUFFIXE_FIELDWIZ,
    ALL_METRIC_LABELS,
    METRICS,
    CATEGORY_COLORS,
    METRIC_CATEGORY_COLOR,
    NON_SUMMABLE_COLS,
    metric_fmt,
    heatmap_styles,
    HEATMAP_LEGEND,
    CATALOG as DEFAULT_CATALOG,
    MetricCatalog,
)
# Constantes de utils/pdf_export.py (app d'Emma), recopiées pour ne pas porter le module PDF.
JOURS_FR = {0: "Lun", 1: "Mar", 2: "Mer", 3: "Jeu", 4: "Ven", 5: "Sam", 6: "Dim"}
_COACH_WEEKLY_METRICS = (
    ("distance_m", "Distance totale", "#0680BE"),
    ("dist_13_15", "13-15 km/h", "#3DA5D9"),
    ("dist_15_19", "15-19 km/h", "#00A676"),
    ("dist_19_23", "19-23 km/h", "#F2A900"),
    ("sprint_distance", "Distance sprint", "#EA7317"),
)

_COL_GROUP = {col: grp for grp, cols in METRICS.items() for col in cols}
_HIST_COLS = [c for c in ALL_METRIC_COLS if c != "speed_avg_mmin"]
_HEATMAP_ORDER = [
    "distance_m", "speed_avg_mmin",
    "dist_13_15", "dist_15_19", "dist_19_23", "sprint_distance", "sprint_count",
    "speed_max_kmh", "accel_max", "accel_min",
    "ad2", "ad3",
]
# Libellés compacts réservés à la carte de charge affichée dans Streamlit.
# Les libellés complets du catalogue restent la référence pour les graphiques,
# sélecteurs et rapports PDF.
_HEATMAP_HEADERS = {
    "duration_min": "Temps\n(min)",
    "distance_m": "Distance\n(m)",
    "speed_avg_mmin": "Vol.\n(m/min)",
    "dist_13_19": "13-19 km/h\n(m)",
    "dist_13_15": "13-15 km/h\n(m)",
    "dist_15_19": "15-19 km/h\n(m)",
    "dist_19_23": "19-23 km/h\n(m)",
    "sprint_distance": "Dist. sprint\n(m)",
    "sprint_count": "#\nSprints",
    "speed_max_kmh": "Vmax\nkm/h",
    "accel_max": "Accél.\nmax",
    "accel_min": "Décél.\nmax",
    "ad2": "AD2",
    "ad3": "AD3",
}
# Palette jour (ordre chronologique dans la semaine), partagée équipe/indiv.
DAY_PALETTE = ["#0680BE", "#3DA5D9", "#F2A900", "#EA7317", "#BD3032", "#00A676", "#6C5B7B"]

# Métriques du graphique "barres accolées" (Séance/Semaine D1) -- mêmes
# libellés/couleurs que build_coach_daily_chart_figure côté PDF pour rester
# visuellement cohérent.
D1_BAR_METRICS = _COACH_WEEKLY_METRICS
# Barre AD2 découpée : la partie foncée en bas = celles qui étaient des AD3
# (AD3 ⊂ AD2), à passer en `decoupe` de build_grouped_bar_week_figure.
AD_BAR_METRIC = ("ad2", "AD2", "#8E7DA6")
AD_DECOUPE = {"ad2": ("ad3", "dont AD3", "#4B3A63")}


def build_grouped_bar_week_figure(x_labels, values_by_metric, metrics_spec, focus_col=None,
                                   selected_idx=None, match_mask=None,
                                   catalog: MetricCatalog = DEFAULT_CATALOG,
                                   height=420, decoupe=None, details=None, details_match=None):
    """Barres accolées par jour, une métrique = un axe Y indépendant (mêmes
    échelles réelles que build_coach_daily_chart_figure côté PDF).

    barmode="group" de plotly ne décale les barres que par paire
    (xaxis, yaxis) : avec un axe Y par métrique, des traces sur des axes
    différents se superposeraient toutes sur la même position X plutôt que
    de s'accoler. On calcule donc l'offset des barres à la main (x
    numérique), comme le fait matplotlib avec ses barres+twinx.

    x_labels        : libellés d'axe X, un par jour/séance (jour abrégé + date, sans nom de séance).
    values_by_metric: dict col -> liste/Series de valeurs (même longueur que x_labels).
    metrics_spec    : [(col, label, color), ...] -- métriques à tracer, dans l'ordre.
    focus_col       : col à mettre en avant (opacité pleine + ligne superposée) ;
                       None = toutes les métriques à pleine opacité, pas de ligne.
    selected_idx    : position (int, dans x_labels) à marquer d'une ligne verticale rouge.
    match_mask      : liste de bool (même longueur que x_labels) -- colore en rouge
                       la date et ajoute un petit ballon au-dessus des jours de match.
    decoupe         : {col: (sous_col, libellé, couleur)} -- part de la barre `col`
                       superposée en bas, sur le même axe (ex. AD3 dans AD2, cf.
                       AD_DECOUPE) ; valeurs lues dans values_by_metric[sous_col].
    details         : texte de survol par position (titre de séance, participantes...),
                       en tête du survol de chaque barre à la place du libellé d'axe ;
                       HTML Plotly (<b>, <br>) accepté.
    details_match   : texte de survol du ballon, par position (None = pas de ballon
                       survolable à cette position).
    """
    decoupe = decoupe or {}
    n_days    = len(x_labels)
    n_metrics = len(metrics_spec)
    if n_days == 0 or n_metrics == 0:
        return go.Figure()

    n_right      = n_metrics - 1
    domain_right = max(1 - 0.08 * n_right, 0.55) if n_right else 1.0
    right_step   = (1 - domain_right) / n_right if n_right else 0.0
    x_base       = list(range(n_days))
    bar_width    = 0.8 / n_metrics
    match_mask   = list(match_mask) if match_mask is not None else [False] * n_days
    if details is not None:
        hover_labels = list(details)
        _entete = "%{hovertext}"
    else:
        hover_labels = [
            f"{lbl} — MATCH" if is_match else lbl for lbl, is_match in zip(x_labels, match_mask)
        ]
        _entete = "<b>%{hovertext}</b>"
    tick_labels = [
        f"<span style='fill:#BD3032;font-weight:700'>{lbl}</span>" if is_match else lbl
        for lbl, is_match in zip(x_labels, match_mask)
    ]

    fig             = go.Figure()
    layout_axes     = {}
    axis_refs       = []
    focus_positions = x_base
    for i, (col, label, color) in enumerate(metrics_spec):
        values   = pd.to_numeric(pd.Series(values_by_metric.get(col, [])), errors="coerce").fillna(0)
        v_max    = float(values.max()) if len(values) else 0.0
        is_focus = focus_col is None or col == focus_col
        axis_ref = "y" if i == 0 else f"y{i + 1}"
        axis_key = "yaxis" if i == 0 else f"yaxis{i + 1}"
        axis_refs.append(axis_ref)

        offset    = (i - (n_metrics - 1) / 2) * bar_width
        positions = [xb + offset for xb in x_base]
        if col == focus_col:
            focus_positions = positions  # la ligne se cale pile sur les barres de la métrique en avant

        fig.add_trace(go.Bar(
            x=positions, y=values, width=bar_width * 0.9, name=label,
            marker_color=color, opacity=1.0 if is_focus else 0.2,
            yaxis=axis_ref, hovertext=hover_labels,
            hovertemplate=f"{_entete}<br>{label} : %{{y:{catalog.format_for(col)}}}<extra></extra>",
        ))
        if col in decoupe:
            sous_col, sous_label, sous_color = decoupe[col]
            sous = pd.to_numeric(pd.Series(values_by_metric.get(sous_col, [])), errors="coerce").fillna(0)
            fig.add_trace(go.Bar(
                x=positions, y=sous, width=bar_width * 0.9, name=sous_label,
                marker_color=sous_color, opacity=1.0 if is_focus else 0.2,
                yaxis=axis_ref, hovertext=hover_labels,
                hovertemplate=f"{_entete}<br>{sous_label} : %{{y:{catalog.format_for(sous_col)}}}<extra></extra>",
            ))

        axis_cfg = dict(
            title=dict(text=label, font=dict(color=color, size=11)),
            tickfont=dict(color=color, size=10),
            zeroline=False, rangemode="tozero", range=[0, v_max * 1.15 if v_max else 1],
            showgrid=(i == 0), gridcolor="rgba(128,128,128,0.12)",
        )
        if i == 0:
            axis_cfg["side"] = "left"
        else:
            axis_cfg.update(overlaying="y", side="right", anchor="free",
                             position=domain_right + right_step * i)
        layout_axes[axis_key] = axis_cfg

    if focus_col is not None and focus_col in values_by_metric:
        focus_idx    = next(i for i, (c, _, _) in enumerate(metrics_spec) if c == focus_col)
        focus_values = pd.to_numeric(pd.Series(values_by_metric[focus_col]), errors="coerce").fillna(0)
        fig.add_trace(go.Scatter(
            x=focus_positions, y=focus_values,
            mode="lines+markers", line=dict(color=ink(1), width=2, dash="dot"),
            marker=dict(size=7, color=ink(1)),
            yaxis=axis_refs[focus_idx], showlegend=False, hoverinfo="skip",
        ))

    fig.update_layout(
        barmode="overlay",
        margin=dict(t=20, b=20, l=60, r=10),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, domain=[0, domain_right if n_right else 1],
                   range=[-0.5, n_days - 0.5], tickmode="array",
                   tickvals=x_base, ticktext=tick_labels),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        height=height,
        **layout_axes,
    )

    details_match = list(details_match) if details_match is not None else [None] * n_days
    for i, is_match in enumerate(match_mask):
        if is_match:
            fig.add_annotation(
                x=x_base[i], xref="x", y=0.96, yref="paper",
                text="⚽", showarrow=False, xanchor="center", yanchor="middle",
                font=dict(size=13),
                hovertext=details_match[i] or None,
                hoverlabel=dict(bgcolor="white", font=dict(color="#111111")),
            )

    if selected_idx is not None:
        fig.add_vline(x=x_base[selected_idx], line_dash="dot", line_color="rgba(189,48,50,0.6)")

    return fig


def _profile_cols(catalog: MetricCatalog):
    return _HIST_COLS if catalog is DEFAULT_CATALOG else catalog.metric_cols


def _libelle_ref(ref_label: str) -> str:
    """Libellé de la ligne 100 % posé dans la marge droite (95 px) : coupé
    sur deux lignes au dernier espace s'il est long (« Record match<br>FieldWiz »)."""
    if len(ref_label) <= 12 or " " not in ref_label:
        return ref_label
    debut, fin = ref_label.rsplit(" ", 1)
    return f"{debut}<br>{fin}"


def profile_bars(player_s, ref_s, ref_label, ymax=None, catalog: MetricCatalog = DEFAULT_CATALOG):
    rows = []
    for col in _profile_cols(catalog):
        pv  = pd.to_numeric(player_s.get(col, float("nan")), errors="coerce")
        avg = pd.to_numeric(ref_s.get(col,    float("nan")), errors="coerce")
        if pd.isna(pv) or pd.isna(avg) or avg == 0:
            continue
        pct = abs(float(pv)) / abs(float(avg)) * 100 if col == "accel_min" else float(pv) / float(avg) * 100
        rows.append({"label": catalog.labels[col], "pct": round(pct, 1),
                     "raw": f"{pv:{catalog.format_for(col)}}", "color": catalog.category_color_by_metric.get(col, "#0680BE")})
    if not rows:
        return None
    d = pd.DataFrame(rows)
    fig = go.Figure(go.Bar(
        x=d["label"], y=d["pct"], text=d["raw"],
        textposition="inside", insidetextanchor="end",
        textfont=dict(size=15, color="white"),
        marker_color=d["color"],
        hovertemplate="%{x}<br>%{y:.1f}% de la référence<extra></extra>",
    ))
    fig.add_shape(
        type="line",
        x0=0, x1=1, xref="paper",
        y0=100, y1=100, yref="y",
        line=dict(color=ink(0.6), dash="dash", width=1.5),
    )
    fig.add_annotation(
        y=100, x=1.01,
        xref="paper", yref="y",
        text=_libelle_ref(ref_label),
        showarrow=False,
        xanchor="left",
        yanchor="middle",
        font=dict(size=11, color=ink(0.7)),
    )
    fig.update_layout(
        margin=dict(t=40, b=20, l=60, r=95),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, tickangle=-30),
        yaxis=dict(gridcolor="rgba(128,128,128,0.15)", zeroline=False, ticksuffix="%",
                   range=[0, max(float(d["pct"].max()) * 1.05, ymax or 115)]),
        showlegend=False, height=420,
    )
    return fig


def profile_bars_stacked_core(seg_df, seg_col, seg_order, seg_label, week_s, ref_s, ref_label, ymax=None,
                              catalog: MetricCatalog = DEFAULT_CATALOG, seg_colors=None, cols_en_plus=None):
    """Cœur générique du profil détaillé empilé. Chaque barre-métrique (base 100
    = référence) est découpée en segments (jour ou période) pour les métriques
    sommables ; les non sommables (max/min/moyenne) restent en barre simple
    (segment gris « non cumulable »). Total en % conservé, hover par segment.
    week_s = valeur agrégée (cumul semaine ou ligne Globale) ; ref_s = référence.
    seg_colors = couleur imposée par segment (sinon palette dans l'ordre).
    cols_en_plus = {col: libellé} de métriques sommables hors catalogue,
    placées en tête (ex. temps de jeu)."""
    cols_en_plus = cols_en_plus or {}
    libelles = {**catalog.labels, **cols_en_plus}
    metric_cols, tot_pct = [], {}
    for col in [*cols_en_plus, *_profile_cols(catalog)]:
        pv  = pd.to_numeric(week_s.get(col, float("nan")), errors="coerce")
        avg = pd.to_numeric(ref_s.get(col,  float("nan")), errors="coerce")
        if pd.isna(pv) or pd.isna(avg) or avg == 0:
            continue
        metric_cols.append(col)
        tot_pct[col] = abs(float(pv)) / abs(float(avg)) * 100 if col == "accel_min" else float(pv) / float(avg) * 100
    if not metric_cols:
        return None
    labels = [libelles[c] for c in metric_cols]

    fig = go.Figure()
    for i, seg in enumerate(seg_order):
        sub = seg_df[seg_df[seg_col] == seg]
        lbl = seg_label.get(seg, str(seg))
        ys, cd = [], []
        for col in metric_cols:
            avg = float(pd.to_numeric(ref_s.get(col), errors="coerce"))
            if col in catalog.non_summable_cols:
                ys.append(0.0); cd.append(["", ""])
            else:
                dv  = float(pd.to_numeric(sub[col], errors="coerce").sum())
                pct = dv / avg * 100 if avg else 0.0
                ys.append(round(pct, 1)); cd.append([lbl, f"{dv:{catalog.format_for(col)}}"])
        fig.add_trace(go.Bar(
            x=labels, y=ys, name=lbl,
            marker_color=(seg_colors or {}).get(seg, DAY_PALETTE[i % len(DAY_PALETTE)]),
            customdata=cd,
            hovertemplate="%{x}<br>%{customdata[0]} : %{customdata[1]} (%{y:.1f}%)<extra></extra>",
        ))
    ys_nc, cd_nc = [], []
    for col in metric_cols:
        if col in catalog.non_summable_cols:
            ys_nc.append(round(tot_pct[col], 1))
            cd_nc.append(f"{pd.to_numeric(week_s.get(col)):{catalog.format_for(col)}}")
        else:
            ys_nc.append(0.0); cd_nc.append("")
    if any(v > 0 for v in ys_nc):
        fig.add_trace(go.Bar(
            x=labels, y=ys_nc, name="non cumulable",
            marker_color="#9AA6B2", customdata=cd_nc,
            hovertemplate="%{x}<br>%{customdata} (%{y:.1f}%)<extra></extra>",
        ))
    for col in metric_cols:
        fig.add_annotation(
            x=libelles[col], y=tot_pct[col],
            text=f"{pd.to_numeric(week_s.get(col)):{catalog.format_for(col)}}",
            showarrow=False, yshift=9, font=dict(size=11, color=ink(0.85)),
        )
    fig.add_shape(
        type="line", x0=0, x1=1, xref="paper", y0=100, y1=100, yref="y",
        line=dict(color=ink(0.6), dash="dash", width=1.5),
    )
    fig.add_annotation(
        y=100, x=1.01, xref="paper", yref="y", text=_libelle_ref(ref_label),
        showarrow=False, xanchor="left", yanchor="middle",
        font=dict(size=11, color=ink(0.7)),
    )
    fig.update_layout(
        barmode="stack",
        margin=dict(t=40, b=70, l=60, r=95),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, tickangle=-30),
        yaxis=dict(gridcolor="rgba(128,128,128,0.15)", zeroline=False, ticksuffix="%",
                   range=[0, max(max(tot_pct.values()) * 1.10, ymax or 115)]),
        legend=dict(orientation="h", traceorder="normal", yanchor="top", y=-0.22, xanchor="center", x=0.5),
        height=470,
    )
    return fig


def profile_bars_stacked(df_week, week_s, ref_s, ref_label, ymax=None, catalog: MetricCatalog = DEFAULT_CATALOG):
    """Profil détaillé empilé par jour (onglet Semaine)."""
    dw = df_week.copy()
    dw["__day"] = dw["session_date"].dt.date
    days = sorted(dw["__day"].dropna().unique())
    seg_label = {dd: f"{JOURS_FR[dd.weekday()]} {dd.strftime('%d/%m')}" for dd in days}
    return profile_bars_stacked_core(dw, "__day", days, seg_label, week_s, ref_s, ref_label, ymax, catalog)


def profile_bars_seance(row, ref_s, ref_label, ymax=None, catalog: MetricCatalog = DEFAULT_CATALOG,
                        seg="Séance entière", cols_en_plus=None):
    """Profil d'une séance sans période (U19, import CSV) au rendu du profil
    par période : un seul segment, une seule couleur, valeur au-dessus.
    Sert aussi aux profils Période et Match de la vue individuelle (`seg` =
    libellé du segment en légende)."""
    seg_df = pd.DataFrame([row]).assign(__seg=seg)
    return profile_bars_stacked_core(seg_df, "__seg", [seg], {seg: seg}, row, ref_s, ref_label, ymax, catalog,
                                      cols_en_plus=cols_en_plus)


def profile_bars_stacked_periods(period_df, week_s, ref_s, ref_label, ymax=None,
                                 catalog: MetricCatalog = DEFAULT_CATALOG, seulement=None,
                                 cols_en_plus=None):
    """Profil détaillé empilé par période Catapult (onglet Séance).
    period_df = lignes period != Globale d'une même séance pour la joueuse.
    seulement = une période (ou une liste de périodes) : même rendu, réduit à
    ces segments, qui gardent la couleur qu'ils ont dans la vue séance entière
    (week_s = valeurs de ces seules périodes)."""
    order = period_df.drop_duplicates("period").sort_values("period_start")["period"].tolist()
    colors = {p: DAY_PALETTE[i % len(DAY_PALETTE)] for i, p in enumerate(order)}
    if seulement is not None:
        garder = {seulement} if isinstance(seulement, str) else set(seulement)
        order = [p for p in order if p in garder]
    return profile_bars_stacked_core(period_df, "period", order, {p: p for p in order},
                                      week_s, ref_s, ref_label, ymax, catalog, seg_colors=colors,
                                      cols_en_plus=cols_en_plus)


def bar_chart_team(df_in, metric_col, catalog: MetricCatalog = DEFAULT_CATALOG):
    """Barres horizontales, une par joueuse. La colonne "position" est
    optionnelle (absente côté u19.joueuses) : le hover l'affiche si
    présente, sinon s'en passe."""
    has_position = "position" in df_in.columns
    cols = ["full_name", metric_col] + (["position"] if has_position else [])
    df_chart = df_in[cols].dropna(subset=[metric_col]).sort_values(metric_col, ascending=True)
    if has_position:
        df_chart["position"] = df_chart["position"].fillna("Non renseigné")
    avg = df_chart[metric_col].mean()
    color = catalog.category_color_by_metric.get(metric_col, "#0680BE")
    hover = "<b>%{y}</b><br>" + ("Poste : %{customdata}<br>" if has_position else "") + f"%{{x:{catalog.format_for(metric_col)}}}<extra></extra>"
    fig = go.Figure(go.Bar(
        x=df_chart[metric_col], y=df_chart["full_name"],
        orientation="h",
        marker_color=color,
        text=df_chart[metric_col].apply(lambda v: f"{v:{catalog.format_for(metric_col)}}"),
        textposition="auto",
        textfont=dict(size=11, color="white"),
        customdata=df_chart["position"] if has_position else None,
        hovertemplate=hover,
    ))
    fig.add_shape(
        type="line", x0=avg, x1=avg, xref="x", y0=0, y1=1, yref="paper",
        line=dict(color=ink(0.55), dash="dash", width=1.5),
    )
    fig.add_annotation(
        x=avg, y=1.01, xref="x", yref="paper",
        text=f"Moy. : {avg:{catalog.format_for(metric_col)}}",
        showarrow=False, xanchor="center", yanchor="bottom",
        font=dict(size=11, color=ink(0.7)),
    )
    fig.update_layout(
        margin=dict(t=40, b=20, l=160, r=0),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="rgba(128,128,128,0.15)", zeroline=False, rangemode="tozero"),
        yaxis=dict(showgrid=False),
        showlegend=False,
        height=max(360, 26 * len(df_chart) + 90),
    )
    return fig, color


def stacked_team_hbar(d, metric_col, seg_col, seg_order, seg_label, catalog: MetricCatalog = DEFAULT_CATALOG):
    """Cœur générique : barres horizontales empilées (une par joueuse) selon
    seg_col (jour ou période), pour métrique sommable. Total en bout de barre,
    hover par segment. seg_order = segments dans l'ordre voulu (gauche→droite)."""
    d = d[["full_name", seg_col, metric_col]].dropna(subset=[metric_col]).copy()
    if d.empty:
        return None
    agg    = d.groupby(["full_name", seg_col], as_index=False)[metric_col].sum()
    totals = agg.groupby("full_name")[metric_col].sum()
    order  = totals.sort_values(ascending=True).index.tolist()
    _fmt   = catalog.format_for(metric_col)
    avg    = totals.mean()

    fig = go.Figure()
    for i, seg in enumerate(seg_order):
        sub = agg[agg[seg_col] == seg].set_index("full_name").reindex(order)[metric_col]
        if sub.notna().sum() == 0:
            continue
        sub = sub.fillna(0)
        lbl = seg_label.get(seg, str(seg))
        fig.add_trace(go.Bar(
            y=order, x=sub, orientation="h", name=lbl,
            marker_color=DAY_PALETTE[i % len(DAY_PALETTE)],
            hovertemplate=f"<b>%{{y}}</b><br>{lbl} : %{{x:{_fmt}}}<extra></extra>",
        ))
    for name in order:
        fig.add_annotation(
            x=totals[name], y=name, text=f"{totals[name]:{_fmt}}",
            showarrow=False, xanchor="left", xshift=4,
            font=dict(size=11, color=ink(0.85)),
        )
    fig.add_shape(
        type="line", x0=avg, x1=avg, xref="x", y0=0, y1=1, yref="paper",
        line=dict(color=ink(0.55), dash="dash", width=1.5),
    )
    fig.add_annotation(
        x=avg, y=1.01, xref="x", yref="paper",
        text=f"Moy. : {avg:{_fmt}}",
        showarrow=False, xanchor="center", yanchor="bottom",
        font=dict(size=11, color=ink(0.7)),
    )
    fig.update_layout(
        barmode="stack",
        margin=dict(t=40, b=60, l=160, r=40),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="rgba(128,128,128,0.15)", zeroline=False, rangemode="tozero"),
        yaxis=dict(showgrid=False, categoryorder="array", categoryarray=order),
        legend=dict(orientation="h", traceorder="normal", yanchor="top", y=-0.08, xanchor="center", x=0.5),
        height=max(360, 26 * len(order) + 120),
    )
    return fig


def bar_chart_team_stacked(df_raw, metric_col, catalog: MetricCatalog = DEFAULT_CATALOG):
    """Empilé par jour (onglet Semaine). df_raw = lignes brutes de la semaine (Globale)."""
    d = df_raw.copy()
    d["__day"] = d["session_date"].dt.date
    days = sorted(d["__day"].dropna().unique())
    seg_label = {dd: f"{JOURS_FR[dd.weekday()]} {dd.strftime('%d/%m')}" for dd in days}
    return stacked_team_hbar(d, metric_col, "__day", days, seg_label, catalog)


def bar_chart_team_stacked_periods(df_periods, metric_col, catalog: MetricCatalog = DEFAULT_CATALOG):
    """Empilé par période Catapult (onglet Séance). df_periods = lignes period != Globale
    d'une même séance (déjà filtrées poste le cas échéant)."""
    order = df_periods.drop_duplicates("period").sort_values("period_start")["period"].tolist()
    return stacked_team_hbar(df_periods, metric_col, "period", order, {p: p for p in order}, catalog)


def _add_duration_column(df_in, duration_source=None):
    """Ajoute le temps cumulé par joueuse aux cartes semaine/période.

    Les agrégats de charge n'emportent volontairement pas ``duration_min``.
    La carte, elle, en a besoin pour reproduire le tableau de séance, sans
    modifier l'agrégation partagée avec les exports PDF.
    """
    out = df_in.copy()
    if "duration_min" in out.columns or duration_source is None:
        return out
    if not {"full_name", "duration_min"}.issubset(duration_source.columns):
        return out
    duration = (
        duration_source[["full_name", "duration_min"]]
        .assign(duration_min=lambda d: pd.to_numeric(d["duration_min"], errors="coerce"))
        .groupby("full_name", as_index=False)["duration_min"].sum(min_count=1)
    )
    return out.merge(duration, on="full_name", how="left")


def _platform_heatmap_styles(column, catalog):
    """Réutilise les couleurs métier sans dépendre du libellé affiché court."""
    label = catalog.labels.get(column.name, column.name)
    return heatmap_styles(column.rename(label))



def render_heatmap(
    df_in,
    season_vmax=None,
    catalog: MetricCatalog = DEFAULT_CATALOG,
    player_column_width="medium",
    wrap_headers=False,
    duration_source=None,
):
    """Carte de charge équipe, au code visuel calé sur les rapports PDF.

    Rendu via AG Grid (DOM, pas canvas) plutôt que st.dataframe : la table
    reste nette à tout niveau de zoom/écran et garde le tri par colonne --
    un st.dataframe/ProgressColumn rendait cette carte spécifiquement floue
    (glide-data-grid rasterise tout son contenu sur un <canvas>, contrairement
    au reste de l'app qui est vectoriel/DOM).

    ``duration_source`` contient les lignes brutes pour les cumuls semaine et
    période, afin d'afficher le temps cumulé sans changer leurs agrégats.
    Une colonne ``<col>__fieldwiz`` de ``df_in`` (semaine D1 + U19, comptage
    FieldWiz gardé à part) s'affiche juste après ``<col>``, alors titrée
    Catapult, avec son propre dégradé.
    """
    df_in = _add_duration_column(df_in, duration_source)
    heatmap_order = _HEATMAP_ORDER if catalog is DEFAULT_CATALOG else catalog.metric_cols
    # speed_max_ms fait doublon avec speed_max_kmh (même mesure, autre unité) --
    # catalogue U19 seul concerné, _HEATMAP_ORDER (D1) ne l'a jamais listé.
    heatmap_cols = [c for c in heatmap_order if c in df_in.columns and c != "speed_max_ms"]
    heatmap_cols = [x for c in heatmap_cols
                    for x in ([c, f"{c}{SUFFIXE_FIELDWIZ}"] if f"{c}{SUFFIXE_FIELDWIZ}" in df_in.columns else [c])]
    capteurs_separes = {c.removesuffix(SUFFIXE_FIELDWIZ) for c in heatmap_cols if c.endswith(SUFFIXE_FIELDWIZ)}

    def _base(col):
        return col.removesuffix(SUFFIXE_FIELDWIZ)
    if not heatmap_cols:
        st.info("Pas assez de métriques disponibles.")
        return
    has_position = "position" in df_in.columns
    sort_col = "distance_m" if "distance_m" in df_in.columns else heatmap_cols[0]
    has_duration = "duration_min" in df_in.columns
    base_cols = (["duration_min"] if has_duration else []) + ["full_name"] + (["position"] if has_position else [])
    hm = df_in[base_cols + heatmap_cols].copy().sort_values(sort_col, ascending=False)

    pct_col = None
    if season_vmax is not None and "speed_max_kmh" in heatmap_cols:
        pct_col = "% Vmax saison"
        hm[pct_col] = hm.apply(
            lambda r: r["speed_max_kmh"] / season_vmax[r["full_name"]] * 100
            if r["full_name"] in season_vmax.index
               and pd.notna(r["speed_max_kmh"]) and pd.notna(season_vmax[r["full_name"]])
               and season_vmax[r["full_name"]] != 0
            else float("nan"),
            axis=1,
        )

    if pct_col:
        vmax_label = "speed_max_kmh"
        cols = list(hm.columns)
        cols.remove(pct_col)
        cols.insert(cols.index(vmax_label) + 1, pct_col)
        hm = hm[cols]

    # La ligne moyenne donne le même point de repère que sur les rapports.
    average_cols = (["duration_min"] if has_duration else []) + heatmap_cols + ([pct_col] if pct_col else [])
    average = {col: pd.to_numeric(hm[col], errors="coerce").mean() for col in average_cols}
    average.update({"full_name": "MOYENNE ÉQUIPE"})
    if has_position:
        average["position"] = ""
    hm = pd.concat([hm, pd.DataFrame([average])], ignore_index=True)
    is_average = hm["full_name"] == "MOYENNE ÉQUIPE"
    # Rendu : l'original passe par AG Grid (streamlit-aggrid, absent de cette app) ;
    # ici st.dataframe stylé avec les mêmes couleurs métier (heatmap_styles) et légende.
    _lib = {c: catalog.labels.get(c.removesuffix(SUFFIXE_FIELDWIZ), c) for c in heatmap_cols}
    _lib.update({"full_name": "Joueuse", "duration_min": "Temps (min)", "position": "Poste"})
    aff = hm.rename(columns=_lib)
    num = [_lib[c] for c in heatmap_cols]
    fmt = {_lib[c]: f"{{:{catalog.format_for(c.removesuffix(SUFFIXE_FIELDWIZ))}}}" for c in heatmap_cols}
    if has_duration:
        fmt["Temps (min)"] = "{:.0f}"
    if pct_col:
        fmt[pct_col] = "{:.0f} %"
    styled = (aff.style.format(fmt, na_rep="")
              .apply(heatmap_styles, subset=num)
              .apply(lambda r: ["font-weight:700; border-top:2px solid #888" if is_average.iloc[r.name] else ""
                                for _ in r], axis=1))
    st.dataframe(styled, width="stretch", hide_index=True,
                 height=min(38 * (len(aff) + 1) + 4, 900))
    st.markdown(HEATMAP_LEGEND, unsafe_allow_html=True)


def render_comparison_heatmap(df_compare, label_col, catalog: MetricCatalog = DEFAULT_CATALOG):
    heatmap_order = _HEATMAP_ORDER if catalog is DEFAULT_CATALOG else catalog.metric_cols
    heatmap_cols = [c for c in heatmap_order if c in df_compare.columns]
    if not heatmap_cols:
        return
    hm = df_compare[[label_col] + heatmap_cols].copy()
    hm = hm.rename(columns={label_col: "Séance", **catalog.labels})
    num_cols = [catalog.labels[c] for c in heatmap_cols]
    styled = (
        hm.style
        .format({catalog.labels[c]: f"{{:{catalog.format_for(c)}}}" for c in heatmap_cols})
        .apply(heatmap_styles, subset=num_cols)
    )
    st.dataframe(styled, width="stretch", hide_index=True)
    st.markdown(HEATMAP_LEGEND, unsafe_allow_html=True)


def week_label(df, w, week_col="week", date_col="session_date"):
    sub = df[df[week_col] == w][date_col]
    return f"{sub.min().strftime('%d/%m')} – {sub.max().strftime('%d/%m')}"


