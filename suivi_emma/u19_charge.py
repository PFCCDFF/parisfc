# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/u19_charge.py, version du 14/10/2026).
# Seuls les imports ont été adaptés ; seuil de séance collective recopié (5) ; template appliqué à la figure. Garder la logique alignée sur l'original.
"""
utils/u19_charge.py

Graphique de charge des vues U19 (équipe et individuelle, onglets Séance,
Semaine et Période) : une barre de distance totale par jour ou par semaine,
plus des lignes de haute intensité et d'accélérations / décélérations, chacune
sur son axe.

Équipe : seules les séances collectives comptent (au moins
SEUIL_SEANCE_COLLECTIVE joueuses avec une ligne GPS), pour écarter les
séances individuelles (réathlé, compensation à deux) et les séances D1 des
quelques joueuses mappées. Valeur d'un jour ou d'une semaine = cumul de
chaque joueuse sur ces séances, puis moyenne des joueuses présentes au moins
une fois. Individuel : toutes les séances de la joueuse (sa charge réelle).

FieldWiz n'exporte pas le découpage 13-15 / 15-19 km/h : il est déduit à
l'import (``dist_13_15`` / ``dist_15_19``) mais pas affiché ici, le graphique
garde la bande 13-19 (``dist_13_19``).
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from suivi_emma.chart import appliquer_template

SEUIL_SEANCE_COLLECTIVE = 5   # utils/talent_u19_monthly.py (app d'Emma)

JOURS = {0: "Lun", 1: "Mar", 2: "Mer", 3: "Jeu", 4: "Ven", 5: "Sam", 6: "Dim"}

# (colonne, libellé, couleur, axe) ; axe 1 = barres, 2 à 4 = axes de droite.
CHARGE_METRICS = (
    ("distance_m", "Distance totale", "#0680BE", 1),
    ("dist_13_19", "13-19 km/h", "#3DA5D9", 2),
    ("dist_19_23", "19-23 km/h", "#F2A900", 3),
    ("sprint_distance", ">23 km/h", "#BD3032", 3),
    ("accel_gt2_count", "Accél. >2 m/s²", "#00A676", 4),
    ("decel_gt2_count", "Décél. >2 m/s²", "#6C5B7B", 4),
)

_AXES = {
    2: "13-19 km/h (m)",
    3: "19-23 / >23 km/h (m)",
    4: "Accél. / Décél. (nb)",
}


def _lundi(date):
    date = pd.Timestamp(date).normalize()
    return date - pd.Timedelta(days=date.weekday())


def charge_par_periode(df, granularite="semaine", debut=None, fin=None,
                       collectives=True, seuil=SEUIL_SEANCE_COLLECTIVE):
    """Une ligne par jour (``granularite="jour"``, jours avec séance seulement)
    ou par semaine (lundi ; de la première semaine avec séance à celle de
    ``fin``, semaines vides du milieu gardées en NaN pour un axe continu).

    ``collectives`` : ne garder que les séances d'au moins ``seuil`` joueuses
    (vue équipe) ; False pour une joueuse seule.

    Colonnes : ``cle`` (Timestamp du jour ou du lundi), ``label``,
    ``has_match``, ``nb_seances``, ``nb_joueuses`` et une colonne par
    métrique de CHARGE_METRICS présente dans ``df``."""
    colonnes = [col for col, _, _, _ in CHARGE_METRICS if col in df.columns]
    vide = pd.DataFrame(columns=["cle", "label", "has_match", "nb_seances", "nb_joueuses"] + colonnes)
    if df.empty or not {"session_id", "joueuse_id", "session_date"} <= set(df.columns):
        return vide

    src = df.copy()
    if debut is not None:
        src = src[src["session_date"] >= pd.Timestamp(debut)]
    if fin is not None:
        src = src[src["session_date"] < pd.Timestamp(fin) + pd.Timedelta(days=1)]
    if collectives:
        nb_par_seance = src.groupby("session_id")["joueuse_id"].nunique()
        src = src[src["session_id"].isin(nb_par_seance[nb_par_seance >= seuil].index)]
    if src.empty:
        return vide

    src = src.copy()
    jour = src["session_date"].dt.normalize()
    src["cle"] = jour if granularite == "jour" else jour - pd.to_timedelta(jour.dt.weekday, unit="D")
    for col in colonnes:
        src[col] = pd.to_numeric(src[col], errors="coerce")
    if "session_type" not in src.columns:
        src["session_type"] = None

    par_joueuse = src.groupby(["cle", "joueuse_id"], as_index=False)[colonnes].sum(min_count=1)
    out = par_joueuse.groupby("cle")[colonnes].mean()
    out["nb_joueuses"] = par_joueuse.groupby("cle")["joueuse_id"].nunique()
    out["nb_seances"] = src.groupby("cle")["session_id"].nunique()
    out["has_match"] = src.groupby("cle")["session_type"].apply(lambda types: (types == "MATCH").any())
    out = out.reset_index()

    if granularite != "jour":
        derniere = _lundi(fin) if fin is not None else out["cle"].max()
        semaines = pd.DataFrame({"cle": pd.date_range(out["cle"].min(), max(derniere, out["cle"].min()), freq="7D")})
        out = semaines.merge(out, on="cle", how="left")
        out["has_match"] = out["has_match"].astype("boolean").fillna(False).astype(bool)
    out[["nb_seances", "nb_joueuses"]] = out[["nb_seances", "nb_joueuses"]].fillna(0).astype(int)

    if granularite == "jour":
        out["label"] = [f"{JOURS[d.weekday()]} {d.strftime('%d/%m')}" for d in out["cle"]]
    else:
        out["label"] = [f"S{d.isocalendar()[1]}<br>{d.strftime('%d/%m')}" for d in out["cle"]]
    return out


def figure_charge(frame, equipe=True, selected_idx=None, height=480):
    """Barres de distance totale (axe gauche) et lignes sur trois axes de
    droite : 13-19, 19-23 et >23 ensemble (même ordre de grandeur), puis
    accélérations / décélérations. ``selected_idx`` = position (dans
    ``frame``) marquée d'une ligne verticale rouge ; jours / semaines de match
    en rouge sous l'axe avec un ballon."""
    fig = go.Figure()
    if frame.empty:
        return fig

    metriques = [m for m in CHARGE_METRICS if m[0] in frame.columns]
    axes_droite = sorted({axe for _, _, _, axe in metriques if axe > 1})
    domain_right = 1 - 0.07 * len(axes_droite) if axes_droite else 1.0
    pas = (1 - domain_right) / len(axes_droite) if axes_droite else 0.0
    ref_axe = {1: "y"}
    layout_axes = {}
    for i, axe in enumerate(axes_droite):
        ref_axe[axe] = f"y{i + 2}"
        couleur = next(c for _, _, c, a in metriques if a == axe)
        layout_axes[f"yaxis{i + 2}"] = dict(
            title=dict(text=_AXES[axe], font=dict(color=couleur, size=11)),
            tickfont=dict(color=couleur, size=10),
            overlaying="y", side="right", anchor="free", position=domain_right + pas * i,
            rangemode="tozero", zeroline=False, showgrid=False,
        )

    matchs = frame["has_match"].fillna(False).astype(bool).tolist()
    ticks = [
        f"<span style='fill:#BD3032;font-weight:700'>{lbl}</span>" if m else lbl
        for lbl, m in zip(frame["label"], matchs)
    ]
    # Axe catégoriel sur un titre lisible : c'est lui qu'affiche l'en-tête du survol unifié.
    x = [lbl.replace("<br>", " ") + (" · match" if m else "") for lbl, m in zip(frame["label"], matchs)]
    survol = [
        f"{s} séance{'s' if s > 1 else ''}" + (f", {j} joueuse{'s' if j > 1 else ''}" if equipe else "")
        for s, j in zip(frame["nb_seances"], frame["nb_joueuses"])
    ]
    for col, label, couleur, axe in metriques:
        commun = dict(x=x, y=frame[col], name=label, yaxis=ref_axe[axe])
        if axe == 1:
            fig.add_trace(go.Bar(
                marker_color=couleur, opacity=0.35, customdata=survol,
                hovertemplate=f"{label} : %{{y:.0f}} m (%{{customdata}})<extra></extra>",
                **commun,
            ))
        else:
            fig.add_trace(go.Scatter(
                mode="lines+markers", line=dict(color=couleur, width=2.5),
                marker=dict(size=7, color=couleur), connectgaps=False,
                hovertemplate=f"{label} : %{{y:.0f}}<extra></extra>", **commun,
            ))

    fig.update_layout(
        margin=dict(t=20, b=20, l=60, r=10),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, domain=[0, domain_right], range=[-0.5, len(x) - 0.5],
                   type="category", tickmode="array", tickvals=x, ticktext=ticks),
        yaxis=dict(
            title=dict(text="Distance totale (m)", font=dict(color="#0680BE", size=11)),
            tickfont=dict(color="#0680BE", size=10), rangemode="tozero", zeroline=False,
            gridcolor="rgba(128,128,128,0.12)",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        hovermode="x unified",
        height=height,
        **layout_axes,
    )
    for i, est_match in enumerate(matchs):
        if est_match:
            fig.add_annotation(x=i, xref="x", y=0.97, yref="paper", text="⚽", showarrow=False,
                               font=dict(size=13))
    if selected_idx is not None:
        fig.add_vline(x=selected_idx, line_dash="dot", line_color="rgba(189,48,50,0.6)")
    return fig


def afficher_charge(frame, key, equipe=True, selected_cle=None, vide="Pas de données à afficher."):
    """Affiche ``figure_charge`` dans Streamlit avec sa légende de lecture.
    ``selected_cle`` : jour ou lundi (Timestamp) à marquer."""
    if frame.empty or frame["nb_seances"].sum() == 0:
        st.info(vide)
        return
    selected_idx = None
    if selected_cle is not None:
        positions = [i for i, cle in enumerate(frame["cle"]) if cle == pd.Timestamp(selected_cle).normalize()]
        selected_idx = positions[0] if positions else None
    st.plotly_chart(
        appliquer_template(figure_charge(frame, equipe=equipe, selected_idx=selected_idx)),
        width="stretch", key=key,
        config={"toImageButtonOptions": {"scale": 3, "format": "png"}},
    )
    lecture = (
        f"Séances d'au moins {SEUIL_SEANCE_COLLECTIVE} joueuses · cumul par joueuse, moyenne des joueuses présentes"
        if equipe else "Toutes les séances de la joueuse, cumulées"
    )
    if selected_idx is not None:
        lecture = "Pointillé rouge = sélection · " + lecture
    st.caption(
        f"{lecture} · ⚽ et date rouge = match · clic sur la légende pour masquer une courbe · "
        "13-15 / 15-19 km/h non exportés par FieldWiz (bande 13-19 seule)"
    )
