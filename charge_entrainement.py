"""
Suivi de la charge d'entraînement GPS — module Streamlit pour l'app Paris FC.

Utilisation dans l'app :
    from gps_compilation import compiler_sessions_gps
    from charge_entrainement import render_charge_tab
    render_charge_tab(compiler_sessions_gps(["data/gps", "data/gps_match"]))

Autonome (test) :
    streamlit run charge_entrainement.py -- data/gps data/gps_match

Principes :
- Charge quotidienne par joueuse = somme de toutes ses lignes du jour (match, séance,
  compléments post-match, séances individuelles).
- Référence match individuelle : médiane de ses matchs (≥ 60 min) ramenée à 90 min,
  calculée séparément pour chaque système GPS (A = 2025-26, B = 2026-27), car les
  deux systèmes ne comptent pas les accélérations de la même façon. Toute charge est
  exprimée en « % d'un match » → comparable entre saisons et entre indicateurs.
- ACWR par moyennes mobiles exponentielles (EWMA, 7 j aiguë / 28 j chronique).
- Semaine = microcycle du lundi au dimanche : charge, variation vs semaine précédente,
  monotonie et contrainte (Foster).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ----------------------------------------------------------------------------
# Schéma attendu (une ligne = une joueuse × une session GPS)
# ----------------------------------------------------------------------------
SCHEMA = {
    "date": "datetime64 — jour de la session",
    "saison": "str — ex. 2025-26",
    "type_session": "str — 'Match' | 'Entraînement'",
    "ligne": "str — 'principal' | 'complement' | 'individuel' | 'ajout_manuel'",
    "equipe": "str — U19, U18, U23, GF1, GF2…",
    "session_id": "str — identifiant unique de la session",
    "session_label": "str — libellé lisible",
    "joueuse": "str — nom harmonisé",
    "temps_min": "float",
    "distance_m": "float",
    "p1_m…p6_m": "float — distance par plage (0-7, 7-13, 13-15, 15-19, 19-23, >23 km/h)",
    "acc2, acc3, dec2, dec3": "float — nb d'accélérations / décélérations",
    "systeme": "str — 'A' (2025-26) | 'B' (2026-27)",
    "qualite_ok": "bool — False si ligne signalée suspecte",
}

METRICS = {
    "Distance totale": ["distance_m"],
    "Distance > 13 km/h": ["p3_m", "p4_m", "p5_m", "p6_m"],
    "Distance > 19 km/h (HSR)": ["p5_m", "p6_m"],
    "Sprint > 23 km/h": ["p6_m"],
    "Accél. + décél. > 3 m/s²": ["acc3", "dec3"],
}
ACWR_ZONES = [(0, 0.8, "Sous-charge", "#9DB4D3"), (0.8, 1.3, "Zone cible", "#8CC084"),
              (1.3, 1.5, "Vigilance", "#F2C14E"), (1.5, 9, "Risque", "#E4572E")]
TYPE_COLORS = {"Match": "#1F2A44", "Entraînement": "#4C7BD9", "Complément": "#A0AEC0"}
LAMBDA_A, LAMBDA_C = 2 / (7 + 1), 2 / (28 + 1)
GAP_RESET = 21  # jours sans aucune session GPS → redémarrage de l'historique


# ----------------------------------------------------------------------------
# Calculs
# ----------------------------------------------------------------------------
def prepare_sessions(df: pd.DataFrame, inclure_ajouts: bool = True, exclure_suspects: bool = False) -> pd.DataFrame:
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"]).dt.normalize()
    if not inclure_ajouts:
        d = d[d["ligne"] != "ajout_manuel"]
    if exclure_suspects and "qualite_ok" in d:
        d = d[d["qualite_ok"]]
    for name, cols in METRICS.items():
        d[name] = d[cols].sum(axis=1, min_count=1)
    d["type_affiche"] = np.where(d["ligne"].isin(["complement"]), "Complément", d["type_session"])
    return d


def match_reference(d: pd.DataFrame, metric: str, min_minutes: float = 60) -> pd.DataFrame:
    """Médiane par joueuse × système de la charge d'un match ramenée à 90 min.
    Repli sur la médiane de l'équipe si la joueuse a moins de 3 matchs de référence."""
    m = d[(d.type_session == "Match") & (d.ligne == "principal") & (d.temps_min >= min_minutes)].copy()
    m["per90"] = m[metric] / m["temps_min"] * 90
    ind = m.groupby(["joueuse", "systeme"]).agg(ref=("per90", "median"), n_ref=("per90", "size")).reset_index()
    team = m.groupby("systeme")["per90"].median().rename("ref_equipe").reset_index()
    pairs = d[["joueuse", "systeme"]].drop_duplicates().merge(ind, how="left").merge(team, how="left")
    use_team = pairs["n_ref"].fillna(0) < 3
    pairs["ref_utilisee"] = np.where(use_team, pairs["ref_equipe"], pairs["ref"])
    pairs["source_ref"] = np.where(use_team, "équipe", "individuelle")
    return pairs


def daily_load(d: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Charge quotidienne en % d'un match, série complète (jours sans session = 0)."""
    ref = match_reference(d, metric)
    x = d.merge(ref[["joueuse", "systeme", "ref_utilisee"]], on=["joueuse", "systeme"], how="left")
    x["pct_match"] = x[metric] / x["ref_utilisee"] * 100
    day = x.groupby(["joueuse", "date"]).agg(charge=(metric, "sum"), pct_match=("pct_match", "sum"),
                                             n_sessions=("session_id", "nunique"),
                                             match=("type_session", lambda s: (s == "Match").any())).reset_index()
    out = []
    fin_globale = d.date.max()
    for j, g in day.groupby("joueuse"):
        # une interruption de plus de GAP_RESET jours (intersaison, blessure, changement de groupe
        # sans GPS…) redémarre le calcul : l'historique chronique n'a plus de sens après un tel trou.
        seg = (g.date.diff().dt.days > GAP_RESET).cumsum()
        for k, gs in g.groupby(seg):
            fin = min(gs.date.max() + pd.Timedelta(days=14), fin_globale)
            idx = pd.date_range(gs.date.min(), fin, freq="D")
            gs = gs.set_index("date").reindex(idx)
            gs["joueuse"] = j
            gs[["charge", "pct_match", "n_sessions"]] = gs[["charge", "pct_match", "n_sessions"]].fillna(0)
            gs["match"] = gs["match"].fillna(False).astype(bool)
            a = c = None
            acute, chronic = [], []
            for v in gs["pct_match"].to_numpy():   # EWMA (Williams et al., 2017)
                a = v if a is None else LAMBDA_A * v + (1 - LAMBDA_A) * a
                c = v if c is None else LAMBDA_C * v + (1 - LAMBDA_C) * c
                acute.append(a); chronic.append(c)
            gs["aigue"], gs["chronique"] = acute, chronic
            gs["jours_historique"] = np.arange(1, len(gs) + 1)
            gs["acwr"] = np.where((gs["jours_historique"] >= 21) & (gs["chronique"] > 0), gs["aigue"] / gs["chronique"], np.nan)
            gs["segment"] = k
            out.append(gs.rename_axis("date").reset_index())
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def weekly_load(daily: pd.DataFrame) -> pd.DataFrame:
    w = daily.assign(semaine=daily.date.dt.to_period("W-SUN").dt.start_time)
    agg = w.groupby(["joueuse", "semaine"]).agg(
        charge_pct=("pct_match", "sum"), charge=("charge", "sum"), sessions=("n_sessions", "sum"),
        matchs=("match", "sum"), moy_j=("pct_match", "mean"), sd_j=("pct_match", "std"),
        acwr_fin=("acwr", "last")).reset_index()
    agg["monotonie"] = np.where(agg.sd_j > 0, agg.moy_j / agg.sd_j, np.nan)
    agg["contrainte"] = agg.charge_pct * agg.monotonie
    agg = agg.sort_values(["joueuse", "semaine"])
    agg["var_pct"] = agg.groupby("joueuse").charge_pct.pct_change() * 100
    agg.loc[agg.groupby("joueuse").charge_pct.shift(1) == 0, "var_pct"] = np.nan
    agg.loc[agg.sessions == 0, ["var_pct", "monotonie", "contrainte"]] = np.nan
    agg[["sessions", "matchs"]] = agg[["sessions", "matchs"]].astype(int)
    agg[["monotonie", "contrainte", "var_pct"]] = agg[["monotonie", "contrainte", "var_pct"]].astype(float)
    return agg


def _f(fmt):
    return lambda v: "–" if v is None or pd.isna(v) else fmt.format(v)


def zone(acwr: float) -> tuple[str, str]:
    if pd.isna(acwr):
        return "Historique insuffisant", "#E2E8F0"
    for lo, hi, lab, col in ACWR_ZONES:
        if lo <= acwr < hi:
            return lab, col
    return "Risque", "#E4572E"


def alertes(row) -> str:
    if row.sessions == 0:
        return "Aucune donnée GPS cette semaine"
    a = []
    if pd.notna(row.acwr_fin) and row.acwr_fin >= 1.5:
        a.append("ACWR ≥ 1,5")
    elif pd.notna(row.acwr_fin) and row.acwr_fin < 0.8:
        a.append("ACWR < 0,8")
    if pd.notna(row.var_pct) and row.var_pct > 30:
        a.append(f"+{row.var_pct:.0f} % vs S-1")
    if pd.notna(row.monotonie) and row.monotonie > 2:
        a.append("Monotonie > 2")
    return " · ".join(a)


# ----------------------------------------------------------------------------
# Interface
# ----------------------------------------------------------------------------

def _show(df: pd.DataFrame, fmts: dict, couleur_acwr: str | None = None) -> None:
    """Affiche un tableau déjà trié ; valeurs formatées en texte ('–' si manquant)."""
    out = df.copy()
    for col, f in fmts.items():
        if col in out:
            out[col] = [f(v) for v in df[col]]
    for col in out.columns:
        if out[col].dtype == object or str(out[col].dtype).startswith("str"):
            out[col] = out[col].fillna("")
    sty = out.style
    if couleur_acwr and couleur_acwr in df:
        cols = [f"background-color:{zone(v)[1]}33" for v in df[couleur_acwr]]
        sty = sty.apply(lambda s: cols, subset=[couleur_acwr])
    st.dataframe(sty, hide_index=True, width="stretch")


def _break_segments(g: pd.DataFrame) -> pd.DataFrame:
    """Insère une ligne vide entre deux segments pour ne pas relier les courbes à travers une interruption."""
    if "segment" not in g or g.segment.nunique() <= 1:
        return g
    parts = []
    for _, gs in g.groupby("segment"):
        parts.append(gs)
        parts.append(pd.DataFrame({"date": [gs.date.max() + pd.Timedelta(days=1)]}))
    return pd.concat(parts, ignore_index=True)


def _acwr_chart(g: pd.DataFrame, titre: str) -> go.Figure:
    g = _break_segments(g)
    fig = go.Figure()
    for lo, hi, lab, col in ACWR_ZONES:
        fig.add_hrect(y0=lo, y1=min(hi, 2.2), fillcolor=col, opacity=0.15, line_width=0,
                      annotation_text=lab, annotation_position="right", annotation_font_size=10)
    fig.add_trace(go.Scatter(x=g.date, y=g.acwr, mode="lines", line=dict(color="#1F2A44", width=2), name="ACWR"))
    fig.update_layout(title=titre, height=260, margin=dict(l=10, r=80, t=40, b=10), yaxis=dict(range=[0, 2.2]),
                      showlegend=False, plot_bgcolor="white")
    return fig


def _daily_chart(sess: pd.DataFrame, g: pd.DataFrame, metric: str) -> go.Figure:
    fig = go.Figure()
    for t, col in TYPE_COLORS.items():
        s = sess[sess.type_affiche == t]
        if len(s):
            fig.add_trace(go.Bar(x=s.date, y=s.pct_match, name=t, marker_color=col,
                                 customdata=np.stack([s.session_label, s[metric].round(0)], axis=1),
                                 hovertemplate="%{x|%d/%m}<br>%{customdata[0]}<br>%{y:.0f} % d'un match<br>%{customdata[1]}<extra></extra>"))
    g = _break_segments(g)
    fig.add_trace(go.Scatter(x=g.date, y=g.aigue, name="Aiguë (EWMA 7 j)", line=dict(color="#E4572E", width=2)))
    fig.add_trace(go.Scatter(x=g.date, y=g.chronique, name="Chronique (EWMA 28 j)", line=dict(color="#1F2A44", width=2, dash="dot")))
    fig.update_layout(barmode="stack", height=340, margin=dict(l=10, r=10, t=30, b=10), plot_bgcolor="white",
                      yaxis_title="% d'un match", legend=dict(orientation="h", y=1.12))
    return fig


def render_charge_tab(df: pd.DataFrame) -> None:
    st.subheader("Suivi de la charge d'entraînement")
    c1, c2, c3, c4 = st.columns([2, 2, 1.3, 1.3])
    metric = c1.selectbox("Indicateur", list(METRICS), index=2)
    equipes = sorted(df["equipe"].dropna().unique())
    equipe = c2.multiselect("Équipe / groupe (définit l'effectif)", equipes, default=[e for e in equipes if e.startswith("U19")] or equipes[:1],
                            help="Joueuses ayant au moins une session dans ces équipes/groupes. Leur charge inclut toutes leurs sessions, quel que soit le groupe.")
    inclure_ajouts = c3.toggle("Lignes ajoutées à la main", value=True,
                               help="Lignes sans date ni capteur ajoutées en fin de fichier (souvent des estimations).")
    exclure = c4.toggle("Exclure lignes suspectes", value=False)

    d = prepare_sessions(df, inclure_ajouts, exclure)
    daily = daily_load(d, metric)
    weekly = weekly_load(daily)
    ref = match_reference(d, metric)
    x = d.merge(ref[["joueuse", "systeme", "ref_utilisee"]], on=["joueuse", "systeme"], how="left")
    x["pct_match"] = x[metric] / x["ref_utilisee"] * 100

    # L'équipe/groupe définit l'effectif suivi ; la charge de chaque joueuse inclut toutes ses sessions
    # (ex. une U19 qui s'entraîne en GF1 et joue en U19).
    effectif = sorted(x[x.equipe.isin(equipe)].joueuse.unique())
    xe = x[x.joueuse.isin(effectif)]

    tab_eq, tab_j, tab_s, tab_q = st.tabs(["Équipe — semaine", "Joueuse", "Séance", "Qualité des données"])

    # ---------------- Équipe
    with tab_eq:
        sem_dispo = sorted(xe.date.dt.to_period("W-SUN").dt.start_time.unique())
        if not sem_dispo:
            st.info("Aucune session pour cette sélection."); return
        nb = xe.assign(s=lambda t: t.date.dt.to_period("W-SUN").dt.start_time).groupby("s").session_id.nunique()
        defaut = nb[nb >= 3].index.max() if (nb >= 3).any() else sem_dispo[-1]
        sem = st.select_slider("Semaine (lundi)", options=sem_dispo, value=defaut,
                               format_func=lambda t: pd.Timestamp(t).strftime("%d/%m/%y"))
        sem = pd.Timestamp(sem)
        joueuses = sorted(xe[(xe.date >= sem - pd.Timedelta(days=28)) & (xe.date < sem + pd.Timedelta(days=7))].joueuse.unique())
        w = weekly[(weekly.semaine == sem) & (weekly.joueuse.isin(joueuses))].copy()
        w["Zone ACWR"] = w.acwr_fin.map(lambda v: zone(v)[0])
        w["Alertes"] = w.apply(alertes, axis=1)
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Joueuses avec données", f"{int((w.sessions > 0).sum())} / {len(w)}")
        k2.metric("Charge médiane", f"{w[w.sessions > 0].charge_pct.median():.0f} % match" if (w.sessions > 0).any() else "–",
                  help="Joueuses ayant au moins une session GPS dans la semaine")
        k3.metric("En zone de risque", int((w.acwr_fin >= 1.5).sum()))
        k4.metric("En sous-charge", int((w.acwr_fin < 0.8).sum()))
        tab = w[["joueuse", "sessions", "matchs", "charge_pct", "var_pct", "acwr_fin", "Zone ACWR", "monotonie", "Alertes"]].rename(columns={
            "joueuse": "Joueuse", "sessions": "Sessions", "matchs": "Matchs", "charge_pct": "Charge (% match)",
            "var_pct": "Δ vs S-1 (%)", "acwr_fin": "ACWR", "monotonie": "Monotonie"}).sort_values("ACWR", ascending=False)
        _show(tab, {"Charge (% match)": _f("{:.0f}"), "Δ vs S-1 (%)": _f("{:+.0f}"), "ACWR": _f("{:.2f}"), "Monotonie": _f("{:.1f}")}, couleur_acwr="ACWR")
        # heatmap 8 dernières semaines
        last = [s for s in sem_dispo if s <= sem][-8:]
        h = weekly[weekly.semaine.isin(last) & weekly.joueuse.isin(joueuses)].pivot(index="joueuse", columns="semaine", values="charge_pct")
        if len(h):
            fig = go.Figure(go.Heatmap(z=h.values, x=[pd.Timestamp(c).strftime("%d/%m") for c in h.columns], y=h.index,
                                       colorscale="Blues", colorbar_title="% match", hovertemplate="%{y}<br>Sem. %{x}<br>%{z:.0f} % d'un match<extra></extra>",
                                       text=np.where(np.isnan(h.values), "", np.round(np.nan_to_num(h.values)).astype(int).astype(str)),
                                       texttemplate="%{text}"))
            fig.update_layout(title=f"{metric} — charge hebdomadaire (% d'un match), 8 dernières semaines",
                              height=max(300, 26 * len(h)), margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, width="stretch")

    # ---------------- Joueuse
    with tab_j:
        js = effectif or sorted(x.joueuse.unique())
        j = st.selectbox("Joueuse", js)
        g = daily[daily.joueuse == j]
        s = x[x.joueuse == j]
        dmin, dmax = g.date.min().date(), g.date.max().date()
        p = st.slider("Période", min_value=dmin, max_value=dmax, value=(max(dmin, dmax - pd.Timedelta(days=90).to_pytimedelta()), dmax), format="DD/MM/YY")
        g = g[(g.date.dt.date >= p[0]) & (g.date.dt.date <= p[1])]
        s = s[(s.date.dt.date >= p[0]) & (s.date.dt.date <= p[1])]
        r = ref[ref.joueuse == j]
        last = g.tail(1)
        if len(last) and pd.isna(last.acwr.iloc[0]):
            last = last.iloc[0:0]
        a1, a2, a3 = st.columns(3)
        a1.metric("ACWR actuel", f"{last.acwr.iloc[0]:.2f}" if len(last) else "–")
        if not len(last) and len(g):
            a1.caption(f"Historique insuffisant : {int(g.jours_historique.iloc[-1])} j depuis la reprise des données (21 j requis).")
        if len(last):
            zl, zc = zone(last.acwr.iloc[0])
            a1.markdown(f"<span style='background:{zc};padding:2px 8px;border-radius:8px;font-size:0.85em'>{zl}</span>"
                        f" <span style='color:#718096;font-size:0.8em'>au {last.date.iloc[0]:%d/%m/%y}</span>", unsafe_allow_html=True)
        a2.metric("Charge 7 derniers jours", f"{g.tail(7).pct_match.sum():.0f} % match")
        a3.metric("Référence match (/90 min)", " · ".join(f"{sy} : {v:,.0f}" for sy, v in zip(r.systeme, r.ref_utilisee) if pd.notna(v)).replace(",", " "),
                  help="Médiane de ses matchs ≥ 60 min ramenés à 90 min ; repli sur la référence équipe si < 3 matchs.")
        st.plotly_chart(_daily_chart(s, g, metric), width="stretch")
        st.plotly_chart(_acwr_chart(g, "ACWR (EWMA 7/28 j)"), width="stretch")
        ww = weekly[(weekly.joueuse == j) & (weekly.semaine.dt.date >= p[0])].sort_values("semaine", ascending=False)
        _show(ww[["semaine", "sessions", "matchs", "charge_pct", "var_pct", "acwr_fin", "monotonie", "contrainte"]].rename(columns={
            "semaine": "Semaine", "sessions": "Sessions", "matchs": "Matchs", "charge_pct": "Charge (% match)", "var_pct": "Δ vs S-1 (%)",
            "acwr_fin": "ACWR", "monotonie": "Monotonie", "contrainte": "Contrainte"}),
            {"Semaine": lambda t: t.strftime("%d/%m/%y"), "Charge (% match)": _f("{:.0f}"), "Δ vs S-1 (%)": _f("{:+.0f}"), "ACWR": _f("{:.2f}"),
             "Monotonie": _f("{:.1f}"), "Contrainte": _f("{:.0f}")}, couleur_acwr="ACWR")

    # ---------------- Séance
    with tab_s:
        ss = x[x.equipe.isin(equipe)].drop_duplicates("session_id").sort_values("date", ascending=False)
        sid = st.selectbox("Session", ss.session_id, format_func=lambda i: f"{ss.set_index('session_id').date[i]:%d/%m/%y} — {ss.set_index('session_id').session_label[i]} ({ss.set_index('session_id').equipe[i]})")
        v = x[x.session_id == sid].sort_values("pct_match")
        fig = go.Figure(go.Bar(x=v.pct_match, y=v.joueuse, orientation="h",
                               marker_color=np.where(v.ligne == "principal", "#4C7BD9", "#A0AEC0"),
                               text=[f"{p:.0f} %" for p in v.pct_match], textposition="outside",
                               hovertemplate="%{y}<br>%{x:.0f} % d'un match<extra></extra>"))
        fig.add_vline(x=100, line_dash="dot", annotation_text="1 match")
        fig.update_layout(title=f"{metric} — % de la référence match", height=max(300, 26 * len(v)), margin=dict(l=10, r=40, t=40, b=10), plot_bgcolor="white")
        st.plotly_chart(fig, width="stretch")
        cols = ["joueuse", "ligne", "temps_min", "distance_m", "p5_m", "p6_m", "acc3", "dec3", "pct_match", "remarques"]
        _show(v[cols].rename(columns={"joueuse": "Joueuse", "ligne": "Ligne", "temps_min": "Temps (min)", "distance_m": "Distance (m)",
                                     "p5_m": "19-23 km/h (m)", "p6_m": ">23 km/h (m)", "acc3": "Accél. >3", "dec3": "Décél. >3",
                                     "pct_match": "% match", "remarques": "Remarques"}),
              {"Temps (min)": _f("{:.0f}"), "Distance (m)": lambda v: "–" if pd.isna(v) else f"{v:,.0f}".replace(",", " "), "19-23 km/h (m)": _f("{:.0f}"),
               ">23 km/h (m)": _f("{:.0f}"), "% match": _f("{:.0f}"), "Accél. >3": _f("{:.0f}"), "Décél. >3": _f("{:.0f}")})

    # ---------------- Qualité
    with tab_q:
        q = d[d.joueuse.isin(effectif)]
        st.markdown(f"**{q.session_id.nunique()} sessions**, {len(q)} lignes joueuse, "
                    f"{(~q.qualite_ok).sum()} lignes signalées suspectes, {(q.ligne == 'ajout_manuel').sum()} lignes ajoutées à la main.")
        cov = q.assign(semaine=q.date.dt.to_period("W-SUN").dt.start_time).groupby(["semaine", "type_session"]).session_id.nunique().unstack(fill_value=0)
        fig = go.Figure([go.Bar(x=cov.index, y=cov[c], name=c, marker_color=TYPE_COLORS.get(c)) for c in cov.columns])
        fig.update_layout(barmode="stack", title="Sessions GPS par semaine (un creux = semaine sans données, pas forcément sans entraînement)",
                          height=280, margin=dict(l=10, r=10, t=40, b=10), plot_bgcolor="white")
        st.plotly_chart(fig, width="stretch")
        st.dataframe(q[~q.qualite_ok][["date", "session_label", "joueuse", "remarques"]].sort_values("date", ascending=False)
                     .assign(date=lambda t: t.date.dt.strftime("%d/%m/%y")).rename(columns={"date": "Date", "session_label": "Session", "joueuse": "Joueuse", "remarques": "Remarques"}),
                     hide_index=True, width="stretch")

    with st.expander("Méthode et limites"):
        st.markdown(f"""
- **% d'un match** : charge de la session ÷ référence match de la joueuse (médiane de ses matchs ≥ 60 min, ramenés à 90 min,
  calculée séparément pour chaque système GPS). Repli sur la médiane de l'équipe si moins de 3 matchs.
- **ACWR** : EWMA aiguë 7 j (λ = {LAMBDA_A:.3f}) / chronique 28 j (λ = {LAMBDA_C:.3f}) sur la charge quotidienne en % match ;
  affiché après 21 jours d'historique ; redémarré après {GAP_RESET} jours sans aucune donnée (intersaison, absence). Zones indicatives 0,8–1,3 / 1,3–1,5 / > 1,5 : l'ACWR est un outil de suivi,
  pas un prédicteur de blessure validé — à croiser avec le ressenti (RPE, wellness) et le staff médical.
- **Jours sans données GPS = 0** : une séance non enregistrée fait baisser la charge aiguë. Vérifier l'onglet « Qualité des données ».
- **Monotonie** = moyenne / écart-type de la charge quotidienne sur 7 jours ; **contrainte** = charge hebdomadaire × monotonie (Foster, 1998).
""")


# ----------------------------------------------------------------------------
if __name__ == "__main__":
    # Test autonome : streamlit run charge_entrainement.py -- data/gps data/gps_match
    import sys
    from gps_compilation import compiler_sessions_gps
    st.set_page_config(page_title="Charge d'entraînement", layout="wide")
    dossiers = sys.argv[1:] or ["data/gps", "data/gps_match"]
    render_charge_tab(st.cache_data(compiler_sessions_gps)(dossiers))
