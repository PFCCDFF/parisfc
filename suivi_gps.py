"""Suivi de la performance → onglets Séance / Semaine / Période (vue individuelle ou équipe).

Reprend les onglets du même nom des vues U19 de l'app d'Emma (paris-fc-charge,
views/vue_individuelle_u19.py et views/vue_equipe_u19.py, version du 14/10/2026), alimentés
par les GPS locaux de l'app (séances data/gps + matchs data/gps_match) au lieu de Supabase.
Les modules graphiques sont portés dans suivi_emma/.

Différences assumées avec l'original :
- pas de découpage par mi-temps (les CSV locaux n'ont que la séance entière) ;
- pas de séances D1 (Catapult) ;
- carte de charge en st.dataframe (pas d'AG Grid) ;
- pas de st.stop() (il couperait le reste de la page hôte) ;
- semaine ISO (%G-S%V) partout, dates « Du » par défaut au début de saison.
"""
import re
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from suivi_emma.chart import appliquer_template
from suivi_emma.date_format import fmt_date
from suivi_emma.metric_selector import render_metric_selector
from suivi_emma.metrics import (
    AGG_MAX_COLS, AGG_MEAN_COLS, AGG_MIN_COLS, ALL_METRIC_COLS, ALL_METRIC_LABELS, METRICS,
    NON_SUMMABLE_COLS, is_summable, metric_fmt, player_aggregation,
)
from suivi_emma.records_match import couleur_pct_record, libelles_records, pct_du_record, record_joueuse
from suivi_emma.session_charts import (
    JOURS_FR, bar_chart_team, bar_chart_team_stacked, profile_bars, profile_bars_seance,
    profile_bars_stacked, render_heatmap, week_label,
)
from suivi_emma.talent_agenda import agenda_seances, detail_distance, detail_nb_joueuses, seances_equipe
from suivi_emma.u19_charge import afficher_charge, charge_par_periode

VUE_INDIV, VUE_EQUIPE = "Vue individuelle", "Vue équipe"
_CFG = {"toImageButtonOptions": {"scale": 3, "format": "png"}}
_INVERSE = ["accel_min"]


# ─────────────────────────────────────────────────────────────────────────────
# Données : GPS locaux → contrat de colonnes des vues d'Emma
# ─────────────────────────────────────────────────────────────────────────────
def _num(df: pd.DataFrame, *cols) -> pd.Series:
    """Première colonne renseignée parmi `cols` (les exports GF1 / v2 n'ont pas les mêmes noms)."""
    out = pd.Series(float("nan"), index=df.index)
    for c in cols:
        if c in df.columns:
            out = out.fillna(pd.to_numeric(df[c], errors="coerce"))
    return out


def _nom_seance(fichier: str) -> str:
    stem = re.sub(r"__[A-Za-z0-9_-]{6,10}$", "", re.sub(r"\.csv$", "", str(fichier), flags=re.I))
    return "Séance" if re.match(r"^\d{4}_\d{2}-\d{2}-\d{4}", stem) else stem.strip()


def _vers_contrat(src: pd.DataFrame, match: bool) -> pd.DataFrame:
    if src is None or src.empty or "Player" not in src.columns:
        return pd.DataFrame()
    d = src.copy()
    d["session_date"] = pd.to_datetime(d.get("DATE"), errors="coerce").dt.tz_localize(None).dt.normalize()
    d = d[d["session_date"].notna() & d["Player"].notna() & (d["Player"].astype(str).str.strip() != "")]
    if d.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=d.index)
    fichier = d["__source_file"].astype(str) if "__source_file" in d.columns else pd.Series("", index=d.index)
    out["session_date"] = d["session_date"]
    out["start_date"] = d["session_date"]
    out["session_id"] = ("M|" if match else "S|") + d["session_date"].dt.strftime("%Y-%m-%d") + "|" + fichier
    if match:
        adv = d["__adversaire"].astype(str).str.strip() if "__adversaire" in d.columns else pd.Series("", index=d.index)
        out["session_name"] = adv.where(adv.ne("") & adv.ne("nan"), "").map(lambda a: f"Match · {a}" if a else "Match")
    else:
        out["session_name"] = fichier.map(_nom_seance)
    out["session_type"] = "MATCH" if match else "TRAINING"
    out["joueuse_id"] = d["Player"].astype(str)
    out["full_name"] = d["Player"].astype(str)
    out["source"] = "U19"
    out["duration_min"] = _num(d, "Durée_min")
    out["distance_m"] = _num(d, "Distance (m)")
    out["speed_avg_mmin"] = (out["distance_m"] / out["duration_min"]).where(out["duration_min"] > 0)
    out["dist_13_15"] = _num(d, "V_13_15", "Distance par plage de vitesse (13-15 km/h)")
    out["dist_15_19"] = _num(d, "V_15_19", "Distance par plage de vitesse (15-19 km/h)")
    out["dist_19_23"] = _num(d, "Distance 19-23 (m)", "V_19_23", "Distance par plage de vitesse (19-23 km/h)")
    out["dist_13_19"] = _num(d, "Distance 13-19 (m)")
    out["dist_13_19"] = out["dist_13_19"].fillna(out["dist_13_15"] + out["dist_15_19"])
    out["sprint_distance"] = _num(d, "Distance >23 (m)")
    out["sprint_count"] = _num(d, "Sprints_23")
    out["speed_max_kmh"] = _num(d, "Vitesse max (km/h)")
    out["accel_max"] = _num(d, "Accélération maximale (m/s²)", "Acc_max")
    out["accel_min"] = float("nan")          # décélération max : absente des exports locaux
    out["accel_gt2_count"] = _num(d, "Acc_2", "# of Accelerations (>2 m/s²)")
    out["decel_gt2_count"] = _num(d, "Dec_2", "# of Decélerations (>2 m/s²)")
    acc3 = _num(d, "Acc_3", "# of Accelerations (>3 m/s²)")
    dec3 = _num(d, "Dec_3", "# of Decélerations (>3 m/s²)")
    out["ad2"] = _num(d, "#accel/decel").fillna(out["accel_gt2_count"] + out["decel_gt2_count"])
    out["ad3"] = acc3 + dec3
    return out


@st.cache_data(ttl=1800, show_spinner=False)
def construire_donnees_suivi(gps_raw: pd.DataFrame, gps_match: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par (séance × joueuse) : séances d'entraînement + matchs."""
    parts = [p for p in (_vers_contrat(gps_match, True), _vers_contrat(gps_raw, False)) if not p.empty]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    # Un match exporté en plusieurs fichiers (un par type de balise : GF1 + nouveau format) :
    # les fichiers sans adversaire rejoignent le match nommé du même jour s'il est unique
    # (Fontainebleau 10/10/2026 : GUILLET Anna seule dans le 2e fichier).
    _m = df["session_type"] == "MATCH"
    for jour, g in df[_m].groupby("session_date"):
        nommes = g.loc[g["session_name"] != "Match", "session_id"].unique()
        if len(nommes) == 1:
            sans = g.loc[g["session_name"] == "Match", "session_id"].unique()
            cible = g.loc[g["session_id"] == nommes[0]].iloc[0]
            sel = df["session_id"].isin(sans)
            df.loc[sel, ["session_id", "session_name"]] = [cible["session_id"], cible["session_name"]]
    # Même session présente deux fois (export GF1 + copie renommée, ou match aussi dans les
    # séances) : on garde la première (les matchs passent avant).
    df["_dist_r"] = df["distance_m"].round(0)
    df = df.drop_duplicates(["joueuse_id", "session_date", "_dist_r"]).drop(columns="_dist_r")
    df = df[df["distance_m"].fillna(0) > 0]
    for c in ALL_METRIC_COLS:
        if c not in df.columns:
            df[c] = float("nan")
    df["week"] = df["session_date"].dt.strftime("%G-S%V")
    return df.sort_values(["session_date", "session_id"]).reset_index(drop=True)


def rattacher_roster(noms_gestion: list, gps_df: pd.DataFrame, nettoyer, sous_ensemble) -> dict:
    """Joueuse de Gestion → nom GPS (joueuse_id). Absente du GPS : None."""
    ids = sorted(gps_df["joueuse_id"].unique()) if not gps_df.empty else []
    cles = {nettoyer(i): i for i in ids}
    out = {}
    for n in noms_gestion:
        k = nettoyer(n)
        g = cles.get(k)
        if g is None:
            tk = k.split()
            g = next((i for kk, i in cles.items()
                      if sous_ensemble(tk, kk.split()) or sous_ensemble(kk.split(), tk)), None)
        out[n] = g
    return out


def _debut_saison() -> date:
    t = date.today()
    return date(t.year if t.month >= 7 else t.year - 1, 7, 1)


def _aff(fig, key=None):
    if fig is not None:
        st.plotly_chart(appliquer_template(fig), width="stretch", key=key, config=_CFG)


def _cumul(df: pd.DataFrame) -> pd.Series:
    s = [c for c in ALL_METRIC_COLS if c not in NON_SUMMABLE_COLS]
    return pd.concat([
        df[s].sum(), df[[c for c in ALL_METRIC_COLS if c in AGG_MAX_COLS]].max(),
        pd.Series({c: df[c].min() for c in ALL_METRIC_COLS if c in AGG_MIN_COLS}),
        df[[c for c in ALL_METRIC_COLS if c in AGG_MEAN_COLS]].mean(),
    ])


def _cumul_equipe(df: pd.DataFrame) -> pd.Series:
    g = df.groupby("joueuse_id")
    s = [c for c in ALL_METRIC_COLS if c not in NON_SUMMABLE_COLS]
    return pd.concat([
        g[s].sum().mean(), g[[c for c in ALL_METRIC_COLS if c in AGG_MAX_COLS]].max().mean(),
        pd.Series({c: g[c].min().mean() for c in ALL_METRIC_COLS if c in AGG_MIN_COLS}),
        g[[c for c in ALL_METRIC_COLS if c in AGG_MEAN_COLS]].mean().mean(),
    ])


def aggregate_gps_par_joueuse(df_long: pd.DataFrame) -> pd.DataFrame:
    """utils/talent_u19_data.py::aggregate_gps_par_joueuse (app d'Emma)."""
    if df_long.empty or "joueuse_id" not in df_long.columns:
        return pd.DataFrame(columns=["joueuse_id"] + ALL_METRIC_COLS)
    aggs = {col: player_aggregation(col) for col in ALL_METRIC_COLS if col in df_long.columns}
    for col in ("dist_13_15", "dist_15_19"):
        if col in df_long.columns:
            aggs[col] = lambda s: s.sum(min_count=1)
    out = df_long.groupby("joueuse_id", as_index=False).agg(aggs)
    out["full_name"] = out["joueuse_id"]
    return out


def _fmt(val, decimals=0):
    return "—" if pd.isna(val) else f"{val:.{decimals}f}"


def _dec(col):
    return 1 if col in ("speed_avg_mmin", "speed_max_kmh", "accel_min", "accel_max") else 0


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée : un onglet (« seance », « semaine », « periode »)
# ─────────────────────────────────────────────────────────────────────────────
def render_onglet(onglet: str, gps_df: pd.DataFrame, roster: dict, *, joueuse_fixe=None, defaut=None):
    """`roster` : {nom Gestion: nom GPS ou None}. `joueuse_fixe` : profil joueuse (vue
    individuelle seule, sur elle-même). `defaut` : joueuse présélectionnée."""
    if gps_df is None or gps_df.empty:
        st.info("Aucune donnée GPS disponible.")
        return
    if joueuse_fixe is not None:
        vue = VUE_INDIV
    else:
        vue = st.radio("Affichage", [VUE_INDIV, VUE_EQUIPE], horizontal=True, key=f"sv_{onglet}_vue",
                       label_visibility="collapsed")
    if vue == VUE_EQUIPE:
        {"seance": _equipe_seance, "semaine": _equipe_semaine, "periode": _equipe_periode}[onglet](gps_df)
        return
    if joueuse_fixe is not None:
        nom, gid = joueuse_fixe
        st.caption(f"Joueuse : **{nom}**")
    else:
        noms = list(roster)
        if not noms:
            st.info("Aucune joueuse dans Gestion → Fiches joueuses.")
            return
        idx = next((i for i, n in enumerate(noms) if defaut and roster.get(n) == defaut), 0)
        nom = st.selectbox("Joueuse", noms, index=idx, key=f"sv_{onglet}_joueuse")
        gid = roster.get(nom)
    perso = gps_df[gps_df["joueuse_id"] == gid] if gid else gps_df.iloc[0:0]
    if perso.empty:
        st.info(f"Aucune donnée GPS pour {nom}.")
        return
    {"seance": _indiv_seance, "semaine": _indiv_semaine, "periode": _indiv_periode}[onglet](perso, gps_df, nom, gid)


# ─────────────────────────────────────────────────────────────────────────────
# Vue individuelle (views/vue_individuelle_u19.py)
# ─────────────────────────────────────────────────────────────────────────────
def _records(perso: pd.DataFrame):
    """Record match de la saison (Globale des matchs, capteur FieldWiz / GF1)."""
    debut = pd.Timestamp(_debut_saison())
    m = perso[(perso["session_type"] == "MATCH") & (perso["session_date"] >= debut)]
    records, sessions = record_joueuse(m, ALL_METRIC_COLS)
    return records, libelles_records(sessions, m), debut


def _indiv_seance(perso, team_df, nom, gid):
    seances = perso.assign(_sid=perso["session_id"].astype(str)).drop_duplicates("_sid")
    sid, lundi = agenda_seances(seances, "sv_ind_seance", contexte=gid, detail=detail_distance)
    df_seance = perso[perso["session_id"].astype(str) == sid]
    last = df_seance.iloc[0]
    selected_date = last["session_date"].date()
    team_same = team_df[team_df["session_id"] == last["session_id"]]

    records, record_labels, debut = _records(perso)
    ref_mode = st.radio("Référence du profil", ["Record match", "Moyenne séance"], horizontal=True,
                        key="sv_ind_seance_ref")
    if ref_mode == "Record match":
        ref = pd.Series(records, dtype=float)
        ref_label = "Record match"
    else:
        ref = team_same[[c for c in ALL_METRIC_COLS if c in team_same.columns]].mean()
        ref_label = "Moyenne séance"

    st.subheader("Profil détaillé")
    if not ref.dropna().empty:
        _aff(profile_bars_seance(last, ref, ref_label), "sv_ind_seance_profil")
        st.caption(f"Base 100 = {ref_label} | séance entière (gris = non cumulable) | valeur brute au-dessus")
    elif ref_mode == "Record match":
        st.caption(f"Pas de match depuis le {debut.strftime('%d/%m/%Y')} : pas de record match pour servir de référence.")

    for col_ui, (cat_name, cat_metrics) in zip(st.columns(len(METRICS)), METRICS.items()):
        with col_ui:
            st.markdown(f"**{cat_name}**")
            for col_key, col_label in cat_metrics.items():
                val, dec = last.get(col_key), _dec(col_key)
                rec_v, rec_l = records.get(col_key), record_labels.get(col_key)
                pct = pct_du_record(val, rec_v, col_key)
                st.metric(col_label, _fmt(val, dec),
                          delta=f"**{pct:.0f} %**" if pct is not None else None,
                          delta_color=couleur_pct_record(pct) if pct is not None else "off",
                          delta_arrow="off", delta_description="du record match",
                          help=(f"Record match depuis le {debut.strftime('%d/%m/%Y')} : {rec_v:.{dec}f} ({rec_l})"
                                if rec_v is not None and rec_l else None),
                          border=True)
    st.caption(f"Δ = pourcentage du record personnel en match depuis le {debut.strftime('%d/%m/%Y')} "
               "| ? = valeur et match du record")

    semaine = perso[perso["session_date"].dt.date.between(lundi, lundi + timedelta(days=6))].sort_values("session_date")
    st.subheader(f"Séances du {lundi.strftime('%d/%m')} au {(lundi + timedelta(days=6)).strftime('%d/%m')}")
    cols = ["session_date", "session_name"] + ALL_METRIC_COLS
    st.dataframe(semaine[[c for c in cols if c in semaine.columns]].sort_values("session_date", ascending=False)
                 .rename(columns={"session_date": "Date", "session_name": "Séance", **ALL_METRIC_LABELS})
                 .assign(Date=lambda d: d["Date"].apply(fmt_date)), hide_index=True, width="stretch")

    st.subheader("Charge de la semaine")
    afficher_charge(charge_par_periode(semaine, "jour", collectives=False), "sv_ind_seance_wk",
                    equipe=False, selected_cle=pd.Timestamp(selected_date))


def _indiv_semaine(perso, team_df, nom, gid):
    weeks = sorted(perso["week"].unique(), reverse=True)
    selected_day = st.date_input("Choisir une date dans la semaine", value=perso["session_date"].max().date(),
                                 min_value=perso["session_date"].min().date(),
                                 max_value=perso["session_date"].max().date(),
                                 format="DD/MM/YYYY", key="sv_ind_sem_date")
    sel_week = pd.Timestamp(selected_day).strftime("%G-S%V")
    df_week = perso[perso["week"] == sel_week]
    team_week = team_df[team_df["week"] == sel_week]
    if df_week.empty:
        st.warning("Aucune séance cette semaine. Choisis une autre date.")
        return
    st.subheader(f"Cumul — Semaine du {fmt_date(df_week['session_date'].min())} au {fmt_date(df_week['session_date'].max())}")
    i = weeks.index(sel_week) if sel_week in weeks else 0
    prev4 = weeks[i + 1:i + 5]
    df_prev4 = perso[perso["week"].isin(prev4)]
    cumul, team_cumul = _cumul(df_week), _cumul_equipe(team_week)
    s_cols = [c for c in ALL_METRIC_COLS if c not in NON_SUMMABLE_COLS]
    avg_4w = pd.concat([
        df_prev4.groupby("week")[s_cols].sum().mean(),
        df_prev4.groupby("week")[[c for c in ALL_METRIC_COLS if c in AGG_MAX_COLS]].max().mean(),
        pd.Series({c: df_prev4.groupby("week")[c].min().mean() for c in ALL_METRIC_COLS if c in AGG_MIN_COLS}),
        df_prev4[[c for c in ALL_METRIC_COLS if c in AGG_MEAN_COLS]].mean(),
    ]) if not df_prev4.empty else pd.Series(dtype=float)

    st.subheader("Profil détaillé")
    _aff(profile_bars_stacked(df_week, cumul, team_cumul, "moyenne équipe"), "sv_ind_sem_profil")
    st.caption("Base 100 = moyenne équipe sur la même semaine | barres empilées par jour (gris = non cumulable) "
               "| valeur brute au-dessus de chaque barre")
    for col_ui, (cat_name, cat_metrics) in zip(st.columns(len(METRICS)), METRICS.items()):
        with col_ui:
            st.markdown(f"**{cat_name}**")
            for col_key, col_label in cat_metrics.items():
                val, dec = cumul.get(col_key), _dec(col_key)
                d_eq = (round(val - team_cumul[col_key], 1)
                        if col_key in team_cumul and pd.notna(team_cumul[col_key]) and pd.notna(val) else None)
                avg = avg_4w.get(col_key) if col_key in avg_4w and pd.notna(avg_4w[col_key]) else None
                st.metric(col_label, _fmt(val, dec),
                          delta=f"{d_eq} vs équipe" if d_eq is not None else None,
                          delta_color="inverse" if col_key in _INVERSE else "normal",
                          help=f"Moy. 4 sem. préc. : {avg:.{dec}f}" if avg is not None else None, border=True)
    st.caption("Δ = écart par rapport à la moyenne équipe | ? = moyenne sur les 4 semaines précédentes")

    st.subheader("Charge quotidienne")
    afficher_charge(charge_par_periode(df_week, "jour", collectives=False), "sv_ind_sem_jour", equipe=False)
    st.subheader("Charge hebdomadaire (semaine et 4 précédentes)")
    afficher_charge(charge_par_periode(perso[perso["week"].isin([sel_week] + prev4)], "semaine", collectives=False),
                    "sv_ind_sem_trend", equipe=False,
                    selected_cle=pd.Timestamp(selected_day) - pd.Timedelta(days=selected_day.weekday()))

    rows = []
    for w in prev4:
        d = perso[perso["week"] == w]
        if d.empty:
            continue
        c = _cumul(d)
        row = {"Semaine": f"{fmt_date(d['session_date'].min())} → {fmt_date(d['session_date'].max())}"}
        row.update({ALL_METRIC_LABELS[k]: round(float(c[k]), 1) if pd.notna(c[k]) else None
                    for k in ALL_METRIC_COLS if k in c})
        rows.append(row)
    if rows:
        st.subheader("4 semaines précédentes")
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    if len(weeks) > 1:
        st.subheader("Comparaison semaines précédentes")
        metric = render_metric_selector("Métrique", "sv_ind_sem_metric")
        agg = ("max" if metric in AGG_MAX_COLS else "min" if metric in AGG_MIN_COLS
               else "mean" if metric in AGG_MEAN_COLS else "sum")
        wk = perso[perso["week"].isin([sel_week] + prev4)].groupby("week").agg(
            Valeur=(metric, agg), debut=("session_date", "min"), fin=("session_date", "max")).reset_index()
        wk = wk.sort_values("debut")
        wk["Label"] = wk["debut"].dt.strftime("%d/%m") + " → " + wk["fin"].dt.strftime("%d/%m")
        avg_val = wk[wk["week"] != sel_week]["Valeur"].mean()
        f = metric_fmt(metric)
        fig = go.Figure(go.Scatter(
            x=wk["Label"], y=wk["Valeur"], mode="lines+markers", line=dict(color="#0680BE", width=2),
            marker=dict(color=["#BD3032" if w == sel_week else "#0680BE" for w in wk["week"]], size=10,
                        line=dict(width=1, color="white")),
            cliponaxis=False, hovertemplate=f"<b>%{{x}}</b><br>%{{y:{f}}}<extra></extra>"))
        if pd.notna(avg_val):
            fig.add_shape(type="line", x0=0, x1=1, xref="paper", y0=avg_val, y1=avg_val, yref="y",
                          line=dict(color="rgba(100,100,100,0.6)", dash="dash", width=1.5))
            fig.add_annotation(y=avg_val, x=1.01, xref="paper", yref="y", text=f"Moy. 4 sem. préc. : {avg_val:{f}}",
                               showarrow=False, xanchor="left", yanchor="middle",
                               font=dict(size=11, color="rgba(100,100,100,0.8)"))
        fig.update_layout(margin=dict(t=20, b=20, l=60, r=130), plot_bgcolor="rgba(0,0,0,0)",
                          paper_bgcolor="rgba(0,0,0,0)", showlegend=False, height=300,
                          yaxis=dict(gridcolor="rgba(128,128,128,0.15)", zeroline=False, rangemode="tozero"),
                          xaxis=dict(showgrid=False, categoryorder="array", categoryarray=wk["Label"].tolist()))
        _aff(fig, "sv_ind_sem_comp")
        st.caption("Point rouge = semaine sélectionnée")


def _indiv_periode(perso, team_df, nom, gid):
    c1, c2, c3 = st.columns([1.3, 1.3, 2.6])
    d0 = c1.date_input("Du", value=_debut_saison(), format="DD/MM/YYYY", key="sv_ind_p_start")
    d1 = c2.date_input("Au", value=date.today(), format="DD/MM/YYYY", key="sv_ind_p_end")
    tout = c3.checkbox("Voir tout l'historique", value=False, key="sv_ind_p_all",
                       help="Ignore le filtre Du/Au et affiche toutes les séances.")
    per = perso if tout else perso[perso["session_date"].between(pd.Timestamp(d0), pd.Timestamp(d1))]
    if per.empty:
        st.info("Aucune donnée sur cette période.")
        return
    st.subheader("Tout l'historique" if tout else f"Du {fmt_date(pd.Timestamp(d0))} au {fmt_date(pd.Timestamp(d1))}")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Séances", per["session_date"].dt.date.nunique())
    k2.metric("Matchs", per[per["session_type"] == "MATCH"]["session_date"].dt.date.nunique())
    k3.metric("Dist. moy.", f"{per['distance_m'].mean():.0f} m")
    k4.metric("Sprints moy.", f"{per['sprint_count'].mean():.1f}" if per["sprint_count"].notna().any() else "—")

    st.subheader("Profil détaillé")
    team_p = team_df if tout else team_df[team_df["session_date"].between(pd.Timestamp(d0), pd.Timestamp(d1))]
    if not team_p.empty:
        _aff(profile_bars(_cumul(per), _cumul_equipe(team_p), "moyenne équipe"), "sv_ind_p_profil")
        st.caption("Base 100 = moyenne équipe sur la période | valeur brute sur chaque barre")

    st.subheader("Charge par jour ou semaine")
    gran = st.segmented_control("Granularité", ["Semaine", "Jour"], default="Semaine", required=True,
                                key="sv_ind_p_gran")
    afficher_charge(charge_par_periode(per, "jour" if gran == "Jour" else "semaine",
                                       fin=None if tout else d1, collectives=False),
                    f"sv_ind_p_charge_{gran}", equipe=False)

    metric = render_metric_selector("Métrique", "sv_ind_p_metric")
    pe = per[["session_date", "session_type", "week", metric]].copy()
    te = team_df.groupby("session_date")[metric].mean().reset_index()
    mg = pe.merge(te, on="session_date", how="left", suffixes=("_player", "_team")).sort_values("session_date")
    mg["hover"] = mg["session_date"].apply(lambda d: JOURS_FR[d.weekday()] + " " + d.strftime("%d/%m/%y"))
    tr, ma = mg[mg["session_type"] != "MATCH"], mg[mg["session_type"] == "MATCH"]
    ticks = mg.groupby("week")["session_date"].first().sort_values()
    f = metric_fmt(metric)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=mg["session_date"], y=mg[f"{metric}_player"], name=nom, mode="lines",
                             line=dict(color="#0680BE", width=2), cliponaxis=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=tr["session_date"], y=tr[f"{metric}_player"], name="Entraînement", mode="markers",
                             marker=dict(symbol="circle", size=7, color="#0680BE"), cliponaxis=False,
                             customdata=tr["hover"], hovertemplate=f"%{{customdata}}<br>%{{y:{f}}}<extra></extra>"))
    if not ma.empty:
        fig.add_trace(go.Scatter(x=ma["session_date"], y=ma[f"{metric}_player"], name="Match ⚽", mode="text",
                                 text=["⚽"] * len(ma), textfont=dict(size=18), cliponaxis=False,
                                 customdata=ma["hover"], hovertemplate=f"%{{customdata}}<br>%{{y:{f}}}<extra></extra>"))
    fig.add_trace(go.Scatter(x=mg["session_date"], y=mg[f"{metric}_team"], name="Moyenne équipe",
                             mode="lines+markers", line=dict(dash="dash", color="rgba(100,100,100,0.5)", width=1.5),
                             marker=dict(size=5, color="rgba(100,100,100,0.4)"), cliponaxis=False,
                             customdata=mg["hover"], hovertemplate=f"%{{customdata}}<br>Moy. : %{{y:{f}}}<extra></extra>"))
    fig.update_layout(margin=dict(t=20, b=10, l=60, r=0), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      yaxis=dict(gridcolor="rgba(128,128,128,0.15)", zeroline=False, rangemode="tozero"),
                      xaxis=dict(showgrid=False, tickmode="array", tickvals=ticks.values,
                                 ticktext=[d.strftime("%b %Y") if tout else d.strftime("%d/%m") for d in ticks]),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0), height=420)
    _aff(fig, "sv_ind_p_evo")
    st.caption("⚽ Match | ● Entraînement | -- Moyenne équipe")
    with st.expander("Voir les données détaillées"):
        cols = ["session_date", "session_name"] + ALL_METRIC_COLS
        st.dataframe(per[[c for c in cols if c in per.columns]].sort_values("session_date", ascending=False)
                     .rename(columns={"session_date": "Date", "session_name": "Séance", **ALL_METRIC_LABELS})
                     .assign(Date=lambda d: d["Date"].apply(fmt_date)), width="stretch", hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Vue équipe (views/vue_equipe_u19.py)
# ─────────────────────────────────────────────────────────────────────────────
def _season_vmax(df):
    return df.groupby("full_name")["speed_max_kmh"].max()


def _equipe_seance(df):
    sid, lundi = agenda_seances(seances_equipe(df), "sv_eq_seance", detail=detail_nb_joueuses)
    df_seance = df[df["session_id"].astype(str) == sid].copy()
    selected_date = df_seance["session_date"].iloc[0].date()
    st.subheader("Carte de charge")
    render_heatmap(df_seance, season_vmax=_season_vmax(df))
    nav = df[df["session_date"].dt.date.between(lundi, lundi + timedelta(days=6))].copy()
    st.subheader(f"Séances du {lundi.strftime('%d/%m')} au {(lundi + timedelta(days=6)).strftime('%d/%m')}")
    afficher_charge(charge_par_periode(nav, "jour"), "sv_eq_seance_wk", selected_cle=pd.Timestamp(selected_date),
                    vide="Aucune séance collective cette semaine.")
    metric = render_metric_selector("Métrique", "sv_eq_seance_metric")
    fig, _ = bar_chart_team(df_seance, metric)
    _aff(fig, "sv_eq_seance_bar")


def _equipe_semaine(df):
    weeks = sorted(df["week"].unique())
    _, lundi = agenda_seances(seances_equipe(df), "sv_eq_sem", detail=detail_nb_joueuses)
    sel_week = pd.Timestamp(lundi).strftime("%G-S%V")
    if st.session_state.get("_sv_eq_sem_filtre_week") != sel_week:
        st.session_state["sv_eq_sem_filtre"] = "Avec match"
        st.session_state["_sv_eq_sem_filtre_week"] = sel_week
    raw = df[df["week"] == sel_week].copy()
    if raw.empty:
        st.warning("Aucune séance cette semaine.")
        return
    st.subheader(f"Semaine du {fmt_date(raw['session_date'].min())} au {fmt_date(raw['session_date'].max())}")
    filtre = st.segmented_control("Charges affichées", ["Avec match", "Sans match"], default="Avec match",
                                  required=True, key="sv_eq_sem_filtre")
    ch = raw if filtre == "Avec match" else raw[raw["session_type"] != "MATCH"].copy()
    agg = None if ch.empty else aggregate_gps_par_joueuse(ch)
    _kpis_equipe(ch, agg)
    st.subheader("Carte de charge (cumul semaine)")
    if agg is None:
        st.info("Aucune séance hors match cette semaine.")
    else:
        render_heatmap(agg, duration_source=ch)
    st.subheader("Charge quotidienne")
    afficher_charge(charge_par_periode(ch, "jour"), "sv_eq_sem_jour", vide="Aucune séance collective cette semaine.")
    metric = render_metric_selector("Métrique", "sv_eq_sem_metric")
    fig = bar_chart_team_stacked(ch, metric) if is_summable(metric) and not ch.empty else None
    if fig is not None:
        _aff(fig, "sv_eq_sem_bar")
        st.caption("Barres empilées par jour · total en bout de barre")
    elif agg is not None:
        _aff(bar_chart_team(agg, metric)[0], "sv_eq_sem_bar")
    i = weeks.index(sel_week) if sel_week in weeks else 0
    prev4 = weeks[max(0, i - 4):i]
    rows = []
    for w in prev4:
        d = df[df["week"] == w]
        if filtre == "Sans match":
            d = d[d["session_type"] != "MATCH"]
        if d.empty:
            continue
        a = aggregate_gps_par_joueuse(d)
        means = a[[c for c in ALL_METRIC_COLS if c in a.columns]].mean()
        row = {"Semaine": week_label(df, w)}
        row.update({ALL_METRIC_LABELS[c]: round(means[c], 1) for c in ALL_METRIC_COLS if c in means})
        rows.append(row)
    if rows:
        st.subheader("4 semaines précédentes")
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    trend = df[df["week"].isin(prev4 + [sel_week])]
    if filtre == "Sans match":
        trend = trend[trend["session_type"] != "MATCH"]
    st.subheader("Charge hebdomadaire (semaine et 4 précédentes)")
    afficher_charge(charge_par_periode(trend, "semaine"), "sv_eq_sem_trend", selected_cle=pd.Timestamp(lundi))


def _kpis_equipe(ch, agg):
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Joueuses", len(agg) if agg is not None else 0)
    c2.metric("Séances", ch["session_date"].dt.date.nunique())
    c3.metric("Matchs", ch[ch["session_type"] == "MATCH"]["session_date"].dt.date.nunique())
    c4.metric("Dist. moy.", f"{agg['distance_m'].mean():.0f} m" if agg is not None else "—")
    c5.metric("Sprints moy.", f"{agg['sprint_count'].mean():.1f}" if agg is not None and agg["sprint_count"].notna().any() else "—")


def _equipe_periode(df):
    c1, c2 = st.columns(2)
    d0 = c1.date_input("Du", value=_debut_saison(), format="DD/MM/YYYY", key="sv_eq_p_start")
    d1 = c2.date_input("Au", value=date.today(), format="DD/MM/YYYY", key="sv_eq_p_end")
    raw = df[df["session_date"].between(pd.Timestamp(d0), pd.Timestamp(d1))].copy()
    if raw.empty:
        st.info("Aucune donnée sur cette période.")
        return
    st.subheader(f"Du {fmt_date(pd.Timestamp(d0))} au {fmt_date(pd.Timestamp(d1))}")
    filtre = st.segmented_control("Charges affichées", ["Avec match", "Sans match"], default="Avec match",
                                  required=True, key="sv_eq_p_filtre")
    ch = raw if filtre == "Avec match" else raw[raw["session_type"] != "MATCH"].copy()
    agg = None if ch.empty else aggregate_gps_par_joueuse(ch)
    _kpis_equipe(ch, agg)
    st.subheader("Carte de charge (cumul période)")
    if agg is None:
        st.info("Aucune séance hors match sur cette période.")
    else:
        render_heatmap(agg, duration_source=ch)
    st.subheader("Charge par jour ou semaine")
    gran = st.segmented_control("Granularité", ["Semaine", "Jour"], default="Semaine", required=True,
                                key="sv_eq_p_gran")
    afficher_charge(charge_par_periode(ch, "jour" if gran == "Jour" else "semaine", d0, d1),
                    f"sv_eq_p_charge_{gran}", vide="Aucune séance collective sur cette période.")
    metric = render_metric_selector("Métrique", "sv_eq_p_metric")
    if agg is not None:
        _aff(bar_chart_team(agg, metric)[0], "sv_eq_p_bar")
