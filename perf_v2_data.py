"""Calculs de l'interface Performance v2 (branche beta) — sans dépendance Streamlit.

Bien-être (questionnaire Hooper / McLean), charge interne (sRPE de Foster,
monotonie, contrainte, ACWR EWMA), présence et agrégations pour les bilans de
période (onglet Synthèse). Tout est testable hors app (tests/test_perf_v2_data.py).
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

# ── Bien-être ──────────────────────────────────────────────────────────────
# Échelle commune 1 → 5, 5 = état optimal (on inverse le sens « fatigue /
# courbatures / stress » à la saisie pour que « plus haut = mieux » partout).
WELLNESS_ITEMS = {
    "sommeil_qualite": "Qualité du sommeil",
    "fatigue":         "Fraîcheur (fatigue)",
    "courbatures":     "Courbatures",
    "stress":          "Sérénité (stress)",
    "humeur":          "Humeur",
}
WELLNESS_ECHELLE = {
    1: "1 · Très mauvais",
    2: "2 · Mauvais",
    3: "3 · Moyen",
    4: "4 · Bon",
    5: "5 · Très bon",
}
WELLNESS_MAX = 5 * len(WELLNESS_ITEMS)   # 25

# Seuils d'alerte : item individuel ≤ 2, ou score total à ≤ -1 écart-type de
# la référence individuelle glissante (28 jours, hors jour courant).
WELLNESS_SEUIL_ITEM = 2
WELLNESS_SEUIL_Z = -1.0

# ── RPE / charge interne ───────────────────────────────────────────────────
RPE_ECHELLE_CR10 = {
    0: "0 · Repos", 1: "1 · Très, très facile", 2: "2 · Facile", 3: "3 · Modéré",
    4: "4 · Un peu dur", 5: "5 · Dur", 6: "6 · Dur +", 7: "7 · Très dur",
    8: "8 · Très dur +", 9: "9 · Presque maximal", 10: "10 · Maximal",
}
CRENEAUX = ["Unique", "Matin", "Après-midi", "Match"]
TYPES_SEANCE = ["Entraînement", "Match", "Réathlé", "Autre"]
MICROCYCLE_JOURS = ["MD+1", "MD+2", "MD-5", "MD-4", "MD-3", "MD-2", "MD-1", "MD", "Récupération", "Hors microcycle"]

# Zones ACWR (Gabbett 2016) — mêmes bornes que charge_entrainement.zone()
ACWR_ZONES = [
    (0.0, 0.8, "Sous-charge", "#1FA8E0"),
    (0.8, 1.3, "Zone optimale", "#2E9E5B"),
    (1.3, 1.5, "Vigilance", "#F2A900"),
    (1.5, 99., "Risque élevé", "#BD3032"),
]


def _to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.normalize()


def filtre_periode(df: pd.DataFrame, col: str, deb=None, fin=None) -> pd.DataFrame:
    """Garde les lignes dont `col` est dans [deb, fin] (bornes incluses, None = ouvert)."""
    if df is None or df.empty or col not in df.columns:
        return df if df is not None else pd.DataFrame()
    d = df.copy()
    dt = _to_date_series(d[col])
    m = dt.notna()
    if deb is not None:
        m &= dt >= pd.Timestamp(deb)
    if fin is not None:
        m &= dt <= pd.Timestamp(fin)
    return d[m]


# ══════════════════════════════════════════════════════════════════════════
# BIEN-ÊTRE
# ══════════════════════════════════════════════════════════════════════════

def wellness_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute score_total (/25, somme des items renseignés ramenée à 5 items)
    et score_pct (0-100). Les items manquants ne pénalisent pas le score."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["joueuse", "date", "score_total", "score_pct"])
    d = df.copy()
    d["date"] = _to_date_series(d["date"])
    items = [c for c in WELLNESS_ITEMS if c in d.columns]
    for c in items:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    n = d[items].notna().sum(axis=1)
    moy = d[items].mean(axis=1)
    d["score_total"] = (moy * len(WELLNESS_ITEMS)).where(n > 0).round(1)
    d["score_pct"] = ((d["score_total"] - len(WELLNESS_ITEMS)) / (WELLNESS_MAX - len(WELLNESS_ITEMS)) * 100).round(0)
    return d.sort_values(["joueuse", "date"]).reset_index(drop=True)


def wellness_zscores(df: pd.DataFrame, fenetre_jours: int = 28, min_obs: int = 5) -> pd.DataFrame:
    """z-score du score total de chaque jour vs la référence individuelle des
    `fenetre_jours` précédents (jour courant exclu). NaN tant que moins de
    `min_obs` réponses de référence."""
    d = wellness_scores(df)
    if d.empty:
        d["z_score"] = pd.Series(dtype=float)
        return d
    out = []
    for _, g in d.groupby("joueuse", sort=False):
        g = g.sort_values("date").copy()
        # Référence = réponses des `fenetre_jours` jours calendaires précédents.
        moy, et = [], []
        dates = g["date"].tolist()
        vals = g["score_total"].tolist()
        for i, dt in enumerate(dates):
            ref = [v for dj, v in zip(dates[:i], vals[:i])
                   if pd.notna(v) and (dt - dj).days <= fenetre_jours]
            if len(ref) >= min_obs:
                moy.append(float(np.mean(ref)))
                et.append(float(np.std(ref, ddof=1)) if len(ref) > 1 else np.nan)
            else:
                moy.append(np.nan)
                et.append(np.nan)
        g["ref_moyenne"] = moy
        g["ref_ecart_type"] = et
        g["z_score"] = np.where(
            (g["ref_ecart_type"] > 0) & g["score_total"].notna(),
            (g["score_total"] - g["ref_moyenne"]) / g["ref_ecart_type"], np.nan)
        g["z_score"] = g["z_score"].round(2)
        out.append(g)
    return pd.concat(out, ignore_index=True)


def wellness_alertes(row: pd.Series) -> list[str]:
    """Liste lisible des signaux d'alerte d'une réponse (item bas, z-score bas, douleur)."""
    al = []
    for c, lbl in WELLNESS_ITEMS.items():
        v = row.get(c)
        if pd.notna(v) and float(v) <= WELLNESS_SEUIL_ITEM:
            al.append(f"{lbl} : {int(v)}/5")
    z = row.get("z_score")
    if pd.notna(z) and float(z) <= WELLNESS_SEUIL_Z:
        al.append(f"Score global inhabituel (z = {float(z):+.1f})")
    h = row.get("sommeil_heures")
    if pd.notna(h) and float(h) < 6:
        al.append(f"Sommeil court ({float(h):.1f} h)")
    zone = row.get("douleur_zone")
    if isinstance(zone, str) and zone.strip():
        al.append(f"Douleur : {zone.strip()}")
    return al


def wellness_equipe_jour(df: pd.DataFrame, jour, roster: list | None = None) -> pd.DataFrame:
    """Tableau du jour : une ligne par joueuse (répondantes + non-répondantes du roster)."""
    dz = wellness_zscores(df)
    j = pd.Timestamp(jour).normalize()
    dj = dz[dz["date"] == j].copy() if not dz.empty else pd.DataFrame()
    if not dj.empty:
        dj["alertes"] = dj.apply(lambda r: " · ".join(wellness_alertes(r)), axis=1)
    if roster:
        repondu = set(dj["joueuse"]) if not dj.empty else set()
        manquantes = [p for p in roster if p not in repondu]
        if manquantes:
            dj = pd.concat([dj, pd.DataFrame({"joueuse": manquantes, "alertes": "Pas de réponse"})],
                           ignore_index=True)
    return dj


# ══════════════════════════════════════════════════════════════════════════
# CHARGE INTERNE (sRPE)
# ══════════════════════════════════════════════════════════════════════════

def prepare_rpe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["joueuse", "date", "rpe", "duree_min", "charge_ua"])
    d = df.copy()
    d["date"] = _to_date_series(d["date"])
    d["rpe"] = pd.to_numeric(d["rpe"], errors="coerce")
    d["duree_min"] = pd.to_numeric(d["duree_min"], errors="coerce")
    d["charge_ua"] = (d["rpe"] * d["duree_min"]).round(1)
    return d.dropna(subset=["date", "charge_ua"])


def charge_quotidienne(df_rpe: pd.DataFrame, joueuse: str | None = None,
                       deb=None, fin=None) -> pd.DataFrame:
    """Charge interne par jour calendaire (jours sans séance = 0), par joueuse."""
    d = prepare_rpe(df_rpe)
    if joueuse:
        d = d[d["joueuse"] == joueuse]
    if d.empty:
        return pd.DataFrame(columns=["joueuse", "date", "charge_ua"])
    out = []
    for p, g in d.groupby("joueuse"):
        jour = g.groupby("date")["charge_ua"].sum()
        idx = pd.date_range(pd.Timestamp(deb) if deb else jour.index.min(),
                            pd.Timestamp(fin) if fin else jour.index.max(), freq="D")
        jour = jour.reindex(idx, fill_value=0.0)
        out.append(pd.DataFrame({"joueuse": p, "date": idx, "charge_ua": jour.values}))
    return pd.concat(out, ignore_index=True)


def charge_hebdo(df_rpe: pd.DataFrame, joueuse: str | None = None) -> pd.DataFrame:
    """Par semaine ISO (lundi) : charge totale, nb séances, monotonie et contrainte
    (Foster 1998 : monotonie = moyenne / écart-type des 7 charges quotidiennes,
    contrainte = charge hebdo × monotonie)."""
    d = prepare_rpe(df_rpe)
    if joueuse:
        d = d[d["joueuse"] == joueuse]
    if d.empty:
        return pd.DataFrame(columns=["joueuse", "semaine", "charge_ua", "nb_seances",
                                     "monotonie", "contrainte"])
    deb = d["date"].min() - pd.Timedelta(days=d["date"].min().weekday())
    fin = d["date"].max() + pd.Timedelta(days=6 - d["date"].max().weekday())
    q = charge_quotidienne(d, deb=deb, fin=fin)
    q["semaine"] = q["date"] - pd.to_timedelta(q["date"].dt.weekday, unit="D")
    nb = d.assign(semaine=d["date"] - pd.to_timedelta(d["date"].dt.weekday, unit="D")) \
          .groupby(["joueuse", "semaine"]).size().rename("nb_seances")
    agg = q.groupby(["joueuse", "semaine"])["charge_ua"].agg(["sum", "mean", "std"])
    agg = agg.join(nb, how="left").fillna({"nb_seances": 0}).reset_index()
    agg["monotonie"] = np.where(agg["std"] > 0, agg["mean"] / agg["std"], np.nan)
    agg["contrainte"] = agg["sum"] * agg["monotonie"]
    agg = agg.rename(columns={"sum": "charge_ua"}).drop(columns=["mean", "std"])
    agg["nb_seances"] = agg["nb_seances"].astype(int)
    for c in ["charge_ua", "contrainte"]:
        agg[c] = agg[c].round(0)
    agg["monotonie"] = agg["monotonie"].round(2)
    return agg


def acwr_ewma(df_rpe: pd.DataFrame, joueuse: str, aigu: int = 7, chronique: int = 28) -> pd.DataFrame:
    """ACWR EWMA (Williams et al. 2017) sur la charge interne quotidienne."""
    q = charge_quotidienne(df_rpe, joueuse=joueuse)
    if q.empty:
        return pd.DataFrame(columns=["date", "charge_ua", "aigue", "chronique", "acwr"])
    q = q.sort_values("date").reset_index(drop=True)
    la, lc = 2 / (aigu + 1), 2 / (chronique + 1)
    q["aigue"] = q["charge_ua"].ewm(alpha=la, adjust=False).mean().round(1)
    q["chronique"] = q["charge_ua"].ewm(alpha=lc, adjust=False).mean().round(1)
    q["acwr"] = np.where(q["chronique"] > 0, q["aigue"] / q["chronique"], np.nan)
    # Pas d'ACWR interprétable avant 3 semaines d'historique.
    q.loc[q["date"] < q["date"].min() + pd.Timedelta(days=21), "acwr"] = np.nan
    q["acwr"] = q["acwr"].round(2)
    return q.drop(columns=["joueuse"])


def zone_acwr(v) -> tuple[str, str]:
    if v is None or pd.isna(v):
        return "—", "#6A8090"
    for lo, hi, lbl, col in ACWR_ZONES:
        if lo <= float(v) < hi:
            return lbl, col
    return "—", "#6A8090"


# ══════════════════════════════════════════════════════════════════════════
# PRÉSENCE
# ══════════════════════════════════════════════════════════════════════════

def presence_resume(df_pres: pd.DataFrame, statuts: list, statuts_presente: tuple) -> pd.DataFrame:
    """Une ligne par joueuse : nombre de séances par statut + taux de présence (%)."""
    if df_pres is None or df_pres.empty:
        return pd.DataFrame(columns=["joueuse", *statuts, "Séances", "Taux de présence (%)"])
    t = pd.crosstab(df_pres["joueuse"], df_pres["statut"])
    for s in statuts:
        if s not in t.columns:
            t[s] = 0
    t = t[statuts]
    t["Séances"] = t.sum(axis=1)
    pres = t[[s for s in statuts if s in statuts_presente]].sum(axis=1)
    t["Taux de présence (%)"] = (pres / t["Séances"].replace(0, np.nan) * 100).round(0)
    return t.reset_index().sort_values("joueuse")


# ══════════════════════════════════════════════════════════════════════════
# MATCHS — lignes de tendance
# ══════════════════════════════════════════════════════════════════════════

def ligne_match_collectif(report: dict, info: dict) -> dict:
    """Aplatit un rapport collectif (compute_collective_report) en une ligne :
    indicateurs PFC + adversaire pour le suivi match après match."""
    if not report:
        return {}
    pfc, adv = report.get("pfc_name", "PFC"), report.get("adv_name", "ADV")
    sp, sa = report["stats"].get(pfc, {}), report["stats"].get(adv, {})
    poss = report.get("poss_total", {})
    try:
        sc_p, sc_a = int(info.get("score_pfc")), int(info.get("score_adv"))
        res = "V" if sc_p > sc_a else ("N" if sc_p == sc_a else "D")
    except (TypeError, ValueError):
        sc_p = sc_a = None
        res = ""
    return {
        "Date": pd.to_datetime(info.get("date"), errors="coerce"),
        "Match": info.get("label", ""),
        "Adversaire": adv,
        "Résultat": res,
        "Score": f"{sc_p}–{sc_a}" if sc_p is not None else "",
        "Buts PFC": sc_p, "Buts ADV": sc_a,
        "Possession PFC (%)": poss.get(pfc),
        "Possessions PFC": sp.get("possessions"),
        "Durée moy. poss. PFC (s)": round(sp.get("duree_moyenne", 0) or 0, 1),
        "Tirs PFC": sp.get("tirs"), "Tirs ADV": sa.get("tirs"),
        "Tirs cadrés PFC": sp.get("tirs_cadres"), "Tirs cadrés ADV": sa.get("tirs_cadres"),
        "Entrées 1/3 PFC": sp.get("entrees_1_3"), "Entrées 1/3 ADV": sa.get("entrees_1_3"),
        "Pertes PFC (%)": sp.get("pct_pertes"), "Pertes ADV (%)": sa.get("pct_pertes"),
        "Fautes PFC": sp.get("fautes"), "Fautes ADV": sa.get("fautes"),
    }


INDICATEURS_TENDANCE_COLLECTIF = [
    "Possession PFC (%)", "Tirs PFC", "Tirs cadrés PFC", "Entrées 1/3 PFC",
    "Pertes PFC (%)", "Durée moy. poss. PFC (s)", "Tirs ADV", "Entrées 1/3 ADV",
]


def bilan_resultats(df_matchs: pd.DataFrame) -> dict:
    """Bilan V/N/D, buts pour/contre, points (3/1/0) d'un ensemble de matchs."""
    if df_matchs is None or df_matchs.empty:
        return {"matchs": 0, "V": 0, "N": 0, "D": 0, "bp": 0, "bc": 0, "points": 0}
    r = df_matchs["Résultat"].value_counts()
    v, n, dd = int(r.get("V", 0)), int(r.get("N", 0)), int(r.get("D", 0))
    return {
        "matchs": len(df_matchs), "V": v, "N": n, "D": dd,
        "bp": int(pd.to_numeric(df_matchs["Buts PFC"], errors="coerce").fillna(0).sum()),
        "bc": int(pd.to_numeric(df_matchs["Buts ADV"], errors="coerce").fillna(0).sum()),
        "points": 3 * v + n,
    }


# ══════════════════════════════════════════════════════════════════════════
# SYNTHÈSE — agrégations par période
# ══════════════════════════════════════════════════════════════════════════

DIMENSIONS_SYNTHESE = {
    "technique":  "⚽ Technique",
    "tactique":   "🧠 Tactique",
    "physique":   "🏃 Physique (GPS)",
    "presence":   "✅ Présence",
    "objectifs":  "🎯 Réponse aux objectifs",
    "bien_etre":  "💚 Bien-être",
    "charge":     "⚖️ Charge interne (RPE)",
    "collectif":  "🤝 Performance collective (matchs)",
}

INDICATEURS_TACTIQUES = ["Rigueur", "Récupération", "Distribution", "Percussion", "Finition", "Créativité"]
INDICATEURS_POSTES = ["Défenseur central", "Défenseur latéral", "Milieu défensif",
                      "Milieu relayeur", "Milieu offensif", "Attaquant"]
_META_KPI = {"Player", "Adversaire", "Journée", "Catégorie", "Date", "Saison",
             "Temps de jeu (en minutes)"}


def indicateurs_techniques_disponibles(pfc_kpi: pd.DataFrame) -> list[str]:
    """Colonnes numériques de pfc_kpi hors méta / tactique / postes."""
    if pfc_kpi is None or pfc_kpi.empty:
        return []
    excl = _META_KPI | set(INDICATEURS_TACTIQUES) | set(INDICATEURS_POSTES)
    return [c for c in pfc_kpi.columns
            if c not in excl and not str(c).startswith("_")
            and pd.api.types.is_numeric_dtype(pfc_kpi[c])]


def synthese_kpi_match(pfc_kpi: pd.DataFrame, joueuses: list | None, indicateurs: list,
                       deb=None, fin=None, cle_nom=None) -> pd.DataFrame:
    """Moyenne par joueuse des indicateurs match (pondérée par le temps de jeu
    quand il est disponible) + nb de matchs et minutes cumulées, sur la période."""
    if pfc_kpi is None or pfc_kpi.empty or not indicateurs:
        return pd.DataFrame()
    d = filtre_periode(pfc_kpi, "Date", deb, fin) if "Date" in pfc_kpi.columns and (deb or fin) else pfc_kpi.copy()
    if cle_nom is not None:
        # Comparaison sur la forme normalisée des deux côtés, affichage avec le
        # nom tel qu'il figure dans la liste demandée (effectif).
        vers_affiche = {cle_nom(j): j for j in (joueuses or [])}
        d = d.assign(Player=d["Player"].astype(str).map(lambda n: vers_affiche.get(cle_nom(n), cle_nom(n))))
    if joueuses:
        d = d[d["Player"].isin(joueuses)]
    ind = [c for c in indicateurs if c in d.columns]
    if d.empty or not ind:
        return pd.DataFrame()
    mins = pd.to_numeric(d.get("Temps de jeu (en minutes)", pd.Series(1.0, index=d.index)),
                         errors="coerce").fillna(0)
    w = mins.where(mins > 0, np.nan)
    rows = []
    for p, g in d.groupby("Player"):
        wg = w.loc[g.index]
        r = {"Joueuse": p, "Matchs": len(g), "Minutes": int(mins.loc[g.index].sum())}
        for c in ind:
            v = pd.to_numeric(g[c], errors="coerce")
            if wg.notna().any() and (v.notna() & wg.notna()).any():
                m = v.notna() & wg.notna()
                r[c] = round(float(np.average(v[m], weights=wg[m])), 1)
            else:
                r[c] = round(float(v.mean()), 1) if v.notna().any() else np.nan
        rows.append(r)
    return pd.DataFrame(rows).sort_values("Joueuse").reset_index(drop=True)


def synthese_gps(gps_df: pd.DataFrame, joueuses: list | None, colonnes: list,
                 deb=None, fin=None, pics: set | None = None) -> pd.DataFrame:
    """Par joueuse : nb de séances, total et moyenne par séance des colonnes GPS
    (max pour les métriques de pic, ex. vitesse max)."""
    if gps_df is None or gps_df.empty or "Player" not in gps_df.columns:
        return pd.DataFrame()
    col_date = "DATE" if "DATE" in gps_df.columns else None
    d = filtre_periode(gps_df, col_date, deb, fin) if col_date else gps_df.copy()
    if joueuses:
        d = d[d["Player"].isin(joueuses)]
    cols = [c for c in colonnes if c in d.columns]
    if d.empty or not cols:
        return pd.DataFrame()
    pics = pics or set()
    rows = []
    for p, g in d.groupby("Player"):
        n = g[col_date].dt.normalize().nunique() if col_date else len(g)
        r = {"Joueuse": p, "Séances": int(n)}
        for c in cols:
            v = pd.to_numeric(g[c], errors="coerce")
            if c in pics:
                r[f"{c} (max)"] = round(float(v.max()), 1) if v.notna().any() else np.nan
            else:
                r[f"{c} (total)"] = round(float(v.sum()), 0) if v.notna().any() else np.nan
                r[f"{c} (moy.)"] = round(float(v.mean()), 1) if v.notna().any() else np.nan
        rows.append(r)
    return pd.DataFrame(rows).sort_values("Joueuse").reset_index(drop=True)


def synthese_objectifs(df_obj: pd.DataFrame, df_eval: pd.DataFrame) -> pd.DataFrame:
    """Version lisible : Catégorie, Objectif, Statut, Note joueuse, Note coach,
    Écart (joueuse − coach), Matchs évalués."""
    if df_obj is None or df_obj.empty:
        return pd.DataFrame()
    o = df_obj.rename(columns={"id": "objectif_id"}).copy()
    res = pd.DataFrame({
        "objectif_id": o["objectif_id"],
        "Catégorie": o.get("categorie", ""),
        "Objectif": o.get("objectif", ""),
        "Statut": o.get("statut_label", o.get("statut", "")),
    })
    res["Note joueuse"] = np.nan
    res["Note coach"] = np.nan
    res["Matchs évalués"] = 0
    if df_eval is not None and not df_eval.empty and "objectif_id" in df_eval.columns:
        e = df_eval.copy()
        e["note"] = pd.to_numeric(e["note"], errors="coerce")
        for ev, col in [("joueuse", "Note joueuse"), ("coach", "Note coach")]:
            m = e[e["evaluateur"] == ev].groupby("objectif_id")["note"].mean()
            res[col] = res["objectif_id"].map(m)
        key = "match_date" if "match_date" in e.columns else "note"
        res["Matchs évalués"] = res["objectif_id"].map(
            e.groupby("objectif_id")[key].nunique()).fillna(0).astype(int)
    res["Note joueuse"] = res["Note joueuse"].round(1)
    res["Note coach"] = res["Note coach"].round(1)
    res["Écart (J − C)"] = (res["Note joueuse"] - res["Note coach"]).round(1)
    return res.drop(columns="objectif_id")


def synthese_bien_etre(df_w: pd.DataFrame, joueuses: list | None, deb=None, fin=None) -> pd.DataFrame:
    d = wellness_zscores(df_w)
    d = filtre_periode(d, "date", deb, fin)
    if joueuses:
        d = d[d["joueuse"].isin(joueuses)]
    if d.empty:
        return pd.DataFrame()
    rows = []
    for p, g in d.groupby("joueuse"):
        r = {"Joueuse": p, "Réponses": len(g),
             "Score moyen (/25)": round(float(g["score_total"].mean()), 1)}
        for c, lbl in WELLNESS_ITEMS.items():
            if c in g.columns:
                r[lbl] = round(float(pd.to_numeric(g[c], errors="coerce").mean()), 1)
        if "sommeil_heures" in g.columns:
            r["Sommeil (h)"] = round(float(pd.to_numeric(g["sommeil_heures"], errors="coerce").mean()), 1)
        r["Jours en alerte"] = int(g.apply(lambda x: bool(wellness_alertes(x)), axis=1).sum())
        rows.append(r)
    return pd.DataFrame(rows)


def synthese_charge(df_rpe: pd.DataFrame, joueuses: list | None, deb=None, fin=None) -> pd.DataFrame:
    d = filtre_periode(prepare_rpe(df_rpe), "date", deb, fin)
    if joueuses:
        d = d[d["joueuse"].isin(joueuses)]
    if d.empty:
        return pd.DataFrame()
    nb_sem = max(1.0, ((d["date"].max() - d["date"].min()).days + 1) / 7)
    g = d.groupby("joueuse").agg(Séances=("charge_ua", "size"),
                                 **{"RPE moyen": ("rpe", "mean"),
                                    "Durée moy. (min)": ("duree_min", "mean"),
                                    "Charge totale (UA)": ("charge_ua", "sum")})
    g["Charge / semaine (UA)"] = g["Charge totale (UA)"] / nb_sem
    return g.round({"RPE moyen": 1, "Durée moy. (min)": 0, "Charge totale (UA)": 0,
                    "Charge / semaine (UA)": 0}).reset_index().rename(columns={"joueuse": "Joueuse"})


def periodes_predefinies(aujourdhui: date | None = None) -> dict:
    """Raccourcis de période pour l'onglet Synthèse."""
    t = aujourdhui or date.today()
    debut_saison = date(t.year if t.month >= 7 else t.year - 1, 7, 1)
    lundi = t - timedelta(days=t.weekday())
    return {
        "7 derniers jours": (t - timedelta(days=6), t),
        "Semaine en cours": (lundi, t),
        "4 dernières semaines": (t - timedelta(days=27), t),
        "Mois en cours": (t.replace(day=1), t),
        "Depuis le début de saison": (debut_saison, t),
        "Phase aller (juil. → déc.)": (debut_saison, date(debut_saison.year, 12, 31)),
        "Phase retour (janv. → juin)": (date(debut_saison.year + 1, 1, 1), date(debut_saison.year + 1, 6, 30)),
        "Personnalisée": (None, None),
    }
