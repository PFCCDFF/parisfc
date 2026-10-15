# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/records_match.py, version du 14/10/2026).
# Seuls les imports ont été adaptés. Garder la logique alignée sur l'original.
"""Records de match D1 calculés sur les seules mi-temps.

La ligne Globale d'un match Catapult contient tout ce que la joueuse a porté
le pod : échauffement, et surtout le travail physique fait après le match par
les remplaçantes (sprints, « W PHYSIQUE », période au nom de la joueuse...).
Un record pris sur la Globale peut donc venir de ce complément et pas du match.
Seules les périodes OpenField dont le nom contient « mi-temps » (MI TEMPS 1,
MI TEMPS 2, MI TEMPS 2 MANOLY...) comptent ici, cumulées par match."""

import re
import unicodedata

import pandas as pd

from suivi_emma.metrics import AGG_MAX_COLS, AGG_MIN_COLS, AGG_MEAN_COLS

MI_TEMPS = re.compile(r"\bmi[\s-]*temps\b", re.IGNORECASE)


def est_mi_temps(period) -> bool:
    return isinstance(period, str) and bool(MI_TEMPS.search(period))


def periode_hors_jeu(period) -> bool:
    """Travail physique, échauffement, sprints ou course, accents et casse
    ignorés ; une période mi-temps est toujours jouée, même si son nom cite
    l'échauffement (« MI TEMPS 1 + ECHAU EQUIPE 2 »)."""
    if not isinstance(period, str) or est_mi_temps(period):
        return False
    nom = unicodedata.normalize("NFKD", period).encode("ascii", "ignore").decode().upper()
    return any(mot in nom for mot in ("PHYSIQUE", "ECHAU", "SPRINT", "COURSE"))


def matchs_mi_temps(periodes: pd.DataFrame, metric_cols) -> pd.DataFrame:
    """Une ligne par (player_id, session_id) : cumul des périodes mi-temps.
    Cumuls sommés, pics en max / min, vitesse moyenne recalculée sur
    distance / durée. Un match sans période mi-temps n'a pas de ligne."""
    if periodes.empty or "period" not in periodes.columns:
        return _cumul_par_match(periodes, metric_cols)
    return _cumul_par_match(periodes[periodes["period"].map(est_mi_temps)], metric_cols)


def matchs_joues(periodes: pd.DataFrame, metric_cols) -> pd.DataFrame:
    """Comme ``matchs_mi_temps``, mais sur toutes les périodes jouées : tout
    sauf le travail physique, l'échauffement, les sprints et la course
    (``periode_hors_jeu``). Une remplaçante joue dans une période à son
    prénom (« KENZA », « MATCH KENZA ») ou « Period N », pas en « MI TEMPS »."""
    if periodes.empty or "period" not in periodes.columns:
        return _cumul_par_match(periodes, metric_cols)
    return _cumul_par_match(periodes[~periodes["period"].map(periode_hors_jeu)], metric_cols)


def _cumul_par_match(d: pd.DataFrame, metric_cols) -> pd.DataFrame:
    cols = [c for c in metric_cols if c in d.columns]
    if d.empty:
        return pd.DataFrame(columns=["player_id", "session_id", *cols])
    num = d[[c for c in [*cols, "duration_min"] if c in d.columns]].apply(pd.to_numeric, errors="coerce")
    num["player_id"] = d["player_id"].values
    num["session_id"] = d["session_id"].values
    g = num.groupby(["player_id", "session_id"])
    out = {}
    for c in num.columns.drop(["player_id", "session_id"]):
        if c in AGG_MEAN_COLS:
            continue
        if c in AGG_MAX_COLS:
            out[c] = g[c].max()
        elif c in AGG_MIN_COLS:
            out[c] = g[c].min()
        else:
            out[c] = g[c].sum(min_count=1)
    res = pd.DataFrame(out)
    if "speed_avg_mmin" in metric_cols and {"distance_m", "duration_min"} <= set(res.columns):
        duree = res["duration_min"].where(res["duration_min"] > 0)
        res["speed_avg_mmin"] = res["distance_m"] / duree
    return res.reset_index()


def record_joueuse(matchs: pd.DataFrame, metric_cols) -> tuple[dict, dict]:
    """(records, session_id du record) par métrique pour les matchs d'une
    joueuse. `accel_min` est une décélération : son record est le minimum."""
    records, sessions = {}, {}
    for m in metric_cols:
        if m not in matchs.columns:
            continue
        vals = pd.to_numeric(matchs[m], errors="coerce").dropna()
        if vals.empty:
            continue
        idx = vals.idxmin() if m in AGG_MIN_COLS else vals.idxmax()
        records[m] = float(vals.loc[idx])
        sessions[m] = matchs.loc[idx, "session_id"]
    return records, sessions


def moyenne_records_poste(matchs: pd.DataFrame, positions: dict, poste, metric_cols,
                          exclure=None) -> tuple[dict, int]:
    """Moyenne, par métrique, des records des joueuses du poste `poste`
    (hors `exclure`). Renvoie (moyennes, nombre de joueuses retenues)."""
    if poste is None or (isinstance(poste, float) and pd.isna(poste)) or matchs.empty:
        return {}, 0
    ids = [p for p in matchs["player_id"].unique() if positions.get(p) == poste and p != exclure]
    par_joueuse = [record_joueuse(matchs[matchs["player_id"] == p], metric_cols)[0] for p in ids]
    par_joueuse = [r for r in par_joueuse if r]
    if not par_joueuse:
        return {}, 0
    moyennes = pd.DataFrame(par_joueuse).mean().dropna()
    return moyennes.to_dict(), len(par_joueuse)


def meilleurs_records(par_capteur: dict, exclure=()) -> tuple[dict, dict]:
    """Meilleur record de plusieurs capteurs, métrique par métrique (semaine
    qui mêle séances Catapult et FieldWiz). `par_capteur` = {capteur:
    (records, libellés)} ; le libellé gagnant est suffixé de son capteur.
    `accel_min` : la valeur la plus basse est la meilleure."""
    records, labels = {}, {}
    for capteur, (recs, labs) in par_capteur.items():
        for m, v in recs.items():
            if m in exclure or v is None or pd.isna(v):
                continue
            meilleur = m not in records or (v < records[m] if m in AGG_MIN_COLS else v > records[m])
            if meilleur:
                records[m] = v
                labels[m] = f"{labs.get(m, '')} ({capteur})"
    return records, labels


def libelles_records(sessions: dict, seances: pd.DataFrame) -> dict:
    """« Nom de la séance — date » du match de chaque record. `sessions` =
    {métrique: session_id} (record_joueuse), `seances` = au moins
    session_id, session_name, session_date."""
    from suivi_emma.date_format import fmt_date_short
    info = seances.drop_duplicates("session_id").set_index("session_id")
    return {
        m: f"{info.loc[sid, 'session_name']} — {fmt_date_short(info.loc[sid, 'session_date'])}"
        for m, sid in sessions.items() if sid in info.index
    }


def pct_du_record(value, record, metric):
    """Part du record de la saison atteinte par une valeur."""
    if record is None or not pd.notna(value) or not pd.notna(record):
        return None
    valeur = abs(float(value)) if metric in AGG_MIN_COLS else float(value)
    record = abs(float(record)) if metric in AGG_MIN_COLS else float(record)
    if record == 0:
        return None
    return valeur / record * 100


def couleur_pct_record(pct):
    if pct < 50:
        return "red"
    if pct < 75:
        return "orange"
    if pct < 90:
        return "yellow"
    return "green"
