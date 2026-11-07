"""
Import des exports Bepro « JSON des Raw Event » (space.bepro.ai → Stats → Télécharger).

Format de l'export : un zip contenant un JSON par période
(« 2026-09-12_Paris FC U19 Féminines vs Le Havre AC U19 Féminines_1st Half.json »),
chacun de la forme {"data": [événement, …]} où un événement vaut :

    {"period_name": "1st Half", "event_time": 18515,            # ms depuis le début du match
     "team_name": "Paris FC U19 Féminines", "player_name": "Binta Traore",
     "player_shirt_number": "7",
     "events": [{"event_name": "Passes", "property": {"Outcome": "Succeeded",
                 "Area": "Passes In Final Third", "Direction": "Passes Sideways",
                 "Distance": "Short Passes"}}, …],
     "x": 0.7778, "y": 0.1573, "to_x": 0.7841, "to_y": 0.2141,   # 0-1, repère absolu
     "attack_direction": "RIGHT"}                               # sens d'attaque PFC

Seules les actions du Paris FC sont exportées (pas l'adversaire, donc pas de score adverse).

Repère : x = longueur, y = largeur, absolus (le terrain ne tourne pas à la mi-temps) ;
quand le PFC attaque vers la DROITE, le couloir gauche du PFC est vers y = 1 (vérifié sur
la latérale gauche : y ≈ 0,77 en 1re mi-temps, ≈ 0,09 en 2e quand le PFC attaque à gauche).
On ramène tout dans le repère des rapports de l'app (compute_tactical_stats) :
x 0-100 attaque vers la droite, y 0-68 avec y = 0 en haut = couloir gauche.

Sans dépendance Streamlit (testable seul : tests/test_bepro_utils.py).
"""
from __future__ import annotations

import io
import json
import os
import re
import unicodedata
import zipfile
from typing import Dict, List, Optional

import pandas as pd

BEPRO_FOLDER = os.path.join("data", "bepro")

# Joueuses exportées sans nom par Bepro (« Player 4 ») : (date du match, n° de maillot) → nom
# au format Bepro « Prénom Nom ». Complétable sans toucher au code via data/bepro_noms.json :
# {"2026-09-27|4": "Chloe Niama Mahoukou"}.
BEPRO_JOUEUSES_SANS_NOM = {("2026-09-27", "4"): "Chloe Niama Mahoukou"}

# Matchs dont Bepro a exporté des noms FAUX pour le Paris FC : (date, n° de maillot) → vrai nom
# (« Prénom Nom »), appliqué à toutes les joueuses du Paris FC de ce match, nommées ou non.
# Challenge Espoirs LFFP Paris FC - AJ Auxerre du 07/10/2026 : liste corrigée par Olivier
# (export Stats Bepro, n° 7 « B. Guppillote » = Kadidia Traoré ; n° 10 = joueuse pro).
BEPRO_NOMS_PAR_MAILLOT = {
    ("2026-10-07", n): nom for n, nom in {
        "1": "Clélia Ducreux", "2": "Sharlie Yerro", "3": "Saki Ishizaka", "4": "Malya Gaba",
        "5": "Sergyna Loubli", "6": "Dissya Minyemeck", "7": "Kadidia Traore", "8": "Nakhim Dosso",
        "9": "Christy Maalouf", "10": "Kenza Roche Dufour", "11": "Karla Torres", "12": "Hawa Sangare",
        "13": "Sylia Renai", "14": "Ely Guppillotte Bulver", "15": "Noellie Koutassila",
        "17": "Nessma Mouradi", "18": "Adela Sinani",
    }.items()
}
_SANS_NOM = re.compile(r"(player\s*\d+|\d+\s*player)", re.IGNORECASE)


def _noms_manquants() -> dict:
    out = dict(BEPRO_JOUEUSES_SANS_NOM)
    try:
        with open(os.path.join("data", "bepro_noms.json"), encoding="utf-8") as f:
            for k, v in json.load(f).items():
                d, n = str(k).split("|", 1)
                out[(d.strip(), n.strip())] = str(v).strip()
    except Exception:
        pass
    return out


def nommer_joueuses_sans_nom(df: pd.DataFrame, date) -> pd.DataFrame:
    """Remplace « Player 4 » par le nom connu pour ce match et ce numéro, s'il existe ; pour les
    matchs de BEPRO_NOMS_PAR_MAILLOT, remplace aussi les noms faux des joueuses du Paris FC."""
    if df is None or df.empty or date is None:
        return df
    table = _noms_manquants()
    d = pd.Timestamp(date).strftime("%Y-%m-%d")
    mask = df["player"].astype(str).str.strip().str.fullmatch(_SANS_NOM.pattern, case=False)
    if mask.any():
        df = df.copy()
        df.loc[mask, "player"] = [table.get((d, str(sh).strip()), p)
                                  for p, sh in zip(df.loc[mask, "player"], df.loc[mask, "shirt"])]
    if any(k[0] == d for k in BEPRO_NOMS_PAR_MAILLOT):
        pfc = df["team"].astype(str).map(_norm).str.contains("paris")
        cles = list(zip([d] * len(df), df["shirt"].astype(str).str.strip()))
        connu = pfc & pd.Series([c in BEPRO_NOMS_PAR_MAILLOT for c in cles], index=df.index)
        if connu.any():
            df = df.copy()
            df.loc[connu, "player"] = [BEPRO_NOMS_PAR_MAILLOT[c] for c, k in zip(cles, connu) if k]
    return df


def est_sans_nom(name: str) -> bool:
    return bool(_SANS_NOM.fullmatch(str(name or "").strip()))

_SVG_W, _SVG_H = 100.0, 68.0

# Ballons touchés/joués (même règle que le tagging Sportscode, compute_tactical_stats) :
# instants contenant une passe (centres compris), un tir, une interception, un dribble
# ou un duel gagné.
TOUCH_EVENTS = {"Passes", "Crosses", "Shots & Goals", "Interceptions", "Take-on"}

# Événements « avec ballon » : localisés sur la heatmap.
BALL_EVENTS = {
    "Passes", "Passes Received", "Crosses", "Crosses Received", "Take-on",
    "Shots & Goals", "Recoveries", "Interceptions", "Step-in", "Aerial Control",
    "Set Pieces", "Clearances",
}


# ── Lecture ──────────────────────────────────────────────────────────────────

def _norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9 ]+", " ", s.lower()).strip()


def is_bepro_file(name: str) -> bool:
    n = str(name).lower()
    return ("raw event" in n and n.endswith((".zip", ".json"))) or (
        n.endswith(".json") and re.search(r"_(1st|2nd) half|_extra", n) is not None)


def parse_bepro_filename(name: str) -> dict:
    """« 2026-09-12_Paris FC U19 Féminines vs Le Havre AC U19 Féminines(Compétition)_Raw Event
    Data_JSON.zip » → {date, home, away, competition}."""
    base = os.path.basename(str(name))
    out = {"date": None, "home": "", "away": "", "competition": ""}
    m = re.match(r"(\d{4}-\d{2}-\d{2})_(.+?) vs (.+?)(?:\((.*?)\))?(?:_Raw Event Data.*|_(?:1st|2nd|\d+(?:st|nd|rd|th)) .*)?\.(?:zip|json)$",
                 base, re.IGNORECASE)
    if m:
        out["date"] = pd.Timestamp(m.group(1))
        out["home"] = m.group(2).strip()
        out["away"] = m.group(3).strip()
        out["competition"] = (m.group(4) or "").strip()
    return out


def read_bepro_events(path_or_bytes, name: str = "") -> List[dict]:
    """Lit un export Bepro (.zip de JSON ou .json seul) → liste d'événements bruts."""
    if isinstance(path_or_bytes, (bytes, bytearray)):
        raw, name = bytes(path_or_bytes), name
    else:
        name = name or str(path_or_bytes)
        with open(path_or_bytes, "rb") as f:
            raw = f.read()
    events: List[dict] = []
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            for member in sorted(z.namelist()):
                if member.lower().endswith(".json"):
                    events.extend(json.loads(z.read(member).decode("utf-8")).get("data", []))
    else:
        events.extend(json.loads(raw.decode("utf-8")).get("data", []))
    return events


def _to_frame(xv, yv, direction):
    """Coordonnées Bepro (0-1, absolues) → repère rapport (x 0-100 attaque à droite, y 0-68)."""
    try:
        x, y = float(xv), float(yv)
    except (TypeError, ValueError):
        return None, None
    if x != x or y != y:  # NaN
        return None, None
    if str(direction).upper() == "LEFT":
        x, y_left = 1.0 - x, y          # demi-tour : couloir gauche vers y = 0
    else:
        y_left = 1.0 - y                # attaque à droite : couloir gauche vers y = 1
    return (round(max(0.0, min(_SVG_W, x * _SVG_W)), 2),
            round(max(0.0, min(_SVG_H, y_left * _SVG_H)), 2))


def bepro_events_frame(events: List[dict], match_id: str = "") -> pd.DataFrame:
    """Une ligne par instant Bepro, coordonnées normalisées, événements gardés en liste."""
    rows = []
    for e in events:
        x, y = _to_frame(e.get("x"), e.get("y"), e.get("attack_direction"))
        tx, ty = _to_frame(e.get("to_x"), e.get("to_y"), e.get("attack_direction"))
        evs = [ev for ev in (e.get("events") or []) if ev.get("event_name")]
        rows.append({
            "match_id": match_id,
            "period": e.get("period_name", ""),
            "time_ms": e.get("event_time"),
            "team": e.get("team_name", ""),
            "player": e.get("player_name", "") or "",
            "shirt": str(e.get("player_shirt_number", "") or ""),
            "events": evs,
            "x": x, "y": y, "to_x": tx, "to_y": ty,
        })
    return pd.DataFrame(rows, columns=["match_id", "period", "time_ms", "team", "player", "shirt",
                                       "events", "x", "y", "to_x", "to_y"])


def load_bepro_folder(folder: str = BEPRO_FOLDER) -> Dict[str, dict]:
    """Tous les exports du dossier, un par match : {nom: {"info", "key", "df"}}.
    Clé de match = (date, domicile, extérieur) ; si le zip ET ses JSON par période sont
    présents, seul le zip est lu (sinon les événements seraient comptés deux fois)."""
    out: Dict[str, dict] = {}
    if not os.path.isdir(folder):
        return out
    groups: Dict[tuple, List[str]] = {}
    for n in sorted(os.listdir(folder)):
        if not is_bepro_file(n):
            continue
        info = parse_bepro_filename(n)
        key = (info["date"], _norm(info["home"]), _norm(info["away"])) if info["date"] is not None else (n,)
        groups.setdefault(key, []).append(n)
    for key, files in groups.items():
        zips = [f for f in files if f.lower().endswith(".zip")]
        chosen = [max(zips, key=lambda f: os.path.getmtime(os.path.join(folder, f)))] if zips else files
        ev: List[dict] = []
        for f in chosen:
            try:
                ev.extend(read_bepro_events(os.path.join(folder, f)))
            except Exception:
                continue
        if not ev:
            continue
        name = chosen[0]
        info = parse_bepro_filename(name)
        out[name] = {"info": info, "key": key,
                     "df": nommer_joueuses_sans_nom(bepro_events_frame(ev, name), info.get("date"))}
    return out


# Compétitions U23 : Bepro nomme l'équipe « Paris FC U19 Féminines » même au Challenge Espoirs
# LFFP (« 2026-10-07_Paris FC U19 Féminines vs AJ Auxerre Féminines(Challenge Espoirs LFFP)… »).
_COMPET_U23 = re.compile(r"espoir|challenge|\bu23\b|r[ée]gional", re.IGNORECASE)


def bepro_categorie(info: dict) -> str:
    """Catégorie d'un export (« U23 », « U19 »…, "" si inconnue) : la compétition prime sur le
    nom des équipes, qui reste « U19 » pour les matchs U23."""
    if _COMPET_U23.search(str(info.get("competition", "") or "")):
        return "U23"
    m = re.search(r"U(\d{2})", f"{info.get('home', '')} {info.get('away', '')}".upper())
    return f"U{m.group(1)}" if m else ""


def find_bepro_match(bepro: Dict[str, dict], date, adversaire: str = "",
                     categorie: str = "") -> Optional[pd.DataFrame]:
    """Export Bepro d'un match : même date, même catégorie (« U19 ») si elle est connue des
    deux côtés — sinon un export U19 servirait au match U23 du même jour —, et adversaire
    présent dans le nom s'il reste plusieurs candidats."""
    d = pd.to_datetime(date, errors="coerce")
    if pd.isna(d):
        return None
    cands = [v for v in bepro.values() if v["info"]["date"] is not None
             and v["info"]["date"].normalize() == d.normalize()]
    if categorie:
        cat = categorie.strip().upper()
        cands = [v for v in cands if not bepro_categorie(v["info"]) or bepro_categorie(v["info"]) == cat]
    if len(cands) > 1 and adversaire:
        a = _norm(adversaire)
        toks = [t for t in a.split() if len(t) > 2]
        c2 = [v for v in cands if a and (a in _norm(v["info"]["home"] + " " + v["info"]["away"])
                                         or any(t in _norm(v["info"]["home"] + " " + v["info"]["away"]) for t in toks))]
        cands = c2 or cands
    return cands[0]["df"] if cands else None


# ── Joueuses ─────────────────────────────────────────────────────────────────

def match_player_name(player: str, bepro_names) -> Optional[str]:
    """Nom Sportscode/référentiel (« MANE Mélita ») → nom Bepro (« Melita Mane »).
    Mêmes tokens (ordre et accents ignorés), sinon tous les tokens Bepro inclus, sinon
    nom de famille unique."""
    target = set(_norm(player).split())
    if not target:
        return None
    names = [n for n in set(bepro_names) if n]
    for n in names:
        if set(_norm(n).split()) == target:
            return n
    for n in names:
        nt = set(_norm(n).split())
        if len(nt) >= 2 and (nt <= target or target <= nt):
            return n
    # nom de famille seul (premier token majuscule côté référentiel), si unique côté Bepro
    surname = _norm(str(player).split()[0]) if str(player).split() else ""
    hits = [n for n in names if surname and surname in _norm(n).split()]
    return hits[0] if len(hits) == 1 else None


# ── Temps de jeu ─────────────────────────────────────────────────────────────

def bepro_minutes(df: pd.DataFrame, player: str, marge_min: float = 5.0) -> float:
    """Temps joué estimé depuis l'export (minutes), match par match puis cumulé : début du
    match si la 1re action a lieu dans les `marge_min` premières minutes, sinon minute de
    la 1re action (entrée en jeu) ; fin du match si la dernière action a lieu dans les
    `marge_min` dernières minutes, sinon minute de la dernière action (sortie). Plus proche
    du temps affiché sur Bepro que le temps GPS (présence sur le terrain)."""
    if df is None or df.empty:
        return 0.0
    total = 0.0
    for _mid, g in df.groupby("match_id") if "match_id" in df.columns else [("", df)]:
        t = pd.to_numeric(g["time_ms"], errors="coerce").dropna()
        tp = pd.to_numeric(g.loc[g["player"] == player, "time_ms"], errors="coerce").dropna()
        if t.empty or tp.empty:
            continue
        debut, fin = t.min(), t.max()
        a, b = tp.min(), tp.max()
        start = debut if a - debut <= marge_min * 60000 else a
        end = fin if fin - b <= marge_min * 60000 else b
        total += max(0.0, (end - start) / 60000.0)
    return round(total, 1)


# ── Statistiques ─────────────────────────────────────────────────────────────

def compute_bepro_player_stats(df: pd.DataFrame, player: str) -> dict:
    """Indicateurs du rapport individuel pour une joueuse, à partir d'un ou plusieurs matchs Bepro.
    Mêmes clés que compute_tactical_stats + "pass_breakdown" (format _report_pass_breakdown).
    Retourne {} si la joueuse n'est pas dans l'export."""
    if df is None or df.empty:
        return {}
    name = match_player_name(player, df["player"].unique())
    if not name:
        return {}
    d = df[df["player"] == name]

    s = {k: 0 for k in ("passes_ok", "passes_ko", "drib_ok", "drib_ko", "tirs_tot", "tirs_cadres",
                        "tirs_buts", "assists", "interceptions", "recuperations", "sol_ok", "sol_ko",
                        "aer_ok", "aer_ko", "duels_gagnes", "duels_perdus", "ballons", "pertes",
                        "passes_cles", "courtes_ok", "courtes_ko", "longues_ok", "longues_ko")}
    pb = {"avant": [0, 0], "arriere": [0, 0], "cotes": [0, 0], "dernier_tiers": [0, 0], "diago": 0,
          "moitie_def": [0, 0], "moitie_off": [0, 0]}
    locs, passes_map = [], []

    for _, r in d.iterrows():
        names = {ev["event_name"] for ev in r["events"]}
        has_ball = bool(names & BALL_EVENTS)
        lost = False
        for ev in r["events"]:
            en, pr = ev["event_name"], ev.get("property") or {}
            out = pr.get("Outcome", "")
            if en == "Passes":
                ok = out == "Succeeded"
                s["passes_ok" if ok else "passes_ko"] += 1
                lost |= not ok
                dirn = pr.get("Direction", "")
                cat = {"Passes Forward": "avant", "Passes Backward": "arriere",
                       "Passes Sideways": "cotes"}.get(dirn)
                if cat:
                    pb[cat][0] += 1; pb[cat][1] += ok
                if pr.get("Area") == "Passes In Final Third":
                    pb["dernier_tiers"][0] += 1; pb["dernier_tiers"][1] += ok
                dist = pr.get("Distance", "")
                if dist == "Long Passes":
                    s["longues_ok" if ok else "longues_ko"] += 1
                elif dist:
                    s["courtes_ok" if ok else "courtes_ko"] += 1
                if r["x"] is not None:
                    half = "moitie_off" if r["x"] >= _SVG_W / 2 else "moitie_def"
                    pb[half][0] += 1; pb[half][1] += ok
                    passes_map.append({"x": r["x"], "y": r["y"], "ok": ok,
                                       "longue": dist == "Long Passes",
                                       "to_x": r["to_x"], "to_y": r["to_y"]})
            elif en == "Crosses":
                lost |= out == "Failed"
            elif en == "Take-on":
                if out == "Succeeded":
                    s["drib_ok"] += 1
                else:
                    s["drib_ko"] += 1
                    lost |= out == "Failed"
            elif en == "Shots & Goals":
                s["tirs_tot"] += 1
                if out in ("Shots On Target", "Goals"):
                    s["tirs_cadres"] += 1
                if out == "Goals":
                    s["tirs_buts"] += 1
            elif en == "Assists":
                s["assists"] += 1
            elif en == "Key Passes":
                s["passes_cles"] += 1
            elif en == "Interceptions":
                s["interceptions"] += 1
            elif en == "Recoveries":
                s["recuperations"] += 1
            elif en == "Mistakes":
                lost = True
            elif en == "Duels":
                won = out == "Succeeded"
                s["duels_gagnes" if won else "duels_perdus"] += 1
                if pr.get("Type") == "Aerial Duels":
                    s["aer_ok" if won else "aer_ko"] += 1
                else:  # Physical Duels, Loose Ball Duels, duels sans type (fautes)
                    s["sol_ok" if won else "sol_ko"] += 1
            elif en == "Tackles":
                # Tacle = duel défensif au sol (comme dans les indicateurs, bepro_kpi_inputs)
                won = out.startswith("Tackle Succeeded")
                s["duels_gagnes" if won else "duels_perdus"] += 1
                s["sol_ok" if won else "sol_ko"] += 1
        touche = bool(names & TOUCH_EVENTS) or any(
            ev["event_name"] == "Duels" and (ev.get("property") or {}).get("Outcome") == "Succeeded"
            for ev in r["events"])
        if touche:
            s["ballons"] += 1
            # Heatmap = mêmes actions que « ballons joués » (sinon plus de ballons localisés
            # que de ballons joués : réceptions, récupérations… comptées en plus)
            if r["x"] is not None:
                locs.append({"x": r["x"], "y": r["y"]})
        if lost:
            s["pertes"] += 1

    s["locs"] = locs
    s["passes_map"] = passes_map
    s["pass_breakdown"] = pb
    s["nom_bepro"] = name
    s["n_matchs_bepro"] = int(d["match_id"].nunique()) if "match_id" in d.columns else 1
    s["temps_jeu_min"] = bepro_minutes(df, name)
    return s


# ── Compteurs des indicateurs tactiques (create_metrics / create_kpis de l'app) ──

KPI_INPUT_COLS = ["Duels défensifs", "Duels défensifs gagnés", "Fautes", "Interceptions", "Passes",
                  "Passes courtes", "Passes réussies (courtes)", "Passes longues", "Passes réussies (longues)",
                  "Dribbles", "Dribbles réussis", "Tirs", "Tirs cadrés",
                  "__total_passes", "__last_third", "__assists", "__deseq"]


def bepro_kpi_inputs(df: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par joueuse de l'export, avec les compteurs attendus par create_metrics
    (mêmes noms de colonnes que create_data sur Sportscode). Correspondances :
    duels défensifs = Duels + Tackles (gagnés : Succeeded / « Tackle Succeeded… ») ;
    fautes = Fouls de type « Fouls » / « Handball Foul » (commises, pas subies) ;
    passes courtes = Short + Medium Range, longues = Long Passes ; dribbles = Take-on ;
    tirs cadrés = Shots On Target + Goals ; dernier tiers = Area « Passes In Final Third » ;
    créations de déséquilibre (__deseq) = Key Passes, équivalent Bepro le plus proche."""
    rows = []
    if df is None or df.empty:
        return pd.DataFrame(columns=["Player"] + KPI_INPUT_COLS)
    for player, g in df.groupby("player"):
        if not str(player).strip():
            continue
        c = dict.fromkeys(KPI_INPUT_COLS, 0)
        for evs in g["events"]:
            for ev in evs:
                en, pr = ev.get("event_name"), ev.get("property") or {}
                out = str(pr.get("Outcome", ""))
                if en == "Duels":
                    c["Duels défensifs"] += 1
                    c["Duels défensifs gagnés"] += out == "Succeeded"
                elif en == "Tackles":
                    c["Duels défensifs"] += 1
                    c["Duels défensifs gagnés"] += out.startswith("Tackle Succeeded")
                elif en == "Fouls" and pr.get("Type") in ("Fouls", "Handball Foul"):
                    c["Fautes"] += 1
                elif en == "Interceptions":
                    c["Interceptions"] += 1
                elif en == "Passes":
                    ok = out == "Succeeded"
                    c["Passes"] += 1
                    c["__total_passes"] += 1
                    if pr.get("Distance") == "Long Passes":
                        c["Passes longues"] += 1
                        c["Passes réussies (longues)"] += ok
                    else:
                        c["Passes courtes"] += 1
                        c["Passes réussies (courtes)"] += ok
                    if pr.get("Area") == "Passes In Final Third":
                        c["__last_third"] += 1
                elif en == "Take-on":
                    c["Dribbles"] += 1
                    c["Dribbles réussis"] += out == "Succeeded"
                elif en == "Shots & Goals":
                    c["Tirs"] += 1
                    c["Tirs cadrés"] += out in ("Shots On Target", "Goals")
                elif en == "Assists":
                    c["__assists"] += 1
                elif en == "Key Passes":
                    c["__deseq"] += 1
        rows.append({"Player": player, **c})
    return pd.DataFrame(rows, columns=["Player"] + KPI_INPUT_COLS)



# ── Rapport collectif ────────────────────────────────────────────────────────

# Actions avec ballon qui ouvrent / prolongent une possession ; START en ouvre toujours une
# nouvelle (récupération, interception, coup de pied arrêté, remise en jeu).
_POSS_START = {"Recoveries", "Interceptions", "Set Pieces", "Step-in"}
_POSS_BALL = {"Passes", "Passes Received", "Crosses", "Crosses Received", "Take-on",
              "Shots & Goals", "Aerial Control"} | _POSS_START
_POSS_GAP_MS = 15000  # plus de 15 s sans action de l'équipe : le ballon a changé de mains
_TIERS = _SVG_W * 2 / 3
_ZONE_ROWS, _ZONE_COLS = ["Def", "MDef", "MOf", "Off"], ["G", "C", "D"]


def _zone(x, y):
    if x is None or y is None or x != x or y != y:
        return None
    ri = min(3, max(0, int(x // (_SVG_W / 4))))
    ci = min(2, max(0, int(y // (_SVG_H / 3))))
    return _ZONE_ROWS[ri] + _ZONE_COLS[ci]


def _couloir(y):
    return "Couloir Gauche" if y < _SVG_H / 3 else ("Couloir Droit" if y > _SVG_H * 2 / 3 else "Couloir Central")


def _mi_temps(period: str) -> str:
    p = str(period or "").lower()
    return "MT1" if p.startswith("1st") else ("MT2" if p.startswith("2nd") else "")


def compute_bepro_collective_report(df: pd.DataFrame, adv_name: str = "Adversaire") -> dict:
    """Rapport collectif (mêmes clés que compute_collective_report sur Sportscode) depuis un
    export Bepro. La plupart des exports ne contiennent que les actions du Paris FC : les
    valeurs adverses sont alors None (« — » dans le rapport), sauf les fautes adverses
    (= fautes subies par le Paris FC). Possessions reconstituées : suite d'actions avec ballon
    de la même équipe, close par une perte (passe, centre ou dribble raté, erreur), un tir,
    un hors-jeu, une récupération / un coup de pied arrêté (nouvelle possession) ou 15 s sans
    action. Pourcentages de possession et temps de jeu effectif seulement si l'export contient
    les deux équipes. {} si aucune action du Paris FC."""
    if df is None or df.empty:
        return {}
    d = df.copy()
    d["_pfc"] = d["team"].astype(str).map(_norm).str.contains("paris")
    if not d["_pfc"].any():
        return {}
    # Adversaire « complet » seulement si l'export suit vraiment ses actions (l'export de
    # Montpellier du 04/11/2026 ne contient que ses 4 buts) ; sinon seuls ses buts sont repris.
    _n_adv = int((~d["_pfc"] & d["team"].astype(str).str.strip().ne("")).sum())
    has_adv = _n_adv >= 0.3 * int(d["_pfc"].sum())
    d["_mt"] = d["period"].map(_mi_temps)
    d["_po"] = d["period"].map(lambda p: {"MT1": 0, "MT2": 1}.get(_mi_temps(p), 2))
    d["_t"] = pd.to_numeric(d["time_ms"], errors="coerce")
    d = d.sort_values(["match_id", "_po", "_t"], kind="stable")
    PFC, ADV = "PFC", (adv_name or "Adversaire")

    poss = []          # {team, mt, t0, t1, perte, entree, couloir, x_min}
    recup_pfc, perte_pfc = [], []
    cnt = {k: dict.fromkeys(("tirs", "tirs_cadres", "buts", "hors_jeu", "fautes", "fautes_subies",
                             "passes", "passes_ok", "passes_avant", "passes_longues",
                             "passes_dt", "passes_dt_ok", "centres"), 0) for k in (PFC, ADV)}
    cur = None

    def _close():
        nonlocal cur
        if cur is not None:
            poss.append(cur)
        cur = None

    for _, r in d.iterrows():
        team = PFC if r["_pfc"] else ADV
        evs = r["events"] or []
        names = {e.get("event_name") for e in evs}
        c = cnt[team]
        perte = False
        for e in evs:
            en, pr = e.get("event_name"), e.get("property") or {}
            out = str(pr.get("Outcome", ""))
            if en == "Shots & Goals":
                c["tirs"] += 1
                c["tirs_cadres"] += out in ("Shots On Target", "Goals")
                c["buts"] += out == "Goals"
            elif en == "Offsides":
                c["hors_jeu"] += 1
            elif en == "Fouls":
                c["fautes"] += pr.get("Type") in ("Fouls", "Handball Foul")
                c["fautes_subies"] += pr.get("Type") == "Fouls Won"
            elif en == "Passes":
                ok = out == "Succeeded"
                c["passes"] += 1
                c["passes_ok"] += ok
                c["passes_avant"] += pr.get("Direction") == "Passes Forward"
                c["passes_longues"] += pr.get("Distance") == "Long Passes"
                if pr.get("Area") == "Passes In Final Third":
                    c["passes_dt"] += 1
                    c["passes_dt_ok"] += ok
                perte |= out == "Failed"
            elif en == "Crosses":
                c["centres"] += 1
                perte |= out == "Failed"
            elif en in ("Take-on", "Aerial Control"):
                perte |= out == "Failed"
            elif en == "Mistakes":
                perte = True
        tacle_poss = any(e.get("event_name") == "Tackles"
                         and str((e.get("property") or {}).get("Outcome", "")).endswith(": Possession")
                         for e in evs)
        x, y = r["x"], r["y"]
        if team == PFC and (names & {"Recoveries", "Interceptions"} or tacle_poss):
            z = _zone(x, y)
            if z:
                recup_pfc.append(z)
        if not (names & _POSS_BALL or tacle_poss):
            continue
        t = r["_t"] if pd.notna(r["_t"]) else (cur["t1"] if cur else 0)
        if (cur is None or cur["team"] != team or cur["mt"] != r["_mt"] or cur["mid"] != r["match_id"]
                or names & _POSS_START or tacle_poss or t - cur["t1"] > _POSS_GAP_MS):
            _close()
            cur = {"team": team, "mt": r["_mt"], "mid": r["match_id"], "t0": t, "t1": t,
                   "perte": False, "entree": False, "couloir": None,
                   "bas": x is not None and x == x and x < _TIERS}
        cur["t1"] = t
        # Entrée dans le dernier 1/3 : action avec ballon au-delà des 2/3, ou passe / centre
        # réussi qui y arrive, après être passé par les deux premiers tiers.
        if not cur["entree"]:
            pts = []
            for e in evs:
                if e.get("event_name") in ("Passes", "Crosses") and \
                        (e.get("property") or {}).get("Outcome") == "Succeeded" and r["to_x"] is not None:
                    pts.append((r["to_x"], r["to_y"]))
            if x is not None and x == x:
                pts.insert(0, (x, y))
            for px, py in pts:
                if px is None or px != px:
                    continue
                if px >= _TIERS and cur["bas"] and py is not None and py == py:
                    cur["entree"], cur["couloir"] = True, _couloir(py)
                    break
                if px < _TIERS:
                    cur["bas"] = True
        if perte:
            cur["perte"] = True
            if team == PFC:
                z = _zone(x, y)
                if z:
                    perte_pfc.append(z)
            _close()
        elif names & {"Shots & Goals", "Offsides"}:
            _close()
    _close()

    def team_stats(team):
        if team == ADV and not has_adv:
            st = dict.fromkeys(("possessions", "duree_totale", "duree_moyenne", "tirs", "tirs_cadres",
                                "tirs_non_cadres", "buts", "hors_jeu", "pertes", "pct_pertes",
                                "pct_tirs_cadres", "entrees_1_3", "poss_par_entree", "entrees_par_tir"))
            st["fautes"] = cnt[PFC]["fautes_subies"]
            if cnt[ADV]["buts"]:
                st["buts"] = cnt[ADV]["buts"]
            return st
        p = [q for q in poss if q["team"] == team]
        durs = [max(0.0, (q["t1"] - q["t0"]) / 1000.0) for q in p]
        n, c = len(p), cnt[team]
        pertes = sum(q["perte"] for q in p)
        entrees = sum(q["entree"] for q in p)
        tirs = c["tirs"]
        return {
            "possessions": n,
            "duree_totale": float(sum(durs)),
            "duree_moyenne": float(sum(durs) / n) if n else 0.0,
            "tirs": tirs, "tirs_cadres": c["tirs_cadres"], "tirs_non_cadres": tirs - c["tirs_cadres"],
            "buts": c["buts"], "hors_jeu": c["hors_jeu"], "pertes": pertes,
            "pct_pertes": round(pertes / n * 100, 1) if n else 0.0,
            "pct_tirs_cadres": round(c["tirs_cadres"] / tirs * 100, 1) if tirs else 0.0,
            "entrees_1_3": entrees,
            "poss_par_entree": round(n / entrees, 1) if entrees else None,
            "entrees_par_tir": round(entrees / tirs, 1) if tirs else None,
            "fautes": c["fautes"],
        }

    stats = {PFC: team_stats(PFC), ADV: team_stats(ADV)}

    def _dur(team, mt=None):
        return sum(max(0.0, (q["t1"] - q["t0"]) / 1000.0) for q in poss
                   if q["team"] == team and (mt is None or q["mt"] == mt))

    def _pct(mt=None):
        if not has_adv:
            return {PFC: None, ADV: None}
        a, b = _dur(PFC, mt), _dur(ADV, mt)
        return {PFC: round(a / (a + b) * 100, 1) if a + b else 50.0,
                ADV: round(b / (a + b) * 100, 1) if a + b else 50.0}

    def _temps(mt=None):
        return (_dur(PFC, mt) + _dur(ADV, mt)) if has_adv else None

    entrees = [q["couloir"] for q in poss if q["team"] == PFC and q["entree"]]
    entree_tiers = {k: round(entrees.count(k) / len(entrees) * 100, 1) if entrees else 0.0
                    for k in ("Couloir Gauche", "Couloir Central", "Couloir Droit")}

    def _grid(zs):
        n = len(zs)
        return [[round(zs.count(r + c) / n * 100, 1) if n else 0.0 for c in _ZONE_COLS] for r in _ZONE_ROWS]

    c = cnt[PFC]
    pc = lambda a, b: round(a / b * 100, 1) if b else 0.0  # noqa: E731
    return {
        "pfc_name": PFC, "adv_name": ADV, "source": "bepro", "adv_absent": not has_adv,
        "stats": stats,
        "temps_total": _temps(), "temps_mt1": _temps("MT1"), "temps_mt2": _temps("MT2"),
        "poss_total": _pct(), "poss_mt1": _pct("MT1"), "poss_mt2": _pct("MT2"),
        "animation": {}, "circulation": {},
        "passes": {"Passes réussies": pc(c["passes_ok"], c["passes"]),
                   "Passes vers l'avant": pc(c["passes_avant"], c["passes"]),
                   "Passes longues": pc(c["passes_longues"], c["passes"]),
                   "Réussite dans le dernier 1/3": pc(c["passes_dt_ok"], c["passes_dt"])},
        "n_passes": c["passes"],
        "entree_tiers": entree_tiers,
        "grid_recup": _grid(recup_pfc), "grid_perte": _grid(perte_pfc),
        "zone_rows": list(_ZONE_ROWS), "zone_cols": list(_ZONE_COLS),
    }
