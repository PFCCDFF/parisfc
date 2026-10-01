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

_SVG_W, _SVG_H = 100.0, 68.0

# Événements « avec ballon » : comptés comme ballons joués et localisés sur la heatmap.
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
        out[name] = {"info": parse_bepro_filename(name), "key": key, "df": bepro_events_frame(ev, name)}
    return out


def find_bepro_match(bepro: Dict[str, dict], date, adversaire: str = "",
                     categorie: str = "") -> Optional[pd.DataFrame]:
    """Export Bepro d'un match : même date, même catégorie (« U19 ») si elle figure des deux
    côtés — sinon un export U19 servirait au match U23 du même jour —, et adversaire présent
    dans le nom s'il reste plusieurs candidats."""
    d = pd.to_datetime(date, errors="coerce")
    if pd.isna(d):
        return None
    cands = [v for v in bepro.values() if v["info"]["date"] is not None
             and v["info"]["date"].normalize() == d.normalize()]
    if categorie:
        cat = categorie.strip().upper()

        def _cats(v):
            return set(re.findall(r"U\d{2}", (v["info"]["home"] + " " + v["info"]["away"]).upper()))
        cands = [v for v in cands if not _cats(v) or cat in _cats(v)]
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
        if has_ball:
            s["ballons"] += 1
            if r["x"] is not None:
                locs.append({"x": r["x"], "y": r["y"]})
        if lost:
            s["pertes"] += 1

    s["locs"] = locs
    s["passes_map"] = passes_map
    s["pass_breakdown"] = pb
    s["nom_bepro"] = name
    s["n_matchs_bepro"] = int(d["match_id"].nunique()) if "match_id" in d.columns else 1
    return s
