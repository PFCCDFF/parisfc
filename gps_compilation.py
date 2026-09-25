"""
Compilation des exports GPS bruts (dossier Drive « CSV GPS », synchronisé en local dans
data/gps et data/gps_match) en une table longue : une ligne = une joueuse × une session.

Sans dépendance Streamlit : utilisable par l'app, par sync_drive_to_supabase.py ou en
ligne de commande :
    python3 gps_compilation.py data/gps data/gps_match -o data/gps_sessions.csv

Deux formats d'export sont gérés :
- Format A (saison 2025-26) : CSV plat, une ligne par joueuse (« Activity Date, Capteur,
  … »), en français ou en anglais (Game Report). Plages de vitesse fournies directement.
- Format B (saison 2026-27) : CSV multi-sections séparé par « ; » (en-tête de session,
  Periods, Global Metrics, Effective Metrics, Periods Metrics).

Plages de vitesse (identiques pour les deux formats) :
    P1 0-7 · P2 7-13 · P3 13-15 · P4 15-19 · P5 19-23 · P6 > 23 km/h
Format B : HID1 = 13-19 km/h, HID2 = 19-23 km/h, Sprint = > 23 km/h, d'où
    P1 = R<7 ; P2 = D − (R<7 + HID1 + HID2 + Sprint) ; P4 = R>15 − (HID2 + Sprint) ;
    P3 = HID1 − P4 ; P5 = HID2 ; P6 = Sprint.

Règles de nettoyage (issues de l'audit du dossier, septembre 2026) :
- un même contenu stocké plusieurs fois (racine, GPS Matchs, copies U18/U19) n'est gardé
  qu'une fois ; un fichier « partiel » (ex. un tiers-temps) est écarté quand un fichier du
  même jour contient les mêmes joueuses avec des distances supérieures (le cumul) ;
- les lignes sans nom (totaux/moyennes de l'export) et les joueuses à 0 m sont retirées ;
- les lignes sans date ni capteur ajoutées en fin de fichier deviennent « complement »
  (fichiers match) ou « ajout_manuel » (fichiers séance) ;
- format B match : les deux premières périodes = le match, les suivantes = complément ;
  une session INDIVIDUAL_MATCH (course individuelle après match) = complément.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import sys
import unicodedata
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd

BANDES = ["p1_m", "p2_m", "p3_m", "p4_m", "p5_m", "p6_m"]
COLONNES = ["date", "saison", "type_session", "ligne", "equipe", "session_id", "session_label", "joueuse",
            "temps_min", "distance_m", *BANDES, "acc2", "acc3", "acc4", "dec2", "dec3", "dec4", "vmax_kmh",
            "systeme", "remarques", "qualite_ok", "fichier"]
_SUFFIXE_ID = re.compile(r"__[A-Za-z0-9_-]{8}$")   # suffixe ajouté par _safe_local_path()
_MARQUEURS_B = {"Periods", "Global Metrics", "Effective Metrics", "Periods Metrics"}

# Format A : en-têtes anglais (Game Report) → français
_COLS_EN = {"Sensor": "Capteur", "Player No.": "Numéro de joueur", "Player Name": "Nom de joueur",
            "Time Played": "Temps joué", "HID Distance (>13 km/h)": "Distance HID (>13 km/h)",
            "HID Distance (>19 km/h)": "Distance HID (>19 km/h)", "Max Speed (km/h)": "Vitesse max (km/h)",
            "Max Acceleration (m/s²)": "Accélération maximale (m/s²)"}


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────────────────────────────────────────
def nom_fichier(path: str) -> str:
    """Nom Drive d'origine (sans extension ni suffixe __<id8> du téléchargement local)."""
    base = os.path.splitext(os.path.basename(path))[0]
    return _SUFFIXE_ID.sub("", base).strip()


def _cle_nom(n: str) -> str:
    n = unicodedata.normalize("NFKD", str(n)).encode("ascii", "ignore").decode().upper()
    return " ".join(sorted(n.replace("-", " ").split()))


def regrouper_variantes(noms: pd.Series, seuil: float = 0.8) -> dict:
    """Rattache les variantes orthographiques à la forme la plus fréquente :
    fautes de frappe (« MUPSAFOSI » → « MUPFASONI »), prénom seul quand il est
    sans ambiguïté (« Louane » → « Louane EXILIE »), prénom et nom inversés."""
    import difflib
    freq = noms.value_counts()
    cles = {n: _cle_nom(n) for n in freq.index}
    canon: dict[str, str] = {}
    representants: list[str] = []                  # du plus fréquent au moins fréquent
    for n in freq.index:
        k = cles[n]
        cible = None
        for r in representants:
            kr = cles[r]
            tk, tr = set(k.split()), set(kr.split())
            ratio = max(difflib.SequenceMatcher(None, k.replace(" ", ""), kr.replace(" ", "")).ratio(),
                        difflib.SequenceMatcher(None, "".join(sorted(k.replace(" ", ""))), "".join(sorted(kr.replace(" ", "")))).ratio() - 0.1)
            if k == kr or ratio >= seuil or (len(tk) >= 2 and len(tr) >= 2 and (tk <= tr or tr <= tk)):
                cible = r
                break
        if cible is None and len(k.split()) == 1:  # prénom seul
            cands = [r for r in representants if k in cles[r].split()]
            if len(cands) == 1:
                cible = cands[0]
        if cible is None:
            representants.append(n)
            cible = n
        canon[n] = cible
    return canon


def nom_lisible(n: str) -> str:
    """« MINYEMECK DIssya » / « Dumans Nina » / « Nina DUMANS » → « Prénom NOM »."""
    toks = str(n).replace(".", " ").split()
    if not toks:
        return ""
    nom = [t for t in toks if t.isupper() and len(t) > 1]
    prenom = [t for t in toks if not (t.isupper() and len(t) > 1)]
    if not nom:                      # « Mouradi Nessma » : ordre inconnu, on garde tel quel
        return " ".join(t.capitalize() for t in toks)
    if not prenom:                   # « MAELLINE MEGEVAND »
        prenom, nom = [nom[0].capitalize()], nom[1:]
    return " ".join([p[0].upper() + p[1:].lower() for p in prenom] + [" ".join(nom).upper()])


def _minutes(v) -> float:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    s = str(v).strip()
    if not s:
        return np.nan
    if ":" in s:
        p = [float(x) for x in s.split(":")]
        return p[0] * 60 + p[1] + p[2] / 60 if len(p) == 3 else p[0] + p[1] / 60
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return np.nan


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False).str.strip(), errors="coerce")


def _date_nom_fichier(nom: str) -> Optional[pd.Timestamp]:
    m = re.search(r"(\d{1,2})[._\-/](\d{1,2})[._\-/](\d{2,4})(?!.*\d{1,2}[._\-/]\d{1,2}[._\-/]\d{2,4})", nom)
    if not m:
        return None
    d, mo, y = m.groups()
    y = int(y) + 2000 if len(y) == 2 else int(y)
    try:
        return pd.Timestamp(year=y, month=int(mo), day=int(d))
    except ValueError:
        return None


def saison(d: pd.Timestamp) -> str:
    y = d.year if d.month >= 7 else d.year - 1
    return f"{y}-{str(y + 1)[-2:]}"


def est_format_b(path: str) -> bool:
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            tete = "".join(f.readline() for _ in range(60))
    except OSError:
        return False
    return "Global Metrics" in tete


def est_seance(nom: str) -> bool:
    n = unicodedata.normalize("NFKD", nom).encode("ascii", "ignore").decode().lower()
    return "seance" in n


# ─────────────────────────────────────────────────────────────────────────────
# Format A
# ─────────────────────────────────────────────────────────────────────────────
def lire_format_a(path: str) -> pd.DataFrame:
    with open(path, "rb") as f:
        brut = f.read().decode("utf-8-sig", errors="replace")
    d = pd.read_csv(io.StringIO(brut), dtype=str)
    d.columns = [_COLS_EN.get(c, c).replace("Distance Speed Range", "Distance par plage de vitesse")
                 .replace("Decélerations", "Decelerations") for c in d.columns]
    if "Nom de joueur" not in d.columns or "Distance (m)" not in d.columns:
        return pd.DataFrame()
    nom = d["Nom de joueur"].fillna("").str.strip()
    d = d[nom != ""].copy()                                     # lignes total / moyenne
    col = lambda c: _num(d[c]) if c in d.columns else pd.Series(np.nan, index=d.index)
    plage = lambda k: col(f"Distance par plage de vitesse ({k} km/h)")
    out = pd.DataFrame({
        "joueuse_brut": d["Nom de joueur"].str.strip(),
        "date_brute": d.get("Activity Date"),
        "temps_min": d["Temps joué"].map(_minutes) if "Temps joué" in d else np.nan,
        "distance_m": col("Distance (m)"),
        "p1_m": plage("0-7"), "p2_m": plage("7-13"), "p3_m": plage("13-15"), "p4_m": plage("15-19"),
        "p5_m": plage("19-23"), "p6_m": plage("23-25").fillna(0) + plage(">25").fillna(0),
        "acc2": col("# of Accelerations (>2 m/s²)"), "acc3": col("# of Accelerations (>3 m/s²)"),
        "acc4": col("# of Accelerations (>4 m/s²)"), "dec2": col("# of Decelerations (>2 m/s²)"),
        "dec3": col("# of Decelerations (>3 m/s²)"), "dec4": col("# of Decelerations (>4 m/s²)"),
        "vmax_kmh": col("Vitesse max (km/h)"),
    })
    out["sans_date"] = out["date_brute"].isna()
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Format B
# ─────────────────────────────────────────────────────────────────────────────
def _bandes_b(t: pd.DataFrame) -> dict:
    g = lambda k: _num(t[k]) if k in t.columns else pd.Series(np.nan, index=t.index)
    D, R7, H1, R15, H2, S = (g("Distance (m)"), g("R<7 distance (m)"), g("Hid1 distance (m)"),
                             g("R>15 distance (m)"), g("Hid2 distance (m)"), g("Sprint distance (m)"))
    p4 = R15 - (H2 + S)
    return {"distance_m": D, "p1_m": R7, "p2_m": D - (R7 + H1 + H2 + S), "p3_m": H1 - p4, "p4_m": p4,
            "p5_m": H2, "p6_m": S}


def _table_b(t: pd.DataFrame) -> pd.DataFrame:
    g = lambda k: _num(t[k]) if k in t.columns else pd.Series(np.nan, index=t.index)
    prenom = t.get("First Name", pd.Series("", index=t.index)).fillna("").astype(str).str.replace(".", "", regex=False)
    out = pd.DataFrame({"joueuse_brut": (prenom + " " + t["Last Name"].fillna("").astype(str)).str.strip(),
                        "temps_min": g("Time (min)"), **_bandes_b(t),
                        "acc2": g("Accel > 2 m/s² (nb)"), "acc3": g("Accel > 3 m/s² (nb)"), "acc4": np.nan,
                        "dec2": g("Decel > 2 m/s² (nb)"), "dec3": g("Decel > 3 m/s² (nb)"), "dec4": np.nan,
                        "vmax_kmh": g("Speed max (km/h)")})
    return out[out.joueuse_brut != ""]


def lire_format_b(path: str) -> tuple[dict, pd.DataFrame]:
    """Retourne (en-tête de session, lignes). Chaque ligne porte 'ligne' = principal/complement/individuel."""
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        rows = list(csv.reader(f, delimiter=";"))
    vide = lambda r: not r or all(not str(c).strip() for c in r)
    non_vides = [r for r in rows if not vide(r)]
    entete = dict(zip([c.strip() for c in non_vides[0]], non_vides[1])) if len(non_vides) >= 2 else {}

    def bloc(debut: int) -> pd.DataFrame:
        b = []
        for r in rows[debut:]:
            if vide(r) or (len(r) == 1 and r[0].strip() in _MARQUEURS_B):
                break
            b.append(r)
        if len(b) < 2:
            return pd.DataFrame()
        cols = [c.strip() for c in b[0]]
        n = len(cols)
        return pd.DataFrame([(r + [""] * n)[:n] for r in b[1:]], columns=cols)

    sections = {}
    for i, r in enumerate(rows):
        if len(r) == 1 and r[0].strip() in ("Global Metrics", "Effective Metrics"):
            sections[r[0].strip()] = bloc(i + 1)
    periodes = []                                  # [(début, nom, table)]
    if any(len(r) == 1 and r[0].strip() == "Periods Metrics" for r in rows):
        k0 = next(i for i, r in enumerate(rows) if len(r) == 1 and r[0].strip() == "Periods Metrics")
        for i in range(k0, len(rows) - 2):
            if rows[i][:2] == ["Name", "Start Date"] and rows[i + 2][:2] == ["First Name", "Last Name"]:
                t = bloc(i + 2)
                if len(t):
                    periodes.append((rows[i + 1][1], rows[i + 1][0], t))

    typ = str(entete.get("Type", "")).upper()
    # Un export INDIVIDUAL_MATCH découpé en périodes est un vrai match (export d'une seule joueuse) ;
    # sans périodes, c'est une course individuelle enregistrée après le match (complément).
    if typ == "INDIVIDUAL_MATCH" and len(periodes) >= 2:
        typ = "MATCH"
    if typ == "MATCH" and periodes:
        periodes.sort(key=lambda p: p[0])
        parts = []
        for rang, (_, _, t) in enumerate(periodes):
            x = _table_b(t)
            x["ligne"] = "principal" if rang < 2 else "complement"
            parts.append(x)
        p = pd.concat(parts, ignore_index=True)
        somme = [c for c in ["temps_min", "distance_m", *BANDES, "acc2", "acc3", "dec2", "dec3"]]
        out = p.groupby(["joueuse_brut", "ligne"], as_index=False).agg({**{c: "sum" for c in somme}, "vmax_kmh": "max"})
        out["acc4"] = out["dec4"] = np.nan
    else:
        t = sections.get("Effective Metrics") if typ == "MATCH" else sections.get("Global Metrics")
        if t is None or t.empty or _num(t.get("Distance (m)", pd.Series(dtype=str))).fillna(0).sum() == 0:
            t = sections.get("Global Metrics", pd.DataFrame())
        out = _table_b(t) if len(t) else pd.DataFrame(columns=["joueuse_brut"])
        out["ligne"] = ("complement" if typ == "INDIVIDUAL_MATCH" else
                        "individuel" if typ.startswith("INDIVIDUAL") else "principal")
    return entete, out


# ─────────────────────────────────────────────────────────────────────────────
# Métadonnées de session
# ─────────────────────────────────────────────────────────────────────────────
def _equipe_depuis_nom(nom: str) -> Optional[str]:
    m = re.match(r"\s*(U\d\d)\b", nom)
    if m:
        return m.group(1)
    if nom.startswith("GF1_U19"):
        return "U19"
    if nom.startswith("Game_Report_ParisFCU23"):
        return "U23"
    return None


def meta_match_a(noms: list[str]) -> dict:
    """noms : tous les noms de fichiers portant le même contenu (copies)."""
    descriptif = sorted(noms, key=lambda n: (n.startswith("GF1"), n))[0]
    equipes = sorted({e for e in map(_equipe_depuis_nom, noms) if e})
    if not equipes:
        equipes = ["U18"] if any(n.startswith("GF1_Paris_FC") for n in noms) else ["?"]
    b = descriptif
    comp = "Amical"
    for motif, lab in [(r"Vinci Cup", "Vinci Cup"), (r"Essone Cup|Essonne Cup", "Essonne Cup")]:
        if re.search(motif, b):
            comp = lab
    if m := re.search(r"Coupe Nike (\d+e)", b):
        comp = f"Coupe Nike {m.group(1)}"
    elif m := re.search(r"Coupe de Paris (\d+e)", b):
        comp = f"Coupe de Paris {m.group(1)}"
    elif m := re.search(r"U19 ([12]):(J\d+)", b):
        comp = f"Championnat phase {m.group(1)} {m.group(2)}"
    elif m := re.search(r"U\d\d (J\d+)", b):
        comp = f"Championnat {m.group(1)}"
    elif m := re.search(r"_(J\d+)_", b):
        comp = f"Championnat {m.group(1)}"
    if b.startswith("GF1_"):
        lab = re.sub(r"^GF1_(U\d\d_)?", "", b)
        lab = re.sub(r"(_J\d+)?_\d\d_\d\d_\d\d$", "", lab).replace("_", " ")
    elif b.startswith("Game_Report"):
        lab = "Paris FC U23"
    else:
        lab = re.sub(r"^(U\d\d\s*)?(\((A|Vinci Cup|Essone Cup)\)|[12]:J\d+|J\d+|Coupe Nike \d+e|Coupe de Paris \d+e\s*:)?\s*", "", b)
        lab = re.sub(r"^Coupe Nike \d+e\s*", "", lab)
        lab = re.sub(r"\s*\d\d\.\d\d\.\d{2,4}.*$", "", lab).strip()
    lab = lab.replace("PAris", "Paris").replace("Montefremeil", "Montfermeil")
    return {"equipe": " / ".join(equipes), "competition": comp, "label": lab}


def meta_seance_a(nom: str) -> dict:
    m = re.match(r"(GF\d)\s+S(\d+)\s+séance\s+(\d+)", nom.strip(), re.IGNORECASE)
    if m:
        return {"equipe": m.group(1), "label": f"S{int(m.group(2)):02d} séance {int(m.group(3))}"}
    return {"equipe": "?", "label": nom}


def meta_b(entete: dict, equipe_defaut: str) -> dict:
    nom = re.sub(r"\s+", " ", str(entete.get("Name", ""))).strip()
    equipe = "U23" if "U23" in nom else equipe_defaut
    comp = ""
    if nom.startswith("(A)"):
        comp = "Amical"
    elif m := re.match(r"R1 (J\d+)", nom):
        comp = f"Championnat R1 {m.group(1)}"
    elif m := re.match(r"(\d)\s*/\s*(J\d+)", nom):
        comp = f"Championnat phase {m.group(1)} {m.group(2)}"
    lab = re.sub(r"^\(A\)\s*|^R1 J\d+\s*|^\d\s*/\s*J\d+\s*", "", nom)
    lab = re.sub(r"\s*-?\s*\d{1,2}/\d{1,2}/\d{2,4}$", "", lab).strip(" -")
    return {"equipe": equipe, "competition": comp, "label": lab or "Séance"}


# ─────────────────────────────────────────────────────────────────────────────
# Compilation
# ─────────────────────────────────────────────────────────────────────────────
def lister_csv(dossiers: Iterable[str]) -> list[str]:
    out = []
    for d in dossiers:
        if d and os.path.isdir(d):
            for racine, _, fichiers in os.walk(d):
                out += [os.path.join(racine, f) for f in fichiers if f.lower().endswith(".csv")]
    return sorted(set(out))


def _signature(t: pd.DataFrame) -> str:
    p = t[t.ligne == "principal"]
    if p.empty:                                  # fichier sans ligne principale (course individuelle…)
        p = t
    cle = sorted(set(zip(p.joueuse_brut.str.upper(), p.distance_m.round(0))))
    return hashlib.md5(repr(cle).encode()).hexdigest()


def compiler_sessions_gps(dossiers: Iterable[str], normaliser_nom: Optional[Callable[[str], str]] = None,
                          equipe_b_defaut: str = "U19") -> pd.DataFrame:
    """Lit tous les CSV GPS des dossiers et renvoie la table longue (colonnes COLONNES).

    normaliser_nom : fonction nom brut → nom canonique (ex. mapping du référentiel de l'app).
    Si None, les noms sont harmonisés par une clé insensible à l'ordre et aux accents.
    """
    fichiers = []                                            # dicts {nom, systeme, type, entete, table}
    for path in lister_csv(dossiers):
        nom = nom_fichier(path)
        try:
            if est_format_b(path):
                entete, t = lire_format_b(path)
                typ = "Match" if "MATCH" in str(entete.get("Type", "")).upper() else "Entraînement"
                date = pd.to_datetime(str(entete.get("Start Date", ""))[:10], errors="coerce")
                if pd.isna(date):
                    date = _date_nom_fichier(nom)
                fichiers.append(dict(nom=nom, systeme="B", type=typ, entete=entete, table=t, date=date))
            else:
                t = lire_format_a(path)
                if t.empty:
                    continue
                typ = "Entraînement" if est_seance(nom) else "Match"
                t["ligne"] = np.where(t.sans_date, "ajout_manuel" if typ == "Entraînement" else "complement", "principal")
                dates = pd.to_datetime(t.date_brute.dropna().astype(str).str[:10], errors="coerce").dropna()
                date = dates.iloc[0] if len(dates) else _date_nom_fichier(nom)
                fichiers.append(dict(nom=nom, systeme="A", type=typ, entete={}, table=t, date=date))
        except Exception as e:                                # un fichier illisible ne bloque pas le reste
            print(f"[gps_compilation] {nom} ignoré : {e}", file=sys.stderr)
    fichiers = [f for f in fichiers if f["date"] is not None and pd.notna(f["date"]) and len(f["table"])]
    for f in fichiers:
        t = f["table"]
        f["table"] = t[t.distance_m.fillna(0) > 0].drop_duplicates(["joueuse_brut", "distance_m", "temps_min", "ligne"])
        f["sig"] = _signature(f["table"])

    # 1) copies identiques → un seul fichier, en gardant la liste des noms (utile pour l'équipe)
    groupes: dict[str, list] = {}
    for f in fichiers:
        groupes.setdefault(f["sig"], []).append(f)
    uniques = []
    for g in groupes.values():
        g.sort(key=lambda f: (f["nom"].startswith("GF"), f["nom"]))
        base = dict(g[0])
        base["copies"] = [f["nom"] for f in g]
        uniques.append(base)

    # 2) fichiers partiels (tiers-temps…) contenus dans un cumul du même jour
    def partiel(x, y) -> bool:
        px = x["table"][x["table"].ligne == "principal"].set_index("joueuse_brut").distance_m
        py = y["table"][y["table"].ligne == "principal"].groupby("joueuse_brut").distance_m.max()
        return (len(px) > 0 and px.index.isin(py.index).all() and (py.reindex(px.index) >= px - 0.5).all()
                and py.sum() > px.sum() * 1.05)
    matchs = [f for f in uniques if f["type"] == "Match" and f["systeme"] == "A"]
    exclus = {id(x) for x in matchs for y in matchs if x is not y and x["date"] == y["date"] and partiel(x, y)}
    uniques = [f for f in uniques if id(f) not in exclus]

    # 3) mise en forme
    lignes = []
    for f in uniques:
        t = f["table"].copy()
        if f["systeme"] == "A":
            if f["type"] == "Match":
                m = meta_match_a(f["copies"])
                label = f"{m['competition']} — {m['label']}"
            else:
                m = meta_seance_a(f["nom"])
                label = m["label"]
        else:
            m = meta_b(f["entete"], equipe_b_defaut)
            label = f"{m['competition']} — {m['label']}" if f["type"] == "Match" and m["competition"] else m["label"]
        t["date"] = pd.Timestamp(f["date"]).normalize()
        t["type_session"], t["equipe"], t["session_label"], t["systeme"] = f["type"], m["equipe"], label, f["systeme"]
        t["fichier"] = " | ".join(sorted(set(f["copies"])))
        t["session_id"] = t.date.dt.strftime("%Y-%m-%d") + " | " + m["equipe"] + " | " + label
        lignes.append(t)
    if not lignes:
        return pd.DataFrame(columns=COLONNES)
    U = pd.concat(lignes, ignore_index=True)

    # Game Report (format A) et export B du même match U23 → même session
    for i in U.index[U.fichier.str.startswith("Game_Report")]:
        cand = U[(U.systeme == "B") & (U.type_session == "Match") & (U.date == U.at[i, "date"]) & (U.equipe == U.at[i, "equipe"])]
        if len(cand):
            U.loc[i, ["session_id", "session_label"]] = cand.iloc[0][["session_id", "session_label"]].values

    # noms
    noms = [normaliser_nom(n) if normaliser_nom else None for n in U.joueuse_brut]
    U["joueuse"] = [m or nom_lisible(n) for m, n in zip(noms, U.joueuse_brut)]
    canon = regrouper_variantes(U["joueuse"])
    U["joueuse"] = U["joueuse"].map(canon)
    U["saison"] = U.date.map(saison)

    # contrôles qualité
    rem = [[] for _ in range(len(U))]
    U = U.reset_index(drop=True)
    for _, g in U[U.ligne == "principal"].groupby("session_id"):
        if len(g) > 2:
            for i in g.index[g.duplicated(["distance_m", "vmax_kmh"], keep=False)]:
                rem[i].append("Valeurs identiques à une autre joueuse")
    bandes_ko = (U[BANDES] < -0.5).any(axis=1) | ((U[BANDES].sum(axis=1) - U.distance_m).abs() > 0.02 * U.distance_m)
    for i in U.index:
        r = U.loc[i]
        if r.vmax_kmh > 33:
            rem[i].append(f"Vmax aberrante ({r.vmax_kmh:.1f} km/h)")
        if r.temps_min > 0 and r.distance_m / r.temps_min > 150:
            rem[i].append("Distance/min > 150 (temps suspect)")
        if bandes_ko[i]:
            rem[i].append("Plages de vitesse incohérentes")
        if r.ligne == "ajout_manuel":
            rem[i].append("Ligne sans date ni capteur ajoutée en fin de fichier")
    U["remarques"] = ["; ".join(x) for x in rem]
    U["qualite_ok"] = ~U.remarques.str.contains("identiques|aberrante|suspect|incohérentes", regex=True)
    return U[COLONNES].sort_values(["date", "equipe", "session_id", "joueuse"]).reset_index(drop=True)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Compile les exports GPS en une table joueuse × session.")
    ap.add_argument("dossiers", nargs="+")
    ap.add_argument("-o", "--sortie", default="data/gps_sessions.csv")
    a = ap.parse_args()
    df = compiler_sessions_gps(a.dossiers)
    df.to_csv(a.sortie, index=False)
    print(f"{len(df)} lignes, {df.session_id.nunique()} sessions → {a.sortie}")


# ─────────────────────────────────────────────────────────────────────────────
# Export (Excel + dépôt Drive)
# ─────────────────────────────────────────────────────────────────────────────
def classeur_excel(df: pd.DataFrame) -> bytes:
    """Classeur récapitulatif : données, synthèse par session, synthèse par joueuse."""
    principal = df[df.ligne != "ajout_manuel"]
    par_session = (principal.groupby(["date", "saison", "type_session", "equipe", "session_label"], as_index=False)
                   .agg(joueuses=("joueuse", "nunique"), distance_moy_m=("distance_m", "mean"),
                        hsr_moy_m=("p5_m", "mean"), sprint_moy_m=("p6_m", "mean"),
                        acc3_moy=("acc3", "mean"), dec3_moy=("dec3", "mean"))
                   .sort_values("date"))
    par_joueuse = (principal.groupby(["joueuse", "saison", "type_session"], as_index=False)
                   .agg(sessions=("session_id", "nunique"), temps_min=("temps_min", "sum"),
                        distance_totale_m=("distance_m", "sum"), distance_moy_m=("distance_m", "mean"),
                        acc2_moy=("acc2", "mean"), dec2_moy=("dec2", "mean")))
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        df.assign(date=df.date.dt.date).to_excel(xw, sheet_name="Sessions joueuse", index=False)
        par_session.assign(date=par_session.date.dt.date).round(1).to_excel(xw, sheet_name="Synthèse par session", index=False)
        par_joueuse.round(1).to_excel(xw, sheet_name="Synthèse par joueuse", index=False)
    return buf.getvalue()


def exporter_vers_drive(service, df: pd.DataFrame, dossier_id: str) -> dict:
    """Dépose (ou met à jour, même id Drive) gps_sessions.csv et Compilation_GPS.xlsx dans le
    dossier Drive dossier_id. Ce dossier doit être HORS du dossier « CSV GPS », sinon la
    compilation serait re-synchronisée comme un export GPS. Retourne {nom: lien}."""
    from googleapiclient.http import MediaIoBaseUpload
    kw = dict(supportsAllDrives=True)
    fichiers = {"gps_sessions.csv": (df.to_csv(index=False).encode("utf-8"), "text/csv"),
                "Compilation_GPS.xlsx": (classeur_excel(df),
                                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    liens = {}
    for nom, (contenu, mime) in fichiers.items():
        media = MediaIoBaseUpload(io.BytesIO(contenu), mimetype=mime, resumable=False)
        q = f"'{dossier_id}' in parents and name = '{nom}' and trashed = false"
        ex = service.files().list(q=q, fields="files(id)", includeItemsFromAllDrives=True, **kw).execute().get("files", [])
        if ex:
            f = service.files().update(fileId=ex[0]["id"], media_body=media, fields="id,webViewLink", **kw).execute()
        else:
            f = service.files().create(body={"name": nom, "parents": [dossier_id]}, media_body=media,
                                       fields="id,webViewLink", **kw).execute()
        liens[nom] = f.get("webViewLink")
    return liens
