"""
Import manuel de CSV (onglet Gestion → Import) et repérage des fichiers en double.

Sans dépendance Streamlit : la détection du type, la validation et la liste des doublons
sont testables seules (tests/test_import_csv.py). Les fonctions propres à l'app
(is_tactical_file, parse_tactical_filename, read_csv_auto, is_gps_match_file) sont
injectées en paramètre pour ne pas dupliquer leur logique (cf. piège #2 du CLAUDE.md).

Types gérés et dossier local de destination (mêmes dossiers que la sync Drive) :
    « Tactique »          → data/          (fichiers PFC_VS_…, Sportscode)
    « GPS match »         → data/gps_match
    « GPS entraînement »  → data/gps
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from typing import Callable, Iterable, Optional

import pandas as pd

import gps_compilation as gc

TYPES = ["Tactique", "GPS match", "GPS entraînement"]


def empreinte(contenu: bytes) -> str:
    return hashlib.md5(contenu).hexdigest()


def _avec_fichier_temp(nom: str, contenu: bytes, fn: Callable[[str], object]):
    """Les lecteurs GPS/tactiques de l'app attendent un chemin : fichier temporaire."""
    ext = os.path.splitext(nom)[1] or ".csv"
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
        f.write(contenu)
        chemin = f.name
    try:
        return fn(chemin)
    finally:
        os.unlink(chemin)


def infos_depuis_timeline(timeline: str) -> dict:
    """« J3 U23 FC Mantois - Paris FC » / « Paris FC U23F vs AAS Sarcelles - R1F J2 » →
    {journee: "3", categorie: "U23", adversaire: "FC Mantois"} (champs vides si absents).
    Titre saisi à la main dans Sportscode : peut être faux (projet dupliqué d'un autre match)."""
    t = " ".join(str(timeline or "").split())
    j = re.search(r"\bJ(\d{1,2})\b", t, re.IGNORECASE)
    c = re.search(r"\bU(\d{2})F?\b", t, re.IGNORECASE)
    reste = re.sub(r"\bJ\d{1,2}\b|\bU\d{2}F?\b|\b[RD]\dF?\b", " ", t, flags=re.IGNORECASE)
    equipes = [" ".join(e.split()) for e in re.split(r"\s(?:[-–]|vs\.?)\s", reste, flags=re.IGNORECASE)]
    adv = next((e for e in equipes if e and not re.search(r"paris\s*fc", e, re.IGNORECASE)), "")
    return {"journee": j.group(1) if j else "", "categorie": f"U{c.group(1)}" if c else "", "adversaire": adv}


def _premiere_valeur(df: pd.DataFrame, col: str) -> str:
    """Première valeur d'une colonne Sportscode multi-valeurs (« HAC,HAC » → « HAC »)."""
    if col not in df.columns:
        return ""
    v = df[col].dropna().astype(str).str.split(",").str[0].str.strip()
    v = v[(v != "") & (v.str.lower() != col.lower())]
    return v.mode().iloc[0] if len(v) else ""


def _meme_equipe(a: str, b: str) -> bool:
    na, nb = (gc._cle_nom(x).replace(" ", "") for x in (a, b))
    return bool(na) and bool(nb) and (na in nb or nb in na)


def infos_sportscode(df: pd.DataFrame) -> tuple[dict, str]:
    """Proposition (journee, categorie, adversaire) pour un export Sportscode brut, et une
    alerte si le titre de la Timeline contredit les colonnes du fichier.
    Les colonnes Teamersaire / Journée / Compétition (lignes adversaire) priment sur le titre,
    comme dans load_tactical_files : c'est un titre recopié d'un autre match (HAC J2 titré
    « AAS Sarcelles - Paris FC ») qui a fait masquer le vrai Sarcelles R1 J2 le 28/09/2026."""
    tl = df["Timeline"].dropna()
    titre = str(tl.iloc[0]) if len(tl) else ""
    prop = infos_depuis_timeline(titre)
    adv, jr, comp = _premiere_valeur(df, "Teamersaire"), _premiere_valeur(df, "Journée"), _premiere_valeur(df, "Compétition")
    alerte = ""
    if adv:
        if prop["adversaire"] and not _meme_equipe(adv, prop["adversaire"]):
            alerte = (f"Le titre de la Timeline (« {titre} ») indique {prop['adversaire']}, mais les actions "
                      f"du fichier sont celles de {adv} : vérifier qu'il s'agit du bon match.")
        prop["adversaire"] = adv
    if jr:
        try:
            prop["journee"] = str(int(float(jr)))
        except ValueError:
            pass
    c = re.search(r"\bU(\d{2})", comp, re.IGNORECASE)
    if c:
        prop["categorie"] = f"U{c.group(1)}"
    return prop, alerte


def nom_standard_tactique(date, categorie: str, adversaire: str, journee: str = "") -> str:
    """Nom reconnu par l'app (is_tactical_file / parse_tactical_filename) :
    « PFC_VS_ 2627 U23F FC Mantois_J3_U23_27-09-2026.csv »."""
    d = pd.Timestamp(date)
    y = d.year if d.month >= 7 else d.year - 1
    cat = categorie.strip().upper()
    adv = re.sub(r'[\\/:*?"<>|_]+', " ", adversaire).strip()
    adv = " ".join(adv.split())
    j = f"_J{int(journee)}" if str(journee).strip().isdigit() else ""
    return f"PFC_VS_ {y % 100:02d}{(y + 1) % 100:02d} {cat}F {adv}{j}_{cat}_{d:%d-%m-%Y}.csv"


def analyser_gps(chemin: str, nom: str, est_match_nom: Callable[[str], bool]) -> dict:
    """Lit un export GPS (format A 2025-26 ou B 2026-27) : type probable, nb de joueuses."""
    if gc.est_format_b(chemin):
        entete, t = gc.lire_format_b(chemin)
        typ = "GPS match" if "MATCH" in str(entete.get("Type", "")).upper() else "GPS entraînement"
        date = pd.to_datetime(str(entete.get("Start Date", ""))[:10], errors="coerce")
        fmt = "B (2026-27)"
    else:
        t = gc.lire_format_a(chemin)
        # Même règle que la sync Drive de l'app (sync_gps_from_drive_autonomous) pour le format A.
        typ = "GPS match" if est_match_nom(nom) else "GPS entraînement"
        dates = pd.to_datetime(t["date_brute"].dropna().astype(str).str[:10], errors="coerce").dropna() if len(t) else []
        date = dates.iloc[0] if len(dates) else gc._date_nom_fichier(gc.nom_fichier(nom))
        fmt = "A (2025-26)"
    if t is None or t.empty or "distance_m" not in t.columns:
        raise ValueError("colonnes GPS attendues absentes")
    n = int((t["distance_m"].fillna(0) > 0).sum())
    return {"type": typ, "format": fmt, "date": date, "joueuses": n}


def analyser_fichier(nom: str, contenu: bytes, est_tactique: Callable[[str], bool],
                     lire_csv: Callable[[str], pd.DataFrame], infos_tactique: Callable[[str], dict],
                     est_match_nom: Callable[[str], bool]) -> dict:
    """Détecte le type d'un CSV déposé et vérifie qu'il est exploitable par l'app.

    Retourne {type, valide, detail, date, empreinte, a_nommer, proposition}. `type` vaut
    None si le fichier n'est reconnu ni comme tactique ni comme GPS. `a_nommer` : export
    Sportscode brut de la plateforme (nom sans date) — à renommer au format standard
    (nom_standard_tactique) avec la date du match, `proposition` pré-remplie depuis Timeline."""
    res = {"type": None, "valide": False, "detail": "", "date": None, "empreinte": empreinte(contenu),
           "a_nommer": False, "proposition": {}, "alerte": ""}
    if not nom.lower().endswith(".csv"):
        res["detail"] = "Extension .csv attendue"
        return res
    if est_tactique(nom):
        res["type"] = "Tactique"
        try:
            df = _avec_fichier_temp(nom, contenu, lire_csv)
        except Exception as e:
            res["detail"] = f"CSV illisible : {e}"
            return res
        manquantes = [c for c in ("Timeline", "Row") if c not in df.columns]
        info = infos_tactique(nom)
        res["date"] = info.get("date")
        if manquantes:
            res["detail"] = "Colonnes Sportscode absentes : " + ", ".join(manquantes)
        elif info.get("date") is None:
            res["detail"] = "Date introuvable dans le nom du fichier"
        else:
            res["valide"] = True
            adv = info.get("adversaire") or "?"
            j = f"J{info['journee']} · " if info.get("journee") else ""
            res["detail"] = f"{j}{adv} · {len(df)} lignes"
        return res
    # Export Sportscode brut (nom de la plateforme, ex. « J3 U23 FC Mantois Paris FC.csv ») :
    # reconnu au contenu, l'app ne le lira qu'une fois renommé au format standard.
    try:
        df = _avec_fichier_temp(nom, contenu, lire_csv)
    except Exception:
        df = None
    if df is not None and {"Timeline", "Row"} <= set(df.columns):
        prop, alerte = infos_sportscode(df)
        res.update(type="Tactique", a_nommer=True, proposition=prop, alerte=alerte,
                   detail=f"Export Sportscode brut ({len(df)} lignes) : renseigner la date du match")
        return res
    try:
        g = _avec_fichier_temp(nom, contenu, lambda p: analyser_gps(p, nom, est_match_nom))
    except Exception as e:
        res["detail"] = f"Ni tactique (nom PFC_VS_…) ni export GPS lisible : {e}"
        return res
    res.update(type=g["type"], date=g["date"])
    if g["joueuses"] == 0:
        res["detail"] = f"Export GPS format {g['format']} sans aucune joueuse exploitable"
    elif g["date"] is None or pd.isna(g["date"]):
        res["detail"] = "Date introuvable (ni dans le fichier ni dans son nom)"
    else:
        res["valide"] = True
        res["detail"] = f"Format {g['format']} · {g['joueuses']} joueuses"
    return res


# ─────────────────────────────────────────────────────────────────────────────
# Doublons
# ─────────────────────────────────────────────────────────────────────────────
def _csv(dossiers: Iterable[str], recursif: bool) -> list[str]:
    out = []
    for d in dossiers:
        if not d or not os.path.isdir(d):
            continue
        if recursif:
            out += gc.lister_csv([d])
        else:
            out += [os.path.join(d, f) for f in os.listdir(d) if f.lower().endswith(".csv")]
    return sorted(set(out))


def cle_match_tactique(nom: str, infos_tactique: Callable[[str], dict]):
    """Clé de dédoublonnage de load_tactical_files : (date, journée, adversaire normalisé)."""
    info = infos_tactique(nom)
    return None if info.get("date") is None else (info["date"], info.get("journee"), info.get("adv_norm"))


def blocages_import(nom_final: str, typ: str, contenu: bytes, existants: dict, tactiques: dict,
                    infos_tactique: Callable[[str], dict]) -> list[str]:
    """Raisons de refuser un import (liste vide = OK).
    existants : {empreinte: chemin} des CSV déjà sur le serveur ;
    tactiques : {cle_match: chemin} des fichiers tactiques déjà présents."""
    raisons = []
    dbl = existants.get(empreinte(contenu))
    if dbl:
        raisons.append(f"contenu identique à « {os.path.basename(dbl)} », déjà présent")
    if typ == "Tactique":
        cle = cle_match_tactique(nom_final, infos_tactique)
        autre = tactiques.get(cle) if cle else None
        if autre and os.path.basename(autre) != nom_final:
            raisons.append(f"même date, journée et adversaire que « {os.path.basename(autre)} » : "
                           "l'app n'afficherait plus que l'un des deux (le plus récent)")
    return raisons


def signature_gps(chemin: str) -> Optional[str]:
    """Empreinte du contenu GPS (joueuses + distances), insensible au nom et au format
    d'enregistrement : deux copies d'une même session ont la même signature."""
    try:
        if gc.est_format_b(chemin):
            _, t = gc.lire_format_b(chemin)
        else:
            t = gc.lire_format_a(chemin)
            if len(t):
                t["ligne"] = "principal"
        if t.empty:
            return None
        t = t[t.distance_m.fillna(0) > 0]
        return gc._signature(t) if len(t) else None
    except Exception:
        return None


def lister_doublons(dossiers_gps: Iterable[str], dossiers_tactique: Iterable[str],
                    est_tactique: Callable[[str], bool], infos_tactique: Callable[[str], dict]) -> pd.DataFrame:
    """Fichiers locaux en double, regroupés. Trois motifs :
    - « Contenu identique » : octets identiques (même fichier stocké deux fois) ;
    - « Même session GPS » : mêmes joueuses et distances sous des noms différents
      (copie U18/U19, racine vs GPS Matchs…) ;
    - « Même match tactique » : même (date, journée, adversaire), ex. renommage U19 → U19F.
      L'app ne garde que le plus récent (load_tactical_files), l'autre est ignoré.
    Une ligne par fichier ; `conserve` indique celui que l'app utilise quand c'est connu."""
    gps = _csv(dossiers_gps, recursif=True)
    tact = [p for p in _csv(dossiers_tactique, recursif=False) if est_tactique(os.path.basename(p))]
    tous = sorted(set(gps + tact))
    lignes, deja = [], set()

    def ajouter(groupe: str, motif: str, chemins: list[str], conserve: Optional[str] = None):
        for p in chemins:
            lignes.append({"groupe": groupe, "motif": motif, "fichier": gc.nom_fichier(p) + os.path.splitext(p)[1],
                           "dossier": os.path.dirname(p), "taille_ko": round(os.path.getsize(p) / 1024, 1),
                           "modifie": pd.Timestamp(os.path.getmtime(p), unit="s").floor("s"),
                           "conserve": None if conserve is None else p == conserve, "chemin": p})

    # 1) octets identiques
    par_hash: dict[str, list[str]] = {}
    for p in tous:
        with open(p, "rb") as f:
            par_hash.setdefault(empreinte(f.read()), []).append(p)
    for i, (h, ps) in enumerate(sorted((h, ps) for h, ps in par_hash.items() if len(ps) > 1)):
        ajouter(f"C{i + 1}", "Contenu identique", ps)
        deja.add(frozenset(ps))

    # 2) même session GPS (hors groupes déjà identiques octet pour octet)
    par_sig: dict[str, list[str]] = {}
    for p in gps:
        s = signature_gps(p)
        if s:
            par_sig.setdefault(s, []).append(p)
    k = 0
    for s, ps in sorted(par_sig.items()):
        if len(ps) > 1 and frozenset(ps) not in deja:
            k += 1
            ajouter(f"G{k}", "Même session GPS", ps)

    # 3) même match tactique
    par_match: dict[tuple, list[str]] = {}
    for p in tact:
        info = infos_tactique(os.path.basename(p))
        if info.get("date") is not None:
            par_match.setdefault((info["date"], info.get("journee"), info.get("adv_norm")), []).append(p)
    k = 0
    for cle, ps in sorted(par_match.items(), key=lambda kv: str(kv[0])):
        if len(ps) > 1 and frozenset(ps) not in deja:
            k += 1
            ajouter(f"T{k}", "Même match tactique", ps, conserve=max(ps, key=os.path.getmtime))

    cols = ["groupe", "motif", "fichier", "dossier", "taille_ko", "modifie", "conserve", "chemin"]
    return pd.DataFrame(lignes, columns=cols)
