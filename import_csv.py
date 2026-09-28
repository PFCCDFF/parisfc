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

    Retourne {type, valide, detail, date, empreinte}. `type` vaut None si le fichier
    n'est reconnu ni comme tactique ni comme GPS."""
    res = {"type": None, "valide": False, "detail": "", "date": None, "empreinte": empreinte(contenu)}
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
