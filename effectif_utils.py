"""Effectif : la liste unique des joueuses qui fait foi dans l'app (Gestion → Effectif).

Fichier `data/effectif.json` :
    {"joueuses": {"NOM Prénom": {"statut": "club" | "partie", "depart": "AAAA-MM-JJ" | "",
                                  "variantes": ["autre orthographe", ...], "ajout": "manuel" | "import"}},
     "ignores": ["nom brut à ne plus proposer", ...]}

Les fiches (date de naissance, latéralité, postes) restent dans `data/fiches_joueuses.json`,
sous le même nom officiel. Les « variantes » sont les orthographes trouvées dans les données
(Sportscode, Bepro, GPS, évaluations, Supabase…) rattachées à la joueuse.
Sans dépendance Streamlit (tests/test_effectif_utils.py).
"""
import difflib
import json
import os
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

STATUT_CLUB = "club"
STATUT_PARTIE = "partie"
LIBELLES_STATUT = {STATUT_CLUB: "Au club", STATUT_PARTIE: "Partie"}


# ─────────────────────────────────────────────────────────────────────────────
# Clés de comparaison
# ─────────────────────────────────────────────────────────────────────────────
def mots(nom) -> List[str]:
    """« Dupont-Martin, Élodie » → ["DUPONT", "MARTIN", "ELODIE"]."""
    s = unicodedata.normalize("NFKD", str(nom or "")).encode("ascii", "ignore").decode().upper()
    for c in "-.,;_'’/()":
        s = s.replace(c, " ")
    return s.split()


def tri(nom) -> str:
    """Clé de tri alphabétique (accents et casse ignorés, ordre des mots conservé)."""
    return " ".join(mots(nom))


def cle(nom) -> str:
    """Clé insensible aux accents, à la casse, à la ponctuation et à l'ordre des mots."""
    return " ".join(sorted(mots(nom)))


def _sous_ensemble_flou(petit: list, grand: list, seuil: float = 0.8) -> bool:
    """Chaque mot de `petit` (au moins 2) correspond à un mot distinct de `grand`, à une faute près
    (même règle que gps_compilation.sous_ensemble_flou)."""
    if len(petit) < 2 or len(petit) > len(grand):
        return False
    reste = list(grand)
    for t in petit:
        m = t if t in reste else next(
            (g for g in reste if difflib.SequenceMatcher(None, t, g).ratio() >= seuil), None)
        if m is None:
            return False
        reste.remove(m)
    return True


def proximite(a, b) -> float:
    """Score 0–1 de ressemblance entre deux noms (1 = même joueuse quasi certaine)."""
    ka, kb = cle(a), cle(b)
    if not ka or not kb:
        return 0.0
    if ka == kb:
        return 1.0
    ma, mb = ka.split(), kb.split()
    if _sous_ensemble_flou(ma, mb) or _sous_ensemble_flou(mb, ma):
        return 0.95
    sc = difflib.SequenceMatcher(None, ka.replace(" ", ""), kb.replace(" ", "")).ratio()
    # Un mot en commun (nom ou prénom seul : « Juliette », « DUPONT J ») : on remonte un peu le score
    communs = set(ma) & set(mb)
    if communs:
        sc = max(sc, 0.6 + 0.1 * len(communs))
    # Initiale du prénom : « DUPONT J » ~ « DUPONT Juliette »
    if len(ma) >= 2 and len(mb) >= 2 and communs:
        ia = {m[0] for m in ma if m not in communs}
        ib = {m[0] for m in mb if m not in communs}
        if ia and ia & ib:
            sc = max(sc, 0.85)
    return round(min(sc, 0.94), 3)


# ─────────────────────────────────────────────────────────────────────────────
# Lecture / écriture
# ─────────────────────────────────────────────────────────────────────────────
def vide() -> dict:
    return {"joueuses": {}, "ignores": []}


def charger(path: str) -> Optional[dict]:
    """Effectif enregistré, ou None si le fichier n'existe pas encore (→ initialiser)."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        return None
    eff = vide()
    for nom, v in (data.get("joueuses") or {}).items():
        v = v or {}
        eff["joueuses"][nom] = {
            "statut": v.get("statut") if v.get("statut") in LIBELLES_STATUT else STATUT_CLUB,
            "depart": v.get("depart", "") or "",
            "variantes": [x for x in (v.get("variantes") or []) if str(x).strip()],
            "ajout": v.get("ajout", "import") or "import",
        }
    eff["ignores"] = [x for x in (data.get("ignores") or []) if str(x).strip()]
    return eff


def enregistrer(path: str, eff: dict) -> None:
    """Écriture atomique (fichier temporaire puis remplacement)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"joueuses": dict(sorted(eff["joueuses"].items(), key=lambda kv: tri(kv[0]))),
                   "ignores": sorted(set(eff.get("ignores", [])), key=tri)},
                  f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ─────────────────────────────────────────────────────────────────────────────
# Interrogation
# ─────────────────────────────────────────────────────────────────────────────
def noms(eff: dict, statut: Optional[str] = None) -> List[str]:
    """Noms officiels (tous, ou d'un statut), triés."""
    return sorted((n for n, v in eff["joueuses"].items() if statut is None or v["statut"] == statut), key=tri)


def resoudre(nom_brut, eff: dict) -> Optional[str]:
    """Nom officiel d'une orthographe déjà connue (nom officiel ou variante rattachée),
    insensible aux accents, à la casse et à l'ordre des mots. None sinon."""
    k = cle(nom_brut)
    if not k:
        return None
    for nom, v in eff["joueuses"].items():
        if cle(nom) == k or any(cle(x) == k for x in v["variantes"]):
            return nom
    return None


def est_ignore(nom_brut, eff: dict) -> bool:
    k = cle(nom_brut)
    return any(cle(x) == k for x in eff.get("ignores", []))


def suggestions(nom_brut, eff: dict, n: int = 3, seuil: float = 0.55) -> List[Tuple[str, float]]:
    """Joueuses de l'effectif les plus proches d'un nom brut : [(nom officiel, score)], meilleure d'abord."""
    scores = []
    for nom, v in eff["joueuses"].items():
        sc = max([proximite(nom_brut, nom)] + [proximite(nom_brut, x) for x in v["variantes"]])
        if sc >= seuil:
            scores.append((nom, sc))
    scores.sort(key=lambda t: (-t[1], tri(t[0])))
    return scores[:n]


def noms_a_rattacher(comptes: Dict[str, Dict[str, int]], eff: dict) -> List[dict]:
    """Noms trouvés dans les données qui ne sont ni un nom officiel, ni une variante, ni ignorés.
    `comptes` = {source: {nom brut: nb de lignes}}. Les orthographes de même clé sont regroupées.
    → [{"nom", "lignes", "sources", "suggestions"}], les plus fréquents d'abord."""
    connus = {cle(x) for x in eff.get("ignores", [])}
    for nom, v in eff["joueuses"].items():
        connus.add(cle(nom))
        connus.update(cle(x) for x in v["variantes"])
    groupes: Dict[str, dict] = {}
    for source, par_nom in comptes.items():
        for brut, nb in (par_nom or {}).items():
            brut = " ".join(str(brut or "").split())
            k = cle(brut)
            if not k or k in connus:
                continue
            g = groupes.setdefault(k, {"nom": brut, "lignes": 0, "sources": {}, "_max": -1})
            g["lignes"] += int(nb or 0)
            g["sources"][source] = g["sources"].get(source, 0) + int(nb or 0)
            if int(nb or 0) > g["_max"]:
                g["nom"], g["_max"] = brut, int(nb or 0)
    out = []
    for g in groupes.values():
        g.pop("_max")
        g["suggestions"] = suggestions(g["nom"], eff)
        out.append(g)
    out.sort(key=lambda g: (-g["lignes"], tri(g["nom"])))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Modification
# ─────────────────────────────────────────────────────────────────────────────
def ajouter(eff: dict, nom: str, statut: str = STATUT_CLUB, depart: str = "", ajout: str = "manuel") -> str:
    """Ajoute une joueuse (ou renvoie le nom officiel existant si elle est déjà connue)."""
    nom = " ".join(str(nom or "").split())
    if not nom:
        raise ValueError("Nom vide.")
    deja = resoudre(nom, eff)
    if deja:
        return deja
    eff["joueuses"][nom] = {"statut": statut, "depart": depart if statut == STATUT_PARTIE else "",
                            "variantes": [], "ajout": ajout}
    eff["ignores"] = [x for x in eff.get("ignores", []) if cle(x) != cle(nom)]
    return nom


def rattacher(eff: dict, nom_brut: str, nom_officiel: str) -> None:
    """Rattache une orthographe à une joueuse (et la retire des noms ignorés)."""
    if nom_officiel not in eff["joueuses"]:
        raise KeyError(nom_officiel)
    nom_brut = " ".join(str(nom_brut or "").split())
    if not nom_brut or cle(nom_brut) == cle(nom_officiel):
        return
    deja = resoudre(nom_brut, eff)
    if deja and deja != nom_officiel:
        eff["joueuses"][deja]["variantes"] = [x for x in eff["joueuses"][deja]["variantes"]
                                              if cle(x) != cle(nom_brut)]
    v = eff["joueuses"][nom_officiel]["variantes"]
    if all(cle(x) != cle(nom_brut) for x in v):
        v.append(nom_brut)
    eff["ignores"] = [x for x in eff.get("ignores", []) if cle(x) != cle(nom_brut)]


def detacher(eff: dict, nom_officiel: str, variante: str) -> None:
    v = eff["joueuses"].get(nom_officiel, {}).get("variantes", [])
    eff["joueuses"][nom_officiel]["variantes"] = [x for x in v if cle(x) != cle(variante)]


def ignorer(eff: dict, nom_brut: str) -> None:
    if not est_ignore(nom_brut, eff):
        eff.setdefault("ignores", []).append(" ".join(str(nom_brut).split()))


def renommer(eff: dict, ancien: str, nouveau: str) -> str:
    """Change le nom officiel ; l'ancien devient une variante (les données restent rattachées)."""
    nouveau = " ".join(str(nouveau or "").split())
    if not nouveau or ancien not in eff["joueuses"] or nouveau == ancien:
        return ancien
    autre = resoudre(nouveau, eff)
    if autre and autre != ancien:
        raise ValueError(f"« {nouveau} » correspond déjà à {autre}.")
    v = eff["joueuses"].pop(ancien)
    v["variantes"] = [x for x in v["variantes"] if cle(x) != cle(nouveau)]
    if cle(ancien) != cle(nouveau):
        v["variantes"].append(ancien)
    eff["joueuses"][nouveau] = v
    return nouveau


def fusionner(eff: dict, source: str, cible: str) -> None:
    """Doublon : `source` devient une variante de `cible`, avec toutes ses variantes."""
    if source == cible or source not in eff["joueuses"] or cible not in eff["joueuses"]:
        return
    s = eff["joueuses"].pop(source)
    for x in [source] + s["variantes"]:
        if cle(x) != cle(cible) and all(cle(y) != cle(x) for y in eff["joueuses"][cible]["variantes"]):
            eff["joueuses"][cible]["variantes"].append(x)


def initialiser(au_club: Iterable[str], parties: Dict[str, str],
                correspondances: Dict[str, str]) -> dict:
    """Premier remplissage depuis l'existant : joueuses de la saison, joueuses parties
    ({nom: date de départ}), et correspondances déjà saisies ({nom brut: nom corrigé} — alias
    de Gestion, correspondances GPS). Les doublons d'orthographe sont regroupés."""
    eff = vide()

    def _placer(nom, statut, depart):
        nom = " ".join(str(nom or "").split())
        if not cle(nom):
            return
        if resoudre(nom, eff):
            return
        proches = [n for n in eff["joueuses"] if proximite(nom, n) >= 0.95]
        if len(proches) == 1:
            rattacher(eff, nom, proches[0])
            return
        ajouter(eff, nom, statut, depart, ajout="import")

    for n in au_club:
        _placer(n, STATUT_CLUB, "")
    for n, d in (parties or {}).items():
        officiel = resoudre(n, eff)
        if officiel:  # présente dans la saison sous la même orthographe mais déclarée partie
            eff["joueuses"][officiel]["statut"] = STATUT_PARTIE
            eff["joueuses"][officiel]["depart"] = d or ""
        else:
            _placer(n, STATUT_PARTIE, d or "")
    for brut, vrai in (correspondances or {}).items():
        officiel = resoudre(vrai, eff)
        if officiel:
            rattacher(eff, brut, officiel)
    return eff
