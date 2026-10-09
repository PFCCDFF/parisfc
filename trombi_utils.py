"""Lecture du trombinoscope Excel du centre de formation (une feuille par catégorie × tenue,
ex. « U19 domicile », « U23 extérieur »).

Structure du fichier (Excel 365) :
  - les photos sont des images « dans la cellule » (cellules avec attribut vm →
    xl/metadata.xml → xl/richData → xl/media/imageN.png) ;
  - les noms sont des zones de texte (xl/drawings/drawingN.xml), un paragraphe pour le NOM,
    un pour le Prénom, posées sous la photo correspondante.
Chaque nom est rattaché à la photo de sa colonne dont le bas est le plus proche du centre
de la zone de texte.

Sans dépendance Streamlit (testé dans tests/test_trombi_utils.py).
"""
import io
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

_NS_M = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_NS_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_NS_PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
_X = "{http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing}"
_A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
_EMU_PAR_POINT = 12700

# Tenue préférée pour les rapports : domicile = maillot bleu marine (bleu clair à l'extérieur)
TENUES_PRIORITE = ("domicile", "exterieur")


def _sans_accents_min(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c)).lower()


def infos_feuille(nom_feuille: str) -> Dict[str, str]:
    """« U19 domicile » → {"categorie": "U19", "tenue": "domicile"}."""
    s = _sans_accents_min(nom_feuille)
    m = re.search(r"\bu\s*(\d{2})\b", s)
    tenue = "exterieur" if "ext" in s else ("domicile" if "dom" in s else s.strip())
    return {"categorie": f"U{m.group(1)}" if m else nom_feuille.strip(), "tenue": tenue}


def _cible(base_dir: str, target: str) -> str:
    """Résout une cible de relation (relative au dossier de la partie, ou absolue /xl/...)."""
    if target.startswith("/"):
        return target.lstrip("/")
    return os.path.normpath(os.path.join(base_dir, target)).replace("\\", "/")


def _rels(z: zipfile.ZipFile, part: str) -> Dict[str, str]:
    d, f = os.path.split(part)
    rp = f"{d}/_rels/{f}.rels"
    if rp not in z.namelist():
        return {}
    root = ET.fromstring(z.read(rp))
    return {r.get("Id"): _cible(d, r.get("Target")) for r in root.findall(f"{{{_NS_PKG}}}Relationship")}


def _col_ligne(ref: str):
    m = re.match(r"([A-Z]+)(\d+)", ref)
    n = 0
    for ch in m.group(1):
        n = n * 26 + ord(ch) - 64
    return n - 1, int(m.group(2)) - 1


def extraire_trombi(contenu: bytes) -> List[Dict]:
    """Retourne une entrée par photo : {nom, prenom, categorie, tenue, feuille, image (bytes),
    extension}. nom/prenom valent None si aucune zone de texte n'est rattachée à la photo."""
    z = zipfile.ZipFile(io.BytesIO(contenu))
    noms = set(z.namelist())

    # vm (1-based) → chemin du média, via metadata.xml + richData
    vm_vers_media: List[Optional[str]] = []
    if "xl/metadata.xml" in noms and "xl/richData/rdrichvalue.xml" in noms:
        md = z.read("xl/metadata.xml").decode("utf-8")
        rvb = [int(i) for i in re.findall(r'<xlrd:rvb i="(\d+)"\s*/>', md)]
        rv = [[v.text for v in e] for e in ET.fromstring(z.read("xl/richData/rdrichvalue.xml"))]
        rel_ids = [e.get(f"{{{_NS_R}}}id") for e in ET.fromstring(z.read("xl/richData/richValueRel.xml"))]
        rel_cibles = _rels(z, "xl/richData/richValueRel.xml")
        for i in rvb:
            try:
                vm_vers_media.append(rel_cibles[rel_ids[int(rv[i][0])]])
            except (IndexError, KeyError, ValueError, TypeError):
                vm_vers_media.append(None)

    wb = ET.fromstring(z.read("xl/workbook.xml"))
    wb_rels = _rels(z, "xl/workbook.xml")
    sortie: List[Dict] = []
    for sh in wb.iter(f"{{{_NS_M}}}sheet"):
        part = wb_rels.get(sh.get(f"{{{_NS_R}}}id"))
        if not part or part not in noms:
            continue
        infos = infos_feuille(sh.get("name", ""))
        root = ET.fromstring(z.read(part))
        fmt = root.find(f"{{{_NS_M}}}sheetFormatPr")
        h_def = float(fmt.get("defaultRowHeight", 15)) if fmt is not None else 15.0
        hauteurs = {int(r.get("r")) - 1: float(r.get("ht", h_def)) for r in root.iter(f"{{{_NS_M}}}row")}

        def y(ligne: int, off: int = 0) -> float:
            return sum(hauteurs.get(k, h_def) for k in range(ligne)) * _EMU_PAR_POINT + off

        photos = []
        for c in root.iter(f"{{{_NS_M}}}c"):
            vm = c.get("vm")
            if vm and vm.isdigit() and 0 < int(vm) <= len(vm_vers_media) and vm_vers_media[int(vm) - 1]:
                col, lig = _col_ligne(c.get("r"))
                photos.append((col, lig, vm_vers_media[int(vm) - 1]))

        zones = []
        for rid, cible in _rels(z, part).items():
            if "/drawings/" not in cible or cible not in noms:
                continue
            for anc in ET.fromstring(z.read(cible)):
                fr = anc.find(_X + "from")
                if fr is None or anc.find(".//" + _X + "pic") is not None:
                    continue
                paras = [" ".join("".join(t.text or "" for t in p.iter(_A + "t")).split())
                         for p in anc.iter(_A + "p")]
                paras = [p for p in paras if p]
                if not paras:
                    continue
                y0 = y(int(fr.find(_X + "row").text), int(fr.find(_X + "rowOff").text))
                to, ext = anc.find(_X + "to"), anc.find(_X + "ext")
                if to is not None:
                    y1 = y(int(to.find(_X + "row").text), int(to.find(_X + "rowOff").text))
                else:
                    y1 = y0 + int(ext.get("cy", 0)) if ext is not None else y0
                zones.append((int(fr.find(_X + "col").text), (y0 + y1) / 2, paras))

        for col, lig, media in photos:
            bas = y(lig + 1)
            cands = [z_ for z_ in zones if z_[0] == col and z_[1] > y(lig)]
            paras = min(cands, key=lambda z_: abs(z_[1] - bas))[2] if cands else None
            nom = prenom = None
            if paras:
                nom = paras[0].strip()
                prenom = " ".join(paras[1:]).strip() or None
            sortie.append({
                "nom": nom, "prenom": prenom,
                "categorie": infos["categorie"], "tenue": infos["tenue"], "feuille": sh.get("name"),
                "image": z.read(media) if media in noms else None,
                "extension": os.path.splitext(media)[1].lower(),
            })
    return sortie


def joueuses_trombi(entrees: List[Dict]) -> List[Dict]:
    """Une entrée par joueuse (NOM + Prénom), avec la photo de la tenue prioritaire
    (TENUES_PRIORITE : maillot bleu de préférence)."""
    par_joueuse: Dict[tuple, Dict] = {}
    rang = {t: i for i, t in enumerate(TENUES_PRIORITE)}
    for e in entrees:
        if not e.get("nom") or not e.get("image"):
            continue
        cle = (_sans_accents_min(e["nom"]), _sans_accents_min(e.get("prenom") or ""))
        actuel = par_joueuse.get(cle)
        if actuel is None or rang.get(e["tenue"], 99) < rang.get(actuel["tenue"], 99):
            par_joueuse[cle] = e
    return sorted(par_joueuse.values(), key=lambda e: (e["categorie"], e["nom"], e.get("prenom") or ""))


def image_vignette_jpeg(image: bytes, hauteur_max: int = 480, qualite: int = 88) -> bytes:
    """PNG détouré → JPEG sur fond blanc (la conversion RGB brute rendrait la transparence noire)."""
    from PIL import Image
    im = Image.open(io.BytesIO(image))
    im.load()
    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGBA")
        fond = Image.new("RGB", im.size, (255, 255, 255))
        fond.paste(im, mask=im.split()[-1])
        im = fond
    else:
        im = im.convert("RGB")
    if im.height > hauteur_max:
        im = im.resize((round(im.width * hauteur_max / im.height), hauteur_max), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=qualite, optimize=True)
    return buf.getvalue()
