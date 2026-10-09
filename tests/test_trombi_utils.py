import io
import os
import sys
import unittest
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trombi_utils import extraire_trombi, infos_feuille, joueuses_trombi  # noqa: E402

M = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
XDR = "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing"
A = "http://schemas.openxmlformats.org/drawingml/2006/main"


def _zone(col, row, row_off, nom, prenom):
    return (f'<xdr:twoCellAnchor><xdr:from><xdr:col>{col}</xdr:col><xdr:colOff>0</xdr:colOff>'
            f'<xdr:row>{row}</xdr:row><xdr:rowOff>{row_off}</xdr:rowOff></xdr:from>'
            f'<xdr:to><xdr:col>{col}</xdr:col><xdr:colOff>0</xdr:colOff><xdr:row>{row}</xdr:row>'
            f'<xdr:rowOff>{row_off + 200000}</xdr:rowOff></xdr:to><xdr:sp><xdr:txBody>'
            f'<a:p><a:r><a:t>{nom}</a:t></a:r></a:p><a:p><a:r><a:t>{prenom}</a:t></a:r></a:p>'
            f'</xdr:txBody></xdr:sp><xdr:clientData/></xdr:twoCellAnchor>')


def _feuille(cellules_vm, lignes_ht):
    rows = {}
    for ref, vm in cellules_vm:
        rows.setdefault(int(ref[1:]), []).append(f'<c r="{ref}" t="e" vm="{vm}"><v>#VALUE!</v></c>')
    xml_rows = "".join(f'<row r="{r}" ht="{lignes_ht.get(r, 15)}" customHeight="1">{"".join(rows.get(r, []))}</row>'
                       for r in sorted(set(rows) | set(lignes_ht)))
    return (f'<worksheet xmlns="{M}" xmlns:r="{R}"><sheetFormatPr defaultRowHeight="15"/>'
            f'<sheetData>{xml_rows}</sheetData><drawing r:id="rId1"/></worksheet>')


def construire_xlsx(feuilles):
    """feuilles : [(nom_feuille, [(ref, nom_image)], {ligne: hauteur}, [zones])]"""
    buf = io.BytesIO()
    z = zipfile.ZipFile(buf, "w")
    medias, sheets_xml, wb_rels = [], [], []
    for i, (nom, photos, ht, zones) in enumerate(feuilles, 1):
        cells = []
        for ref, img in photos:
            medias.append(img)
            cells.append((ref, len(medias)))
        z.writestr(f"xl/worksheets/sheet{i}.xml", _feuille(cells, ht))
        z.writestr(f"xl/worksheets/_rels/sheet{i}.xml.rels",
                   f'<Relationships xmlns="{PKG}"><Relationship Id="rId1" Target="../drawings/drawing{i}.xml"/></Relationships>')
        z.writestr(f"xl/drawings/drawing{i}.xml",
                   f'<xdr:wsDr xmlns:xdr="{XDR}" xmlns:a="{A}">{"".join(zones)}</xdr:wsDr>')
        sheets_xml.append(f'<sheet name="{nom}" sheetId="{i}" r:id="rId{i}"/>')
        wb_rels.append(f'<Relationship Id="rId{i}" Target="worksheets/sheet{i}.xml"/>')
    z.writestr("xl/workbook.xml", f'<workbook xmlns="{M}" xmlns:r="{R}"><sheets>{"".join(sheets_xml)}</sheets></workbook>')
    z.writestr("xl/_rels/workbook.xml.rels", f'<Relationships xmlns="{PKG}">{"".join(wb_rels)}</Relationships>')
    z.writestr("xl/metadata.xml", "<metadata>" + "".join(f'<xlrd:rvb i="{k}"/>' for k in range(len(medias))) + "</metadata>")
    z.writestr("xl/richData/rdrichvalue.xml",
               '<rvData xmlns="http://schemas.microsoft.com/office/spreadsheetml/2017/richdata">'
               + "".join(f"<rv><v>{k}</v><v>5</v></rv>" for k in range(len(medias))) + "</rvData>")
    z.writestr("xl/richData/richValueRel.xml",
               f'<richValueRels xmlns="http://schemas.microsoft.com/office/spreadsheetml/2022/richvaluerel" xmlns:r="{R}">'
               + "".join(f'<rel r:id="rIdImg{k}"/>' for k in range(len(medias))) + "</richValueRels>")
    z.writestr("xl/richData/_rels/richValueRel.xml.rels",
               f'<Relationships xmlns="{PKG}">'
               + "".join(f'<Relationship Id="rIdImg{k}" Target="../media/{m}"/>' for k, m in enumerate(medias))
               + "</Relationships>")
    for m in medias:
        z.writestr(f"xl/media/{m}", m.encode())
    z.close()
    return buf.getvalue()


class TestTrombi(unittest.TestCase):
    def test_infos_feuille(self):
        self.assertEqual(infos_feuille("U19 domicile"), {"categorie": "U19", "tenue": "domicile"})
        self.assertEqual(infos_feuille("U23 extérieur"), {"categorie": "U23", "tenue": "exterieur"})

    def test_nom_rattache_a_la_photo_au_dessus(self):
        # Photos en lignes 1 et 3 (hautes), noms dans la ligne 2 et en bas de la ligne 3 :
        # la zone de B en ligne 4 et celle de A ancrée tout en bas de la ligne 3 (comme dans
        # le vrai fichier) vont à la photo juste au-dessus.
        ht = {1: 155.75, 2: 30, 3: 155.75, 4: 30}
        zones = [_zone(0, 1, 0, "DUPONT", "Alice"), _zone(1, 1, 0, "MARTIN", "Léa"),
                 _zone(0, 2, 1900000, "BERNARD", "Inès"), _zone(1, 3, 0, "PETIT", "Noëllie")]
        contenu = construire_xlsx([("U19 domicile", [("A1", "i1.png"), ("B1", "i2.png"),
                                                     ("A3", "i3.png"), ("B3", "i4.png")], ht, zones)])
        e = {x["image"].decode(): (x["nom"], x["prenom"]) for x in extraire_trombi(contenu)}
        self.assertEqual(e, {"i1.png": ("DUPONT", "Alice"), "i2.png": ("MARTIN", "Léa"),
                             "i3.png": ("BERNARD", "Inès"), "i4.png": ("PETIT", "Noëllie")})

    def test_tenue_domicile_prioritaire(self):
        ht = {1: 155.75, 2: 30}
        f = lambda nom, img: (nom, [("A1", img)], ht, [_zone(0, 1, 0, "DUPONT", "Alice")])
        contenu = construire_xlsx([f("U19 extérieur", "ext.png"), f("U19 domicile", "dom.png")])
        j = joueuses_trombi(extraire_trombi(contenu))
        self.assertEqual(len(j), 1)
        self.assertEqual((j[0]["tenue"], j[0]["image"]), ("domicile", b"dom.png"))

    def test_photo_sans_nom(self):
        contenu = construire_xlsx([("U23 domicile", [("A1", "i1.png")], {1: 155.75}, [])])
        e = extraire_trombi(contenu)
        self.assertEqual(len(e), 1)
        self.assertIsNone(e[0]["nom"])
        self.assertEqual(joueuses_trombi(e), [])


if __name__ == "__main__":
    unittest.main()
