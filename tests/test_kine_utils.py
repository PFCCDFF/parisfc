import io
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kine_utils as K  # noqa: E402


def classeur(lignes):
    """Classeur au format « Suivi kiné » : titre, consigne, en-tête en ligne 3."""
    entete = ["Date", "Joueuse", "Type de Séance", "Zone / Région", "Type de blessure",
              "Disponibilité lendemain", "Observations / Notes Kiné", "Kiné"]
    df = pd.DataFrame([["⚽ PARIS FC — SUIVI KINÉ"] + [None] * 7, ["Saisir une ligne par séance"] + [None] * 7, entete]
                      + lignes)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf) as w:
        df.to_excel(w, sheet_name="📋 Saisie Séances", header=False, index=False)
        pd.DataFrame({"x": [1]}).to_excel(w, sheet_name="_listes", index=False)
    return buf.getvalue()


L1 = [pd.Timestamp("2026-07-27"), "WAPELABWEBE Sharon", "Renforcement musculaire", "Cheville G", "Aiguë",
      "Indisponible", "Vélo 10'", "Jordan"]
L2 = [pd.Timestamp("2026-07-28"), "MANE Mélita", "Bilan / Évaluation", "Poignet/Main D", "Aiguë",
      "Disponible", "RAS", "Baptiste"]


class TestLecture(unittest.TestCase):
    def test_lecture_et_rejets(self):
        sans_joueuse = [pd.Timestamp("2026-09-03"), None, "Bilan / Évaluation", None, None, None, None, "Baptiste"]
        sans_date = [None, "MANE Mélita", None, None, None, None, None, None]
        ok, rej = K.lire_classeur(classeur([L1, L2, L1, sans_joueuse, sans_date, [None] * 8]))
        self.assertEqual(len(ok), 2)  # doublon exact retiré, ligne vide ignorée
        self.assertEqual(sorted(rej["motif"]), ["date manquante", "joueuse manquante"])
        self.assertEqual(rej[rej.motif == "joueuse manquante"]["ligne_excel"].iloc[0], 7)
        r = ok.iloc[0]
        self.assertEqual((r["joueuse"], r["zone"], r["kine"]), ("WAPELABWEBE Sharon", "Cheville G", "Jordan"))

    def test_onglet_absent(self):
        buf = io.BytesIO()
        pd.DataFrame({"a": [1]}).to_excel(buf, sheet_name="Autre", index=False)
        with self.assertRaises(ValueError):
            K.lire_classeur(buf.getvalue())

    def test_empreinte_insensible_casse_accents(self):
        a = dict(zip(["date", "joueuse", "type_seance", "zone", "type_blessure", "disponibilite", "observations", "kine"], L2))
        b = dict(a, joueuse="MANE MELITA", observations="  ras ")
        self.assertEqual(K.empreinte(a), K.empreinte(b))
        self.assertNotEqual(K.empreinte(a), K.empreinte(dict(a, zone="Poignet/Main G")))

    def test_lignes_supabase(self):
        ok, _ = K.lire_classeur(classeur([L1]))
        row = K.lignes_supabase(ok, "staff")[0]
        self.assertEqual(row["date"], "2026-07-27")
        self.assertEqual(row["saisi_par"], "staff")
        self.assertEqual(len(row["empreinte"]), 40)


class TestSuivi(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame([
            {"id": 1, "date": pd.Timestamp("2026-11-01"), "joueuse": "A", "type_seance": "Renforcement musculaire",
             "zone": "Genou D", "type_blessure": "Aiguë", "disponibilite": "Indisponible", "kine": "Jordan"},
            {"id": 2, "date": pd.Timestamp("2026-11-10"), "joueuse": "A", "type_seance": "Strapping / Tape",
             "zone": "Genou D", "type_blessure": "Aiguë", "disponibilite": "Disponible", "kine": "Jordan"},
            {"id": 3, "date": pd.Timestamp("2026-11-05"), "joueuse": "B", "type_seance": "Bilan / Évaluation",
             "zone": "Dos", "type_blessure": "Surcharge", "disponibilite": "Indisponible", "kine": "Baptiste"},
        ])

    def test_statut_equipe(self):
        s = K.statut_equipe(self.df, "2026-11-12")
        self.assertEqual(s["Joueuse"].tolist(), ["B", "A"])  # indisponible d'abord
        a = s.set_index("Joueuse").loc["A"]
        self.assertEqual((a["Disponibilité"], a["7 j"], a["30 j"], a["Total"]), ("Disponible", 1, 2, 2))

    def test_resume_et_semaines(self):
        r = K.resume_joueuse(self.df[self.df.joueuse == "A"])
        self.assertEqual((r["n"], r["n_reeduc"], r["dispo"]), (2, 1, "Disponible"))
        self.assertEqual(r["zones"], [("Genou D", 2)])
        sem = K.seances_par_semaine(self.df)
        self.assertEqual(int(sem["n"].sum()), 3)

    def test_zone_corps(self):
        self.assertEqual(K.zone_corps("Cheville G"), "Cheville/Pied")
        self.assertEqual(K.zone_corps("Adducteurs D"), "Hanche/Aine")
        self.assertEqual(K.zone_corps("Avant-bras Bilatéral"), "Bras/Coude")
        self.assertEqual(K.zone_corps("Cervicales"), "Cou")
        self.assertIsNone(K.zone_corps("Général / Corps entier"))
        self.assertTrue(all(K.zone_corps(z) for z in K.ZONES if z != "Général / Corps entier"))

    def test_listes(self):
        self.assertEqual(len(K.ZONES), 53)
        self.assertEqual(set(K.COMPTE_REEDUCATION), set(K.TYPES_SEANCE))


if __name__ == "__main__":
    unittest.main()
