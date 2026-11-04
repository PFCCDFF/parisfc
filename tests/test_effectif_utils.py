import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import effectif_utils as E  # noqa: E402


def _eff():
    eff = E.vide()
    E.ajouter(eff, "DUPONT Juliette", ajout="import")
    E.ajouter(eff, "NIAMA MAHOUKOU Chloé", ajout="import")
    E.ajouter(eff, "MARTIN Léa", E.STATUT_PARTIE, "2026-06-30", ajout="import")
    return eff


class TestCles(unittest.TestCase):
    def test_accents_casse_ordre(self):
        self.assertEqual(E.cle("Chloé NIAMA-MAHOUKOU"), E.cle("niama mahoukou chloe"))

    def test_proximite(self):
        self.assertEqual(E.proximite("Juliette DUPONT", "DUPONT Juliette"), 1.0)
        self.assertGreaterEqual(E.proximite("Clhoé NIAMA", "NIAMA MAHOUKOU Chloé"), 0.95)
        self.assertGreaterEqual(E.proximite("DUPONT J", "DUPONT Juliette"), 0.85)
        self.assertLess(E.proximite("BERNARD Inès", "DUPONT Juliette"), 0.55)


class TestEffectif(unittest.TestCase):
    def test_resoudre_nom_et_variante(self):
        eff = _eff()
        self.assertEqual(E.resoudre("juliette dupont", eff), "DUPONT Juliette")
        self.assertIsNone(E.resoudre("DUPONT J", eff))
        E.rattacher(eff, "DUPONT J", "DUPONT Juliette")
        self.assertEqual(E.resoudre("Dupont J.", eff), "DUPONT Juliette")

    def test_ajouter_ne_duplique_pas(self):
        eff = _eff()
        self.assertEqual(E.ajouter(eff, "Juliette Dupont"), "DUPONT Juliette")
        self.assertEqual(len(eff["joueuses"]), 3)

    def test_noms_a_rattacher(self):
        eff = _eff()
        comptes = {"Matchs": {"DUPONT Juliette": 40, "DUPONT J": 12, "Equipe B": 3},
                   "GPS": {"dupont j.": 5, "Lea Martin": 8}}
        E.ignorer(eff, "Equipe B")
        out = E.noms_a_rattacher(comptes, eff)
        self.assertEqual([g["nom"] for g in out], ["DUPONT J"])
        self.assertEqual(out[0]["lignes"], 17)
        self.assertEqual(out[0]["sources"], {"Matchs": 12, "GPS": 5})
        self.assertEqual(out[0]["suggestions"][0][0], "DUPONT Juliette")

    def test_rattacher_deplace_une_variante(self):
        eff = _eff()
        E.rattacher(eff, "C NIAMA", "DUPONT Juliette")
        E.rattacher(eff, "C NIAMA", "NIAMA MAHOUKOU Chloé")
        self.assertEqual(eff["joueuses"]["DUPONT Juliette"]["variantes"], [])
        self.assertEqual(E.resoudre("C NIAMA", eff), "NIAMA MAHOUKOU Chloé")

    def test_renommer_garde_l_ancien_nom(self):
        eff = _eff()
        E.renommer(eff, "DUPONT Juliette", "DUPONT-ROUX Juliette")
        self.assertEqual(E.resoudre("DUPONT Juliette", eff), "DUPONT-ROUX Juliette")

    def test_fusionner(self):
        eff = _eff()
        E.ajouter(eff, "Juju DUPONT")
        E.rattacher(eff, "JD", "Juju DUPONT")
        E.fusionner(eff, "Juju DUPONT", "DUPONT Juliette")
        self.assertNotIn("Juju DUPONT", eff["joueuses"])
        self.assertEqual(E.resoudre("JD", eff), "DUPONT Juliette")
        self.assertEqual(E.resoudre("juju dupont", eff), "DUPONT Juliette")

    def test_initialiser(self):
        eff = E.initialiser(
            ["DUPONT Juliette", "Juliette DUPONT", "NIAMA MAHOUKOU Chloé", "Clhoé NIAMA"],
            {"MARTIN Léa": "2026-06-30"},
            {"X CHRISTY": "DUPONT Juliette", "Inconnue": "PERSONNE Nulle"})
        self.assertEqual(E.noms(eff), ["DUPONT Juliette", "MARTIN Léa", "NIAMA MAHOUKOU Chloé"])
        self.assertEqual(E.noms(eff, E.STATUT_PARTIE), ["MARTIN Léa"])
        self.assertEqual(eff["joueuses"]["MARTIN Léa"]["depart"], "2026-06-30")
        self.assertEqual(E.resoudre("Clhoé NIAMA", eff), "NIAMA MAHOUKOU Chloé")
        self.assertEqual(E.resoudre("Christy X", eff), "DUPONT Juliette")

    def test_aller_retour_fichier(self):
        eff = _eff()
        E.rattacher(eff, "DUPONT J", "DUPONT Juliette")
        E.ignorer(eff, "Equipe B")
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "effectif.json")
            self.assertIsNone(E.charger(p))
            E.enregistrer(p, eff)
            self.assertEqual(E.charger(p), eff)


if __name__ == "__main__":
    unittest.main()
