import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

import import_csv as ic
from tests.test_gps_compilation import ENTETE_A, fichier_b, ligne_a, ligne_b

TACTIQUE = "Timeline,Row,Action\nU19 J3 Paris FC - LOSC,PFC,\nU19 J3 Paris FC - LOSC,RENAI Sylia,Passe\n"


def est_tactique(nom):
    return nom.upper().startswith("PFC_VS")


def infos_tactique(nom):
    import re
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", nom)
    date = pd.Timestamp(f"{m.group(3)}-{m.group(2)}-{m.group(1)}") if m else None
    j = re.search(r"_J(\d+)_", nom)
    return {"date": date, "journee": j.group(1).zfill(2) if j else "", "adversaire": "LOSC", "adv_norm": "losc"}


def lire_csv(chemin):
    return pd.read_csv(chemin)


def est_match_nom(nom):
    return "seance" not in nom.lower() and "séance" not in nom.lower()


def analyser(nom, contenu):
    return ic.analyser_fichier(nom, contenu.encode("utf-8"), est_tactique, lire_csv, infos_tactique, est_match_nom)


class TestAnalyse(unittest.TestCase):
    def test_tactique_valide(self):
        a = analyser("PFC_VS_ 2627 U19F LOSC_J3_U19_20-09-2026.csv", TACTIQUE)
        self.assertEqual((a["type"], a["valide"]), ("Tactique", True))
        self.assertIn("J03", a["detail"])

    def test_tactique_sans_colonnes_sportscode(self):
        a = analyser("PFC_VS_ 2627 U19F LOSC_J3_U19_20-09-2026.csv", "a,b\n1,2\n")
        self.assertEqual((a["type"], a["valide"]), ("Tactique", False))
        self.assertIn("Timeline", a["detail"])

    def test_gps_format_a_seance(self):
        contenu = ENTETE_A + "\n" + ligne_a("2025-09-26T10:00:00+02:00", "Nina DUMANS", 60, [2000, 1500, 200, 200, 100, 20])
        a = analyser("GF1 S09 séance 43 - 26.09.25.csv", contenu)
        self.assertEqual((a["type"], a["valide"]), ("GPS entraînement", True))
        self.assertEqual(a["date"], pd.Timestamp("2025-09-26"))

    def test_gps_format_b_match(self):
        contenu = fichier_b("1 / J02 Paris FC - HAC 12/09/2026", "MATCH", "2026-09-12T13:00:00.000Z",
                            [("P1", "2026-09-12T13:00:00.000Z", [ligne_b("RENAI Sylia", 47, 4000, 500, 1500, 700, 150, 20)])])
        a = analyser("2026_12-09-2026.csv", contenu)
        self.assertEqual((a["type"], a["valide"]), ("GPS match", True))

    def test_fichier_inconnu(self):
        a = analyser("notes.csv", "x;y\n1;2\n")
        self.assertIsNone(a["type"])
        self.assertFalse(a["valide"])


class TestDoublons(unittest.TestCase):
    def setUp(self):
        self.gps = tempfile.mkdtemp()
        self.tact = tempfile.mkdtemp()

    def ecrire(self, dossier, nom, contenu, mtime=None):
        p = os.path.join(dossier, nom)
        with open(p, "w", encoding="utf-8") as f:
            f.write(contenu)
        if mtime:
            os.utime(p, (mtime, mtime))
        return p

    def test_trois_motifs(self):
        l = ligne_a("2025-08-16T15:00:00+02:00", "Nina DUMANS", 60, [2000, 1600, 200, 200, 100, 20])
        self.ecrire(self.gps, "U19 QRM 16.08.25__aaaaaaaa.csv", ENTETE_A + "\n" + l)
        self.ecrire(self.gps, "U19 QRM 16.08.25__bbbbbbbb.csv", ENTETE_A + "\n" + l)            # octets identiques
        self.ecrire(self.gps, "GF1_U19_QRM_16_08_25__cccccccc.csv", ENTETE_A + "\n" + l + "\n")  # même session
        autre = ligne_a("2025-08-17T15:00:00+02:00", "Lana BOUDINE", 60, [2500, 1600, 200, 200, 100, 20])
        self.ecrire(self.gps, "Autre 17.08.25__dddddddd.csv", ENTETE_A + "\n" + autre)
        vieux = self.ecrire(self.tact, "PFC_VS_ 2627 U19 LOSC_J3_U19_20-09-2026.csv", TACTIQUE, mtime=1_700_000_000)
        recent = self.ecrire(self.tact, "PFC_VS_ 2627 U19F LOSC_J3_U19_20-09-2026.csv", TACTIQUE + "x,y,z\n")
        d = ic.lister_doublons([self.gps], [self.tact], est_tactique, infos_tactique)
        motifs = d.groupby("motif").fichier.apply(sorted).to_dict()
        self.assertEqual(motifs["Contenu identique"], ["U19 QRM 16.08.25.csv", "U19 QRM 16.08.25.csv"])
        self.assertEqual(len(motifs["Même session GPS"]), 3)
        self.assertNotIn("Autre 17.08.25.csv", d.fichier.tolist())
        t = d[d.motif == "Même match tactique"].set_index("chemin").conserve
        self.assertTrue(t[recent])
        self.assertFalse(t[vieux])


if __name__ == "__main__":
    unittest.main()
