import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

import gps_compilation as gc

ENTETE_A = ("Activity Date,Capteur,Numéro de joueur,Nom de joueur,Temps joué,Distance (m),Distance HID (>13 km/h),"
            "Distance HID (>19 km/h),Distance par plage de vitesse (0-7 km/h),Distance par plage de vitesse (7-13 km/h),"
            "Distance par plage de vitesse (13-15 km/h),Distance par plage de vitesse (15-19 km/h),"
            "Distance par plage de vitesse (19-23 km/h),Distance par plage de vitesse (23-25 km/h),"
            "Distance par plage de vitesse (>25 km/h),# of Sprints (>23 km/h),# of Sprints (>25 km/h),Vitesse max (km/h),"
            "Accélération maximale (m/s²),# of Accelerations (>2 m/s²),# of Accelerations (>3 m/s²),"
            "# of Accelerations (>4 m/s²),# of Decélerations (>2 m/s²),# of Decélerations (>3 m/s²),# of Decélerations (>4 m/s²)")


def ligne_a(date, nom, temps, bandes, vmax=24.0, acc=(50, 10, 1), dec=(40, 8, 1)):
    d = sum(bandes)
    return (f"{date},1,1,{nom},{temps},{d},{sum(bandes[2:])},{sum(bandes[4:])},"
            + ",".join(str(b) for b in bandes) + f",0,0,{vmax},3.5,{acc[0]},{acc[1]},{acc[2]},{dec[0]},{dec[1]},{dec[2]}")


COLS_B = ['"First Name"', '"Last Name"', '"Number"', '"Position"', '"Profile"', '"Device"', '"Time (min)"',
          '"Distance (m)"', '"R>15 distance (m)"', '"R<7 distance (m)"', '"Hid1 distance (m)"', '"Hid2 distance (m)"',
          '"Sprint distance (m)"', '"Speed max (km/h)"', '"Accel > 2 m/s² (nb)"', '"Accel > 3 m/s² (nb)"',
          '"Decel > 2 m/s² (nb)"', '"Decel > 3 m/s² (nb)"']


def ligne_b(nom, t, D, r15, r7, h1, h2, s, acc2=100, acc3=10):
    return f'".";"{nom}";1;"MIDFIELDER";"INTERMEDIATE";"1";{t};{D};{r15};{r7};{h1};{h2};{s};25;{acc2};{acc3};90;9'


def fichier_b(nom_session, typ, debut, periodes):
    """periodes : [(nom_periode, debut, [lignes])]"""
    out = ['"Name";"Type";"Number";"Week";"Start Date";"End Date"', f'"{nom_session}";"{typ}";1;;"{debut}";"{debut}"', "",
           "Periods", '"Name";"Start Date";"End Date"'] + [f'"{p}";"{d}";"{d}"' for p, d, _ in periodes] + ["",
           "Global Metrics", ";".join(COLS_B)] + periodes[0][2] + ["", "Effective Metrics", ";".join(COLS_B)] + periodes[0][2] + [
           "", "Periods Metrics"]
    for p, d, lignes in periodes:
        out += ['"Name";"Start Date";"End Date"', f'"{p}";"{d}";"{d}"', ";".join(COLS_B)] + lignes + [""]
    return "\n".join(out)


class TestCompilation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def ecrire(self, nom, contenu):
        with open(os.path.join(self.tmp, nom), "w", encoding="utf-8") as f:
            f.write(contenu)

    def test_bandes_format_b(self):
        # D=1000, R<7=400, HID1(13-19)=300, R>15=200, HID2(19-23)=80, Sprint=20
        t = pd.DataFrame({"Last Name": ["X"], "Distance (m)": ["1000"], "R<7 distance (m)": ["400"],
                          "Hid1 distance (m)": ["300"], "R>15 distance (m)": ["200"], "Hid2 distance (m)": ["80"],
                          "Sprint distance (m)": ["20"]})
        b = {k: float(v.iloc[0]) for k, v in gc._bandes_b(t).items()}
        self.assertEqual((b["p1_m"], b["p2_m"], b["p3_m"], b["p4_m"], b["p5_m"], b["p6_m"]), (400, 200, 200, 100, 80, 20))
        self.assertAlmostEqual(sum(b[k] for k in gc.BANDES), b["distance_m"])

    def test_copies_et_tiers_temps_dedoublonnes(self):
        l1 = ligne_a("2025-08-16T15:00:00+02:00", "Nina DUMANS", 30, [1000, 800, 100, 100, 50, 10])
        l2 = ligne_a("2025-08-16T15:00:00+02:00", "Nina DUMANS", 60, [2000, 1600, 200, 200, 100, 20])
        self.ecrire("U19 (A) Paris FC - QRM 16.08.25_1eMT__aaaaaaaa.csv", ENTETE_A + "\n" + l1)
        self.ecrire("GF1_U19_Paris_FC_-_Qrm_16_08_25__bbbbbbbb.csv", ENTETE_A + "\n" + l1)   # copie
        self.ecrire("U19 (A) Paris FC - QRM 16.08.25_Worksheet__cccccccc.csv", ENTETE_A + "\n" + l2)  # cumul
        df = gc.compiler_sessions_gps([self.tmp])
        self.assertEqual(len(df), 1)
        self.assertEqual(df.distance_m.iloc[0], 4120)
        self.assertEqual(df.equipe.iloc[0], "U19")

    def test_lignes_total_et_complement(self):
        corps = "\n".join([ENTETE_A, ligne_a("2025-10-12T14:00:00+02:00", "Lana BOUDINE", 90, [3000, 3000, 500, 500, 200, 30]),
                           ",,,,," + ",".join(["9999"] * 20),                                     # ligne total
                           ligne_a("", "Lana BOUDINE", 6, [100, 300, 100, 100, 100, 10])])      # complément
        self.ecrire("U19 1:J06 Le Mans - Paris FC 12.10.25__dddddddd.csv", corps)
        df = gc.compiler_sessions_gps([self.tmp])
        self.assertEqual(sorted(df.ligne), ["complement", "principal"])
        self.assertEqual(df.session_label.iloc[0], "Championnat phase 1 J06 — Le Mans - Paris FC")

    def test_format_b_match_periodes(self):
        a = ligne_b("RENAI Sylia", 47, 4000, 500, 1500, 700, 150, 20)
        b = ligne_b("RENAI Sylia", 47, 4200, 520, 1600, 720, 160, 30)
        c = ligne_b("RENAI Sylia", 8, 500, 100, 100, 150, 60, 10)
        self.ecrire("2026_12-09-2026__eeeeeeee.csv", fichier_b("1 / J02 Paris FC - HAC 12/09/2026", "MATCH",
                    "2026-09-12T13:00:00.000Z", [("P1", "2026-09-12T13:00:00.000Z", [a]),
                                                 ("P2", "2026-09-12T14:00:00.000Z", [b]),
                                                 ("", "2026-09-12T14:50:00.000Z", [c])]))
        df = gc.compiler_sessions_gps([self.tmp]).set_index("ligne")
        self.assertEqual(df.loc["principal", "distance_m"], 8200)
        self.assertEqual(df.loc["complement", "distance_m"], 500)
        self.assertEqual(df.loc["principal", "equipe"], "U19")
        self.assertEqual(df.loc["principal", "joueuse"], "Sylia RENAI")
        self.assertAlmostEqual(df.loc["principal", gc.BANDES].sum(), 8200)

    def test_variantes_de_noms(self):
        canon = gc.regrouper_variantes(pd.Series(["Maelline MUPFASONI"] * 5 + ["Maeline MUPSAFOSI", "Louane EXILIE", "Louane"]))
        self.assertEqual(canon["Maeline MUPSAFOSI"], "Maelline MUPFASONI")
        self.assertEqual(canon["Louane"], "Louane EXILIE")


class TestCharge(unittest.TestCase):
    def test_ewma_et_reprise_apres_interruption(self):
        from charge_entrainement import daily_load, prepare_sessions
        jours = list(pd.date_range("2025-09-01", periods=40, freq="D")) + list(pd.date_range("2026-01-01", periods=10, freq="D"))
        rows = [dict(date=d, saison="2025-26", type_session="Entraînement", ligne="principal", equipe="GF1",
                     session_id=str(d), session_label="s", joueuse="A", temps_min=60, distance_m=5000,
                     **{b: 0 for b in gc.BANDES}, acc2=0, acc3=0, dec2=0, dec3=0, systeme="A", qualite_ok=True) for d in jours]
        rows.append(dict(rows[0], date=pd.Timestamp("2025-09-06"), type_session="Match", session_id="m", temps_min=90, distance_m=10000))
        d = prepare_sessions(pd.DataFrame(rows))
        g = daily_load(d, "Distance totale")
        self.assertEqual(g.segment.nunique(), 2)                         # > 21 j sans donnée → redémarrage
        seg2 = g[g.segment == g.segment.max()]
        self.assertTrue(seg2.acwr.isna().all())                          # < 21 j d'historique
        acwr = g[g.date == "2025-10-08"].acwr.iloc[0]                    # charge quasi constante
        self.assertTrue(0.9 < acwr < 1.1, acwr)


if __name__ == "__main__":
    unittest.main()
