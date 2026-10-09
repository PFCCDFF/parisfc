import os
import sys
import unittest
from datetime import date, timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import perf_v2_data as D  # noqa: E402


def _wellness(joueuse, jours, valeur=4):
    return pd.DataFrame([{
        "joueuse": joueuse, "date": (date(2026, 9, 1) + timedelta(days=i)).isoformat(),
        **{c: valeur for c in D.WELLNESS_ITEMS}, "sommeil_heures": 8,
    } for i in range(jours)])


class TestWellness(unittest.TestCase):
    def test_score_total(self):
        d = D.wellness_scores(_wellness("A", 1, 4))
        self.assertEqual(d["score_total"].iloc[0], 20)
        self.assertEqual(d["score_pct"].iloc[0], 75)

    def test_items_manquants_ne_penalisent_pas(self):
        w = _wellness("A", 1, 4)
        w.loc[0, "humeur"] = np.nan
        self.assertEqual(D.wellness_scores(w)["score_total"].iloc[0], 20)

    def test_zscore_detecte_baisse(self):
        w = _wellness("A", 10, 4)
        w.loc[:8, "fatigue"] = [4, 5, 4, 5, 4, 5, 4, 5, 4]
        w.loc[9, list(D.WELLNESS_ITEMS)] = 2
        dz = D.wellness_zscores(w)
        self.assertTrue(np.isnan(dz["z_score"].iloc[2]))  # < 5 réponses de référence
        self.assertLess(dz["z_score"].iloc[-1], D.WELLNESS_SEUIL_Z)
        alertes = D.wellness_alertes(dz.iloc[-1])
        self.assertTrue(any("z =" in a for a in alertes))
        self.assertTrue(any("Fraîcheur" in a for a in alertes))

    def test_equipe_jour_non_repondantes(self):
        t = D.wellness_equipe_jour(_wellness("A", 3), date(2026, 9, 3), roster=["A", "B"])
        self.assertEqual(set(t["joueuse"]), {"A", "B"})
        self.assertEqual(t.set_index("joueuse").loc["B", "alertes"], "Pas de réponse")


class TestCharge(unittest.TestCase):
    def setUp(self):
        rows = []
        for i in range(35):
            j = date(2026, 9, 7) + timedelta(days=i)  # lundi 7/09
            if j.weekday() < 5:
                rows.append({"joueuse": "A", "date": j.isoformat(), "rpe": 5, "duree_min": 90})
        self.rpe = pd.DataFrame(rows)

    def test_charge_hebdo(self):
        h = D.charge_hebdo(self.rpe)
        self.assertEqual(len(h), 5)
        self.assertEqual(h["charge_ua"].iloc[0], 5 * 450)
        self.assertEqual(h["nb_seances"].iloc[0], 5)
        # 5 jours à 450, 2 jours à 0 : moyenne / écart-type
        q = np.array([450] * 5 + [0, 0], dtype=float)
        self.assertAlmostEqual(h["monotonie"].iloc[0], round(q.mean() / q.std(ddof=1), 2))

    def test_acwr_stable_proche_de_1(self):
        a = D.acwr_ewma(self.rpe, "A")
        self.assertTrue(a["acwr"].iloc[:21].isna().all())
        self.assertTrue(0.8 < a["acwr"].dropna().iloc[-1] < 1.3)

    def test_zone(self):
        self.assertEqual(D.zone_acwr(1.0)[0], "Zone optimale")
        self.assertEqual(D.zone_acwr(1.6)[0], "Risque élevé")
        self.assertEqual(D.zone_acwr(np.nan)[0], "—")


class TestSynthese(unittest.TestCase):
    def test_presence_resume(self):
        p = pd.DataFrame({"joueuse": ["A", "A", "A", "B"],
                          "statut": ["Présente", "Absente", "Sélection", "Blessée (Kiné)"]})
        t = D.presence_resume(p, ["Présente", "Absente", "Sélection", "Blessée (Kiné)"], ("Présente", "Sélection"))
        a = t.set_index("joueuse").loc["A"]
        self.assertEqual(a["Séances"], 3)
        self.assertEqual(a["Taux de présence (%)"], 67)

    def test_kpi_match_pondere_par_minutes(self):
        k = pd.DataFrame({"Player": ["A", "A"], "Date": ["2026-09-01", "2026-09-08"],
                          "Temps de jeu (en minutes)": [90, 30], "Rigueur": [60, 100]})
        t = D.synthese_kpi_match(k, ["A"], ["Rigueur"])
        self.assertEqual(t["Rigueur"].iloc[0], 70.0)
        self.assertEqual(t["Matchs"].iloc[0], 2)
        t2 = D.synthese_kpi_match(k, ["A"], ["Rigueur"], deb=date(2026, 9, 5))
        self.assertEqual(t2["Matchs"].iloc[0], 1)

    def test_kpi_match_noms_normalises(self):
        k = pd.DataFrame({"Player": ["LEA MARTIN"], "Temps de jeu (en minutes)": [90], "Rigueur": [50]})
        t = D.synthese_kpi_match(k, ["Léa Martin"], ["Rigueur"],
                                 cle_nom=lambda n: n.upper().replace("É", "E"))
        self.assertEqual(t["Joueuse"].tolist(), ["Léa Martin"])

    def test_objectifs(self):
        o = pd.DataFrame({"id": [1, 2], "categorie": ["physique", "incontournable"],
                          "objectif": ["Vitesse", "Placement"], "statut_label": ["En cours", "Acquis"]})
        e = pd.DataFrame({"objectif_id": [1, 1, 1], "evaluateur": ["joueuse", "coach", "coach"],
                          "note": [4, 3, 2], "match_date": ["2026-09-01", "2026-09-01", "2026-09-08"]})
        t = D.synthese_objectifs(o, e).set_index("Objectif")
        self.assertEqual(t.loc["Vitesse", "Note coach"], 2.5)
        self.assertEqual(t.loc["Vitesse", "Écart (J − C)"], 1.5)
        self.assertEqual(t.loc["Vitesse", "Matchs évalués"], 2)
        self.assertEqual(t.loc["Placement", "Matchs évalués"], 0)

    def test_ligne_match_et_bilan(self):
        rep = {"pfc_name": "PFC", "adv_name": "FCN",
               "stats": {"PFC": {"tirs": 10, "possessions": 50}, "FCN": {"tirs": 4}},
               "poss_total": {"PFC": 58.0, "FCN": 42.0}}
        l1 = D.ligne_match_collectif(rep, {"date": "2026-09-01", "score_pfc": 2, "score_adv": 1})
        l2 = D.ligne_match_collectif(rep, {"date": "2026-09-08", "score_pfc": "0", "score_adv": "0"})
        self.assertEqual(l1["Résultat"], "V")
        self.assertEqual(l2["Résultat"], "N")
        b = D.bilan_resultats(pd.DataFrame([l1, l2]))
        self.assertEqual((b["V"], b["N"], b["points"], b["bp"]), (1, 1, 4, 2))

    def test_gps_pics_et_totaux(self):
        g = pd.DataFrame({"Player": ["A", "A"], "DATE": pd.to_datetime(["2026-09-01", "2026-09-02"]),
                          "Distance (m)": [5000, 6000], "Vitesse max (km/h)": [28.0, 30.5]})
        t = D.synthese_gps(g, ["A"], ["Distance (m)", "Vitesse max (km/h)"], pics={"Vitesse max (km/h)"})
        self.assertEqual(t["Distance (m) (total)"].iloc[0], 11000)
        self.assertEqual(t["Vitesse max (km/h) (max)"].iloc[0], 30.5)
        self.assertEqual(t["Séances"].iloc[0], 2)

    def test_periodes(self):
        p = D.periodes_predefinies(date(2026, 10, 9))
        self.assertEqual(p["Depuis le début de saison"][0], date(2026, 7, 1))
        self.assertEqual(p["Semaine en cours"][0], date(2026, 10, 5))


if __name__ == "__main__":
    unittest.main()
