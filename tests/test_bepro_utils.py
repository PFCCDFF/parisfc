import io
import json
import os
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

import bepro_utils as B

NOM_ZIP = ("2026-09-12_Paris FC U19 Féminines vs Le Havre AC U19 Féminines"
           "(Championnat National Féminin U19 - Groupe A)_Raw Event Data_JSON.zip")


def _ev(player, events, x, y, direction="RIGHT", to=(None, None), period="1st Half"):
    return {"period_name": period, "event_time": 1000, "team_name": "Paris FC U19 Féminines",
            "player_name": player, "player_shirt_number": "3", "events": events,
            "x": x, "y": y, "to_x": to[0], "to_y": to[1], "attack_direction": direction}


def _passe(outcome="Succeeded", direction="Passes Forward", area="Passes In Middle Third",
           distance="Short Passes"):
    return {"event_name": "Passes", "property": {"Outcome": outcome, "Direction": direction,
                                                 "Area": area, "Distance": distance}}


def _zip(periodes: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for nom, data in periodes.items():
            z.writestr(nom, json.dumps({"data": data}))
    return buf.getvalue()


class TestNomFichier(unittest.TestCase):
    def test_zip_export(self):
        info = B.parse_bepro_filename(NOM_ZIP)
        self.assertEqual(info["date"], pd.Timestamp("2026-09-12"))
        self.assertEqual(info["home"], "Paris FC U19 Féminines")
        self.assertEqual(info["away"], "Le Havre AC U19 Féminines")
        self.assertTrue(B.is_bepro_file(NOM_ZIP))

    def test_json_periode(self):
        info = B.parse_bepro_filename("2026-09-12_Paris FC U19 Féminines vs Le Havre AC U19 Féminines_2nd Half.json")
        self.assertEqual(info["away"], "Le Havre AC U19 Féminines")


class TestRepere(unittest.TestCase):
    def test_couloir_gauche_en_haut_dans_les_deux_sens(self):
        # Latérale gauche : y Bepro ≈ 0.9 en attaquant à droite, ≈ 0.1 en attaquant à gauche.
        df = B.bepro_events_frame([
            _ev("Sylia Renai", [_passe()], 0.30, 0.90, "RIGHT"),
            _ev("Sylia Renai", [_passe()], 0.70, 0.10, "LEFT", period="2nd Half"),
        ])
        self.assertAlmostEqual(df.loc[0, "x"], 30.0)
        self.assertAlmostEqual(df.loc[0, "y"], 6.8)
        self.assertAlmostEqual(df.loc[1, "x"], 30.0)   # demi-tour : même position défensive
        self.assertAlmostEqual(df.loc[1, "y"], 6.8)

    def test_coordonnees_absentes(self):
        df = B.bepro_events_frame([_ev("A B", [{"event_name": "Fouls", "property": {}}], None, None)])
        self.assertIsNone(df.loc[0, "x"])


class TestNoms(unittest.TestCase):
    def test_correspondance(self):
        noms = ["Melita Mane", "Asmaou Cherif", "Mina Cherif Hadria", "Odelia Tae"]
        self.assertEqual(B.match_player_name("MANE Mélita", noms), "Melita Mane")
        self.assertEqual(B.match_player_name("CHERIF Asmaou", noms), "Asmaou Cherif")
        self.assertEqual(B.match_player_name("CHERIF HADRIA Mina", noms), "Mina Cherif Hadria")
        self.assertEqual(B.match_player_name("TAE Odélia", noms), "Odelia Tae")
        self.assertIsNone(B.match_player_name("DUPONT Julie", noms))


class TestStats(unittest.TestCase):
    def setUp(self):
        self.df = B.bepro_events_frame([
            _ev("Sylia Renai", [_passe()], 0.3, 0.9),
            _ev("Sylia Renai", [_passe("Failed", "Passes Sideways", "Passes In Final Third", "Long Passes")], 0.8, 0.9),
            _ev("Sylia Renai", [{"event_name": "Passes Received", "property": {}},
                                {"event_name": "Take-on", "property": {"Outcome": "Failed"}}], 0.6, 0.9),
            _ev("Sylia Renai", [{"event_name": "Shots & Goals", "property": {"Outcome": "Goals"}}], 0.9, 0.5),
            _ev("Sylia Renai", [{"event_name": "Duels", "property": {"Type": "Aerial Duels", "Outcome": "Succeeded"}}], 0.2, 0.5),
            _ev("Sylia Renai", [{"event_name": "Duels", "property": {"Type": "Physical Duels", "Outcome": "Failed"}},
                                {"event_name": "Fouls", "property": {"Type": "Fouls"}}], 0.2, 0.5),
            _ev("Melita Mane", [_passe()], 0.2, 0.5),
        ])

    def test_compteurs(self):
        s = B.compute_bepro_player_stats(self.df, "RENAI Sylia")
        self.assertEqual((s["passes_ok"], s["passes_ko"]), (1, 1))
        self.assertEqual(s["pass_breakdown"]["avant"], [1, 1])
        self.assertEqual(s["pass_breakdown"]["cotes"], [1, 0])
        self.assertEqual(s["pass_breakdown"]["dernier_tiers"], [1, 0])
        self.assertEqual(s["pass_breakdown"]["moitie_off"], [1, 0])
        self.assertEqual((s["drib_ok"], s["drib_ko"]), (0, 1))
        self.assertEqual((s["tirs_tot"], s["tirs_cadres"], s["tirs_buts"]), (1, 1, 1))
        self.assertEqual((s["aer_ok"], s["aer_ko"], s["sol_ok"], s["sol_ko"]), (1, 0, 0, 1))
        # Ballons joués : passe, tir, interception, dribble ou duel gagné (2 passes,
        # réception+dribble, tir, duel aérien gagné) ; le duel perdu est exclu
        self.assertEqual(s["ballons"], 5)
        self.assertEqual(len(s["locs"]), 4)
        # Ballons perdus : passe ratée + dribble raté
        self.assertEqual(s["pertes"], 2)

    def test_joueuse_absente(self):
        self.assertEqual(B.compute_bepro_player_stats(self.df, "DUPONT Julie"), {})


class TestKpiInputs(unittest.TestCase):
    def test_compteurs_indicateurs(self):
        df = B.bepro_events_frame([
            _ev("Sylia Renai", [_passe(distance="Long Passes", area="Passes In Final Third")], 0.5, 0.5),
            _ev("Sylia Renai", [_passe("Failed")], 0.5, 0.5),
            _ev("Sylia Renai", [{"event_name": "Tackles", "property": {"Outcome": "Tackle Succeeded: Possession"}},
                                {"event_name": "Fouls", "property": {"Type": "Fouls"}}], 0.3, 0.5),
            _ev("Sylia Renai", [{"event_name": "Duels", "property": {"Type": "Aerial Duels", "Outcome": "Failed"}},
                                {"event_name": "Fouls", "property": {"Type": "Fouls Won"}}], 0.3, 0.5),
            _ev("Sylia Renai", [{"event_name": "Key Passes", "property": {}}, {"event_name": "Assists", "property": {}}], 0.8, 0.5),
        ])
        r = B.bepro_kpi_inputs(df).set_index("Player").loc["Sylia Renai"]
        self.assertEqual((r["Passes"], r["Passes longues"], r["Passes réussies (longues)"], r["Passes courtes"]), (2, 1, 1, 1))
        self.assertEqual((r["Duels défensifs"], r["Duels défensifs gagnés"], r["Fautes"]), (2, 1, 1))  # faute subie exclue
        self.assertEqual((r["__last_third"], r["__deseq"], r["__assists"]), (1, 1, 1))


class TestDossier(unittest.TestCase):
    def test_zip_prioritaire_sur_json(self):
        p1 = [_ev("Sylia Renai", [_passe()], 0.3, 0.9)]
        p2 = [_ev("Sylia Renai", [_passe()], 0.7, 0.1, "LEFT", period="2nd Half")]
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, NOM_ZIP), "wb") as f:
                f.write(_zip({"a_1st Half.json": p1, "a_2nd Half.json": p2}))
            # JSON dézippé du même match à côté : ne doit pas doubler les événements
            with open(os.path.join(d, "2026-09-12_Paris FC U19 Féminines vs Le Havre AC U19 Féminines_1st Half.json"), "w") as f:
                json.dump({"data": p1}, f)
            bp = B.load_bepro_folder(d)
            self.assertEqual(len(bp), 1)
            df = B.find_bepro_match(bp, "2026-09-12", "HAC")
            self.assertEqual(len(df), 2)
            self.assertIsNone(B.find_bepro_match(bp, "2026-09-13"))
            # même jour, autre catégorie (match U23) : l'export U19 ne doit pas être utilisé
            self.assertIsNone(B.find_bepro_match(bp, "2026-09-12", "Red Star", "U23"))
            self.assertEqual(len(B.find_bepro_match(bp, "2026-09-12", "HAC", "U19")), 2)


if __name__ == "__main__":
    unittest.main()
