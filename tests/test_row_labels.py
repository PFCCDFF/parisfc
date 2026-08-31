import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from parsing_utils import normalize_str
from sync_drive_to_supabase import _ROW_LABELS_EQUIPE, _extract_row_brut, _is_row_joueuse


class TestExtractRowBrut(unittest.TestCase):
    def test_cellule_vide_pandas_donne_chaine_vide_pas_nan(self):
        # Une cellule vide dans un DataFrame pandas réel devient NaN, pas "" :
        # str(NaN) vaudrait "nan" et _is_row_joueuse la traiterait comme une
        # joueuse (get_or_create_joueuse('NAN') créerait une fausse joueuse).
        df = pd.DataFrame([{"Row": "Camille Dupont"}, {"Row": None}])
        self.assertTrue(pd.isna(df.iloc[1]["Row"]))
        self.assertEqual(_extract_row_brut(df.iloc[1]["Row"]), "")
        self.assertFalse(_is_row_joueuse(_extract_row_brut(df.iloc[1]["Row"]), "Lyon"))

    def test_valeur_normale_inchangee(self):
        self.assertEqual(_extract_row_brut("Camille Dupont"), "Camille Dupont")


class TestRowLabels(unittest.TestCase):
    def test_pfc_est_exclu(self):
        self.assertFalse(_is_row_joueuse("PFC", "Lyon"))

    def test_start_est_exclu(self):
        self.assertFalse(_is_row_joueuse("START", "Lyon"))

    def test_cellule_vide_est_exclue(self):
        self.assertFalse(_is_row_joueuse("", "Lyon"))

    def test_adversaire_est_exclu(self):
        self.assertFalse(_is_row_joueuse("Lyon", "Lyon"))
        self.assertFalse(_is_row_joueuse("lyon", "Lyon"))  # insensible à la casse

    def test_nom_joueuse_est_inclus(self):
        self.assertTrue(_is_row_joueuse("Camille Dupont", "Lyon"))


if __name__ == "__main__":
    unittest.main()
