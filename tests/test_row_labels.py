import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsing_utils import normalize_str
from sync_drive_to_supabase import _ROW_LABELS_EQUIPE, _is_row_joueuse


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
