import os
import sys
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backfill_joueuse_tags import _fetch_evenements_a_backfiller


class _FakeQuery:
    def __init__(self, pages):
        self._pages = pages
        self._range = None

    def select(self, *_args, **_kwargs):
        return self

    def is_(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self._range = (start, end)
        return self

    def execute(self):
        start, end = self._range
        page_index = start // 1000
        data = self._pages[page_index] if page_index < len(self._pages) else []
        return SimpleNamespace(data=data)


class _FakeClient:
    def __init__(self, pages):
        self._pages = pages

    def table(self, _name):
        return _FakeQuery(self._pages)


class TestBackfillPagination(unittest.TestCase):
    def test_fetch_paginates_au_dela_de_1000_lignes(self):
        # PostgREST plafonne une réponse à 1000 lignes : sans pagination,
        # une base avec plus de 1000 événements à backfiller en perdrait
        # silencieusement une partie (bug réel observé sur données live :
        # 2602 événements, 1602 encore non traités après un run).
        page_1 = [{"id": f"e{i}", "match_id": "m1"} for i in range(1000)]
        page_2 = [{"id": f"e{1000 + i}", "match_id": "m1"} for i in range(500)]
        client = _FakeClient([page_1, page_2])

        rows = _fetch_evenements_a_backfiller(client)

        self.assertEqual(len(rows), 1500)
        self.assertEqual(rows[0]["id"], "e0")
        self.assertEqual(rows[-1]["id"], "e1499")

    def test_fetch_page_unique_sous_1000_lignes(self):
        page_1 = [{"id": f"e{i}", "match_id": "m1"} for i in range(42)]
        client = _FakeClient([page_1])

        rows = _fetch_evenements_a_backfiller(client)

        self.assertEqual(len(rows), 42)


if __name__ == "__main__":
    unittest.main()
