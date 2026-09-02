"""
backfill_joueuse_tags.py

Script ponctuel : remplit joueuse_id/row_brut/tags sur les événements déjà
synchronisés dans evenements_match, à partir des lignes evenement_tags
existantes (lues, jamais modifiées ni supprimées). Idempotent — ne
retraite pas les événements déjà backfillés.

Identifiants attendus dans un fichier .env à côté de ce script (mêmes
variables que sync_drive_to_supabase.py) :
    SUPABASE_URL=...
    SUPABASE_SERVICE_KEY=...
"""

import logging

from sync_drive_to_supabase import get_client, get_or_create_joueuse, _is_row_joueuse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill_joueuse_tags")


def _fetch_evenements_a_backfiller(sb) -> list:
    # PostgREST plafonne les réponses à 1000 lignes par défaut : paginer par
    # blocs de 1000 jusqu'à une page vide, sinon seuls les 1000 premiers
    # événements sont traités et le reste silencieusement ignoré.
    rows: list = []
    start = 0
    page_size = 1000
    while True:
        page = (
            sb.table("evenements_match")
            .select("id, match_id")
            .is_("tags", "null")
            .order("id")
            .range(start, start + page_size - 1)
            .execute()
        )
        rows.extend(page.data)
        if len(page.data) < page_size:
            break
        start += page_size
    return rows


def backfill(sb) -> None:
    evs_data = _fetch_evenements_a_backfiller(sb)
    logger.info("%d événements à backfiller", len(evs_data))

    match_cache: dict = {}
    n_ok, n_err = 0, 0
    for ev in evs_data:
        try:
            tags_rows = (
                sb.table("evenement_tags")
                .select("cle, valeur")
                .eq("evenement_id", ev["id"])
                .execute()
            )
            tags = {r["cle"]: r["valeur"] for r in tags_rows.data}
            row_brut = tags.get("Row", "")

            match_id = ev["match_id"]
            if match_id not in match_cache:
                m = sb.table("matchs").select("categorie, adversaire").eq("id", match_id).execute()
                match_cache[match_id] = m.data[0] if m.data else {"categorie": "U19F", "adversaire": ""}
            match_info = match_cache[match_id]

            update = {"tags": tags, "row_brut": row_brut}
            if _is_row_joueuse(row_brut, match_info.get("adversaire", "")):
                update["joueuse_id"] = get_or_create_joueuse(sb, row_brut, match_info["categorie"])

            sb.table("evenements_match").update(update).eq("id", ev["id"]).execute()
            n_ok += 1
        except Exception:
            logger.exception("Échec backfill événement %s", ev["id"])
            n_err += 1

    logger.info("Backfill terminé : %d OK, %d erreurs", n_ok, n_err)


def main():
    sb = get_client()
    backfill(sb)


if __name__ == "__main__":
    main()
