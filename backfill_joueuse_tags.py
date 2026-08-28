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


def backfill(sb) -> None:
    evs = (
        sb.table("evenements_match")
        .select("id, match_id")
        .is_("tags", "null")
        .execute()
    )
    logger.info("%d événements à backfiller", len(evs.data))

    match_cache: dict = {}
    n_ok, n_err = 0, 0
    for ev in evs.data:
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
