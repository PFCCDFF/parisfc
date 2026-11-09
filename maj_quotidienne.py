"""
maj_quotidienne.py

Mise à jour quotidienne des données de l'app, sans intervention : même travail que le bouton
Gestion → « Mettre à jour la base » (téléchargement Drive → data/ des fichiers principaux,
GPS, GPS match, Bepro et photos, puis suppression du cache Parquet pour qu'il soit
reconstruit). Lancé chaque nuit par .github/workflows/maj_quotidienne.yml, sur le serveur,
dans le dossier de l'app (lit .streamlit/secrets.toml comme l'app) ; le workflow redémarre
ensuite le service pour vider les caches en mémoire.

    python3 maj_quotidienne.py      # code de sortie 1 si une étape a échoué
"""

import importlib.util
import logging
import os
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("maj_quotidienne")
# Hors serveur Streamlit, chaque st.cache_data / st.session_state émet un avertissement « No
# runtime found » sans conséquence : on les fait taire.
os.environ.setdefault("STREAMLIT_LOGGER_LEVEL", "error")
logging.getLogger("streamlit").setLevel(logging.ERROR)


def main() -> int:
    ici = os.path.dirname(os.path.abspath(__file__))
    os.chdir(ici)
    sys.path.insert(0, ici)
    spec = importlib.util.spec_from_file_location("pfc", os.path.join(ici, "paris_football_club.py"))
    pfc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pfc)

    etapes = [
        ("Drive (fichiers principaux)", pfc.download_google_drive),
        ("GPS", pfc.sync_gps_from_drive_autonomous),
        ("GPS match", pfc.sync_gps_match_from_drive),
        ("Bepro", pfc.sync_bepro_from_drive),
        ("Photos", pfc.sync_photos_from_drive),
    ]
    echecs = []
    for nom, fn in etapes:
        t0 = time.time()
        try:
            res = fn()
            log.info("%s : ok (%.0f s)%s", nom, time.time() - t0, f" → {res}" if res is not None else "")
        except Exception:
            log.exception("%s : échec", nom)
            echecs.append(nom)

    # Avertissements accumulés par les fonctions de sync (_warn) : visibles dans le journal
    try:
        for w in pfc.st.session_state.get("_system_warnings", []):
            log.warning(w)
    except Exception:
        pass

    # Cache Parquet supprimé, comme le bouton : reconstruit au premier chargement
    for p in [pfc.PARQUET_PFC_PATH, pfc.PARQUET_GPS_PATH, pfc.PARQUET_GPSW_PATH,
              pfc.PARQUET_GPSM_PATH, pfc.PARQUET_EDF_PATH, pfc.PARQUET_META_PATH]:
        try:
            if os.path.exists(p):
                os.remove(p)
        except OSError:
            log.exception("Cache %s non supprimé", p)
    # Les sessions ouvertes ne relanceront pas de sync Drive dans les 10 min suivantes
    try:
        pfc._write_bytes_atomic(pfc.SYNC_SIGNAL_PATH, str(time.time()).encode())
    except Exception:
        pass

    if echecs:
        log.error("Mise à jour terminée avec des échecs : %s", ", ".join(echecs))
        return 1
    log.info("Mise à jour terminée.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
