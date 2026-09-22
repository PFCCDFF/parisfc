# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Vue d'ensemble

App Streamlit pour le centre de formation féminin du Paris FC (suivi GPS, tagging vidéo tactique, médical, présence, recrutement). Tout est en français — noms de variables, textes UI, noms de colonnes, messages de commit — garder le code cohérent avec ça. Le code est concentré dans un seul gros fichier applicatif, avec quelques scripts satellites indépendants qui tournent hors de l'app (cron/systemd) et écrivent dans Supabase.

- `paris_football_club.py` — l'app Streamlit complète (~15 200 lignes, ~236 fonctions) : UI, parsing des exports (GPS, tactique, référentiels Excel), sync Google Drive, génération de rapports (HTML/PNG/Excel).
- `parsing_utils.py` — logique de parsing partagée, utilisée par `sync_drive_to_supabase.py` (copie parallèle de fonctions équivalentes dans `paris_football_club.py`, voir piège #2 ci-dessous).
- `drive_utils.py` — helpers Drive minimalistes utilisés uniquement par `sync_drive_to_supabase.py` (pas par l'app) ; inclut un fetch incrémental via `data/supabase_sync_state.json` (ne retélécharge que les fichiers modifiés depuis le dernier run, via `last_modifiedTime`).
- `sync_drive_to_supabase.py` — job autonome (lancé par un timer systemd sur le VPS), sans lien d'exécution avec l'app Streamlit. Télécharge les nouveaux CSV Drive et upsert dans Supabase.
- `backfill_joueuse_tags.py` — script ponctuel de backfill (voir piège #7).
- `sql/` — migrations SQL appliquées manuellement sur Supabase (pas d'outil de migration).

### Repères dans `paris_football_club.py`

Le fichier n'a pas d'autre structure que des bannières `# ===` — s'appuyer dessus pour naviguer plutôt que de lire de haut en bas. Grandes zones, dans l'ordre :

- **Config & cache Parquet** (`data/_cache_*.parquet` + `_cache_meta.json`) — évite de reparser toutes les sources à chaque session ; invalidé quand une source est plus récente que le cache.
- **Normalisation des noms** (`nettoyer_nom_joueuse`, `map_player_name`, `build_referentiel_players`, fuzzy matching via `difflib`) — une même joueuse est orthographiée différemment selon la source (exports match, GPS, noms de fichiers Drive, formulaires d'évaluation). Le référentiel canonique est l'Excel `Noms Prénoms Paris FC.xlsx` ; `data/gps_name_map.json` et `data/photo_mapping.json` contiennent les correspondances manuelles quand le fuzzy matching échoue — à modifier si les données d'une nouvelle joueuse n'apparaissent pas.
- **Sync Google Drive** (`authenticate_google_drive`, `download_google_drive`, `sync_gps_from_drive_autonomous`, `sync_gps_match_from_drive`, `sync_photos_from_drive`, `sync_logos_from_drive`) — tourne une fois par session (`_run_initial_sync`), puis en tâche de fond (`_bg_sync`) via un thread qui signale sa fin par fichier (`data/_sync_completed_signal.txt`) car il n'a pas de `ScriptRunContext` pour écrire dans `st.session_state`.
- **Calcul des stats/KPI** (`create_data`, `players_*`, `create_kpis`, `create_metrics`, `create_poste`) et radars via `mplsoccer`.
- **Analyse GPS** — parsing export capteur GF1, agrégation de charge hebdomadaire, ACWR, résumés GPS par match.
- **Rapports tactiques/vidéo** — parsing des exports Sportscode (fichiers `PFC_VS_...`) en rapports individuels et collectifs (`compute_tactical_stats`, `compute_collective_report`, `build_collective_report_html`, `build_tactical_report_html`), heatmaps de terrain.
- **Évaluations** — exports Microsoft Forms post-match via `load_evaluations`/`render_evaluation_page`.
- **Auth & permissions** (`load_permissions`, `check_permission`, `get_user_role`) — voir piège #9.
- **`script_streamlit()`** — nav sidebar et routage des pages par rôle.
- **`main()`** — point d'entrée : config page + CSS, login, sync, `collect_data()`, puis `script_streamlit()`.

## Commandes

```bash
# Lancer l'app en local
streamlit run paris_football_club.py

# Installer les dépendances
pip install -r requirements.txt
pip install python-dotenv   # requis par sync_drive_to_supabase.py mais absent de requirements.txt

# Lancer les tests (unittest, pas pytest)
python3 -m unittest discover -s tests -v

# Lancer un seul test
python3 -m unittest tests.test_row_labels.TestRowLabels.test_pfc_est_exclu

# Lancer le sync manuel Drive -> Supabase (nécessite .env, voir piège #6)
python3 sync_drive_to_supabase.py
```

Pas de linter/formatter configuré dans ce repo (pas de `ruff`, etc.) — ne pas en supposer la présence.

## Déploiement

- Remote GitHub : `PFCCDFF/parisfc`. Déploiement automatique via `.github/workflows/deploy.yml` : **tout push sur `main` déclenche une action SSH** qui fait `cd /opt/parisfc && git pull && systemctl restart parisfc` sur le VPS — un push n'est donc pas une étape neutre, c'est un déploiement en prod immédiat.
- Secrets GitHub Actions requis sur le repo : `SSH_HOST`, `SSH_USER`, `SSH_PRIVATE_KEY`.
- Plusieurs clones locaux du même repo existent chez différents contributeurs (même remote `PFCCDFF/parisfc`) — toujours passer par `git pull`/`git push`, ne jamais copier des fichiers d'un clone à l'autre.
- Ne pas confondre avec **paris-fc-charge**, une application différente (autre remote GitHub, autre codebase/architecture views+utils) qui partage juste le même pattern de déploiement.
- Sur le VPS, trois services Streamlit distincts tournent (`parisfc.service`, `apl.service`, `portal.service`) — l'app de ce repo correspond à `parisfc.service` (`/opt/parisfc`, port 8501, servi derrière nginx sur `/cdff/`).

## Architecture — pièges connus

### 1. Deux pipelines de données parallèles et déconnectés

- L'app Streamlit lit les données GPS et tactiques directement depuis des **CSV locaux dans `data/`**, synchronisés depuis Drive au démarrage de session par `_run_initial_sync()` (`download_google_drive()`, `sync_gps_from_drive_autonomous()`, `sync_gps_match_from_drive()`, `sync_photos_from_drive()`).
- `sync_drive_to_supabase.py` est un script **séparé, jamais appelé par l'app**, qui écrit vers Supabase : tables `evenements_match`, `sessions_gps`, `matchs`, `joueuses`, `capteurs_gps`, `zones_vitesse_gps`, `evenement_tags`.
- L'app Streamlit ne lit **jamais** ces tables. Elle interroge seulement `mesures_corporelles`, `objectifs_evaluations`, `objectifs_joueuse`, `presence_entrainement`, `talent_rdv`, `visites_medicales` (fonctionnalités médical/objectifs/présence/recrutement, pas GPS/tactique).
- Conséquence : modifier `evenements_match` ou `sessions_gps` côté Supabase n'a **aucun effet visible** dans l'UI. Les rapports tactiques et GPS affichés viennent uniquement des CSV locaux dans `data/`.
- Trajectoire visée à terme : que `parsing_utils.py` devienne la source unique de parsing pour l'app Streamlit et pour `sync_drive_to_supabase.py`, et que l'app finisse par lire Supabase plutôt que les fichiers locaux. Les deux chemins coexistent pour l'instant (voir piège #2).

### 2. Logique de parsing dupliquée entre les deux fichiers

`is_tactical_file`, `parse_tactical_filename`, `normalize_str`, `nettoyer_nom_joueuse`, etc. existent en **deux copies quasi identiques** — une dans `paris_football_club.py`, une dans `parsing_utils.py`. `paris_football_club.py` n'importe pas `parsing_utils.py` : elles peuvent diverger silencieusement si un correctif n'est appliqué que d'un côté. Avant de corriger un bug de parsing de nom de fichier ou de CSV tactique, vérifier et corriger **les deux implémentations**.

### 3. `data/` n'est jamais commit, et n'a pas de sync Drive automatique complet

- `.gitignore` exclut tout `data/`. Un checkout frais n'a pas ce dossier : l'app le peuple au premier "Mettre à jour la base" (bouton admin) via Drive.
- `DRIVE_TACTICAL_FOLDER_ID` est vide (`paris_football_club.py`, const en tête de fichier) : contrairement au GPS et aux photos, les fichiers tactiques **n'ont pas** de sync Drive automatique intégrée à l'app — ils doivent être présents dans `data/` (ou `data/tactical`) par un autre moyen (dépôt manuel, ou le job `sync_drive_to_supabase.py` qui, lui, télécharge depuis Drive mais écrit vers Supabase, pas vers le dossier local que l'app lit).

### 4. Colonne "Action" optionnelle dans les CSV tactiques

Un match tagué uniquement collectivement (pas de tag individuel par joueuse) n'a pas de colonne `Action` dans son export Sportscode. `load_tactical_files()` l'ajoute artificiellement (`NaN`) pour ne pas exclure le fichier entier — ne pas réintroduire `Action` comme colonne obligatoire de validation.

### 5. Dédoublonnage des fichiers tactiques par (date, journée, adversaire)

`load_tactical_files()` dédoublonne par clé `(date, journee, adv_norm)` et garde le fichier le plus récent (`mtime`). Motif : un renommage de catégorie sur Drive (ex. `U19` → `U19F`) laisse l'ancien fichier orphelin en local sans le supprimer ; sans ce dédoublonnage les stats d'un même match se comptent en double.

### 6. Deux mécanismes de secrets indépendants

- L'app Streamlit lit `st.secrets` (`.streamlit/secrets.toml`, gitignored) : `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, et un bloc `[GOOGLE_SERVICE_ACCOUNT_JSON]` (JSON du compte de service Drive).
- `sync_drive_to_supabase.py` et `backfill_joueuse_tags.py` lisent un **`.env` séparé** (python-dotenv) : `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` (clé `service_role`, jamais la clé `anon`), plus `credentials.json` (fichier de compte de service, chemin via `drive_utils.CREDENTIALS_PATH`) pour l'accès Drive.
- Les deux fichiers doivent être renseignés indépendamment en local ; `get_supabase_client()` (côté app) retourne `None` silencieusement si `st.secrets` n'est pas configuré, plutôt que de lever une exception.

### 7. Pagination PostgREST à 1000 lignes

PostgREST (Supabase) plafonne les réponses à 1000 lignes par défaut. `backfill_joueuse_tags.py` pagine explicitement par blocs de 1000 (`_fetch_evenements_a_backfiller`) — tout nouveau script lisant une table Supabase potentiellement volumineuse doit faire pareil, sinon le reste est silencieusement ignoré.

### 8. Thread-safety matplotlib

`MPL_LOCK` (RLock global, tête de fichier) sérialise tout accès à `pyplot`. Deux sessions Streamlit concurrentes appelant `plt.subplots()`/`plt.close()` en parallèle sans ce lock ont causé des crashs SIGSEGV/ABRT ("malloc(): unsorted double linked list corrupted"). Toute nouvelle fonction de rendu graphique doit passer par le décorateur `@_mpl_safe` ou acquérir `MPL_LOCK` explicitement.

### 9. Permissions par fichier Excel, pas en base

Pas de vrai système d'auth : un fichier `Classeurs permissions streamlit.xlsx` synchronisé depuis Drive mappe `{profil: {password, permissions[], player, role}}`. Le formulaire de login compare le mot de passe en clair à cette feuille. Trois rôles : `ROLE_ADMIN` / `ROLE_STAFF` / `ROLE_JOUEUSE` (ce dernier scopé à une seule joueuse via le champ `player`), ce qui détermine les onglets visibles dans `script_streamlit()`. Le profil "Staff Pro" a un onglet dédié forcé comme page d'accueil à la connexion.

## Tests

- `unittest` (pas pytest). Fichiers `tests/test_*.py`, chaque fichier bricole `sys.path` en tête pour importer les modules racine directement.
- Nécessite un environnement Python avec les dépendances de `requirements.txt` installées (le Python système n'a pas pandas etc. — utiliser un venv).
