# Installer l'app bêta (Interface Performance v2) sur le VPS

Une fois, en SSH sur le VPS (`ssh root@parisfc-data.fr`). La prod (`/opt/parisfc`, `parisfc.service`) n'est jamais modifiée.

## 1. Base Supabase (une fois)

Dans Supabase → SQL Editor, exécuter `sql/2026-10-09_interface_performance_v2.sql`
(crée `wellness_quotidien`, `rpe_seance`, `seances_contenu` et le bucket privé `seances`).

## 2. Dossier et environnement

```bash
git clone -b beta https://github.com/PFCCDFF/parisfc.git /opt/parisfc-beta
cd /opt/parisfc-beta
python3 -m venv venv && venv/bin/pip install -r requirements.txt
mkdir -p .streamlit && cp /opt/parisfc/.streamlit/secrets.toml .streamlit/
cp -r /opt/parisfc/.streamlit/config.toml .streamlit/ 2>/dev/null || true
rsync -a /opt/parisfc/data/ /opt/parisfc-beta/data/     # copie (pas de lien) : caches Parquet séparés
```

> Si le dépôt est privé, cloner avec la même méthode d'authentification que `/opt/parisfc`
> (`git -C /opt/parisfc remote -v` pour la voir).

## 3. Service systemd

```bash
systemctl cat parisfc            # vérifier ExecStart/User de la prod et ajuster le fichier ci-dessous
cp deploy/parisfc-beta.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now parisfc-beta
systemctl status parisfc-beta --no-pager
```

## 4. Nginx

Ajouter le contenu de `deploy/nginx-cdff-beta.conf` dans le bloc `server` de parisfc-data.fr, puis :

```bash
nginx -t && systemctl reload nginx
```

L'app bêta est alors sur **https://parisfc-data.fr/cdff-beta/** (mêmes identifiants que la prod).

## 5. Mises à jour

Chaque `git push origin beta` redéploie automatiquement la bêta (`.github/workflows/deploy-beta.yml`).
Pour passer la v2 en production plus tard : fusionner `beta` dans `main` (PR), ce qui déclenche le déploiement prod habituel.

## Points d'attention

- Même base Supabase que la prod : présence, objectifs, bien-être, RPE et contenus de séance saisis en bêta sont **réels** et visibles en prod (présence, objectifs).
- Le dossier `data/` est une copie : bouton admin « Mettre à jour la base » pour resynchroniser la bêta depuis Drive.
- Fichiers joints : stockés dans le bucket Supabase `seances` ; à défaut, sur le disque du VPS dans `data/seances_fichiers/` (non sauvegardé par git).
