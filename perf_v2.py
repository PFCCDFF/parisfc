"""Interface Performance v2 (branche beta) — composants Streamlit.

Appelé depuis `render_performance_page()` de paris_football_club.py, qui passe
son propre module en argument (`app`) : on réutilise ses fonctions (Supabase,
normalisation des noms, rapports collectifs, présence…) sans import circulaire
(l'app tourne en `__main__` sous Streamlit).

Nouvelles tables Supabase : voir sql/2026-10-09_interface_performance_v2.sql
(wellness_quotidien, rpe_seance, seances_contenu + bucket Storage « seances »).
Les calculs purs sont dans perf_v2_data.py (testés hors Streamlit).
"""
from __future__ import annotations

import io
import json
import os
import re
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import perf_v2_data as D

BETA = os.environ.get("PARISFC_BETA", "").strip() in ("1", "true", "yes")

# Charte (cohérente avec le thème sombre de l'app)
C_BLEU = "#1FA8E0"
C_ROUGE = "#BD3032"
C_OR = "#F2A900"
C_VERT = "#2E9E5B"
C_GRIS = "#6A8090"
C_TEXTE = "#C8D8E8"
C_ADV = "#8A9BB0"

LOCAL_FICHIERS_DIR = os.path.join("data", "seances_fichiers")
SYNTHESE_TEMPLATES_PATH = os.path.join("data", "synthese_templates.json")
PAGE = 1000  # pagination PostgREST (piège #7)


# ══════════════════════════════════════════════════════════════════════════
# OUTILS
# ══════════════════════════════════════════════════════════════════════════

def render_beta_banner():
    if not BETA:
        return
    st.markdown(
        "<div style='background:linear-gradient(90deg,#F2A900,#BD3032);color:#08090D;"
        "padding:6px 14px;border-radius:6px;font-weight:700;letter-spacing:.04em;"
        "font-family:Oswald,sans-serif;margin-bottom:10px'>BÊTA · Interface Performance v2 — "
        "même base Supabase que la prod : les saisies (présence, bien-être, RPE, séances) sont réelles.</div>",
        unsafe_allow_html=True,
    )


def _layout(fig: go.Figure, titre: str = "", h: int = 360) -> go.Figure:
    fig.update_layout(
        title=dict(text=titre, font=dict(size=14, color=C_TEXTE)),
        height=h, margin=dict(l=10, r=10, t=40 if titre else 10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=C_TEXTE), legend=dict(orientation="h", y=-0.18),
        xaxis=dict(gridcolor="#1E2D40"), yaxis=dict(gridcolor="#1E2D40"),
    )
    return fig


def _sb(app):
    try:
        return app.get_supabase_client()
    except Exception:
        return None


def _select_all(sb, table: str, cols: str = "*", eq: dict | None = None,
                gte: tuple | None = None, lte: tuple | None = None) -> list:
    """SELECT paginé par blocs de 1000 lignes."""
    rows, start = [], 0
    while True:
        q = sb.table(table).select(cols)
        for k, v in (eq or {}).items():
            q = q.eq(k, v)
        if gte:
            q = q.gte(gte[0], gte[1])
        if lte:
            q = q.lte(lte[0], lte[1])
        res = q.range(start, start + PAGE - 1).execute()
        data = res.data or []
        rows.extend(data)
        if len(data) < PAGE:
            return rows
        start += PAGE


def _iso(d) -> str | None:
    if d is None:
        return None
    return pd.Timestamp(d).date().isoformat()


def roster(app, pfc_kpi_all: pd.DataFrame | None = None) -> list[str]:
    """Effectif : liste Présence (Excel chargé) sinon joueuses des données match."""
    try:
        r = app.load_presence_roster()
    except Exception:
        r = []
    if r:
        return list(r)
    if pfc_kpi_all is not None and not pfc_kpi_all.empty and "Player" in pfc_kpi_all.columns:
        noms = sorted(pfc_kpi_all["Player"].dropna().apply(app.nettoyer_nom_joueuse).unique().tolist())
        try:
            ps = st.session_state.get("_player_settings") or app.load_player_settings()
            noms = app.apply_player_settings(noms, ps)
        except Exception:
            pass
        return noms
    return []


def meilleur_nom(app, nom: str | None, candidats) -> str | None:
    """Associe un nom (ex. GPS) au candidat qui partage le plus de tokens."""
    if not nom:
        return None
    cands = list(candidats)
    if nom in cands:
        return nom
    t = app.nom_tokens(nom)
    best, sc = None, 0
    for c in cands:
        s = len(t & app.nom_tokens(c))
        if s > sc:
            best, sc = c, s
    return best if sc >= 1 else None


def peut_editer(app, role) -> bool:
    return role in (app.ROLE_ADMIN, app.ROLE_STAFF)


# ══════════════════════════════════════════════════════════════════════════
# ACCÈS DONNÉES (Supabase)
# ══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
def load_wellness(_app, deb=None, fin=None) -> pd.DataFrame:
    sb = _sb(_app)
    if sb is None:
        return pd.DataFrame()
    try:
        rows = _select_all(sb, "wellness_quotidien", "*",
                           gte=("date", _iso(deb)) if deb else None,
                           lte=("date", _iso(fin)) if fin else None)
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_rpe(_app, deb=None, fin=None) -> pd.DataFrame:
    sb = _sb(_app)
    if sb is None:
        return pd.DataFrame()
    try:
        rows = _select_all(sb, "rpe_seance", "*",
                           gte=("date", _iso(deb)) if deb else None,
                           lte=("date", _iso(fin)) if fin else None)
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_seances_contenu(_app, deb=None, fin=None) -> pd.DataFrame:
    sb = _sb(_app)
    if sb is None:
        return pd.DataFrame()
    try:
        rows = _select_all(sb, "seances_contenu", "*",
                           gte=("date", _iso(deb)) if deb else None,
                           lte=("date", _iso(fin)) if fin else None)
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_presence_periode(_app, deb=None, fin=None) -> pd.DataFrame:
    sb = _sb(_app)
    if sb is None:
        return pd.DataFrame()
    try:
        rows = _select_all(sb, "presence_entrainement", "joueuse,date,statut",
                           gte=("date", _iso(deb)) if deb else None,
                           lte=("date", _iso(fin)) if fin else None)
        d = pd.DataFrame(rows)
        if not d.empty:
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
        return d
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_evaluations_objectifs(_app, joueuse: str, deb=None, fin=None) -> pd.DataFrame:
    sb = _sb(_app)
    if sb is None or not joueuse:
        return pd.DataFrame()
    try:
        rows = _select_all(sb, "objectifs_evaluations",
                           "objectif_id,evaluateur,note,match_date,match_label",
                           eq={"joueuse": joueuse},
                           gte=("match_date", _iso(deb)) if deb else None,
                           lte=("match_date", _iso(fin)) if fin else None)
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def _clear_caches():
    for f in (load_wellness, load_rpe, load_seances_contenu, load_presence_periode):
        f.clear()


def _upsert(app, table: str, rows: list, on_conflict: str) -> bool:
    sb = _sb(app)
    if sb is None or not rows:
        return False
    try:
        sb.table(table).upsert(rows, on_conflict=on_conflict).execute()
        _clear_caches()
        return True
    except Exception as e:
        st.session_state["_v2_last_error"] = str(e)
        return False


def _delete(app, table: str, eq: dict) -> bool:
    sb = _sb(app)
    if sb is None:
        return False
    try:
        q = sb.table(table).delete()
        for k, v in eq.items():
            q = q.eq(k, v)
        q.execute()
        _clear_caches()
        return True
    except Exception as e:
        st.session_state["_v2_last_error"] = str(e)
        return False


def _erreur_enregistrement():
    err = st.session_state.pop("_v2_last_error", "")
    msg = "Échec de l'enregistrement (Supabase indisponible ou table absente — voir sql/2026-10-09_interface_performance_v2.sql)."
    st.error(msg + (f"\n\nDétail : {err}" if err else ""))


# ── Fichiers joints (Storage Supabase, repli disque local data/) ───────────

def _nom_fichier_sur(nom: str) -> str:
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", nom).strip("._") or "fichier"
    return base[:120]


def upload_fichier(app, jour: str, creneau: str, fichier) -> dict | None:
    contenu = fichier.getvalue()
    nom = _nom_fichier_sur(fichier.name)
    chemin = f"{jour}/{_nom_fichier_sur(creneau)}/{pd.Timestamp.now().strftime('%H%M%S')}_{nom}"
    meta = {"nom": fichier.name, "chemin": chemin,
            "type": fichier.type or "application/octet-stream", "taille": len(contenu)}
    sb = _sb(app)
    if sb is not None:
        try:
            sb.storage.from_("seances").upload(chemin, contenu, {"content-type": meta["type"]})
            return {**meta, "stockage": "supabase"}
        except Exception:
            pass
    try:
        p = os.path.join(LOCAL_FICHIERS_DIR, chemin)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(contenu)
        return {**meta, "stockage": "local"}
    except Exception:
        return None


def lire_fichier(app, meta: dict) -> bytes | None:
    try:
        if meta.get("stockage") == "supabase":
            sb = _sb(app)
            return sb.storage.from_("seances").download(meta["chemin"]) if sb else None
        p = os.path.join(LOCAL_FICHIERS_DIR, meta["chemin"])
        with open(p, "rb") as f:
            return f.read()
    except Exception:
        return None


def supprimer_fichier(app, meta: dict):
    try:
        if meta.get("stockage") == "supabase":
            sb = _sb(app)
            if sb:
                sb.storage.from_("seances").remove([meta["chemin"]])
        else:
            os.remove(os.path.join(LOCAL_FICHIERS_DIR, meta["chemin"]))
    except Exception:
        pass


def _as_list(v) -> list:
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        try:
            x = json.loads(v)
            return x if isinstance(x, list) else []
        except Exception:
            return []
    return []


# ══════════════════════════════════════════════════════════════════════════
# ENTRAÎNEMENT
# ══════════════════════════════════════════════════════════════════════════

def render_liste_seances(app, gps_raw_df: pd.DataFrame, saison_bornes=None):
    """Toutes les séances : union des jours GPS, présence saisie et contenu renseigné."""
    st.markdown("#### 📋 Toutes les séances")
    gr = app.ensure_date_column(gps_raw_df) if gps_raw_df is not None and not gps_raw_df.empty else pd.DataFrame()
    deb, fin = saison_bornes or (None, None)

    lignes = {}
    if not gr.empty and "DATE" in gr.columns:
        g = gr[gr["DATE"].notna()].copy()
        g["_j"] = g["DATE"].dt.normalize()
        for j, gg in g.groupby("_j"):
            dist = pd.to_numeric(gg.get("Distance (m)"), errors="coerce") if "Distance (m)" in gg else pd.Series(dtype=float)
            hid = pd.to_numeric(gg.get("Distance HID (>19 km/h)"), errors="coerce") if "Distance HID (>19 km/h)" in gg else pd.Series(dtype=float)
            dur = pd.to_numeric(gg.get("Durée_min"), errors="coerce") if "Durée_min" in gg else pd.Series(dtype=float)
            lignes[j] = {
                "Joueuses GPS": int(gg["Player"].nunique()),
                "Durée moy. (min)": round(float(dur.mean()), 0) if dur.notna().any() else np.nan,
                "Distance moy. (m)": round(float(dist.mean()), 0) if dist.notna().any() else np.nan,
                "HID >19 moy. (m)": round(float(hid.mean()), 0) if hid.notna().any() else np.nan,
            }

    pres = load_presence_periode(app, deb, fin)
    if not pres.empty:
        for j, gp in pres.groupby(pres["date"].dt.normalize()):
            l = lignes.setdefault(j, {})
            n_pres = int(gp["statut"].isin(app.PRESENCE_STATUTS_PRESENTE).sum())
            l["Présence"] = f"{n_pres}/{len(gp)}"

    cont = load_seances_contenu(app, deb, fin)
    if not cont.empty:
        cont["date"] = pd.to_datetime(cont["date"], errors="coerce").dt.normalize()
        for j, gc in cont.groupby("date"):
            l = lignes.setdefault(j, {})
            l["Microcycle"] = " / ".join(x for x in gc["microcycle"].dropna().astype(str) if x)
            l["Thème"] = " / ".join(x for x in gc["theme"].dropna().astype(str) if x)
            l["Exercices"] = int(sum(len(_as_list(e)) for e in gc["exercices"]))
            l["Fichiers"] = int(sum(len(_as_list(f)) for f in gc["fichiers"]))

    if not lignes:
        st.info("Aucune séance trouvée (ni GPS, ni présence, ni contenu saisi).")
        return
    df = pd.DataFrame.from_dict(lignes, orient="index")
    df.index.name = "Date"
    df = df.reset_index().sort_values("Date", ascending=False)
    if deb:
        df = df[df["Date"] >= pd.Timestamp(deb)]
    if fin:
        df = df[df["Date"] <= pd.Timestamp(fin)]
    jours = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
    df.insert(1, "Jour", df["Date"].dt.weekday.map(lambda i: jours[i]))
    cols = ["Date", "Jour", "Microcycle", "Thème", "Présence", "Joueuses GPS", "Durée moy. (min)",
            "Distance moy. (m)", "HID >19 moy. (m)", "Exercices", "Fichiers"]
    df = df[[c for c in cols if c in df.columns]]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Séances", len(df))
    c2.metric("Avec GPS", int(df["Joueuses GPS"].notna().sum()) if "Joueuses GPS" in df else 0)
    c3.metric("Avec présence", int(df["Présence"].notna().sum()) if "Présence" in df else 0)
    c4.metric("Avec contenu", int(df["Thème"].fillna("").astype(bool).sum()) if "Thème" in df else 0)

    ev = st.dataframe(
        df, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
        key="v2_liste_seances",
        column_config={"Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY")},
    )
    try:
        sel = ev.selection.rows
    except Exception:
        sel = []
    if sel:
        j = df.iloc[sel[0]]["Date"].date()
        st.session_state["_v2_seance_date"] = j
        st.success(f"Séance du {j.strftime('%d/%m/%Y')} sélectionnée — ouvre l'onglet **📝 Contenu de séance** pour la renseigner.")
    st.caption("Sélectionne une ligne pour ouvrir la séance dans « 📝 Contenu de séance ».")


def render_contenu_seance(app, user_profile: str, role, gps_raw_df: pd.DataFrame | None = None):
    st.markdown("#### 📝 Contenu de séance")
    edit = peut_editer(app, role)
    c1, c2 = st.columns([1, 1])
    with c1:
        jour = st.date_input("Date de la séance",
                             value=st.session_state.get("_v2_seance_date") or date.today(),
                             key="v2_contenu_date", format="DD/MM/YYYY")
    with c2:
        creneau = st.selectbox("Créneau", D.CRENEAUX[:3], key="v2_contenu_creneau")
    jour_iso = jour.isoformat()

    cont = load_seances_contenu(app, jour, jour)
    exist = {}
    if not cont.empty:
        m = cont[cont["creneau"] == creneau]
        if not m.empty:
            exist = m.iloc[0].to_dict()
    exercices = _as_list(exist.get("exercices"))
    fichiers = _as_list(exist.get("fichiers"))

    # Résumé GPS de la journée (lecture seule)
    if gps_raw_df is not None and not gps_raw_df.empty:
        gr = app.ensure_date_column(gps_raw_df)
        gj = gr[gr["DATE"].dt.normalize() == pd.Timestamp(jour)] if "DATE" in gr.columns else pd.DataFrame()
        if not gj.empty:
            txt = f"🛰️ GPS : {gj['Player'].nunique()} joueuse(s)"
            if "Distance (m)" in gj.columns:
                dist = pd.to_numeric(gj["Distance (m)"], errors="coerce")
                if dist.notna().any():
                    txt += f" · distance moyenne {dist.mean():.0f} m"
            st.caption(txt)

    if not edit:
        if not exist:
            st.info("Aucun contenu renseigné pour cette séance.")
            return
        st.markdown(f"**Microcycle :** {exist.get('microcycle') or '—'} · **Thème :** {exist.get('theme') or '—'}")
        if exist.get("objectifs"):
            st.markdown(f"**Objectifs :** {exist['objectifs']}")
        if exercices:
            st.dataframe(pd.DataFrame(exercices), hide_index=True, width="stretch")
        if exist.get("notes"):
            st.markdown(f"**Notes :** {exist['notes']}")
        _render_fichiers(app, fichiers, jour_iso, creneau, editable=False)
        return

    k = f"{jour_iso}_{creneau}"
    with st.form(f"v2_form_contenu_{k}"):
        f1, f2, f3 = st.columns([1, 2, 1])
        mc_opts = [""] + D.MICROCYCLE_JOURS
        mc_val = exist.get("microcycle") or ""
        microcycle = f1.selectbox("Jour du microcycle", mc_opts,
                                  index=mc_opts.index(mc_val) if mc_val in mc_opts else 0,
                                  format_func=lambda v: v or "—")
        theme = f2.text_input("Thème de séance", value=exist.get("theme") or "",
                              placeholder="ex. Conservation sous pression, finition, transitions…")
        duree = f3.number_input("Durée totale (min)", min_value=0, max_value=300, step=5,
                                value=int(float(exist.get("duree_min") or 90)))
        objectifs = st.text_area("Objectifs de la séance", value=exist.get("objectifs") or "", height=80)

        st.markdown("**Exercices**")
        ex_df = pd.DataFrame(exercices) if exercices else pd.DataFrame(
            columns=["nom", "duree_min", "format", "intensite", "consignes"])
        for c in ["nom", "duree_min", "format", "intensite", "consignes"]:
            if c not in ex_df.columns:
                ex_df[c] = None
        ex_df = ex_df[["nom", "duree_min", "format", "intensite", "consignes"]]
        ex_edit = st.data_editor(
            ex_df, num_rows="dynamic", width="stretch", hide_index=True, key=f"v2_ex_{k}",
            column_config={
                "nom": st.column_config.TextColumn("Exercice", required=True),
                "duree_min": st.column_config.NumberColumn("Durée (min)", min_value=0, max_value=180, step=1),
                "format": st.column_config.TextColumn("Format / espace", help="ex. 8v8 + GB sur 1/2 terrain"),
                "intensite": st.column_config.SelectboxColumn("Intensité visée",
                                                              options=["Basse", "Modérée", "Haute", "Maximale"]),
                "consignes": st.column_config.TextColumn("Consignes / critères de réussite"),
            },
        )
        notes = st.text_area("Notes libres du staff", value=exist.get("notes") or "", height=100)
        nouveaux = st.file_uploader("Joindre des fichiers (schémas, PDF de séance, vidéos courtes…)",
                                    accept_multiple_files=True, key=f"v2_up_{k}")
        ok = st.form_submit_button("💾 Enregistrer la séance", type="primary")

    if ok:
        ex_rows = []
        for r in ex_edit.to_dict("records"):
            if not str(r.get("nom") or "").strip():
                continue
            ex_rows.append({kk: (None if (isinstance(v, float) and np.isnan(v)) else v) for kk, v in r.items()})
        tous_fichiers = list(fichiers)
        for f in nouveaux or []:
            meta = upload_fichier(app, jour_iso, creneau, f)
            if meta:
                tous_fichiers.append(meta)
            else:
                st.warning(f"Fichier non enregistré : {f.name}")
        row = {
            "date": jour_iso, "creneau": creneau, "equipe": "",
            "microcycle": microcycle or None, "theme": theme.strip() or None,
            "objectifs": objectifs.strip() or None, "duree_min": duree,
            "exercices": ex_rows, "notes": notes.strip() or None, "fichiers": tous_fichiers,
            "saisi_par": user_profile, "updated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        if _upsert(app, "seances_contenu", [row], "date,creneau,equipe"):
            st.success("Séance enregistrée.")
            st.rerun()
        else:
            _erreur_enregistrement()

    _render_fichiers(app, fichiers, jour_iso, creneau, editable=True, existant=exist)


def _render_fichiers(app, fichiers: list, jour_iso: str, creneau: str, editable: bool, existant: dict | None = None):
    if not fichiers:
        return
    st.markdown("**📎 Fichiers joints**")
    for i, meta in enumerate(fichiers):
        c1, c2, c3 = st.columns([4, 1, 1])
        c1.markdown(f"{meta.get('nom')} · {round((meta.get('taille') or 0) / 1024)} Ko")
        data = lire_fichier(app, meta)
        if data:
            c2.download_button("⬇️", data=data, file_name=meta.get("nom") or "fichier",
                               mime=meta.get("type"), key=f"v2_dl_{jour_iso}_{creneau}_{i}")
        else:
            c2.caption("indisponible")
        if editable and existant and c3.button("🗑️", key=f"v2_rm_{jour_iso}_{creneau}_{i}"):
            supprimer_fichier(app, meta)
            restants = [m for j, m in enumerate(fichiers) if j != i]
            row = {kk: existant.get(kk) for kk in ["date", "creneau", "equipe", "microcycle", "theme",
                                                   "objectifs", "duree_min", "exercices", "notes", "saisi_par"]}
            row["exercices"] = _as_list(row.get("exercices"))
            row["fichiers"] = restants
            if _upsert(app, "seances_contenu", [row], "date,creneau,equipe"):
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# MONITORING — BIEN-ÊTRE
# ══════════════════════════════════════════════════════════════════════════

_ITEM_AIDE = {
    "sommeil_qualite": "1 = nuit très mauvaise · 5 = excellente nuit",
    "fatigue":         "1 = épuisée · 5 = très fraîche",
    "courbatures":     "1 = très courbaturée · 5 = aucune courbature",
    "stress":          "1 = très stressée · 5 = très détendue",
    "humeur":          "1 = très mauvaise humeur · 5 = excellente humeur",
}


def _style_score(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if v <= 2:
        return f"background-color:{C_ROUGE};color:white"
    if v < 3:
        return f"background-color:{C_OR};color:#08090D"
    if v >= 4:
        return f"background-color:{C_VERT};color:white"
    return ""


def _style_z(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if v <= D.WELLNESS_SEUIL_Z:
        return f"background-color:{C_ROUGE};color:white"
    if v <= -0.5:
        return f"background-color:{C_OR};color:#08090D"
    return ""


def render_bien_etre(app, role, joueuse_defaut: str | None, user_profile: str, effectif: list):
    est_joueuse = role == app.ROLE_JOUEUSE
    onglets = ["📝 Saisie", "👤 Suivi individuel"] if est_joueuse else ["📝 Saisie", "👥 Équipe du jour", "👤 Suivi individuel"]
    tabs = st.tabs(onglets)
    with tabs[0]:
        _bien_etre_saisie(app, est_joueuse, joueuse_defaut, user_profile, effectif)
    if not est_joueuse:
        with tabs[1]:
            _bien_etre_equipe(app, effectif)
    with tabs[-1]:
        _bien_etre_individuel(app, est_joueuse, joueuse_defaut, effectif)


def _bien_etre_saisie(app, est_joueuse, joueuse_defaut, user_profile, effectif):
    jour = st.date_input("Date", value=date.today(), key="v2_we_date", format="DD/MM/YYYY",
                         max_value=date.today())
    exist = load_wellness(app, jour, jour)
    exist_map = {r["joueuse"]: r for r in exist.to_dict("records")} if not exist.empty else {}

    if est_joueuse:
        if not joueuse_defaut:
            st.info("Profil joueuse non associé à une joueuse.")
            return
        e = exist_map.get(joueuse_defaut, {})
        st.caption("Réponds avant la séance, en pensant à comment tu te sens **ce matin**.")
        with st.form("v2_we_form_joueuse"):
            vals = {}
            for c, lbl in D.WELLNESS_ITEMS.items():
                vals[c] = st.select_slider(lbl, options=[1, 2, 3, 4, 5],
                                           value=int(e.get(c) or 3), help=_ITEM_AIDE[c],
                                           format_func=lambda v: D.WELLNESS_ECHELLE[v])
            h = st.number_input("Heures de sommeil", 0.0, 14.0, float(e.get("sommeil_heures") or 8.0), 0.5)
            zone = st.text_input("Douleur / gêne (zone, facultatif)", value=e.get("douleur_zone") or "")
            com = st.text_area("Commentaire (facultatif)", value=e.get("commentaire") or "", height=70)
            ok = st.form_submit_button("💾 Envoyer", type="primary")
        if ok:
            row = {"joueuse": joueuse_defaut, "date": jour.isoformat(), **vals, "sommeil_heures": h,
                   "douleur_zone": zone.strip() or None, "commentaire": com.strip() or None,
                   "saisi_par": user_profile, "updated_at": pd.Timestamp.now(tz="UTC").isoformat()}
            if _upsert(app, "wellness_quotidien", [row], "joueuse,date"):
                st.success("Merci, réponse enregistrée.")
            else:
                _erreur_enregistrement()
        return

    if not effectif:
        st.info("Aucun effectif : charge la liste des joueuses dans Entraînement → Présence.")
        return
    st.caption("Saisie staff : une ligne par joueuse (échelle 1 = mauvais → 5 = très bon). "
               "Laisse vide une joueuse qui n'a pas répondu.")
    base = []
    for p in effectif:
        e = exist_map.get(p, {})
        base.append({"Joueuse": p, **{c: e.get(c) for c in D.WELLNESS_ITEMS},
                     "sommeil_heures": e.get("sommeil_heures"), "douleur_zone": e.get("douleur_zone"),
                     "commentaire": e.get("commentaire")})
    df = pd.DataFrame(base)
    cfg = {"Joueuse": st.column_config.TextColumn("Joueuse", disabled=True)}
    for c, lbl in D.WELLNESS_ITEMS.items():
        cfg[c] = st.column_config.NumberColumn(lbl, min_value=1, max_value=5, step=1, help=_ITEM_AIDE[c])
    cfg["sommeil_heures"] = st.column_config.NumberColumn("Sommeil (h)", min_value=0, max_value=14, step=0.5)
    cfg["douleur_zone"] = st.column_config.TextColumn("Douleur (zone)")
    cfg["commentaire"] = st.column_config.TextColumn("Commentaire")
    ed = st.data_editor(df, hide_index=True, width="stretch", column_config=cfg,
                        key=f"v2_we_editor_{jour.isoformat()}")
    if st.button("💾 Enregistrer le bien-être du jour", type="primary", key="v2_we_save"):
        rows = []
        for r in ed.to_dict("records"):
            items = {c: r.get(c) for c in D.WELLNESS_ITEMS}
            if all(v is None or (isinstance(v, float) and np.isnan(v)) for v in items.values()):
                continue
            rows.append({
                "joueuse": r["Joueuse"], "date": jour.isoformat(),
                **{c: (int(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else None)
                   for c, v in items.items()},
                "sommeil_heures": None if r.get("sommeil_heures") is None or pd.isna(r.get("sommeil_heures")) else float(r["sommeil_heures"]),
                "douleur_zone": (r.get("douleur_zone") or None), "commentaire": (r.get("commentaire") or None),
                "saisi_par": user_profile, "updated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            })
        if not rows:
            st.warning("Aucune ligne renseignée.")
        elif _upsert(app, "wellness_quotidien", rows, "joueuse,date"):
            st.success(f"{len(rows)} réponse(s) enregistrée(s).")
        else:
            _erreur_enregistrement()


def _bien_etre_equipe(app, effectif):
    jour = st.date_input("Jour", value=date.today(), key="v2_we_eq_date", format="DD/MM/YYYY")
    hist = load_wellness(app, jour - timedelta(days=42), jour)
    tab = D.wellness_equipe_jour(hist, jour, effectif)
    if tab.empty or "score_total" not in tab.columns or tab["score_total"].notna().sum() == 0:
        st.info("Aucune réponse de bien-être pour ce jour.")
        return
    rep = tab["score_total"].notna().sum()
    alertes = tab[(tab["alertes"].fillna("") != "") & (tab["alertes"] != "Pas de réponse")]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Réponses", f"{rep}/{len(tab)}")
    c2.metric("Score moyen", f"{tab['score_total'].mean():.1f} / {D.WELLNESS_MAX}")
    c3.metric("Joueuses en alerte", len(alertes))
    c4.metric("Sommeil moyen", f"{pd.to_numeric(tab.get('sommeil_heures'), errors='coerce').mean():.1f} h"
              if "sommeil_heures" in tab else "—")

    if not alertes.empty:
        st.markdown("**⚠️ À surveiller**")
        for _, r in alertes.iterrows():
            st.markdown(f"- **{r['joueuse']}** — {r['alertes']}")

    cols = ["joueuse", *D.WELLNESS_ITEMS.keys(), "sommeil_heures", "score_total", "z_score", "alertes"]
    show = tab[[c for c in cols if c in tab.columns]].rename(
        columns={"joueuse": "Joueuse", **D.WELLNESS_ITEMS, "sommeil_heures": "Sommeil (h)",
                 "score_total": f"Score (/{D.WELLNESS_MAX})", "z_score": "z (vs 28 j)", "alertes": "Alertes"})
    sty = show.style.map(_style_score, subset=[v for v in D.WELLNESS_ITEMS.values() if v in show.columns]) \
                    .map(_style_z, subset=["z (vs 28 j)"] if "z (vs 28 j)" in show.columns else []) \
                    .format(precision=1, na_rep="—")
    st.dataframe(sty, hide_index=True, width="stretch")
    st.caption("z = écart du score du jour à la moyenne individuelle des 28 jours précédents (en écarts-types) ; "
               f"alerte si z ≤ {D.WELLNESS_SEUIL_Z:.0f}, item ≤ {D.WELLNESS_SEUIL_ITEM}/5, sommeil < 6 h ou douleur signalée.")

    # Évolution équipe sur 6 semaines
    sc = D.wellness_scores(hist)
    if not sc.empty:
        moy = sc.groupby("date")["score_total"].mean().reset_index()
        fig = go.Figure(go.Scatter(x=moy["date"], y=moy["score_total"], mode="lines+markers",
                                   line=dict(color=C_BLEU, width=2), name="Score moyen équipe"))
        fig.update_yaxes(range=[5, 25])
        st.plotly_chart(_layout(fig, "Score de bien-être moyen de l'équipe (6 semaines)", 280), width="stretch")


def _bien_etre_individuel(app, est_joueuse, joueuse_defaut, effectif):
    if est_joueuse:
        j = joueuse_defaut
    else:
        opts = effectif or []
        if not opts:
            st.info("Aucun effectif.")
            return
        idx = opts.index(joueuse_defaut) if joueuse_defaut in opts else 0
        j = st.selectbox("Joueuse", opts, index=idx, key="v2_we_ind_j")
    c1, c2 = st.columns(2)
    deb = c1.date_input("Du", value=date.today() - timedelta(days=56), key="v2_we_ind_deb", format="DD/MM/YYYY")
    fin = c2.date_input("Au", value=date.today(), key="v2_we_ind_fin", format="DD/MM/YYYY")
    hist = load_wellness(app, deb - timedelta(days=30), fin)
    if hist.empty:
        st.info("Aucune donnée de bien-être.")
        return
    dz = D.wellness_zscores(hist[hist["joueuse"] == j])
    dz = D.filtre_periode(dz, "date", deb, fin)
    if dz.empty:
        st.info(f"Aucune réponse de {j} sur la période.")
        return
    c1, c2, c3 = st.columns(3)
    c1.metric("Réponses", len(dz))
    c2.metric("Score moyen", f"{dz['score_total'].mean():.1f} / {D.WELLNESS_MAX}")
    c3.metric("Jours en alerte", int(dz.apply(lambda r: bool(D.wellness_alertes(r)), axis=1).sum()))

    fig = go.Figure()
    if dz["ref_moyenne"].notna().any():
        hi = dz["ref_moyenne"] + dz["ref_ecart_type"].fillna(0)
        lo = dz["ref_moyenne"] - dz["ref_ecart_type"].fillna(0)
        fig.add_trace(go.Scatter(x=dz["date"], y=hi, line=dict(width=0), showlegend=False, hoverinfo="skip"))
        fig.add_trace(go.Scatter(x=dz["date"], y=lo, line=dict(width=0), fill="tonexty",
                                 fillcolor="rgba(31,168,224,0.15)", name="Référence ±1 ET (28 j)", hoverinfo="skip"))
    colors = [C_ROUGE if (pd.notna(z) and z <= D.WELLNESS_SEUIL_Z) else C_BLEU for z in dz["z_score"]]
    fig.add_trace(go.Scatter(x=dz["date"], y=dz["score_total"], mode="lines+markers", name="Score du jour",
                             line=dict(color=C_BLEU, width=2), marker=dict(color=colors, size=8)))
    fig.update_yaxes(range=[5, 25])
    st.plotly_chart(_layout(fig, f"Bien-être — {j}"), width="stretch")

    items = [c for c in D.WELLNESS_ITEMS if c in dz.columns]
    z = dz[items].T.values.astype(float)
    hm = go.Figure(go.Heatmap(z=z, x=dz["date"].dt.strftime("%d/%m"), y=[D.WELLNESS_ITEMS[c] for c in items],
                              zmin=1, zmax=5, colorscale=[[0, C_ROUGE], [0.5, C_OR], [1, C_VERT]],
                              colorbar=dict(title="1→5")))
    st.plotly_chart(_layout(hm, "Détail par item", 260), width="stretch")
    with st.expander("Voir les réponses"):
        _cols = [c for c in ["date", *items, "sommeil_heures", "score_total", "z_score", "douleur_zone", "commentaire"]
                 if c in dz.columns]
        show = dz[_cols] \
            .rename(columns={"date": "Date", **D.WELLNESS_ITEMS, "sommeil_heures": "Sommeil (h)",
                             "score_total": "Score", "z_score": "z", "douleur_zone": "Douleur", "commentaire": "Commentaire"})
        st.dataframe(show.sort_values("Date", ascending=False), hide_index=True, width="stretch",
                     column_config={"Date": st.column_config.DateColumn(format="DD/MM/YYYY")})


# ══════════════════════════════════════════════════════════════════════════
# MONITORING — RPE & CHARGE INTERNE
# ══════════════════════════════════════════════════════════════════════════

def render_rpe(app, role, joueuse_defaut: str | None, user_profile: str, effectif: list):
    est_joueuse = role == app.ROLE_JOUEUSE
    onglets = ["📝 Saisie", "👤 Suivi individuel"] if est_joueuse else ["📝 Saisie", "👥 Équipe", "👤 Suivi individuel"]
    tabs = st.tabs(onglets)
    with tabs[0]:
        _rpe_saisie(app, est_joueuse, joueuse_defaut, user_profile, effectif)
    if not est_joueuse:
        with tabs[1]:
            _rpe_equipe(app, effectif)
    with tabs[-1]:
        _rpe_individuel(app, est_joueuse, joueuse_defaut, effectif)


def _rpe_saisie(app, est_joueuse, joueuse_defaut, user_profile, effectif):
    c1, c2, c3 = st.columns(3)
    jour = c1.date_input("Date", value=date.today(), key="v2_rpe_date", format="DD/MM/YYYY", max_value=date.today())
    creneau = c2.selectbox("Créneau", D.CRENEAUX, key="v2_rpe_creneau")
    type_s = c3.selectbox("Type de séance", D.TYPES_SEANCE,
                          index=1 if creneau == "Match" else 0, key="v2_rpe_type")
    cont = load_seances_contenu(app, jour, jour)
    duree_def = 90.0
    if not cont.empty:
        m = cont[cont["creneau"] == creneau]
        if not m.empty and pd.notna(m.iloc[0].get("duree_min")):
            duree_def = float(m.iloc[0]["duree_min"])
    exist = load_rpe(app, jour, jour)
    exist = exist[exist["creneau"] == creneau] if not exist.empty else exist
    emap = {r["joueuse"]: r for r in exist.to_dict("records")} if not exist.empty else {}

    if est_joueuse:
        if not joueuse_defaut:
            st.info("Profil joueuse non associé à une joueuse.")
            return
        e = emap.get(joueuse_defaut, {})
        st.caption("À remplir ~30 min après la séance : « Comment as-tu trouvé la séance dans son ensemble ? »")
        with st.form("v2_rpe_form_joueuse"):
            rpe = st.select_slider("RPE (CR-10)", options=list(range(11)), value=int(float(e.get("rpe") or 5)),
                                   format_func=lambda v: D.RPE_ECHELLE_CR10[v])
            duree = st.number_input("Durée de la séance (min)", 0, 300, int(float(e.get("duree_min") or duree_def)), 5)
            com = st.text_input("Commentaire (facultatif)", value=e.get("commentaire") or "")
            ok = st.form_submit_button("💾 Envoyer", type="primary")
        if ok:
            row = {"joueuse": joueuse_defaut, "date": jour.isoformat(), "creneau": creneau, "type_seance": type_s,
                   "rpe": rpe, "duree_min": duree, "commentaire": com.strip() or None,
                   "saisi_par": user_profile, "updated_at": pd.Timestamp.now(tz="UTC").isoformat()}
            if _upsert(app, "rpe_seance", [row], "joueuse,date,creneau"):
                st.success(f"RPE enregistrée — charge de séance : {rpe * duree:.0f} UA.")
            else:
                _erreur_enregistrement()
        return

    if not effectif:
        st.info("Aucun effectif : charge la liste des joueuses dans Entraînement → Présence.")
        return
    # Pré-remplissage : présentes du jour si la présence est saisie.
    try:
        pres = app.load_presence_for_date(jour.isoformat())
    except Exception:
        pres = {}
    base = []
    for p in effectif:
        e = emap.get(p, {})
        statut = pres.get(p, "")
        base.append({"Joueuse": p, "Présence": statut, "rpe": e.get("rpe"),
                     "duree_min": e.get("duree_min") if e else (duree_def if statut in ("", *app.PRESENCE_STATUTS_PRESENTE) else None),
                     "commentaire": e.get("commentaire")})
    df = pd.DataFrame(base)
    st.caption(f"Durée par défaut : {duree_def:.0f} min (depuis le contenu de séance si renseigné). "
               "Laisse la RPE vide pour une joueuse absente.")
    ed = st.data_editor(df, hide_index=True, width="stretch", key=f"v2_rpe_ed_{jour.isoformat()}_{creneau}",
                        column_config={
                            "Joueuse": st.column_config.TextColumn(disabled=True),
                            "Présence": st.column_config.TextColumn(disabled=True),
                            "rpe": st.column_config.NumberColumn("RPE (0-10)", min_value=0, max_value=10, step=0.5),
                            "duree_min": st.column_config.NumberColumn("Durée (min)", min_value=0, max_value=300, step=5),
                            "commentaire": st.column_config.TextColumn("Commentaire"),
                        })
    if st.button("💾 Enregistrer les RPE", type="primary", key="v2_rpe_save"):
        rows = []
        for r in ed.to_dict("records"):
            if r.get("rpe") is None or pd.isna(r.get("rpe")):
                continue
            rows.append({"joueuse": r["Joueuse"], "date": jour.isoformat(), "creneau": creneau,
                         "type_seance": type_s, "rpe": float(r["rpe"]),
                         "duree_min": float(r["duree_min"]) if pd.notna(r.get("duree_min")) else duree_def,
                         "commentaire": r.get("commentaire") or None, "saisi_par": user_profile,
                         "updated_at": pd.Timestamp.now(tz="UTC").isoformat()})
        if not rows:
            st.warning("Aucune RPE renseignée.")
        elif _upsert(app, "rpe_seance", rows, "joueuse,date,creneau"):
            st.success(f"{len(rows)} RPE enregistrée(s).")
        else:
            _erreur_enregistrement()


def _rpe_equipe(app, effectif):
    nb_sem = st.slider("Semaines affichées", 4, 20, 8, key="v2_rpe_eq_sem")
    fin = date.today()
    deb = fin - timedelta(days=7 * nb_sem + 35)
    df = load_rpe(app, deb, fin)
    if df.empty:
        st.info("Aucune RPE saisie.")
        return
    hebdo = D.charge_hebdo(df)
    hebdo = hebdo[hebdo["semaine"] >= pd.Timestamp(fin - timedelta(days=7 * nb_sem))]
    if hebdo.empty:
        st.info("Aucune RPE sur la période.")
        return
    piv = hebdo.pivot_table(index="joueuse", columns="semaine", values="charge_ua", aggfunc="sum")
    piv.columns = [c.strftime("S%V · %d/%m") for c in piv.columns]
    # ACWR du jour par joueuse
    acwr = {}
    hist90 = load_rpe(app, fin - timedelta(days=90), fin)
    for p in piv.index:
        a = D.acwr_ewma(hist90, p)
        acwr[p] = a["acwr"].dropna().iloc[-1] if not a.empty and a["acwr"].notna().any() else np.nan
    piv["ACWR actuel"] = pd.Series(acwr)
    piv = piv.reset_index().rename(columns={"joueuse": "Joueuse"})

    def _sty_acwr(v):
        lbl, col = D.zone_acwr(v)
        return f"background-color:{col};color:white" if lbl != "—" else ""
    num = [c for c in piv.columns if c not in ("Joueuse", "ACWR actuel")]
    sty = piv.style.background_gradient(cmap="Blues", subset=num).map(_sty_acwr, subset=["ACWR actuel"]) \
                   .format(precision=0, na_rep="—").format({"ACWR actuel": "{:.2f}"}, na_rep="—")
    st.markdown("**Charge interne hebdomadaire (UA = RPE × min)**")
    st.dataframe(sty, hide_index=True, width="stretch")
    moy = hebdo.groupby("semaine")[["charge_ua", "monotonie"]].mean().reset_index()
    fig = go.Figure(go.Bar(x=moy["semaine"], y=moy["charge_ua"], marker_color=C_BLEU, name="Charge moyenne / joueuse"))
    fig.add_trace(go.Scatter(x=moy["semaine"], y=moy["monotonie"], yaxis="y2", mode="lines+markers",
                             line=dict(color=C_OR), name="Monotonie moyenne"))
    fig.update_layout(yaxis2=dict(overlaying="y", side="right", title="Monotonie", showgrid=False))
    st.plotly_chart(_layout(fig, "Charge hebdomadaire moyenne de l'équipe"), width="stretch")
    st.caption("Zones ACWR (EWMA 7/28 j) : < 0,8 sous-charge · 0,8–1,3 optimale · 1,3–1,5 vigilance · > 1,5 risque. "
               "Monotonie > 2 = semaine peu variée (Foster).")


def _rpe_individuel(app, est_joueuse, joueuse_defaut, effectif):
    if est_joueuse:
        j = joueuse_defaut
    else:
        if not effectif:
            st.info("Aucun effectif.")
            return
        j = st.selectbox("Joueuse", effectif,
                         index=effectif.index(joueuse_defaut) if joueuse_defaut in effectif else 0, key="v2_rpe_ind_j")
    fin = date.today()
    df = load_rpe(app, fin - timedelta(days=180), fin)
    df = df[df["joueuse"] == j] if not df.empty else df
    if df.empty:
        st.info(f"Aucune RPE pour {j}.")
        return
    a = D.acwr_ewma(df, j)
    vue = st.radio("Fenêtre", ["4 semaines", "8 semaines", "6 mois"], index=1, horizontal=True, key="v2_rpe_ind_win")
    jours = {"4 semaines": 28, "8 semaines": 56, "6 mois": 180}[vue]
    a = a[a["date"] >= pd.Timestamp(fin - timedelta(days=jours))]
    last = a["acwr"].dropna().iloc[-1] if a["acwr"].notna().any() else np.nan
    lbl, col = D.zone_acwr(last)
    hebdo = D.charge_hebdo(df, j)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ACWR (EWMA)", f"{last:.2f}" if pd.notna(last) else "—", lbl)
    c2.metric("Charge semaine en cours", f"{hebdo['charge_ua'].iloc[-1]:.0f} UA" if not hebdo.empty else "—")
    c3.metric("Monotonie", f"{hebdo['monotonie'].iloc[-1]:.2f}" if not hebdo.empty and pd.notna(hebdo['monotonie'].iloc[-1]) else "—")
    c4.metric("RPE moyenne (4 sem.)", f"{D.filtre_periode(D.prepare_rpe(df), 'date', fin - timedelta(days=27), fin)['rpe'].mean():.1f}")

    fig = go.Figure()
    fig.add_trace(go.Bar(x=a["date"], y=a["charge_ua"], marker_color=C_BLEU, name="Charge du jour (UA)", opacity=0.7))
    fig.add_trace(go.Scatter(x=a["date"], y=a["aigue"], line=dict(color=C_OR, width=2), name="Aiguë (EWMA 7 j)"))
    fig.add_trace(go.Scatter(x=a["date"], y=a["chronique"], line=dict(color=C_TEXTE, width=2, dash="dot"), name="Chronique (EWMA 28 j)"))
    st.plotly_chart(_layout(fig, f"Charge interne — {j}"), width="stretch")

    fa = go.Figure()
    for lo, hi, zl, zc in D.ACWR_ZONES:
        fa.add_hrect(y0=lo, y1=min(hi, 2.5), fillcolor=zc, opacity=0.12, line_width=0)
    fa.add_trace(go.Scatter(x=a["date"], y=a["acwr"], mode="lines", line=dict(color="white", width=2), name="ACWR"))
    fa.update_yaxes(range=[0, 2.2])
    st.plotly_chart(_layout(fa, "ACWR (EWMA 7/28 j)", 260), width="stretch")
    with st.expander("Bilan hebdomadaire"):
        st.dataframe(hebdo.drop(columns="joueuse").rename(columns={
            "semaine": "Semaine", "charge_ua": "Charge (UA)", "nb_seances": "Séances",
            "monotonie": "Monotonie", "contrainte": "Contrainte"}).sort_values("Semaine", ascending=False),
            hide_index=True, width="stretch",
            column_config={"Semaine": st.column_config.DateColumn(format="DD/MM/YYYY")})


# ══════════════════════════════════════════════════════════════════════════
# MONITORING — MATCHS (collectif & individuel)
# ══════════════════════════════════════════════════════════════════════════

def tableau_matchs_collectifs(app, tac_files: list, gps_match_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Une ligne par match tagué (rapport collectif Sportscode + GPS collectif).
    Mis en cache en session (un rapport collectif coûte ~0,1-0,5 s)."""
    cache = st.session_state.setdefault("_v2_coll_cache", {})
    rows = []
    for tac in tac_files or []:
        df = tac.get("df")
        if df is None or getattr(df, "empty", True):
            continue
        key = (tac.get("filename"), len(df), str(tac.get("date")))
        if key not in cache:
            try:
                ctx = app._get_match_context(df)
                rep = app.compute_collective_report(df)
                info = {"date": tac.get("date"), "score_pfc": ctx.get("score_pfc"), "score_adv": ctx.get("score_adv"),
                        "label": " · ".join(p for p in [tac.get("competition", ""),
                                                          f"J{tac['journee']}" if tac.get("journee") else "",
                                                          tac.get("adversaire", "")] if p)}
                ligne = D.ligne_match_collectif(rep, info)
                if ligne:
                    ligne["Compétition"] = tac.get("competition", "")
                    ligne["Journée"] = tac.get("journee", "")
                    try:
                        g = app.compute_collective_gps_stats(gps_match_df, match_date=tac.get("date"),
                                                             adversaire=tac.get("adversaire"),
                                                             journee=tac.get("journee")) if gps_match_df is not None and not gps_match_df.empty else {}
                    except Exception:
                        g = {}
                    ligne["Distance moy. (m)"] = g.get("distance_moyenne")
                    ligne["HID >19 équipe (m)"] = g.get("hid19_totale")
                    ligne["Sprints >23 équipe"] = g.get("sprints23_totaux")
                    ligne["Vmax équipe (km/h)"] = g.get("vmax_equipe")
                cache[key] = ligne
            except Exception:
                cache[key] = {}
        if cache[key]:
            rows.append(cache[key])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("Date").reset_index(drop=True)


def render_tendances_collectives(app, tac_files: list, gps_match_df: pd.DataFrame):
    st.markdown("#### 🤝 Performance collective — match après match")
    with st.spinner("Calcul des rapports collectifs…"):
        df = tableau_matchs_collectifs(app, tac_files, gps_match_df)
    if df.empty:
        st.info("Aucun match tagué collectivement (fichiers Sportscode PFC_VS_…).")
        return
    comps = sorted(x for x in df["Compétition"].dropna().unique() if x)
    if comps:
        sel = st.multiselect("Compétitions", comps, default=comps, key="v2_tc_comp")
        df = df[df["Compétition"].isin(sel) | (df["Compétition"] == "")]
    b = D.bilan_resultats(df)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Matchs", b["matchs"])
    c2.metric("V / N / D", f"{b['V']} / {b['N']} / {b['D']}")
    c3.metric("Buts pour / contre", f"{b['bp']} / {b['bc']}")
    c4.metric("Points / match", f"{b['points'] / b['matchs']:.2f}" if b["matchs"] else "—")
    c5.metric("Possession moy.", f"{pd.to_numeric(df['Possession PFC (%)'], errors='coerce').mean():.0f} %")

    dispo = [c for c in D.INDICATEURS_TENDANCE_COLLECTIF + ["Distance moy. (m)", "HID >19 équipe (m)"]
             if c in df.columns and pd.to_numeric(df[c], errors="coerce").notna().any()]
    ind = st.multiselect("Indicateurs", dispo, default=dispo[:3], key="v2_tc_ind")
    lissage = st.toggle("Moyenne glissante (3 matchs)", value=True, key="v2_tc_roll")
    x = df["Date"].dt.strftime("%d/%m") + " " + df["Adversaire"].astype(str).str[:12]
    for c in ind:
        y = pd.to_numeric(df[c], errors="coerce")
        fig = go.Figure()
        couleurs = [C_VERT if r == "V" else (C_OR if r == "N" else C_ROUGE) for r in df["Résultat"]]
        fig.add_trace(go.Bar(x=x, y=y, marker_color=couleurs, name=c, opacity=0.85))
        miroir = c.replace("PFC", "ADV")
        if miroir != c and miroir in df.columns:
            fig.add_trace(go.Scatter(x=x, y=pd.to_numeric(df[miroir], errors="coerce"), mode="markers",
                                     marker=dict(color=C_ADV, symbol="diamond", size=9), name=miroir))
        if lissage and len(y) >= 3:
            fig.add_trace(go.Scatter(x=x, y=y.rolling(3, min_periods=1).mean(), mode="lines",
                                     line=dict(color="white", width=2), name="Moy. glissante 3"))
        fig.add_hline(y=float(y.mean()), line_dash="dot", line_color=C_GRIS)
        st.plotly_chart(_layout(fig, c, 280), width="stretch")
    st.caption("Barres colorées selon le résultat (vert = victoire, jaune = nul, rouge = défaite) · "
               "losanges = valeur adverse · pointillés = moyenne de la période.")
    with st.expander("Tableau des matchs"):
        st.dataframe(df.drop(columns=["Compétition"], errors="ignore"), hide_index=True, width="stretch",
                     column_config={"Date": st.column_config.DateColumn(format="DD/MM/YYYY")})


def render_tendance_individuelle(app, pfc_kpi_all: pd.DataFrame, joueuse: str | None):
    """Évolution match après match des indicateurs technico-tactiques d'une joueuse."""
    if pfc_kpi_all is None or pfc_kpi_all.empty or not joueuse:
        return
    cle = app.nettoyer_nom_joueuse
    d = pfc_kpi_all[pfc_kpi_all["Player"].astype(str).map(cle) == cle(joueuse)].copy()
    if d.empty:
        cand = meilleur_nom(app, joueuse, pfc_kpi_all["Player"].dropna().astype(str).unique())
        if cand:
            d = pfc_kpi_all[pfc_kpi_all["Player"] == cand].copy()
    if d.empty:
        st.info(f"Aucune donnée technico-tactique pour {joueuse}.")
        return
    st.markdown(f"#### 🎯 Performance individuelle en match — {joueuse}")
    if "Date" in d.columns:
        d["_dt"] = pd.to_datetime(d["Date"], errors="coerce")
        d = d.sort_values("_dt")
    lab = (d["_dt"].dt.strftime("%d/%m") + " " if "_dt" in d.columns else "") + d.get("Adversaire", pd.Series("", index=d.index)).astype(str).str[:12]
    tact = [c for c in D.INDICATEURS_TACTIQUES if c in d.columns]
    tech = D.indicateurs_techniques_disponibles(d)
    ind = st.multiselect("Indicateurs", tact + tech, default=tact[:4], key="v2_ti_ind")
    if not ind:
        return
    fig = go.Figure()
    for c in ind:
        fig.add_trace(go.Scatter(x=lab, y=pd.to_numeric(d[c], errors="coerce"), mode="lines+markers", name=c))
    st.plotly_chart(_layout(fig, "Indicateurs par match", 340), width="stretch")
    if "Temps de jeu (en minutes)" in d.columns:
        st.caption(f"{len(d)} match(s) · {pd.to_numeric(d['Temps de jeu (en minutes)'], errors='coerce').sum():.0f} min cumulées "
                   "(temps normalisé de l'export match).")


# ══════════════════════════════════════════════════════════════════════════
# SYNTHÈSE — bilans de période
# ══════════════════════════════════════════════════════════════════════════

def _load_templates() -> dict:
    try:
        with open(SYNTHESE_TEMPLATES_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_templates(t: dict) -> bool:
    try:
        os.makedirs(os.path.dirname(SYNTHESE_TEMPLATES_PATH), exist_ok=True)
        with open(SYNTHESE_TEMPLATES_PATH, "w", encoding="utf-8") as f:
            json.dump(t, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


_GPS_DEFAUT = ["Distance (m)", "Distance relative (m/min)", "V_19_23", "Sprints_23", "Vitesse max (km/h)", "Acc_3", "Dec_3"]
_GPS_PICS = {"Vitesse max (km/h)", "Accélération maximale (m/s²)"}


def _gps_par_joueuse(app, gps_df: pd.DataFrame, effectif: list) -> pd.DataFrame:
    """Renomme les joueuses GPS vers les noms de l'effectif (correspondance par tokens)."""
    if gps_df is None or gps_df.empty or "Player" not in gps_df.columns:
        return pd.DataFrame()
    g = app.ensure_date_column(gps_df).copy()
    noms = g["Player"].dropna().astype(str).unique()
    mapping = {n: (meilleur_nom(app, n, effectif) if effectif else n) or n for n in noms}
    g["Player"] = g["Player"].astype(str).map(mapping)
    return g


def render_synthese(app, role, player_name, pfc_kpi_all, gps_raw_df, gps_match_df, tac_files, effectif):
    st.markdown("#### 🧾 Bilan de période")
    est_joueuse = role == app.ROLE_JOUEUSE
    tpl = _load_templates()

    # ── Portée et période ───────────────────────────────────────────────
    c1, c2, c3 = st.columns([1, 2, 2])
    if est_joueuse:
        portee = "Joueuse(s)"
        joueuses = [player_name] if player_name else []
        c1.markdown(f"**Joueuse :** {player_name}")
    else:
        portee = c1.radio("Portée", ["Équipe", "Joueuse(s)"], key="v2_syn_portee")
        joueuses = list(effectif) if portee == "Équipe" else c2.multiselect(
            "Joueuses", effectif, default=effectif[:1], key="v2_syn_joueuses")
    presets = D.periodes_predefinies()
    preset = c3.selectbox("Période", list(presets), index=2, key="v2_syn_preset")
    deb, fin = presets[preset]
    if preset == "Personnalisée" or deb is None:
        p1, p2 = st.columns(2)
        deb = p1.date_input("Du", value=date.today() - timedelta(days=27), key="v2_syn_deb", format="DD/MM/YYYY")
        fin = p2.date_input("Au", value=date.today(), key="v2_syn_fin", format="DD/MM/YYYY")
    st.caption(f"Période : du **{deb.strftime('%d/%m/%Y')}** au **{fin.strftime('%d/%m/%Y')}**")

    # ── Dimensions & indicateurs ─────────────────────────────────────────
    tpl_names = ["— Personnalisé —"] + sorted(tpl)
    t1, t2 = st.columns([2, 1])
    choix_tpl = t1.selectbox("Modèle de bilan", tpl_names, key="v2_syn_tpl")
    if choix_tpl != "— Personnalisé —" and st.session_state.get("_v2_syn_tpl_applique") != choix_tpl:
        for k, v in tpl[choix_tpl].items():
            st.session_state[k] = v
        st.session_state["_v2_syn_tpl_applique"] = choix_tpl
        st.rerun()

    dims_opts = list(D.DIMENSIONS_SYNTHESE)
    if est_joueuse:
        dims_opts = [d for d in dims_opts if d != "collectif"]
    dims = st.multiselect("Dimensions du bilan", dims_opts,
                          default=[d for d in ["technique", "tactique", "physique", "presence", "objectifs"] if d in dims_opts],
                          format_func=lambda k: D.DIMENSIONS_SYNTHESE[k], key="v2_syn_dims")

    tech_dispo = D.indicateurs_techniques_disponibles(pfc_kpi_all)
    gps_cols_dispo = []
    for g in (gps_raw_df, gps_match_df):
        if g is not None and not g.empty:
            gps_cols_dispo += [c for c in list(app.MONITORING_ALL_COLS) + ["Distance HID (>19 km/h)", "Durée_min"]
                               if c in g.columns and c not in gps_cols_dispo]
    with st.expander("🎛️ Indicateurs par dimension", expanded=False):
        if "technique" in dims:
            st.multiselect("Technique", tech_dispo, default=tech_dispo[:8], key="v2_syn_tech")
        if "tactique" in dims:
            st.multiselect("Tactique", D.INDICATEURS_TACTIQUES + D.INDICATEURS_POSTES,
                           default=D.INDICATEURS_TACTIQUES, key="v2_syn_tact")
        if "physique" in dims:
            st.multiselect("Physique (GPS)", gps_cols_dispo,
                           default=[c for c in _GPS_DEFAUT if c in gps_cols_dispo],
                           format_func=lambda c: app.MONITORING_ALL_LABELS.get(c, c), key="v2_syn_gps")
            st.radio("Source GPS", ["Entraînements + matchs", "Entraînements", "Matchs"], horizontal=True, key="v2_syn_gps_src")
        if "collectif" in dims:
            st.multiselect("Indicateurs collectifs", D.INDICATEURS_TENDANCE_COLLECTIF,
                           default=D.INDICATEURS_TENDANCE_COLLECTIF[:5], key="v2_syn_coll")
        st.divider()
        n1, n2 = st.columns([3, 1])
        nom_tpl = n1.text_input("Enregistrer ces choix comme modèle", key="v2_syn_tpl_nom")
        if n2.button("💾 Enregistrer", key="v2_syn_tpl_save") and nom_tpl.strip():
            keys = ["v2_syn_dims", "v2_syn_tech", "v2_syn_tact", "v2_syn_gps", "v2_syn_gps_src", "v2_syn_coll", "v2_syn_preset"]
            tpl[nom_tpl.strip()] = {k: st.session_state[k] for k in keys if k in st.session_state}
            st.success("Modèle enregistré.") if _save_templates(tpl) else st.error("Échec de l'enregistrement.")

    commentaire = st.text_area("Commentaire du staff (repris dans l'export)", key="v2_syn_comment", height=90)

    if st.button("⚙️ Générer le bilan", type="primary", key="v2_syn_go"):
        st.session_state["_v2_syn_params"] = (portee, tuple(joueuses), deb, fin, tuple(dims))
    params = st.session_state.get("_v2_syn_params")
    if not params or params != (portee, tuple(joueuses), deb, fin, tuple(dims)):
        st.caption("Choisis la portée, la période et les dimensions puis clique sur **Générer le bilan**.")
        return
    if not joueuses and "collectif" not in dims:
        st.warning("Aucune joueuse sélectionnée.")
        return

    sections: dict[str, pd.DataFrame] = {}
    resume: dict[str, str] = {}
    cle = app.nettoyer_nom_joueuse

    with st.spinner("Calcul du bilan…"):
        # Technique / tactique
        for dim, key in (("technique", "v2_syn_tech"), ("tactique", "v2_syn_tact")):
            if dim in dims:
                t = D.synthese_kpi_match(pfc_kpi_all, joueuses, st.session_state.get(key, []), deb, fin, cle_nom=cle)
                sections[D.DIMENSIONS_SYNTHESE[dim]] = t
                if dim == "technique" and not t.empty:
                    resume["Matchs joués (moy.)"] = f"{t['Matchs'].mean():.1f}"
                    resume["Minutes (moy.)"] = f"{t['Minutes'].mean():.0f}"
        # Physique
        if "physique" in dims:
            src = st.session_state.get("v2_syn_gps_src", "Entraînements + matchs")
            parts = []
            if src in ("Entraînements + matchs", "Entraînements"):
                parts.append(_gps_par_joueuse(app, gps_raw_df, effectif))
            if src in ("Entraînements + matchs", "Matchs"):
                parts.append(_gps_par_joueuse(app, gps_match_df, effectif))
            g = pd.concat([p for p in parts if not p.empty], ignore_index=True) if any(not p.empty for p in parts) else pd.DataFrame()
            t = D.synthese_gps(g, joueuses, st.session_state.get("v2_syn_gps", _GPS_DEFAUT), deb, fin, _GPS_PICS)
            if not t.empty:
                t = t.rename(columns=lambda c: re.sub(r"^(.+?) \((total|moy\.|max)\)$",
                                                      lambda m: f"{app.MONITORING_ALL_LABELS.get(m.group(1), m.group(1))} ({m.group(2)})", c))
                resume["Séances GPS (moy.)"] = f"{t['Séances'].mean():.1f}"
            sections[D.DIMENSIONS_SYNTHESE["physique"]] = t
        # Présence
        if "presence" in dims:
            pr = load_presence_periode(app, deb, fin)
            if not pr.empty:
                pr = pr[pr["joueuse"].isin(joueuses)]
            t = D.presence_resume(pr, list(app.PRESENCE_STATUTS), app.PRESENCE_STATUTS_PRESENTE)
            t = t.rename(columns={"joueuse": "Joueuse"})
            sections[D.DIMENSIONS_SYNTHESE["presence"]] = t
            if not t.empty:
                resume["Taux de présence"] = f"{t['Taux de présence (%)'].mean():.0f} %"
        # Objectifs
        if "objectifs" in dims:
            try:
                avec_plan = app.load_objectifs_joueuses()
            except Exception:
                avec_plan = []
            blocs = []
            for j in joueuses:
                jo = meilleur_nom(app, j, avec_plan) if avec_plan else None
                if not jo:
                    continue
                o = app.load_objectifs_pour_joueuse(jo)
                e = load_evaluations_objectifs(app, jo, deb, fin)
                t = D.synthese_objectifs(o, e)
                if not t.empty:
                    t.insert(0, "Joueuse", j)
                    blocs.append(t)
            t = pd.concat(blocs, ignore_index=True) if blocs else pd.DataFrame()
            sections[D.DIMENSIONS_SYNTHESE["objectifs"]] = t
            if not t.empty and t["Note coach"].notna().any():
                resume["Note objectifs (coach)"] = f"{t['Note coach'].mean():.1f}"
        # Bien-être
        if "bien_etre" in dims:
            w = load_wellness(app, deb - timedelta(days=30), fin)
            t = D.synthese_bien_etre(w, joueuses, deb, fin)
            sections[D.DIMENSIONS_SYNTHESE["bien_etre"]] = t
            if not t.empty:
                resume["Bien-être moyen"] = f"{t['Score moyen (/25)'].mean():.1f} / 25"
        # Charge interne
        if "charge" in dims:
            t = D.synthese_charge(load_rpe(app, deb, fin), joueuses, deb, fin)
            sections[D.DIMENSIONS_SYNTHESE["charge"]] = t
            if not t.empty:
                resume["Charge / semaine"] = f"{t['Charge / semaine (UA)'].mean():.0f} UA"
        # Collectif
        if "collectif" in dims:
            m = tableau_matchs_collectifs(app, tac_files, gps_match_df)
            m = D.filtre_periode(m, "Date", deb, fin) if not m.empty else m
            if not m.empty:
                b = D.bilan_resultats(m)
                resume["Bilan V/N/D"] = f"{b['V']}/{b['N']}/{b['D']} ({b['bp']}–{b['bc']})"
                ind = st.session_state.get("v2_syn_coll", D.INDICATEURS_TENDANCE_COLLECTIF[:5])
                m = m[["Date", "Adversaire", "Score", "Résultat", *[c for c in ind if c in m.columns]]]
            sections[D.DIMENSIONS_SYNTHESE["collectif"]] = m

    # ── Affichage ───────────────────────────────────────────────────────
    titre = (f"Bilan {'équipe' if portee == 'Équipe' else ', '.join(joueuses[:3]) + ('…' if len(joueuses) > 3 else '')}"
             f" — {deb.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')}")
    st.divider()
    st.markdown(f"### {titre}")
    if resume:
        cols = st.columns(min(len(resume), 5))
        for i, (k, v) in enumerate(resume.items()):
            cols[i % len(cols)].metric(k, v)
    for nom, t in sections.items():
        st.markdown(f"##### {nom}")
        if t is None or t.empty:
            st.caption("Aucune donnée sur la période.")
            continue
        _afficher_section(t, portee)

    # ── Exports ─────────────────────────────────────────────────────────
    e1, e2 = st.columns(2)
    xls = _export_excel(sections, titre, commentaire)
    e1.download_button("⬇️ Excel (un onglet par dimension)", data=xls,
                       file_name=f"bilan_{deb.isoformat()}_{fin.isoformat()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="v2_syn_xls")
    html = _export_html(sections, titre, resume, commentaire)
    e2.download_button("⬇️ HTML (à imprimer en PDF)", data=html.encode("utf-8"),
                       file_name=f"bilan_{deb.isoformat()}_{fin.isoformat()}.html", mime="text/html", key="v2_syn_html")


def _afficher_section(t: pd.DataFrame, portee: str):
    num = [c for c in t.columns if pd.api.types.is_numeric_dtype(t[c]) and c not in ("Matchs", "Minutes", "Séances", "Réponses")]
    show = t.copy()
    if portee == "Équipe" and "Joueuse" in show.columns and len(show) > 1 and num:
        moy = {c: round(float(pd.to_numeric(show[c], errors="coerce").mean()), 1) for c in num}
        ligne = {c: "" for c in show.columns if c not in num}
        ligne.update({"Joueuse": "Moyenne équipe", **moy})
        show = pd.concat([show, pd.DataFrame([ligne])], ignore_index=True)
    sty = show.style.format(precision=1, na_rep="—")
    if num and len(show) > 1:
        sty = sty.background_gradient(cmap="Blues", subset=num, axis=0)
    cfg = {"Date": st.column_config.DateColumn(format="DD/MM/YYYY")} if "Date" in show.columns else None
    st.dataframe(sty, hide_index=True, width="stretch", column_config=cfg)


def _export_excel(sections: dict, titre: str, commentaire: str) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame({"Bilan": [titre, "", "Commentaire du staff", commentaire or ""]}).to_excel(
            xw, sheet_name="Résumé", index=False)
        for nom, t in sections.items():
            if t is None or t.empty:
                continue
            feuille = re.sub(r"[\[\]\*\?/\\:]", "", re.sub(r"^\W+\s*", "", nom))[:31] or "Feuille"
            t.to_excel(xw, sheet_name=feuille, index=False)
    return buf.getvalue()


def _export_html(sections: dict, titre: str, resume: dict, commentaire: str) -> str:
    import html as _h
    cartes = "".join(f"<div class='k'><div class='kl'>{_h.escape(k)}</div><div class='kv'>{_h.escape(v)}</div></div>"
                     for k, v in resume.items())
    corps = ""
    for nom, t in sections.items():
        corps += f"<h2>{_h.escape(nom)}</h2>"
        if t is None or t.empty:
            corps += "<p class='vide'>Aucune donnée sur la période.</p>"
        else:
            tt = t.copy()
            for c in tt.columns:
                if pd.api.types.is_datetime64_any_dtype(tt[c]):
                    tt[c] = tt[c].dt.strftime("%d/%m/%Y")
            corps += tt.to_html(index=False, na_rep="—", float_format=lambda v: f"{v:.1f}", border=0)
    com = f"<h2>Commentaire du staff</h2><p>{_h.escape(commentaire).replace(chr(10), '<br>')}</p>" if commentaire else ""
    return f"""<!doctype html><html lang="fr"><head><meta charset="utf-8"><title>{_h.escape(titre)}</title>
<style>
body{{font-family:Helvetica,Arial,sans-serif;color:#0B2A5C;margin:28px;}}
h1{{font-size:20px;border-bottom:3px solid #1FA8E0;padding-bottom:6px}}
h2{{font-size:15px;margin-top:22px;color:#0B2A5C}}
table{{border-collapse:collapse;width:100%;font-size:11px}}
th{{background:#0B2A5C;color:#fff;padding:5px;text-align:left}}
td{{padding:4px 5px;border-bottom:1px solid #DDE4EC}}
tr:nth-child(even) td{{background:#F3F6FA}}
.ks{{display:flex;flex-wrap:wrap;gap:10px;margin:12px 0}}
.k{{border:1px solid #1FA8E0;border-radius:6px;padding:8px 12px;min-width:130px}}
.kl{{font-size:10px;text-transform:uppercase;color:#6A8090}}.kv{{font-size:18px;font-weight:700}}
.vide{{color:#6A8090;font-style:italic}}
@media print{{h2{{page-break-after:avoid}} table{{page-break-inside:auto}}}}
</style></head><body>
<h1>Paris FC — {_h.escape(titre)}</h1>
<div class="ks">{cartes}</div>{corps}{com}
<p style="font-size:9px;color:#6A8090;margin-top:30px">Généré le {date.today().strftime('%d/%m/%Y')} · Interface Performance v2</p>
</body></html>"""
