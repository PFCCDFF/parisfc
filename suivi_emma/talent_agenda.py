# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/talent_agenda.py, version du 14/10/2026).
# Seuls les imports ont été adaptés. Garder la logique alignée sur l'original.
"""Briques d'agenda hebdomadaire partagées par le Programme talent (onglet
Semaine et agenda de l'onglet Présences) et par le
sélecteur de séance des Vues individuelle / équipe (D1 et U19)."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

JOURS_FR = {0: "Lun", 1: "Mar", 2: "Mer", 3: "Jeu", 4: "Ven", 5: "Sam", 6: "Dim"}
BLEU = "#0680BE"


def entete_jour(jour: date) -> str:
    """En-tête d'une colonne de l'agenda (jour courant en bleu)."""
    est_jour = jour == date.today()
    couleur = BLEU if est_jour else "inherit"
    return (
        f"<div style='padding-bottom:6px;margin-bottom:4px;"
        f"border-bottom:2px solid {BLEU if est_jour else 'rgba(128,128,128,.25)'}'>"
        f"<span style='font-size:.72em;text-transform:uppercase;letter-spacing:.06em;"
        f"font-weight:600;color:{couleur}'>{JOURS_FR[jour.weekday()]}</span> "
        f"<span style='font-size:1.25em;font-weight:700;color:{couleur}'>{jour.strftime('%d')}</span>"
        f"<span style='opacity:.55;font-size:.85em'>/{jour.strftime('%m')}</span></div>"
    )


def carte_cliquable(carte_html: str, cle: str, couleur: str, css: list, classe: str = "rdv-carte",
                    libelle: str = "Modifier") -> bool:
    """Carte HTML (élément racine de classe ``classe``) recouverte d'un bouton
    transparent ; renvoie True au clic. ``css`` collecte les règles, à poser
    une fois en fin de grille (``<style>``)."""
    css.append(
        # Le bouton est dans son propre conteneur d'élément (position relative,
        # 0 px de haut sous la carte) : c'est ce conteneur qu'il faut étendre
        # sur toute la carte, pas le .stButton. Le conteneur Markdown a une
        # marge basse négative (-1rem) qui ferait déborder la carte de la zone
        # cliquable : annulée ici, et l'espacement entre cartes rattrapé sur le
        # bloc (-7px = même écart que les cartes non cliquables).
        f".st-key-{cle} {{position:relative;margin-bottom:-7px;}}"
        f".st-key-{cle} .{classe} {{margin-bottom:0 !important;}}"
        f".st-key-{cle} [data-testid='stMarkdownContainer'] {{margin-bottom:0 !important;}}"
        f".st-key-{cle} .st-key-btn_{cle} {{position:absolute;inset:0;z-index:1;}}"
        f".st-key-{cle} .st-key-btn_{cle} .stButton {{position:static;width:100%;height:100%;}}"
        f".st-key-{cle} .st-key-btn_{cle} button {{width:100%;height:100%;min-height:0;"
        f"opacity:0;cursor:pointer;}}"
        f".st-key-{cle}:hover .{classe} {{transform:translateY(-1px);"
        f"box-shadow:0 8px 16px -12px {couleur};}}"
    )
    with st.container(key=cle, gap=None):
        st.markdown(carte_html, unsafe_allow_html=True)
        return st.button(libelle, key=f"btn_{cle}", width="stretch")


def lundi_de(d: date) -> date:
    return d - timedelta(days=d.weekday())


def navigation_semaine(cle: str) -> date:
    """◀ / date / ▶ / Cette semaine, état dans ``st.session_state[cle]``.
    Renvoie le lundi de la semaine affichée."""
    if cle not in st.session_state:
        st.session_state[cle] = lundi_de(date.today())

    def _decale(n):
        st.session_state[cle] = lundi_de(st.session_state[cle]) + timedelta(days=7 * n)

    def _courante():
        st.session_state[cle] = lundi_de(date.today())

    with st.container(horizontal=True, vertical_alignment="bottom"):
        st.button("◀", key=f"{cle}_prev", help="Semaine précédente", on_click=_decale, args=(-1,))
        choisi = st.date_input("Semaine du", format="DD/MM/YYYY", key=cle, width=160)
        st.button("▶", key=f"{cle}_next", help="Semaine suivante", on_click=_decale, args=(1,))
        st.button("📍 Cette semaine", key=f"{cle}_today", type="tertiary", on_click=_courante)
    return lundi_de(choisi)


def semaines_avec_seances(dates) -> list[date]:
    """Lundis (triés) des semaines contenant au moins une des ``dates``."""
    return sorted({lundi_de(d) for d in dates})


def semaine_voisine(semaines: list[date], lundi: date, sens: int) -> date | None:
    """Semaine avec séances la plus proche avant (``sens < 0``) ou après
    ``lundi``, en sautant les semaines vides (trêve, vacances). None au bout."""
    if sens < 0:
        avant = [s for s in semaines if s < lundi]
        return max(avant) if avant else None
    apres = [s for s in semaines if s > lundi]
    return min(apres) if apres else None


# Couleurs de l'agenda de séances (mêmes repères que le Programme talent).
COULEUR_AGENDA = {"D1": "#3B82F6", "U19": "#F59E0B"}


def agenda_seances(seances: pd.DataFrame, cle: str, *, contexte=None, detail=None, actions=None):
    """Sélecteur de séance en agenda hebdo : ◀ / Aller au / ▶ / Dernière séance,
    puis 7 colonnes Lun-Dim où chaque séance est un bouton (D1 bleu, U19 orange).

    ``seances`` : une ligne par séance, colonnes ``_sid`` (id texte),
    ``session_date``, ``start_date``, ``session_name``, ``session_type`` et
    ``source`` (facultative, D1 par défaut). Séance retenue mémorisée dans
    ``st.session_state[f"{cle}_sess_id"]`` ; on repart de la plus récente quand
    ``contexte`` change (ex. joueuse) ou si l'id mémorisé n'existe plus.
    ``detail(ligne) -> str`` complète le libellé du bouton ; ``actions(sid)``
    est rendu à droite de la barre de navigation (ex. bouton PDF).
    Renvoie ``(sid, lundi)`` : séance retenue et lundi de la semaine affichée."""
    seances = seances.sort_values(["session_date", "start_date"]).reset_index(drop=True)
    sids = seances["_sid"].tolist()
    cle_sid, cle_ctx = f"{cle}_sess_id", f"{cle}_last_ctx"
    if st.session_state.get(cle_ctx) != contexte or st.session_state.get(cle_sid) not in sids:
        st.session_state[cle_sid] = sids[-1]
        st.session_state[cle_ctx] = contexte
    jours = seances["session_date"].dt.date

    def _choisir(sid):
        st.session_state[cle_sid] = sid

    def _aller_semaine(lundi):
        # Changer de semaine sélectionne sa dernière séance.
        du = seances[jours.between(lundi, lundi + timedelta(days=6))]
        if not du.empty:
            st.session_state[cle_sid] = du["_sid"].iloc[-1]

    def _aller_date(cle_date):
        # Date choisie : séance de ce jour (la dernière), sinon la plus proche.
        d = st.session_state.get(cle_date)
        if d is None:
            return
        ecart = (seances["session_date"].dt.normalize() - pd.Timestamp(d)).abs()
        st.session_state[cle_sid] = seances.loc[ecart == ecart.min(), "_sid"].iloc[-1]

    sid_sel = st.session_state[cle_sid]
    jour_sel = jours[seances["_sid"] == sid_sel].iloc[0]
    lundi = lundi_de(jour_sel)
    semaines = semaines_avec_seances(jours)
    sem_prev = semaine_voisine(semaines, lundi, -1)
    sem_next = semaine_voisine(semaines, lundi, 1)

    with st.container(horizontal=True, vertical_alignment="bottom"):
        st.button("◀", key=f"{cle}_agenda_prev", help="Semaine précédente avec des séances",
                  disabled=sem_prev is None, on_click=_aller_semaine, args=(sem_prev,))
        cle_date = f"{cle}_agenda_date_{sid_sel}"
        st.date_input(
            "Aller au", value=jour_sel, min_value=jours.iloc[0], max_value=jours.iloc[-1],
            format="DD/MM/YYYY", key=cle_date, width=150,
            on_change=_aller_date, args=(cle_date,),
        )
        st.button("▶", key=f"{cle}_agenda_next", help="Semaine suivante avec des séances",
                  disabled=sem_next is None, on_click=_aller_semaine, args=(sem_next,))
        st.button("Dernière séance", key=f"{cle}_agenda_derniere", type="tertiary",
                  icon=":material/last_page:", disabled=sid_sel == sids[-1],
                  on_click=_choisir, args=(sids[-1],))
        if actions is not None:
            st.space("stretch")
            actions(sid_sel)

    st.markdown(
        f"##### Semaine du {lundi.strftime('%d/%m')} au "
        f"{(lundi + timedelta(days=6)).strftime('%d/%m/%y')}"
    )
    css = []
    for col, jour in zip(st.columns(7, gap="small"), [lundi + timedelta(days=i) for i in range(7)]):
        with col:
            st.markdown(entete_jour(jour), unsafe_allow_html=True)
            du_jour = seances[jours == jour]
            if du_jour.empty:
                st.caption("À venir" if jour >= date.today() else "Repos")
            # Séances d'un même jour serrées les unes sous les autres (le gap
            # par défaut d'une colonne, 1rem, les espaçait trop).
            with st.container(gap="xxsmall"):
                for _, s in du_jour.iterrows():
                    c = COULEUR_AGENDA.get(s.get("source"), COULEUR_AGENDA["D1"])
                    cle_css = f"{cle}_ag_" + "".join(ch if ch.isalnum() else "_" for ch in s["_sid"])
                    css.append(_css_bouton(cle_css, c, s["_sid"] == sid_sel))
                    icone = "⚽" if s.get("session_type") == "MATCH" else "🏃"
                    with st.container(key=cle_css):
                        st.button(
                            f"{icone} {s['session_name']}{detail(s) if detail else ''}",
                            key=f"btn_{cle_css}", width="stretch",
                            on_click=_choisir, args=(s["_sid"],),
                        )
    st.markdown(f"<style>{''.join(css)}</style>", unsafe_allow_html=True)
    return sid_sel, lundi


MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet",
           "août", "septembre", "octobre", "novembre", "décembre"]


def _css_bouton(cle_css: str, couleur: str, selectionne: bool) -> str:
    """Style d'un bouton de séance de l'agenda (teinte de sa source)."""
    if selectionne:
        fond = (f".st-key-{cle_css} button {{background:{couleur}40 !important;"
                f"border:1px solid {couleur} !important;border-left:5px solid {couleur} !important;"
                f"font-weight:600;}}")
    else:
        fond = (f".st-key-{cle_css} button {{background:{couleur}12 !important;"
                f"border:1px solid {couleur}33 !important;border-left:4px solid {couleur}88 !important;}}")
    return fond + (
        f".st-key-{cle_css} button {{justify-content:flex-start !important;"
        f"text-align:left !important;padding:6px 9px !important;min-height:0 !important;}}"
        f".st-key-{cle_css} button p {{font-size:.8em;line-height:1.3;white-space:normal;}}"
    )


def agenda_matchs_mois(matchs: pd.DataFrame, cle: str, *, contexte=None, libelle=None):
    """Sélecteur de match en vue mensuelle : ◀ / mois / ▶ / Dernier match,
    puis une colonne par semaine touchant le mois, chaque case listant les
    matchs de la semaine (D1 bleu, U19 orange). Une semaine à cheval sur deux
    mois apparaît dans les deux.

    ``matchs`` : une ligne par match, colonnes ``_sid``, ``session_date``,
    ``start_date``, ``session_name``, ``source`` (facultative).
    ``libelle(ligne) -> str`` = texte du bouton (sinon le nom de séance).
    Match retenu dans ``st.session_state[f"{cle}_sess_id"]`` ; on repart du
    plus récent quand ``contexte`` change. Renvoie le ``_sid`` retenu."""
    matchs = matchs.sort_values(["session_date", "start_date"]).reset_index(drop=True)
    sids = matchs["_sid"].tolist()
    cle_sid, cle_ctx = f"{cle}_sess_id", f"{cle}_last_ctx"
    if st.session_state.get(cle_ctx) != contexte or st.session_state.get(cle_sid) not in sids:
        st.session_state[cle_sid] = sids[-1]
        st.session_state[cle_ctx] = contexte
    jours = matchs["session_date"].dt.date
    mois_cle = jours.map(lambda d: (d.year, d.month))

    def _choisir(sid):
        st.session_state[cle_sid] = sid

    def _aller_mois(m):
        # Changer de mois sélectionne son dernier match.
        st.session_state[cle_sid] = matchs.loc[mois_cle == m, "_sid"].iloc[-1]

    sid_sel = st.session_state[cle_sid]
    jour_sel = jours[matchs["_sid"] == sid_sel].iloc[0]
    mois = (jour_sel.year, jour_sel.month)
    tous = sorted(set(mois_cle))
    m_prev = max((m for m in tous if m < mois), default=None)
    m_next = min((m for m in tous if m > mois), default=None)

    with st.container(horizontal=True, vertical_alignment="center"):
        st.button("◀", key=f"{cle}_mois_prev", help="Mois précédent avec un match",
                  disabled=m_prev is None, on_click=_aller_mois, args=(m_prev,))
        st.markdown(f"##### {MOIS_FR[mois[1] - 1].capitalize()} {mois[0]}", width="content")
        st.button("▶", key=f"{cle}_mois_next", help="Mois suivant avec un match",
                  disabled=m_next is None, on_click=_aller_mois, args=(m_next,))
        st.button("Dernier match", key=f"{cle}_mois_dernier", type="tertiary",
                  icon=":material/last_page:", disabled=sid_sel == sids[-1],
                  on_click=_choisir, args=(sids[-1],))

    premier = date(mois[0], mois[1], 1)
    dernier = (pd.Timestamp(premier) + pd.offsets.MonthEnd(0)).date()
    lundis = []
    lundi = lundi_de(premier)
    while lundi <= dernier:
        lundis.append(lundi)
        lundi += timedelta(days=7)

    css = []
    for col, lundi in zip(st.columns(len(lundis), gap="small"), lundis):
        dimanche = lundi + timedelta(days=6)
        en_cours = lundi <= date.today() <= dimanche
        # En-tête et liste dans deux blocs : un seul bloc serré (xxsmall)
        # faisait chevaucher l'en-tête et le premier match.
        with col, st.container(border=True, gap="small"):
            st.markdown(
                f"<div style='line-height:1.25;padding-bottom:6px;"
                f"border-bottom:2px solid {BLEU if en_cours else 'rgba(128,128,128,.25)'}'>"
                f"<div style='font-size:.72em;text-transform:uppercase;letter-spacing:.06em;"
                f"font-weight:600;opacity:.7'>Semaine</div>"
                f"<div style='font-weight:700;color:{BLEU if en_cours else 'inherit'}'>"
                f"{lundi.strftime('%d/%m')} – {dimanche.strftime('%d/%m')}</div></div>",
                unsafe_allow_html=True,
            )
            de_la_semaine = matchs[jours.between(lundi, dimanche)]
            if de_la_semaine.empty:
                st.caption("Pas de match")
            liste = st.container(gap="xsmall")
            for _, m in de_la_semaine.iterrows():
                couleur = COULEUR_AGENDA.get(m.get("source"), COULEUR_AGENDA["D1"])
                cle_css = f"{cle}_mm_" + "".join(ch if ch.isalnum() else "_" for ch in m["_sid"])
                css.append(_css_bouton(cle_css, couleur, m["_sid"] == sid_sel)
                           + f".st-key-{cle_css} button p {{font-size:.85em;}}"
                           f".st-key-{cle_css} button > div {{justify-content:flex-start;}}")
                with liste, st.container(key=cle_css):
                    st.button(
                        f"⚽ {libelle(m) if libelle else m['session_name']}",
                        key=f"btn_{cle_css}", width="stretch",
                        on_click=_choisir, args=(m["_sid"],),
                    )
    st.markdown(f"<style>{''.join(css)}</style>", unsafe_allow_html=True)
    return sid_sel


def detail_distance(s) -> str:
    """Libellé « · 6,2 km » d'une ligne de séance individuelle."""
    dist = pd.to_numeric(pd.Series([s.get("distance_m")]), errors="coerce").iloc[0]
    return "" if pd.isna(dist) else f" · {dist / 1000:.1f} km".replace(".", ",")


def seances_equipe(df: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par séance d'un tableau multi-joueuses, avec ``_sid`` et le
    nombre de joueuses (``_nb``), pour ``agenda_seances``."""
    d = df.assign(_sid=df["session_id"].astype(str))
    nb = d.groupby("_sid")["full_name"].nunique()
    d = d.drop_duplicates("_sid").copy()
    d["_nb"] = d["_sid"].map(nb)
    return d


def detail_nb_joueuses(s) -> str:
    return f" · {int(s['_nb'])} joueuse{'s' if s['_nb'] > 1 else ''}"
