# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/metric_selector.py, version du 14/10/2026).
# Seuls les imports ont été adaptés. Garder la logique alignée sur l'original.
"""Sélecteur compact des métriques GPS, partagé entre les vues de charge."""

import streamlit as st

from suivi_emma.metrics import CATALOG as DEFAULT_CATALOG, MetricCatalog


_CATEGORY_MARKERS = {
    "Faible intensité & Volume": "🟢",
    "Haute intensité": "🔵",
    "Sprints & vitesse": "🔴",
    "Volume": "🟢",
    "Intensité max": "🔴",
    "Accélérations / Décélérations": "🟡",
}


def _pill_label(metric_col, catalog):
    category = next((name for name, metrics in catalog.metrics.items() if metric_col in metrics), "")
    return f"{_CATEGORY_MARKERS.get(category, '⚪')} {catalog.labels[metric_col]}"


def _sync_selection(pill_key, selection_key, pill_keys, category_metrics):
    """Conserve une seule métrique active entre les quatre groupes."""
    selected = st.session_state.get(pill_key)
    if selected is None:
        current = st.session_state.get(selection_key)
        if current in category_metrics:
            st.session_state[pill_key] = current
        return

    st.session_state[selection_key] = selected
    for other_key in pill_keys:
        if other_key != pill_key:
            st.session_state[other_key] = None


def render_metric_selector(label, key, default="distance_m", catalog: MetricCatalog = DEFAULT_CATALOG):
    """Affiche les métriques GPS en quatre colonnes de pastilles compactes."""
    default = default if default in catalog.metric_cols else catalog.metric_cols[0]
    selection_key = f"metric_selector_{key}_selected"
    if st.session_state.get(selection_key) not in catalog.metric_cols:
        st.session_state[selection_key] = default

    pill_keys = [f"metric_selector_{key}_pill_{index}" for index in range(len(catalog.metrics))]
    st.markdown(
        """
<style>
div[class*="st-key-metric_selector_"][class*="_cat_0"] [data-testid="stPills"] button {
    background: rgba(0, 122, 122, 0.13) !important;
    border-color: rgba(0, 122, 122, 0.55) !important;
}
div[class*="st-key-metric_selector_"][class*="_cat_1"] [data-testid="stPills"] button {
    background: rgba(6, 128, 190, 0.14) !important;
    border-color: rgba(6, 128, 190, 0.55) !important;
}
div[class*="st-key-metric_selector_"][class*="_cat_2"] [data-testid="stPills"] button {
    background: rgba(189, 48, 50, 0.14) !important;
    border-color: rgba(189, 48, 50, 0.55) !important;
}
div[class*="st-key-metric_selector_"][class*="_cat_3"] [data-testid="stPills"] button {
    background: rgba(242, 169, 0, 0.16) !important;
    border-color: rgba(242, 169, 0, 0.62) !important;
}
</style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(f"**{label}**")
    columns = st.columns(len(catalog.metrics), gap="small")
    for index, ((category, category_metrics), column) in enumerate(zip(catalog.metrics.items(), columns)):
        with column:
            st.markdown(
                f"<div style='color:{catalog.category_colors[category]}; font-weight:700; margin-bottom:0.3rem'>{category}</div>",
                unsafe_allow_html=True,
            )
            pill_key = pill_keys[index]
            with st.container(key=f"metric_selector_{key}_cat_{index}"):
                st.pills(
                    category,
                    list(category_metrics),
                    default=(st.session_state[selection_key]
                             if st.session_state[selection_key] in category_metrics else None),
                    format_func=lambda metric, _catalog=catalog: _pill_label(metric, _catalog),
                    key=pill_key,
                    label_visibility="collapsed",
                    on_change=_sync_selection,
                    args=(pill_key, selection_key, pill_keys, category_metrics),
                )
    return st.session_state[selection_key]
