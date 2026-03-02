"""主入口 - QuantLab 量化研究平台"""

import ui.bootstrap  # noqa: F401

import streamlit as st
from i18n import t
from style import apply_global_style
from quantlab.data.update_all import update_all as run_update_all

st.set_page_config(
    page_title=t("app.title"),
    page_icon="🧪",
    layout="wide",
)
apply_global_style()

st.title(t("app.title"))
st.markdown(t("app.subtitle"))

st.sidebar.title(t("app.nav"))

with st.sidebar:
    st.page_link("app.py", label=t("app.home"), icon="🏠")
    st.page_link("pages/1_runs.py", label=t("app.runs"), icon="📊")
    st.page_link("pages/2_run_detail.py", label=t("app.run_detail"), icon="🔍")
    st.page_link("pages/3_compare_runs.py", label=t("app.compare_runs"), icon="📈")

    st.markdown("---")
    st.subheader(t("app.update_all.section"))
    force_update_all = st.checkbox(t("app.update_all.force"), value=False)
    if st.button(t("app.update_all.button"), use_container_width=True):
        with st.spinner(t("app.update_all.running")):
            result = run_update_all(force=force_update_all)
        st.cache_data.clear()

        if result.get("ok"):
            st.sidebar.success(
                t(
                    "app.update_all.success",
                    count=result.get("curated_built_count", 0),
                    elapsed=result.get("elapsed_seconds", 0),
                )
            )
        else:
            errors = result.get("errors") or [t("app.update_all.unknown_error")]
            st.sidebar.error(
                t("app.update_all.failed", errors="; ".join(str(e) for e in errors))
            )

        st.rerun()

st.info(t("app.home"))
