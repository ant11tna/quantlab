"""主入口 - QuantLab 量化研究平台

默认跳转到回测列表页
"""

from pathlib import Path
import sys

# 确保能 import i18n
sys.path.insert(0, str(Path(__file__).parent))

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

# 使用内部 key 做判断，显示文本用 format_func 翻译
pages = {
    "runs": t("app.runs"),
    "detail": t("app.run_detail"),
    "compare": t("app.compare_runs"),
    "settings": t("app.settings"),
}

choice = st.sidebar.radio(
    t("app.goto"),
    options=list(pages.keys()),
    format_func=lambda k: pages[k]
)

if choice == "runs":
    st.switch_page("pages/1_runs.py")
elif choice == "detail":
    st.switch_page("pages/2_run_detail.py")
elif choice == "compare":
    st.switch_page("pages/3_compare_runs.py")
else:
    st.info(t("app.settings_soon"))
