"""主入口 - QuantLab 量化研究平台"""

import ui.bootstrap  # noqa: F401

import streamlit as st
from i18n import t
from style import apply_global_style
from quantlab.data.update_all import update_all_stream

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
        progress = st.sidebar.progress(0)
        status = st.sidebar.empty()
        detail = st.sidebar.empty()
        err = st.sidebar.empty()

        errors = []
        current_stage = None

        for ev in update_all_stream(force=force_update_all):
            ev_type = ev.get("type")
            if ev_type == "progress":
                stage = ev.get("stage", "unknown")
                if stage != current_stage:
                    current_stage = stage
                    progress.progress(0)

                done = ev.get("done", 0) or 0
                total = ev.get("total", 0) or 0
                pct = done / max(total, 1)
                progress.progress(min(max(pct, 0.0), 1.0))
                status.info(f"{str(stage).upper()} {done}/{total}")
                detail.write(f"{ev.get('symbol', '')}")
            elif ev_type == "error":
                errors.append(ev)
                stage = str(ev.get("stage", "unknown")).upper()
                symbol = ev.get("symbol", "")
                message = str(ev.get("message", ""))[:120]
                err.warning(f"{stage} {symbol} 失败：{message}")
            elif ev_type == "done" and "stage" not in ev:
                if ev.get("ok"):
                    st.sidebar.success("数据更新完成")
                else:
                    st.sidebar.error(
                        f"更新完成但有错误: raw={ev.get('raw_error_count', 0)}, curated={ev.get('curated_error_count', 0)}"
                    )

        st.cache_data.clear()
        st.rerun()

st.info(t("app.home"))
