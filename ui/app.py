"""主入口 - QuantLab 量化研究平台"""

import ui.bootstrap  # noqa: F401

import pandas as pd
import streamlit as st
from i18n import t
from style import apply_global_style
from ui.components.dashboard import (
    _run_update_stream_ui,
    render_data_health,
    render_portfolio_snapshot,
    render_quick_actions,
    render_recent_runs_table,
    render_system_status,
)
from ui.data.loader import list_runs

st.set_page_config(
    page_title=t("app.title"),
    page_icon="🧪",
    layout="wide",
)
apply_global_style()

st.sidebar.title(t("app.nav"))

with st.sidebar:
    st.page_link("app.py", label=t("app.home"), icon="🏠")
    st.page_link("pages/1_runs.py", label=t("app.runs"), icon="📊")
    st.page_link("pages/2_run_detail.py", label=t("app.run_detail"), icon="🔍")
    st.page_link("pages/3_compare_runs.py", label=t("app.compare_runs"), icon="📈")

    st.markdown("---")
    st.subheader(t("app.update_all.section"))
    force_update_all = st.checkbox(t("app.update_all.force"), value=False, key="force_update_all")

    if st.button(t("app.update_all.button"), use_container_width=True):
        _run_update_stream_ui(force_update_all, sidebar=True)

# Dashboard main area
st.title("QuantLab 研究终端")

try:
    render_system_status()
except Exception as exc:
    st.warning(f"系统状态区块暂不可用：{exc}")

try:
    runs_df = list_runs()
except Exception as exc:
    st.warning(f"运行列表扫描失败：{exc}")
    runs_df = None

if runs_df is None:
    runs_df = pd.DataFrame()

latest_run_id = st.session_state.get("selected_run_id") if isinstance(st.session_state.get("selected_run_id"), str) else None
if (not latest_run_id) and not runs_df.empty and "run_id" in runs_df.columns:
    try:
        ordered = runs_df.sort_values("created_at", ascending=False, na_position="last") if "created_at" in runs_df.columns else runs_df
        latest_run_id = str(ordered.iloc[0]["run_id"])
    except Exception:
        latest_run_id = str(runs_df.iloc[0]["run_id"]) if "run_id" in runs_df.columns and not runs_df.empty else None

if latest_run_id:
    st.session_state["selected_run_id"] = latest_run_id

try:
    render_portfolio_snapshot(latest_run_id or "")
except Exception as exc:
    st.warning(f"组合快照区块暂不可用：{exc}")

try:
    render_recent_runs_table(runs_df)
except Exception as exc:
    st.warning(f"最近运行区块暂不可用：{exc}")

try:
    render_data_health()
except Exception as exc:
    st.warning(f"数据健康区块暂不可用：{exc}")

try:
    render_quick_actions()
except Exception as exc:
    st.warning(f"快捷操作区块暂不可用：{exc}")
