"""主入口 - QuantLab 量化研究平台

默认跳转到回测列表页
"""

from pathlib import Path
import sys

# 确保能 import i18n
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
from i18n import t

st.set_page_config(
    page_title=t("app.title"),
    page_icon="🧪",
    layout="wide",
)

st.title(t("app.title"))
st.markdown(t("app.subtitle"))

st.sidebar.title(t("app.nav"))

# 使用内部 key 做判断，显示文本用 format_func 翻译
pages = {
    "runs": t("app.runs"),
    "detail": t("app.run_detail"),
    "compare": "Compare Runs",
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
