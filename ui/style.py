"""全局 UI 样式。"""

from __future__ import annotations

import streamlit as st


def apply_global_style() -> None:
    """注入统一视觉样式，兼容 Streamlit 1.54.0。"""
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 1.2rem;
        }

        [data-testid="stMetric"],
        [data-testid="stDataFrame"],
        div[data-testid="stVerticalBlock"] > div:has(> [data-testid="stDataFrame"]) {
            background: #ffffff;
            border: 1px solid rgba(49, 51, 63, 0.14);
            border-radius: 14px;
            padding: 0.5rem 0.75rem;
        }

        [data-testid="stMetric"] {
            min-height: 112px;
        }

        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            margin-top: 1.1rem;
            padding-bottom: 0.25rem;
            border-bottom: 1px solid rgba(49, 51, 63, 0.18);
        }

        [data-testid="stSidebar"] .stRadio,
        [data-testid="stSidebar"] .stSelectbox,
        [data-testid="stSidebar"] .stMultiselect,
        [data-testid="stSidebar"] .stTextInput {
            margin-bottom: 0.25rem;
        }

        .ql-page-header {
            margin-bottom: 0.6rem;
        }

        .ql-page-header h1,
        .ql-page-header h2,
        .ql-page-header h3 {
            margin-bottom: 0.1rem;
        }

        .ql-page-header p {
            margin-top: 0;
            color: rgba(49, 51, 63, 0.72);
            font-size: 0.95rem;
        }

        .ql-empty {
            border: 1px dashed rgba(49, 51, 63, 0.25);
            border-radius: 12px;
            padding: 1rem;
            color: rgba(49, 51, 63, 0.78);
            background: rgba(246, 248, 251, 0.9);
        }

        [data-testid="stTabs"] {
            margin-top: 0.3rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
