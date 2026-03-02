"""通用 UI 组件。"""

from __future__ import annotations

from typing import Any

import streamlit as st


def page_header(title: str, subtitle: str | None = None, right: Any | None = None) -> None:
    left_col, right_col = st.columns([5, 2])
    with left_col:
        st.markdown(f"<div class='ql-page-header'><h2>{title}</h2></div>", unsafe_allow_html=True)
        if subtitle:
            st.caption(subtitle)
    with right_col:
        if right is not None:
            if callable(right):
                right()
            else:
                st.write(right)


def section(title: str, right: Any | None = None) -> None:
    left_col, right_col = st.columns([5, 2])
    left_col.markdown(f"### {title}")
    if right is not None:
        with right_col:
            if callable(right):
                right()
            else:
                st.write(right)


def empty_state(text: str, hint: str | None = None) -> None:
    hint_html = f"<div style='margin-top:0.3rem;opacity:0.78'>{hint}</div>" if hint else ""
    st.markdown(f"<div class='ql-empty'><strong>{text}</strong>{hint_html}</div>", unsafe_allow_html=True)
