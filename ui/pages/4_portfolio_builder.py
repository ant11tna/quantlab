from __future__ import annotations

from pathlib import Path

import ui.bootstrap  # noqa: F401

import pandas as pd
import plotly.express as px
import streamlit as st

from components import symbol_search_pro_component
from quantlab.universe.store import UniverseStore

PORTFOLIO_PATH = Path("data/portfolio/holdings.parquet")


st.set_page_config(page_title="Portfolio Builder", page_icon="🧩", layout="wide")
st.title("Portfolio Builder")

store = UniverseStore(base_dir="data/universe")

left, right = st.columns([3, 2])

with left:
    symbol_search_pro_component(store=store, portfolio_path=str(PORTFOLIO_PATH), allow_add=True)

    st.markdown("---")
    st.subheader("当前组合")

    if PORTFOLIO_PATH.exists():
        holdings = pd.read_parquet(PORTFOLIO_PATH, engine="pyarrow")
    else:
        holdings = pd.DataFrame(columns=["listing_id", "weight", "added_at"])

    if holdings.empty:
        st.info("组合为空，请先添加标的。")
    else:
        if "weight" not in holdings.columns:
            holdings["weight"] = 0.0
        editable = holdings[["listing_id", "weight", "added_at"]].copy()
        editable["weight"] = pd.to_numeric(editable["weight"], errors="coerce").fillna(0.0)

        edited = st.data_editor(
            editable,
            hide_index=True,
            use_container_width=True,
            key="portfolio_holdings_editor",
            column_config={
                "weight": st.column_config.NumberColumn("weight", min_value=0.0, step=0.01, format="%.4f")
            },
            disabled=["listing_id", "added_at"],
        )

        if st.button("保存权重", type="primary"):
            to_save = edited.copy()
            to_save.to_parquet(PORTFOLIO_PATH, index=False, engine="pyarrow")
            st.success("权重已保存。")

with right:
    st.subheader("权重分布")
    if PORTFOLIO_PATH.exists():
        weights_df = pd.read_parquet(PORTFOLIO_PATH, engine="pyarrow")
    else:
        weights_df = pd.DataFrame(columns=["listing_id", "weight"])

    if weights_df.empty:
        st.info("暂无组合数据。")
    else:
        weights_df["weight"] = pd.to_numeric(weights_df.get("weight"), errors="coerce").fillna(0.0)
        total_weight = float(weights_df["weight"].sum())
        if total_weight <= 0:
            st.info("请填写权重")
        else:
            fig = px.pie(weights_df, values="weight", names="listing_id", title="Portfolio Weights")
            st.plotly_chart(fig, use_container_width=True)
