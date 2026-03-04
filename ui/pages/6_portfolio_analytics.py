from __future__ import annotations

from datetime import date

import ui.bootstrap  # noqa: F401

import pandas as pd
import plotly.express as px
import streamlit as st

from quantlab.analytics import run_portfolio_analytics
from quantlab.market.store import MarketStore
from quantlab.portfolio.store import PortfolioStore
from quantlab.universe.store import UniverseStore

st.set_page_config(page_title="Portfolio Analytics", page_icon="📈", layout="wide")
st.title("Portfolio Analytics (M1)")

portfolio_store = PortfolioStore(base_dir="data/portfolio")
market_store = MarketStore(base_dir="data/market", universe_dir="data/universe")
universe_store = UniverseStore(base_dir="data/universe")

portfolios_df = portfolio_store.load_portfolios()
portfolio_options = ["default"]
if not portfolios_df.empty and "portfolio_id" in portfolios_df.columns:
    portfolio_options = sorted({str(x) for x in portfolios_df["portfolio_id"].dropna().tolist()} | {"default"})

selected_portfolio = st.selectbox("portfolio_id", options=portfolio_options, index=portfolio_options.index("default"))

targets_df = portfolio_store.load_targets()
available_effective_dates: list[str] = []
if not targets_df.empty:
    mask = targets_df["portfolio_id"].astype(str) == selected_portfolio
    dates = targets_df.loc[mask, "effective_date"].dropna().astype(str).unique().tolist()
    available_effective_dates = sorted(dates)

if available_effective_dates:
    default_effective = available_effective_dates[-1]
    selected_effective_date = st.selectbox(
        "effective_date",
        options=available_effective_dates,
        index=len(available_effective_dates) - 1,
    )
else:
    default_effective = date.today().isoformat()
    selected_effective_date = st.text_input("effective_date", value=default_effective)

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("start", value=date.today() - pd.Timedelta(days=120))
with col2:
    end_date = st.date_input("end", value=date.today())

run_clicked = st.button("运行分析", type="primary")

if run_clicked:
    try:
        result = run_portfolio_analytics(
            portfolio_id=selected_portfolio,
            effective_date=selected_effective_date,
            start=pd.Timestamp(start_date),
            end=pd.Timestamp(end_date),
            portfolio_store=portfolio_store,
            market_store=market_store,
            universe_dir="data/universe",
            freq="1d",
            price_field="close",
            base_nav=1.0,
        )
    except ValueError as exc:
        st.error(str(exc))
        st.info("提示：请先去 Slice2 面板检查 Coverage 缺口，或缩小日期区间后重试。")
    except Exception as exc:  # noqa: BLE001
        st.error(f"运行失败: {exc}")
    else:
        nav_df = result["nav_df"].copy()
        returns_df = result["returns_df"].copy()
        metrics = result["metrics"]
        meta = result["meta"]

        st.subheader("指标")
        st.dataframe(pd.DataFrame([metrics]), use_container_width=True, hide_index=True)

        st.subheader("NAV 曲线")
        fig_nav = px.line(nav_df, x="ts", y="nav", title="Portfolio NAV")
        st.plotly_chart(fig_nav, use_container_width=True)

        st.subheader("回撤曲线")
        nav_series = nav_df.set_index("ts")["nav"]
        drawdown = (nav_series / nav_series.cummax() - 1.0).rename("drawdown").reset_index()
        fig_dd = px.line(drawdown, x="ts", y="drawdown", title="Portfolio Drawdown")
        st.plotly_chart(fig_dd, use_container_width=True)

        st.subheader("元信息")
        st.json(meta)

        current_targets = targets_df[
            (targets_df["portfolio_id"].astype(str) == selected_portfolio)
            & (targets_df["effective_date"].astype(str) == selected_effective_date)
        ].copy()
        if not current_targets.empty:
            listings = universe_store.load_listings()
            instruments = universe_store.load_instruments()
            merged = current_targets.merge(
                listings[["listing_id", "instrument_id"]], on="listing_id", how="left"
            ).merge(
                instruments[["instrument_id", "name"]], on="instrument_id", how="left"
            )
            merged["name"] = merged["name"].fillna("").astype(str).str.strip()
            merged.loc[merged["name"] == "", "name"] = "(name unknown)"
            st.subheader("组合成分")
            st.dataframe(
                merged[["listing_id", "name", "target_weight"]].sort_values("target_weight", ascending=False),
                use_container_width=True,
                hide_index=True,
            )
