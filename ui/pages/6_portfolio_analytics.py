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

st.set_page_config(page_title="组合分析", page_icon="📈", layout="wide")
st.title("组合分析（M3）")


portfolio_store = PortfolioStore(base_dir="data/portfolio")
market_store = MarketStore(base_dir="data/market", universe_dir="data/universe")
universe_store = UniverseStore(base_dir="data/universe")


def _format_pct(v: object) -> object:
    try:
        if pd.isna(v):
            return v
        return f"{float(v) * 100:.2f}%"
    except Exception:  # noqa: BLE001
        return v


portfolios_df = portfolio_store.load_portfolios()
portfolio_options = ["default"]
if not portfolios_df.empty and "portfolio_id" in portfolios_df.columns:
    portfolio_options = sorted({str(x) for x in portfolios_df["portfolio_id"].dropna().tolist()} | {"default"})

selected_portfolio = st.selectbox("组合ID", options=portfolio_options, index=portfolio_options.index("default"))

targets_df = portfolio_store.load_targets()
available_effective_dates: list[str] = []
if not targets_df.empty:
    mask = targets_df["portfolio_id"].astype(str) == selected_portfolio
    dates = targets_df.loc[mask, "effective_date"].dropna().astype(str).unique().tolist()
    available_effective_dates = sorted(dates)

if available_effective_dates:
    default_effective = available_effective_dates[-1]
    selected_effective_date = st.selectbox(
        "生效日期",
        options=available_effective_dates,
        index=len(available_effective_dates) - 1,
    )
else:
    default_effective = date.today().isoformat()
    selected_effective_date = st.text_input("生效日期（手动输入）", value=default_effective)

listings_df = universe_store.load_listings()
portfolio_listing_ids: list[str] = []
if not targets_df.empty:
    scoped_targets = targets_df[targets_df["portfolio_id"].astype(str) == selected_portfolio].copy()
    if not scoped_targets.empty:
        scoped_targets["target_weight"] = pd.to_numeric(scoped_targets["target_weight"], errors="coerce").fillna(0.0)
        portfolio_listing_ids = (
            scoped_targets.loc[scoped_targets["target_weight"] > 0, "listing_id"].astype(str).dropna().unique().tolist()
        )

benchmark_candidates: list[str] = []
if not listings_df.empty and "listing_id" in listings_df.columns:
    candidate_df = listings_df.copy()
    if portfolio_listing_ids:
        scoped_listing_rows = listings_df[listings_df["listing_id"].astype(str).isin(portfolio_listing_ids)]
        regions = {str(x) for x in scoped_listing_rows["region"].dropna().astype(str).tolist() if str(x).strip()}
        exchanges = {str(x) for x in scoped_listing_rows["exchange"].dropna().astype(str).tolist() if str(x).strip()}
        filtered = candidate_df
        if regions and "region" in filtered.columns:
            filtered = filtered[filtered["region"].astype(str).isin(regions)]
        if exchanges and "exchange" in filtered.columns:
            filtered = filtered[filtered["exchange"].astype(str).isin(exchanges)]
        if not filtered.empty:
            candidate_df = filtered
    benchmark_candidates = sorted(candidate_df["listing_id"].dropna().astype(str).unique().tolist())

benchmark_options = ["无基准"] + benchmark_candidates
selected_benchmark_label = st.selectbox("基准标的", options=benchmark_options, index=0)
selected_benchmark = None if selected_benchmark_label == "无基准" else selected_benchmark_label

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("开始日期", value=date.today() - pd.Timedelta(days=120))
with col2:
    end_date = st.date_input("结束日期", value=date.today())

show_contribution = st.checkbox("显示资产贡献（Contribution）", value=True)
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
            benchmark_listing_id=selected_benchmark,
        )
    except ValueError as exc:
        st.error(str(exc))
        st.info("提示：请先去 Slice2（数据覆盖/缺口）检查 Coverage 缺口，或缩小日期区间后重试。")
    except Exception as exc:  # noqa: BLE001
        st.error(f"运行失败: {exc}")
    else:
        nav_df = result["nav_df"].copy()
        bench_nav_df = result.get("bench_nav_df", pd.DataFrame()).copy()
        excess_nav_df = result.get("excess_nav_df", pd.DataFrame()).copy()
        metrics = result["metrics"]
        meta = result["meta"]
        contribution_df = result.get("contribution_df", pd.DataFrame()).copy()
        turnover_df = result.get("turnover_df", pd.DataFrame()).copy()

        st.subheader("核心指标")
        display_metrics = pd.DataFrame([metrics]).copy()
        pct_fields = [
            "total_return",
            "cagr",
            "annual_vol",
            "max_drawdown",
            "avg_daily_turnover",
            "total_turnover",
            "tracking_error",
            "excess_total_return",
            "excess_cagr",
        ]
        for key in pct_fields:
            if key in display_metrics.columns:
                display_metrics[key] = display_metrics[key].map(_format_pct)
        for ratio_col in ["sharpe", "information_ratio"]:
            if ratio_col in display_metrics.columns:
                display_metrics[ratio_col] = pd.to_numeric(display_metrics[ratio_col], errors="coerce").round(2)
        if "rebalance_count" in display_metrics.columns:
            display_metrics["rebalance_count"] = (
                pd.to_numeric(display_metrics["rebalance_count"], errors="coerce").fillna(0).astype(int)
            )
        rename_map = {
            "benchmark_id": "基准标的",
            "total_return": "总收益",
            "cagr": "年化收益(CAGR)",
            "annual_vol": "年化波动",
            "sharpe": "夏普",
            "max_drawdown": "最大回撤",
            "max_drawdown_start": "回撤开始",
            "max_drawdown_end": "回撤结束",
            "sample_days": "样本天数",
            "start_date": "起始日期",
            "end_date": "结束日期",
            "total_turnover": "总换手",
            "avg_daily_turnover": "平均日换手",
            "rebalance_count": "调仓次数",
            "weight_sum_note": "权重说明",
            "tracking_error": "跟踪误差(年化)",
            "information_ratio": "信息比率(IR)",
            "excess_total_return": "超额总收益",
            "excess_cagr": "超额年化收益",
        }
        display_metrics = display_metrics.rename(columns={k: v for k, v in rename_map.items() if k in display_metrics.columns})
        st.dataframe(display_metrics, use_container_width=True, hide_index=True)

        st.subheader("换手率（Turnover）")
        turn_col1, turn_col2, turn_col3 = st.columns(3)
        turn_col1.metric("调仓次数", f"{int(metrics.get('rebalance_count', 0))}")
        turn_col2.metric("总换手", _format_pct(metrics.get("total_turnover", 0.0)))
        turn_col3.metric("平均日换手", _format_pct(metrics.get("avg_daily_turnover", 0.0)))
        if not turnover_df.empty:
            st.dataframe(turnover_df, use_container_width=True, hide_index=True)

        st.subheader("净值曲线（组合 vs 基准）")
        nav_plot = nav_df.rename(columns={"nav": "组合净值"})
        nav_long_frames = [
            nav_plot.assign(序列="组合净值").rename(columns={"组合净值": "净值"})[["ts", "净值", "序列"]]
        ]
        if not bench_nav_df.empty and "bench_nav" in bench_nav_df.columns:
            bench_plot = bench_nav_df.rename(columns={"bench_nav": "基准净值"})
            nav_long_frames.append(
                bench_plot.assign(序列="基准净值").rename(columns={"基准净值": "净值"})[["ts", "净值", "序列"]]
            )
        nav_long = pd.concat(nav_long_frames, ignore_index=True)
        fig_nav = px.line(nav_long, x="ts", y="净值", color="序列", title="组合净值 vs 基准净值")
        st.plotly_chart(fig_nav, use_container_width=True)

        if not excess_nav_df.empty and "excess_nav" in excess_nav_df.columns:
            st.subheader("超额净值")
            fig_excess_nav = px.line(excess_nav_df, x="ts", y="excess_nav", title="超额净值")
            st.plotly_chart(fig_excess_nav, use_container_width=True)

        st.subheader("回撤（Drawdown）")
        nav_series = nav_df.set_index("ts")["nav"]
        drawdown = (nav_series / nav_series.cummax() - 1.0).rename("drawdown").reset_index()
        fig_dd = px.line(drawdown, x="ts", y="drawdown", title="回撤（Drawdown）")
        st.plotly_chart(fig_dd, use_container_width=True)

        if show_contribution and not contribution_df.empty:
            st.subheader("资产贡献（Contribution）")
            tab_daily, tab_cum = st.tabs(["资产贡献（按日）", "累计贡献"])
            contrib_long = (
                contribution_df.reset_index()
                .melt(id_vars=["ts"], var_name="资产", value_name="贡献")
                .dropna(subset=["贡献"])
            )
            with tab_daily:
                fig_contrib = px.area(
                    contrib_long,
                    x="ts",
                    y="贡献",
                    color="资产",
                    title="资产贡献（按日）",
                )
                fig_contrib.update_layout(legend_title_text="资产")
                st.plotly_chart(fig_contrib, use_container_width=True)
            with tab_cum:
                contrib_cum = contrib_long.copy()
                contrib_cum["累计贡献"] = contrib_cum.groupby("资产")["贡献"].cumsum()
                fig_contrib_cum = px.area(
                    contrib_cum,
                    x="ts",
                    y="累计贡献",
                    color="资产",
                    title="累计贡献",
                )
                fig_contrib_cum.update_layout(legend_title_text="资产")
                st.plotly_chart(fig_contrib_cum, use_container_width=True)

        st.subheader("运行信息")
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
            merged.loc[merged["name"] == "", "name"] = "(未知名称)"
            merged["target_weight"] = pd.to_numeric(merged["target_weight"], errors="coerce").fillna(0.0).map(_format_pct)
            merged = merged.rename(
                columns={
                    "listing_id": "标的ID",
                    "name": "名称",
                    "target_weight": "目标权重",
                }
            )
            st.subheader("组合成分（目标权重）")
            st.dataframe(
                merged[["标的ID", "名称", "目标权重"]],
                use_container_width=True,
                hide_index=True,
            )
