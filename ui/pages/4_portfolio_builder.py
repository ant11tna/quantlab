from __future__ import annotations

from datetime import datetime, timedelta, timezone

import ui.bootstrap  # noqa: F401

import pandas as pd
import plotly.express as px
import streamlit as st

from ui.components import symbol_search_pro_component
from quantlab.market.coverage import compute_portfolio_coverage
from quantlab.market.store import MarketStore
from quantlab.portfolio.exposure import compute_concentration, compute_exposure
from quantlab.portfolio import PortfolioStore, enrich_targets_with_universe, normalize_weights, validate_weights
from quantlab.universe import resolver
from quantlab.universe.store import UniverseStore
from quantlab.universe.types import Candidate

DEFAULT_PORTFOLIO_ID = "default"


st.set_page_config(page_title="Portfolio Builder", page_icon="🧩", layout="wide")
st.title("Portfolio Builder")

portfolio_store = PortfolioStore(base_dir="data/portfolio")
market_store = MarketStore(base_dir="data/market", universe_dir="data/universe")
universe_store = UniverseStore(base_dir="data/universe")
portfolio_store.ensure_default_portfolio(
    portfolio_id=DEFAULT_PORTFOLIO_ID,
    name="Default Portfolio",
    base_currency="CNY",
)

active_default = portfolio_store.get_active_effective_date(DEFAULT_PORTFOLIO_ID)
selected_effective_date = st.date_input(
    "当前 effective_date",
    value=pd.to_datetime(active_default).date(),
    help="切换查看/编辑某个生效日的目标权重。",
)
effective_date = selected_effective_date.isoformat()


def _current_targets() -> pd.DataFrame:
    targets = portfolio_store.load_targets()
    if targets.empty:
        return pd.DataFrame(columns=["portfolio_id", "effective_date", "listing_id", "target_weight", "added_at"])
    mask = (
        (targets["portfolio_id"].astype(str) == DEFAULT_PORTFOLIO_ID)
        & (targets["effective_date"].astype(str) == effective_date)
    )
    out = targets.loc[mask].copy()
    if out.empty:
        return pd.DataFrame(columns=targets.columns)
    out["target_weight"] = pd.to_numeric(out["target_weight"], errors="coerce").fillna(0.0)
    return out.sort_values("listing_id").reset_index(drop=True)


def _decorate_with_universe(df: pd.DataFrame) -> pd.DataFrame:
    return enrich_targets_with_universe(df)


def _decorate_for_exposure(df: pd.DataFrame) -> pd.DataFrame:
    out = _decorate_with_universe(df)
    if out.empty:
        return out

    listings = universe_store.load_listings()
    instruments = universe_store.load_instruments()

    if "currency" not in out.columns:
        out["currency"] = ""
    if not listings.empty:
        listing_cols = ["listing_id", "currency"]
        for col in listing_cols:
            if col not in listings.columns:
                listings[col] = None
        out = out.drop(columns=["currency"], errors="ignore").merge(listings[listing_cols], how="left", on="listing_id")

    if "asset_type" not in out.columns:
        out["asset_type"] = ""
    if not instruments.empty:
        instrument_cols = ["instrument_id", "asset_type"]
        for col in instrument_cols:
            if col not in instruments.columns:
                instruments[col] = None
        out = out.drop(columns=["asset_type"], errors="ignore").merge(
            instruments[instrument_cols], how="left", on="instrument_id"
        )

    for col in ["region", "exchange", "ticker", "name", "asset_type", "currency"]:
        if col not in out.columns:
            out[col] = "unknown"
        cleaned = out[col].fillna("").astype(str).str.strip()
        out[col] = cleaned.where(cleaned != "", "unknown")
    return out


def _format_exposure_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["weight_pct"] = pd.to_numeric(out["weight_pct"], errors="coerce").fillna(0.0).map(lambda x: f"{x:.2f}%")
    return out


def _save_editor_rows(editor_df: pd.DataFrame) -> None:
    rows = editor_df.copy()
    if rows.empty:
        existing = portfolio_store.load_targets()
        keep = ~(
            (existing["portfolio_id"].astype(str) == DEFAULT_PORTFOLIO_ID)
            & (existing["effective_date"].astype(str) == effective_date)
        )
        portfolio_store.save_targets(existing.loc[keep].reset_index(drop=True))
        return

    rows["listing_id"] = rows["listing_id"].astype(str).str.strip()
    rows = rows[rows["listing_id"] != ""].copy()
    if "target_weight_pct" in rows.columns:
        rows["target_weight_pct"] = pd.to_numeric(rows["target_weight_pct"], errors="coerce").fillna(0.0)
        rows["target_weight"] = rows["target_weight_pct"] / 100.0
    else:
        rows["target_weight"] = pd.to_numeric(rows["target_weight"], errors="coerce").fillna(0.0)

    existing = portfolio_store.load_targets()
    keep = ~(
        (existing["portfolio_id"].astype(str) == DEFAULT_PORTFOLIO_ID)
        & (existing["effective_date"].astype(str) == effective_date)
    )
    base = existing.loc[keep].reset_index(drop=True)

    now_iso = datetime.now(timezone.utc).isoformat()
    new_rows = []
    for row in rows.itertuples(index=False):
        new_rows.append(
            {
                "portfolio_id": DEFAULT_PORTFOLIO_ID,
                "effective_date": effective_date,
                "listing_id": str(row.listing_id),
                "target_weight": float(row.target_weight),
                "added_at": now_iso,
            }
        )
    final_df = pd.concat([base, pd.DataFrame(new_rows)], ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["portfolio_id", "effective_date", "listing_id"], keep="last")
    portfolio_store.save_targets(final_df)


left, right = st.columns([3, 2])

with left:
    st.subheader("添加持仓")
    symbol_search_pro_component(store=universe_store, allow_add=False)

    selected_candidate = st.session_state.get("symbol_search_pro_selected_candidate")
    selected_query = str(st.session_state.get("symbol_search_pro_query", "")).strip()
    if isinstance(selected_candidate, Candidate):
        st.caption(f"已选择: {selected_candidate.listing_id}")
        if st.button("确认加入组合", type="primary"):
            try:
                resolver.confirm(selected_query or selected_candidate.listing_id, selected_candidate, universe_store)
            except ValueError as exc:
                st.error(str(exc))
            else:
                portfolio_store.upsert_target(
                    portfolio_id=DEFAULT_PORTFOLIO_ID,
                    effective_date=effective_date,
                    listing_id=selected_candidate.listing_id,
                    target_weight=0.0,
                )
                st.success(f"已加入: {selected_candidate.listing_id}")
    else:
        st.caption("请先在候选列表中选中一个标的。")

    st.markdown("---")
    st.subheader("当前组合（可编辑）")

    targets = _current_targets()
    decorated = _decorate_with_universe(targets)

    if decorated.empty:
        st.info("该 effective_date 下暂无持仓。")
        editable_df = pd.DataFrame(columns=["listing_id", "target_weight_pct"])
    else:
        decorated["target_weight_pct"] = pd.to_numeric(decorated["target_weight"], errors="coerce").fillna(0.0) * 100.0
        st.dataframe(
            decorated[["region", "exchange", "ticker", "name", "target_weight_pct", "listing_id"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "target_weight_pct": st.column_config.NumberColumn("target_weight_pct", format="%.2f%%"),
            },
        )
        editable_df = decorated[["listing_id", "target_weight_pct"]].copy()

    edited = st.data_editor(
        editable_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key="portfolio_targets_editor",
        column_config={
            "listing_id": st.column_config.TextColumn("listing_id"),
            "target_weight_pct": st.column_config.NumberColumn(
                "target_weight_pct",
                min_value=0.0,
                max_value=100.0,
                step=0.1,
                format="%.2f%%",
            ),
        },
    )

    c1, c2, c3 = st.columns(3)
    if c1.button("保存编辑", use_container_width=True):
        _save_editor_rows(edited)
        st.success("目标权重已保存。")
        st.rerun()

    if c2.button("归一化权重", use_container_width=True):
        edited_for_normalize = edited.copy()
        edited_for_normalize["target_weight"] = pd.to_numeric(
            edited_for_normalize["target_weight_pct"], errors="coerce"
        ).fillna(0.0) / 100.0
        normalized = normalize_weights(edited_for_normalize[["listing_id", "target_weight"]])
        normalized["target_weight_pct"] = pd.to_numeric(normalized["target_weight"], errors="coerce").fillna(0.0) * 100.0
        _save_editor_rows(normalized[["listing_id", "target_weight_pct"]])
        st.success("已归一化并保存。")
        st.rerun()

    delete_listing_id = c3.text_input("删除 listing_id", value="", placeholder="LISTING:CN:SH:600519")
    if st.button("删除选中行（按 listing_id）") and delete_listing_id.strip():
        portfolio_store.remove_target(DEFAULT_PORTFOLIO_ID, effective_date, delete_listing_id.strip())
        st.success(f"已删除: {delete_listing_id.strip()}")
        st.rerun()

    edited_for_validate = edited.copy()
    edited_for_validate["target_weight"] = pd.to_numeric(edited_for_validate["target_weight_pct"], errors="coerce").fillna(0.0) / 100.0
    total, is_close, message = validate_weights(edited_for_validate[["target_weight"]])
    st.write(f"权重合计: {total:.6f}（合计 {total * 100:.2f}%）")
    if total == 0:
        st.info(message)
    elif is_close:
        st.success(message)
    else:
        st.warning(message)

with right:
    st.subheader("权重扇形图")
    chart_df = _decorate_with_universe(_current_targets())
    if chart_df.empty:
        st.info("暂无组合数据。")
    else:
        chart_df["target_weight"] = pd.to_numeric(chart_df["target_weight"], errors="coerce").fillna(0.0)
        chart_df["target_weight_pct"] = chart_df["target_weight"] * 100.0
        total, is_close, _ = validate_weights(chart_df[["target_weight"]])
        if total == 0:
            st.info("请填写权重（当前全部为 0%）。")
        else:
            if not is_close:
                st.warning(f"当前权重合计 {total:.6f}（{total * 100:.2f}%），未接近 1.0，仍可查看分布。")
            name_candidate = chart_df["name"].astype(str).str.strip()
            ticker_candidate = chart_df["ticker"].astype(str).str.strip()
            listing_candidate = chart_df["listing_id"].astype(str).str.strip()
            name_missing = name_candidate.isin(["", "(name unknown)", "(unknown listing)"])
            ticker_missing = ticker_candidate.isin(["", "(ticker unknown)", "(unknown listing)"])
            chart_df["display_name"] = name_candidate
            chart_df.loc[name_missing, "display_name"] = ticker_candidate
            chart_df.loc[name_missing & ticker_missing, "display_name"] = listing_candidate
            fig = px.pie(
                chart_df,
                values="target_weight_pct",
                names="display_name",
                hover_data=["listing_id", "target_weight", "target_weight_pct", "region", "exchange", "ticker"],
                title=f"{DEFAULT_PORTFOLIO_ID} @ {effective_date}",
            )
            st.plotly_chart(fig, use_container_width=True)


    st.markdown("---")
    st.subheader("组合暴露（Exposure）")

    exposure_df = _decorate_for_exposure(_current_targets())
    if exposure_df.empty:
        st.info("当前 effective_date 下暂无持仓，无法计算 Exposure。")
    else:
        exposure_df["target_weight"] = pd.to_numeric(exposure_df["target_weight"], errors="coerce").fillna(0.0)
        exposure_total = float(exposure_df["target_weight"].sum())
        if exposure_total <= 0:
            st.info("当前 targets 合计为 0，请先填写权重后再查看 Exposure。")
        else:
            region_exposure = compute_exposure(exposure_df, ["region"])
            asset_type_exposure = compute_exposure(exposure_df, ["asset_type"])
            currency_exposure = compute_exposure(exposure_df, ["currency"])
            concentration = compute_concentration(exposure_df)

            ex_col1, ex_col2, ex_col3 = st.columns(3)
            with ex_col1:
                st.caption("Region Exposure")
                st.dataframe(_format_exposure_table(region_exposure), use_container_width=True, hide_index=True)
            with ex_col2:
                st.caption("Asset Type Exposure")
                st.dataframe(_format_exposure_table(asset_type_exposure), use_container_width=True, hide_index=True)
            with ex_col3:
                st.caption("Currency Exposure")
                st.dataframe(_format_exposure_table(currency_exposure), use_container_width=True, hide_index=True)

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Top1", f"{concentration['top1_pct']:.2f}%")
            m2.metric("Top3", f"{concentration['top3_pct']:.2f}%")
            m3.metric("Top5", f"{concentration['top5_pct']:.2f}%")
            m4.metric("HHI", f"{concentration['hhi']:.4f}")

    st.markdown("---")
    st.subheader("数据覆盖 / 缺口（Slice 2）")

    default_start = selected_effective_date - timedelta(days=180)
    default_end = datetime.now(timezone.utc).date()
    cov_col1, cov_col2 = st.columns(2)
    coverage_start_date = cov_col1.date_input(
        "coverage start",
        value=default_start,
        key="coverage_start_date",
    )
    coverage_end_date = cov_col2.date_input(
        "coverage end",
        value=default_end,
        key="coverage_end_date",
    )

    if coverage_start_date > coverage_end_date:
        st.warning("开始日期不能晚于结束日期。")
    else:
        if st.button("检查数据覆盖", use_container_width=True):
            coverage_df = compute_portfolio_coverage(
                portfolio_id=DEFAULT_PORTFOLIO_ID,
                effective_date=effective_date,
                portfolio_store=portfolio_store,
                market_store=market_store,
                start=coverage_start_date.isoformat(),
                end=coverage_end_date.isoformat(),
                freq="1d",
            )
            st.session_state["portfolio_coverage_df"] = coverage_df

        coverage_df = st.session_state.get("portfolio_coverage_df")
        if isinstance(coverage_df, pd.DataFrame) and not coverage_df.empty:
            display_cols = [
                "listing_id",
                "region",
                "exchange",
                "min_ts",
                "max_ts",
                "gap_type",
                "gap_start",
                "gap_end",
                "status",
            ]
            view = coverage_df[display_cols].copy()
            st.dataframe(view, use_container_width=True, hide_index=True)

            status_missing = coverage_df["status"].astype(str).eq("missing")
            gap_type = coverage_df["gap_type"].astype(str)
            missing_count = int(status_missing.sum())
            head_gap_count = int(gap_type.str.contains("missing_head", na=False).sum())
            tail_gap_count = int(gap_type.str.contains("missing_tail", na=False).sum())
            has_gap = missing_count > 0 or (gap_type != "none").any()

            if has_gap:
                st.warning(
                    f"发现覆盖缺口：missing={missing_count}, head_gap={head_gap_count}, tail_gap={tail_gap_count}"
                )
            else:
                st.success("所选区间内，所有持仓均有边界覆盖。")
        elif isinstance(coverage_df, pd.DataFrame):
            st.info("当前 effective_date 下暂无持仓，无需检查覆盖。")

        st.caption("说明：Slice 2 仅基于 metadata.min_ts/max_ts 检查边界缺口，middle gaps not checked in Slice2。")
