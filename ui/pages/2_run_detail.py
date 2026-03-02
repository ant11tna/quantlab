"""单实验详情页。"""

from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import streamlit as st
import yaml


st.set_page_config(page_title="Run Detail", page_icon="🔎", layout="wide")


def _safe_read_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        st.warning(f"文件不存在：{path}")
    except Exception as e:
        st.error(f"读取 JSON 失败：{path.name}（{e}）")
    return {}


def _safe_read_yaml(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        st.warning(f"文件不存在：{path}")
    except Exception as e:
        st.error(f"读取 YAML 失败：{path.name}（{e}）")
    return {}


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        st.warning(f"文件不存在：{path}")
    except Exception as e:
        st.error(f"读取 Parquet 失败：{path.name}（{e}）")
    return pd.DataFrame()


def _pick_metric(metrics: dict, *keys: str):
    for key in keys:
        if key in metrics and metrics[key] is not None:
            return metrics[key]
    return None


def _to_percent_text(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.2f}%"


def _normalize_equity_df(equity_df: pd.DataFrame) -> pd.DataFrame:
    if equity_df.empty:
        return equity_df

    df = equity_df.copy()
    if "ts" not in df.columns:
        if "date" in df.columns:
            df = df.rename(columns={"date": "ts"})
        elif "time" in df.columns:
            df = df.rename(columns={"time": "ts"})

    if "nav" not in df.columns:
        for candidate in ("equity", "portfolio_value", "value"):
            if candidate in df.columns:
                df = df.rename(columns={candidate: "nav"})
                break

    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.dropna(subset=["ts"]).sort_values("ts")

    if "nav" in df.columns:
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["nav"])

    return df


def _render_header(run_id: str, config: dict):
    strategy = config.get("strategy")
    if isinstance(strategy, dict):
        strategy = strategy.get("name") or strategy.get("id")

    start = config.get("start") or config.get("start_date")
    end = config.get("end") or config.get("end_date")

    rebalance = config.get("rebalance") or config.get("rebalance_freq") or config.get("frequency")
    fee = config.get("fee")
    if fee is None and isinstance(config.get("broker"), dict):
        fee = config["broker"].get("fee_rate")

    st.title("🔎 单实验详情")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"**run_id**: `{run_id}`")
    c2.markdown(f"**strategy**: {strategy or '-'}")
    c3.markdown(f"**start/end**: {start or '-'} ~ {end or '-'}")
    fee_text = f"{fee:.4f}" if isinstance(fee, (int, float)) else (fee or "-")
    c4.markdown(f"**参数摘要**: rebalance={rebalance or '-'}, fee={fee_text}")


def _render_metric_cards(metrics: dict):
    total_return = _pick_metric(metrics, "total_return")
    cagr = _pick_metric(metrics, "cagr", "annual_return")
    sharpe = _pick_metric(metrics, "sharpe", "sharpe_ratio")
    max_drawdown = _pick_metric(metrics, "max_drawdown")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("total_return", _to_percent_text(total_return))
    c2.metric("cagr", _to_percent_text(cagr))
    c3.metric("sharpe", "-" if sharpe is None or pd.isna(sharpe) else f"{float(sharpe):.3f}")
    c4.metric("max_drawdown", _to_percent_text(max_drawdown))


def _render_nav_and_drawdown(equity_df: pd.DataFrame, metrics: dict):
    st.subheader("NAV 曲线")
    if equity_df.empty or not {"ts", "nav"}.issubset(equity_df.columns):
        st.info("无净值数据")
        return

    nav_df = equity_df[["ts", "nav"]].set_index("ts")
    st.line_chart(nav_df)

    st.subheader("回撤曲线")
    drawdown_series = None
    drawdown_curve = metrics.get("drawdown") or metrics.get("drawdown_curve")
    if isinstance(drawdown_curve, list) and len(drawdown_curve) == len(nav_df):
        drawdown_series = pd.Series(drawdown_curve, index=nav_df.index, name="drawdown")

    if drawdown_series is None:
        rolling_max = nav_df["nav"].cummax()
        drawdown_series = nav_df["nav"] / rolling_max - 1
        drawdown_series.name = "drawdown"

    st.line_chart(drawdown_series.to_frame())


def _render_weights_heatmap(weights_df: pd.DataFrame):
    st.subheader("权重热力图")
    if weights_df.empty:
        st.info("无权重数据")
        return

    required_cols = {"ts", "symbol", "weight"}
    if not required_cols.issubset(weights_df.columns):
        st.warning("权重数据字段不完整，期望包含: ts, symbol, weight")
        st.dataframe(weights_df, use_container_width=True)
        return

    w = weights_df.copy()
    w["ts"] = pd.to_datetime(w["ts"], errors="coerce")
    w["weight"] = pd.to_numeric(w["weight"], errors="coerce")
    w = w.dropna(subset=["ts", "symbol", "weight"])

    heatmap_df = w.pivot_table(index="ts", columns="symbol", values="weight", aggfunc="last").sort_index()
    st.dataframe(heatmap_df, use_container_width=True)


def main():
    run_id = st.session_state.get("selected_run_id")
    if not run_id:
        st.warning("请先在 Runs 页面选择实验")
        return

    run_dir = Path("runs") / str(run_id)
    results_dir = run_dir / "results"

    metrics = _safe_read_json(results_dir / "metrics.json")
    config = _safe_read_yaml(run_dir / "config.yaml")
    equity_df = _normalize_equity_df(_safe_read_parquet(results_dir / "equity_curve.parquet"))

    weights_path = results_dir / "weights.parquet"
    weights_df = _safe_read_parquet(weights_path) if weights_path.exists() else pd.DataFrame()

    _render_header(str(run_id), config)
    _render_metric_cards(metrics)
    st.divider()

    _render_nav_and_drawdown(equity_df, metrics)
    st.divider()

    _render_weights_heatmap(weights_df)


if __name__ == "__main__":
    main()
