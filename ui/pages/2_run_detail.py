"""单实验详情页。"""

from __future__ import annotations

from pathlib import Path
import json
import sys

import pandas as pd
import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from quantlab.assets import get_asset_class, group_weights_by_asset_class, load_assets_map


st.set_page_config(page_title="Run Detail", page_icon="🔎", layout="wide")


ASSETS_PATH = Path("data/assets.yaml")


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


def _risk_color(level: str) -> str:
    mapping = {
        "HIGH": "red",
        "ELEVATED": "orange",
        "MEDIUM": "goldenrod",
        "NORMAL": "green",
    }
    return mapping.get(str(level).upper(), "gray")


def _render_risk_status(results_dir: Path):
    st.subheader("Risk Status")
    risk_path = results_dir / "risk_status.json"
    if not risk_path.exists():
        st.info("无风险等级数据（risk_status.json）")
        return

    payload = _safe_read_json(risk_path)
    if not payload:
        st.info("风险等级数据为空")
        return

    level = str(payload.get("risk_level", "NORMAL"))
    color = _risk_color(level)
    st.markdown(f"**Current Level:** :{color}[`{level}`]")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("current_drawdown", _to_percent_text(payload.get("current_drawdown")))
    c2.metric("rolling_1y_return", _to_percent_text(payload.get("rolling_1y_return")))
    c3.metric("rolling_1y_vol", _to_percent_text(payload.get("rolling_1y_vol")))
    r3s = payload.get("rolling_3y_sharpe")
    c4.metric("rolling_3y_sharpe", "-" if r3s is None or pd.isna(r3s) else f"{float(r3s):.3f}")

    reason = payload.get("reason")
    if reason:
        st.caption(f"reason: {reason}")


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
    if required_cols.issubset(weights_df.columns):
        w = weights_df.copy()
        w["ts"] = pd.to_datetime(w["ts"], errors="coerce")
        w["weight"] = pd.to_numeric(w["weight"], errors="coerce")
        w = w.dropna(subset=["ts", "symbol", "weight"])
        heatmap_df = w.pivot_table(index="ts", columns="symbol", values="weight", aggfunc="last").sort_index()
        st.dataframe(heatmap_df, use_container_width=True)
        return

    if "ts" not in weights_df.columns:
        st.warning("权重数据字段不完整，缺少 ts")
        st.dataframe(weights_df, use_container_width=True)
        return

    wide_df = weights_df.copy()
    wide_df["ts"] = pd.to_datetime(wide_df["ts"], errors="coerce")
    wide_df = wide_df.dropna(subset=["ts"]).sort_values("ts").set_index("ts")
    st.dataframe(wide_df, use_container_width=True)


def _render_asset_class_exposure(weights_df: pd.DataFrame):
    st.subheader("资产类别暴露")
    if weights_df.empty:
        st.info("无权重数据，跳过资产类别分析")
        return

    if not ASSETS_PATH.exists():
        st.warning("未配置资产元数据（data/assets.yaml）")
        return

    assets_map = load_assets_map(str(ASSETS_PATH))
    if not assets_map:
        st.warning("未配置资产元数据（data/assets.yaml）")
        return

    grouped = group_weights_by_asset_class(weights_df, assets_map)
    if grouped.empty or "ts" not in grouped.columns:
        st.info("权重数据不可用于资产类别汇总")
        return

    symbol_cols = [c for c in weights_df.columns if c != "ts"]
    missing_symbols = sorted({str(s) for s in symbol_cols if get_asset_class(str(s), assets_map) == "other" and str(s) not in assets_map})
    if missing_symbols:
        st.warning("以下 symbol 未在 assets.yaml 配置，已归类为 other：" + ", ".join(missing_symbols))

    grouped = grouped.copy().sort_values("ts")
    class_cols = [c for c in grouped.columns if c != "ts"]
    if not class_cols:
        st.info("没有可展示的资产类别列")
        return

    latest = grouped.iloc[-1][class_cols].to_frame(name="weight")
    latest.index.name = "asset_class"
    latest = latest.reset_index()
    st.markdown("**期末资产类别权重**")
    st.dataframe(latest, use_container_width=True, hide_index=True)

    st.markdown("**资产类别权重时序曲线**")
    st.line_chart(grouped.set_index("ts")[class_cols])


def _render_yearly_and_stress_analysis(results_dir: Path):
    st.subheader("年度收益分析")
    yearly_path = results_dir / "yearly_stats.parquet"
    if not yearly_path.exists():
        st.info("无年度统计数据（yearly_stats.parquet）")
    else:
        yearly_df = _safe_read_parquet(yearly_path)
        if yearly_df.empty:
            st.info("年度统计为空")
        else:
            st.dataframe(yearly_df, use_container_width=True, hide_index=True)

    st.subheader("压力测试")
    stress_path = results_dir / "stress_test.json"
    if not stress_path.exists():
        st.info("无压力测试结果（stress_test.json）")
        return

    stress_payload = _safe_read_json(stress_path)
    results = stress_payload.get("results", []) if isinstance(stress_payload, dict) else []
    if not isinstance(results, list) or not results:
        st.info("压力测试结果为空")
        return

    stress_df = pd.DataFrame(results)
    st.dataframe(stress_df, use_container_width=True, hide_index=True)

    if "max_drawdown" in stress_df.columns:
        risky = stress_df[pd.to_numeric(stress_df["max_drawdown"], errors="coerce") < -0.3]
        if not risky.empty:
            years = ", ".join(str(int(y)) for y in risky["year"].tolist() if pd.notna(y))
            st.error(f"⚠️ 压力年份最大回撤超过 -30%：{years}")


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
    _render_risk_status(results_dir)
    st.divider()

    _render_metric_cards(metrics)
    st.divider()

    _render_nav_and_drawdown(equity_df, metrics)
    st.divider()

    _render_weights_heatmap(weights_df)
    st.divider()

    _render_asset_class_exposure(weights_df)
    st.divider()

    _render_yearly_and_stress_analysis(results_dir)


if __name__ == "__main__":
    main()
