"""单实验详情页。"""

from __future__ import annotations

from pathlib import Path
import json
import sys
from typing import Any

import pandas as pd
import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from components import empty_state, page_header, section
from i18n import t
from quantlab.assets import get_asset_class, group_weights_by_asset_class, load_assets_map

st.set_page_config(page_title=t("detail.title"), page_icon="🔎", layout="wide")

ASSETS_PATH = Path("data/assets.yaml")


def _safe_read_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_read_yaml(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _pick_metric(metrics: dict, *keys: str):
    for key in keys:
        if key in metrics and metrics[key] is not None:
            return metrics[key]
    for block in ("summary", "risk", "performance", "trading", "trade"):
        sub = metrics.get(block, {})
        if isinstance(sub, dict):
            for key in keys:
                if key in sub and sub[key] is not None:
                    return sub[key]
    return None


def _to_percent_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


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
    if "nav" in df.columns:
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    return df.dropna(subset=[c for c in ["ts", "nav"] if c in df.columns]).sort_values("ts")


def _calc_nav_stats(equity_df: pd.DataFrame) -> dict[str, Any]:
    if equity_df.empty or not {"ts", "nav"}.issubset(equity_df.columns):
        return {}
    df = equity_df[["ts", "nav"]].copy().dropna()
    if len(df) < 2:
        return {}
    ret = df["nav"].pct_change().dropna()
    total_return = df["nav"].iloc[-1] / df["nav"].iloc[0] - 1 if df["nav"].iloc[0] != 0 else float("nan")
    days = max((df["ts"].iloc[-1] - df["ts"].iloc[0]).days, 1)
    cagr = (df["nav"].iloc[-1] / df["nav"].iloc[0]) ** (365.0 / days) - 1 if df["nav"].iloc[0] > 0 else float("nan")
    annual_vol = ret.std() * (252 ** 0.5)
    downside = ret[ret < 0]
    downside_vol = downside.std() * (252 ** 0.5) if not downside.empty else float("nan")
    win_rate = (ret > 0).mean() if not ret.empty else float("nan")

    dd = df["nav"] / df["nav"].cummax() - 1
    max_dd = dd.min()
    dd_end = dd.idxmin() if not dd.empty else None
    dd_start = df["nav"].iloc[: dd_end + 1].idxmax() if dd_end is not None else None
    dd_start_date = df.iloc[dd_start]["ts"] if dd_start is not None else None
    dd_end_date = df.iloc[dd_end]["ts"] if dd_end is not None else None
    dd_days = (dd_end_date - dd_start_date).days if dd_start_date is not None and dd_end_date is not None else None

    return {
        "start": df["ts"].iloc[0],
        "end": df["ts"].iloc[-1],
        "points": len(df),
        "total_return": total_return,
        "cagr": cagr,
        "annual_vol": annual_vol,
        "downside_vol": downside_vol,
        "win_rate": win_rate,
        "max_drawdown": max_dd,
        "dd_start": dd_start_date,
        "dd_end": dd_end_date,
        "dd_days": dd_days,
        "best_period": ret.max() if not ret.empty else float("nan"),
        "worst_period": ret.min() if not ret.empty else float("nan"),
    }


def _display(v: Any, percent: bool = False, digits: int = 3) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    if percent:
        return f"{float(v) * 100:.2f}%"
    if isinstance(v, pd.Timestamp):
        return v.date().isoformat()
    if isinstance(v, (int, float)):
        return f"{float(v):.{digits}f}"
    return str(v)


def _render_header(run_id: str, config: dict) -> None:
    strategy = config.get("strategy")
    if isinstance(strategy, dict):
        strategy = strategy.get("name") or strategy.get("id")
    start = config.get("start") or config.get("start_date")
    end = config.get("end") or config.get("end_date")
    page_header(t("detail.title"), t("detail.caption", run_id=run_id, started=f"{start or '-'} ~ {end or '-'}"), right=f"{t('runs.strategy')}: {strategy or '-'}")


def _metric_text(metrics: dict, key_pairs: tuple[str, ...], *, percent: bool = False, digits: int = 3) -> str:
    value = _pick_metric(metrics, *key_pairs)
    if value is None or pd.isna(value):
        return "-"
    if percent:
        return f"{float(value) * 100:.2f}%"
    return f"{float(value):.{digits}f}"


def _render_overview(metrics: dict, equity_df: pd.DataFrame) -> None:
    stats = _calc_nav_stats(equity_df)

    section(t("detail.group_return"))
    r1 = st.columns(4)
    r1[0].metric(t("detail.total_return"), _metric_text(metrics, ("total_return", "return_total"), percent=True))
    r1[1].metric(t("runs.cagr"), _metric_text(metrics, ("cagr", "annual_return", "annualized_return"), percent=True))
    r1[2].metric(t("detail.sharpe"), _metric_text(metrics, ("sharpe", "sharpe_ratio"), digits=3))
    r1[3].metric(t("detail.win_rate"), _display(stats.get("win_rate"), percent=True) if stats else "-")

    section(t("detail.group_risk"))
    r2 = st.columns(4)
    r2[0].metric(t("detail.max_drawdown"), _metric_text(metrics, ("max_drawdown", "drawdown_max"), percent=True))
    r2[1].metric(t("detail.annual_vol"), _display(stats.get("annual_vol"), percent=True) if stats else "-")
    r2[2].metric(t("detail.dd_days"), _display(stats.get("dd_days"), digits=0) if stats else "-")
    dd_range = f"{_display(stats.get('dd_start'))} ~ {_display(stats.get('dd_end'))}" if stats else "-"
    r2[3].metric(t("detail.dd_range"), dd_range)

    section(t("detail.group_cost"))
    r3 = st.columns(4)
    r3[0].metric(t("detail.turnover"), _metric_text(metrics, ("turnover", "avg_turnover"), percent=True))
    r3[1].metric(t("detail.total_fees"), _metric_text(metrics, ("total_fees", "fees_total", "fee_total"), digits=2))
    r3[2].metric(t("detail.impact_cost"), _metric_text(metrics, ("impact_cost", "total_impact_cost"), digits=2))
    r3[3].metric(t("detail.total_cost"), _metric_text(metrics, ("total_cost", "cost_total"), digits=2))

    section(t("detail.nav_summary"))
    if not stats:
        empty_state(t("detail.no_equity"))
        return

    r4 = st.columns(4)
    r4[0].metric(t("detail.sample_range"), f"{_display(stats.get('start'))} ~ {_display(stats.get('end'))}")
    r4[1].metric(t("detail.sample_points"), _display(stats.get("points"), digits=0))
    r4[2].metric(t("detail.downside_vol"), _display(stats.get("downside_vol"), percent=True))
    r4[3].metric(t("detail.best_period"), _display(stats.get("best_period"), percent=True))

    st.caption(f"{t('detail.worst_period')}: {_display(stats.get('worst_period'), percent=True)}")


def _render_nav_and_drawdown(equity_df: pd.DataFrame, metrics: dict) -> None:
    if equity_df.empty or not {"ts", "nav"}.issubset(equity_df.columns):
        empty_state(t("detail.no_equity"))
        return
    nav_df = equity_df[["ts", "nav"]].set_index("ts")
    section(t("detail.nav_curve"))
    st.line_chart(nav_df)
    dd_curve = metrics.get("drawdown") or metrics.get("drawdown_curve")
    if isinstance(dd_curve, list) and len(dd_curve) == len(nav_df):
        dd = pd.Series(dd_curve, index=nav_df.index, name="drawdown")
    else:
        dd = nav_df["nav"] / nav_df["nav"].cummax() - 1
    section(t("detail.drawdown_curve"))
    st.line_chart(dd.to_frame(name="drawdown"))


def _render_weights(weights_df: pd.DataFrame) -> None:
    section(t("detail.allocation"))
    if weights_df.empty:
        empty_state(t("detail.no_weights"))
        return
    if {"ts", "symbol", "weight"}.issubset(weights_df.columns):
        w = weights_df.copy()
        w["ts"] = pd.to_datetime(w["ts"], errors="coerce")
        w["weight"] = pd.to_numeric(w["weight"], errors="coerce")
        view = w.dropna(subset=["ts", "symbol", "weight"]).pivot_table(index="ts", columns="symbol", values="weight", aggfunc="last")
        st.dataframe(view, use_container_width=True)
    else:
        st.dataframe(weights_df, use_container_width=True)


def _render_risk(results_dir: Path) -> None:
    section(t("detail.risk_status"))
    payload = _safe_read_json(results_dir / "risk_status.json")
    if not payload:
        empty_state(t("detail.no_risk"))
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("current_drawdown", _to_percent_text(payload.get("current_drawdown")))
    c2.metric("rolling_1y_return", _to_percent_text(payload.get("rolling_1y_return")))
    c3.metric("rolling_1y_vol", _to_percent_text(payload.get("rolling_1y_vol")))
    s = payload.get("rolling_3y_sharpe")
    c4.metric("rolling_3y_sharpe", "-" if s is None or pd.isna(s) else f"{float(s):.3f}")


def _render_yearly_and_stress(results_dir: Path, weights_df: pd.DataFrame) -> None:
    section(t("detail.yearly_analysis"))
    yearly_df = _safe_read_parquet(results_dir / "yearly_stats.parquet")
    if yearly_df.empty:
        empty_state(t("detail.no_yearly"))
    else:
        st.dataframe(yearly_df, use_container_width=True, hide_index=True)

    section(t("detail.stress_test"))
    stress_payload = _safe_read_json(results_dir / "stress_test.json")
    results = stress_payload.get("results", []) if isinstance(stress_payload, dict) else []
    if not results:
        empty_state(t("detail.no_stress"))
    else:
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

    section(t("detail.asset_class"))
    if weights_df.empty or not ASSETS_PATH.exists():
        empty_state(t("detail.no_asset_class"))
        return
    assets_map = load_assets_map(str(ASSETS_PATH))
    grouped = group_weights_by_asset_class(weights_df, assets_map) if assets_map else pd.DataFrame()
    if grouped.empty or "ts" not in grouped.columns:
        empty_state(t("detail.no_asset_class"))
        return
    class_cols = [c for c in grouped.columns if c != "ts"]
    if not class_cols:
        empty_state(t("detail.no_asset_class"))
        return
    st.line_chart(grouped.set_index("ts")[class_cols])

    symbol_cols = [c for c in weights_df.columns if c != "ts"]
    missing_symbols = sorted({str(s) for s in symbol_cols if get_asset_class(str(s), assets_map) == "other" and str(s) not in assets_map})
    if missing_symbols:
        st.caption(t("detail.unmapped_symbols") + ": " + ", ".join(missing_symbols))


def main() -> None:
    run_id = st.session_state.get("selected_run_id")
    if not run_id:
        empty_state(t("detail.no_run"))
        return
    run_dir = Path("runs") / str(run_id)
    results_dir = run_dir / "results"

    metrics = _safe_read_json(results_dir / "metrics.json")
    config = _safe_read_yaml(run_dir / "config.yaml")
    equity_df = _normalize_equity_df(_safe_read_parquet(results_dir / "equity_curve.parquet"))
    weights_df = _safe_read_parquet(results_dir / "weights.parquet")

    _render_header(str(run_id), config)
    tabs = st.tabs([t("detail.tab_overview"), t("detail.tab_nav_dd"), t("detail.tab_position"), t("detail.tab_risk"), t("detail.tab_yearly_stress")])
    with tabs[0]:
        _render_overview(metrics, equity_df)
    with tabs[1]:
        _render_nav_and_drawdown(equity_df, metrics)
    with tabs[2]:
        _render_weights(weights_df)
    with tabs[3]:
        _render_risk(results_dir)
    with tabs[4]:
        _render_yearly_and_stress(results_dir, weights_df)


if __name__ == "__main__":
    main()
