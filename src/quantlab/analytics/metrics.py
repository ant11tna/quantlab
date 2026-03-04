from __future__ import annotations

import math

import pandas as pd


def compute_metrics(returns: pd.Series, nav: pd.Series, periods_per_year: int = 252) -> dict:
    """Compute core portfolio analytics metrics from return and NAV series."""
    rets = pd.Series(returns).dropna()
    nav_series = pd.Series(nav).dropna()

    if rets.empty or nav_series.empty:
        raise ValueError("Cannot compute metrics on empty returns/nav series")

    nav_start = float(nav_series.iloc[0])
    nav_end = float(nav_series.iloc[-1])
    sample_days = int(len(rets))

    total_return = (nav_end / nav_start) - 1.0
    cagr = (nav_end / nav_start) ** (periods_per_year / sample_days) - 1.0

    vol = float(rets.std(ddof=1)) if sample_days > 1 else 0.0
    annual_vol = vol * math.sqrt(periods_per_year)

    mean_ret = float(rets.mean())
    sharpe = None
    if vol > 0:
        sharpe = (mean_ret / vol) * math.sqrt(periods_per_year)

    rolling_peak = nav_series.cummax()
    drawdown = nav_series / rolling_peak - 1.0
    max_drawdown = float(drawdown.min())
    max_dd_end_idx = drawdown.idxmin()

    peak_nav = float(rolling_peak.loc[max_dd_end_idx])
    drawdown_start_idx = nav_series.loc[:max_dd_end_idx][nav_series.loc[:max_dd_end_idx] == peak_nav].index[-1]

    start_date = pd.Timestamp(rets.index.min()).date().isoformat()
    end_date = pd.Timestamp(rets.index.max()).date().isoformat()
    max_drawdown_start = pd.Timestamp(drawdown_start_idx).date().isoformat()
    max_drawdown_end = pd.Timestamp(max_dd_end_idx).date().isoformat()

    return {
        "total_return": float(total_return),
        "cagr": float(cagr),
        "annual_vol": float(annual_vol),
        "sharpe": None if sharpe is None else float(sharpe),
        "max_drawdown": float(max_drawdown),
        "max_drawdown_start": max_drawdown_start,
        "max_drawdown_end": max_drawdown_end,
        "sample_days": sample_days,
        "start_date": start_date,
        "end_date": end_date,
    }
