from __future__ import annotations

import pandas as pd
import pytest

from quantlab.analytics.portfolio_analytics import run_portfolio_analytics
from quantlab.market.store import MarketStore
from quantlab.portfolio.store import PortfolioStore


def _prepare_stores(tmp_path):
    universe_dir = tmp_path / "universe"
    market_dir = tmp_path / "market"
    portfolio_dir = tmp_path / "portfolio"

    universe_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"listing_id": "AAA", "region": "CN", "exchange": "SSE", "currency": "CNY"},
            {"listing_id": "BBB", "region": "CN", "exchange": "SSE", "currency": "CNY"},
        ]
    ).to_parquet(universe_dir / "listings.parquet", index=False)

    market_store = MarketStore(base_dir=str(market_dir), universe_dir=str(universe_dir))
    portfolio_store = PortfolioStore(base_dir=str(portfolio_dir))

    portfolio_store.upsert_target("p1", "2024-01-01", "AAA", 0.5)
    portfolio_store.upsert_target("p1", "2024-01-01", "BBB", 0.5)

    return market_store, portfolio_store


def test_run_portfolio_analytics_end_date_inclusive(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    market_store, portfolio_store = _prepare_stores(tmp_path)

    bars = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
            "listing_id": ["AAA", "AAA", "BBB", "BBB"],
            "close": [100.0, 101.0, 200.0, 202.0],
        }
    )
    market_store.write_curated(bars, freq="1d")

    result = run_portfolio_analytics(
        portfolio_id="p1",
        effective_date="2024-01-01",
        start="2024-01-01",
        end="2024-01-02",
        portfolio_store=portfolio_store,
        market_store=market_store,
    )

    assert result["meta"]["aligned_end"] == "2024-01-02"
    assert result["meta"]["n_rows_port_ret"] == 1


def test_run_portfolio_analytics_not_drop_single_asset_gap(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    market_store, portfolio_store = _prepare_stores(tmp_path)

    bars = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-01",
                    "2024-01-03",
                ]
            ),
            "listing_id": ["AAA", "AAA", "AAA", "BBB", "BBB"],
            "close": [100.0, 110.0, 121.0, 200.0, 220.0],
        }
    )
    market_store.write_curated(bars, freq="1d")

    result = run_portfolio_analytics(
        portfolio_id="p1",
        effective_date="2024-01-01",
        start="2024-01-01",
        end="2024-01-03",
        portfolio_store=portfolio_store,
        market_store=market_store,
    )

    returns = result["returns_df"].set_index("ts")["ret"]

    assert pd.Timestamp("2024-01-02") in returns.index
    assert pd.Timestamp("2024-01-03") in returns.index
    assert returns.loc[pd.Timestamp("2024-01-02")] == pytest.approx(0.1)
    assert returns.loc[pd.Timestamp("2024-01-03")] == pytest.approx(0.1)
