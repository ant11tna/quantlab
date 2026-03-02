"""Streamlit application for QuantLab.

Main entry point for the web UI.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from quantlab.ui.components.plotly_charts import create_equity_chart, create_drawdown_chart


# Page configuration
st.set_page_config(
    page_title="QuantLab",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar
st.sidebar.title("📈 QuantLab")

# Navigation
page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Run Compare", "Replay", "Research Log"]
)

# Get available runs
runs_dir = Path("runs")
if runs_dir.exists():
    available_runs = sorted([d.name for d in runs_dir.iterdir() if d.is_dir()], reverse=True)
else:
    available_runs = []


# ============================================================================
# Dashboard Page
# ============================================================================
def dashboard_page():
    """Main dashboard showing single run details."""
    st.title("Dashboard")
    
    if not available_runs:
        st.info("No runs found. Run a backtest first.")
        return
    
    # Run selector
    selected_run = st.selectbox("Select Run", available_runs)
    run_dir = runs_dir / selected_run
    
    # Load data
    equity_path = run_dir / "equity_curve.parquet"
    weights_path = run_dir / "weights.parquet"
    trades_path = run_dir / "trades.parquet"
    metrics_path = run_dir / "metrics.json"
    
    col1, col2, col3, col4 = st.columns(4)
    
    if equity_path.exists():
        equity_df = pd.read_parquet(equity_path)
        
        # Calculate metrics
        final_nav = equity_df["nav"].iloc[-1]
        initial_nav = equity_df["nav"].iloc[0]
        total_return = (final_nav / initial_nav) - 1
        
        # Calculate max drawdown
        cummax = equity_df["nav"].cummax()
        drawdown = (equity_df["nav"] - cummax) / cummax
        max_dd = drawdown.min()
        
        # Calculate volatility and Sharpe
        returns = equity_df["nav"].pct_change().dropna()
        vol = returns.std() * (252 ** 0.5)
        sharpe = (returns.mean() * 252) / (returns.std() * (252 ** 0.5)) if vol > 0 else 0
        
        col1.metric("Total Return", f"{total_return:.2%}")
        col2.metric("Max Drawdown", f"{max_dd:.2%}")
        col3.metric("Volatility", f"{vol:.2%}")
        col4.metric("Sharpe Ratio", f"{sharpe:.2f}")
        
        # Charts
        st.subheader("Equity Curve")
        equity_chart = create_equity_chart(equity_df)
        st.plotly_chart(equity_chart, use_container_width=True)
        
        st.subheader("Drawdown")
        drawdown_chart = create_drawdown_chart(equity_df)
        st.plotly_chart(drawdown_chart, use_container_width=True)
        
        # Weights over time
        if weights_path.exists():
            st.subheader("Portfolio Weights")
            weights_df = pd.read_parquet(weights_path)
            
            # Prepare data for stacked area
            weights_chart_df = weights_df.set_index("ts").fillna(0)
            st.area_chart(weights_chart_df, use_container_width=True)
        
        # Trades table
        if trades_path.exists():
            st.subheader("Trades")
            trades_df = pd.read_parquet(trades_path)
            st.dataframe(trades_df, use_container_width=True)
        
        # Metrics JSON
        if metrics_path.exists():
            with open(metrics_path, 'r', encoding="utf-8") as f:
                metrics = json.load(f)
            
            with st.expander("Raw Metrics"):
                st.json(metrics)


# ============================================================================
# Run Compare Page
# ============================================================================
def compare_page():
    """Compare multiple runs."""
    st.title("Run Comparison")
    
    if len(available_runs) < 2:
        st.info("Need at least 2 runs to compare.")
        return
    
    # Multi-select runs
    selected_runs = st.multiselect(
        "Select Runs to Compare",
        available_runs,
        default=available_runs[:3]
    )
    
    if not selected_runs:
        return
    
    # Load and compare
    comparison_data = []
    
    for run_id in selected_runs:
        run_dir = runs_dir / run_id
        equity_path = run_dir / "equity_curve.parquet"
        
        if not equity_path.exists():
            continue
        
        equity_df = pd.read_parquet(equity_path)
        
        # Calculate metrics
        final_nav = equity_df["nav"].iloc[-1]
        initial_nav = equity_df["nav"].iloc[0]
        total_return = (final_nav / initial_nav) - 1
        
        cummax = equity_df["nav"].cummax()
        drawdown = (equity_df["nav"] - cummax) / cummax
        max_dd = drawdown.min()
        
        returns = equity_df["nav"].pct_change().dropna()
        vol = returns.std() * (252 ** 0.5)
        sharpe = (returns.mean() * 252) / (returns.std() * (252 ** 0.5)) if vol > 0 else 0
        
        comparison_data.append({
            "Run": run_id,
            "Total Return": f"{total_return:.2%}",
            "Max DD": f"{max_dd:.2%}",
            "Volatility": f"{vol:.2%}",
            "Sharpe": f"{sharpe:.2f}",
            "Final NAV": f"${final_nav:,.2f}",
        })
    
    if comparison_data:
        comparison_df = pd.DataFrame(comparison_data)
        st.dataframe(comparison_df, use_container_width=True)
        
        # Equity curve comparison
        st.subheader("Equity Curves")
        
        fig_data = []
        for run_id in selected_runs:
            run_dir = runs_dir / run_id
            equity_path = run_dir / "equity_curve.parquet"
            
            if equity_path.exists():
                df = pd.read_parquet(equity_path)
                df["Run"] = run_id
                fig_data.append(df[["ts", "nav", "Run"]])
        
        if fig_data:
            combined_df = pd.concat(fig_data)
            pivot_df = combined_df.pivot(index="ts", columns="Run", values="nav")
            st.line_chart(pivot_df, use_container_width=True)


# ============================================================================
# Replay Page
# ============================================================================
def replay_page():
    """Replay and review trades."""
    st.title("Trade Replay")
    
    if not available_runs:
        st.info("No runs found.")
        return
    
    selected_run = st.selectbox("Select Run", available_runs, key="replay_run")
    run_dir = runs_dir / selected_run
    
    # Load data
    equity_path = run_dir / "equity_curve.parquet"
    trades_path = run_dir / "trades.parquet"
    
    if not equity_path.exists():
        st.error("No equity data found")
        return
    
    equity_df = pd.read_parquet(equity_path)
    
    # Date selector
    min_date = pd.to_datetime(equity_df["ts"]).min()
    max_date = pd.to_datetime(equity_df["ts"]).max()
    
    selected_date = st.date_input(
        "Select Date",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )
    
    # Show trades on that date
    if trades_path.exists():
        trades_df = pd.read_parquet(trades_path)
        trades_df["ts"] = pd.to_datetime(trades_df["ts"])
        
        day_trades = trades_df[
            trades_df["ts"].dt.date == selected_date
        ]
        
        if not day_trades.empty:
            st.subheader(f"Trades on {selected_date}")
            st.dataframe(day_trades, use_container_width=True)
        else:
            st.info("No trades on this date")
    
    # Portfolio snapshot
    snapshot = equity_df[pd.to_datetime(equity_df["ts"]).dt.date <= selected_date]
    if not snapshot.empty:
        st.subheader("Portfolio Value")
        st.line_chart(snapshot.set_index("ts")["nav"], use_container_width=True)


# ============================================================================
# Research Log Page
# ============================================================================
def research_log_page():
    """View research log and agents.md."""
    st.title("Research Log")
    
    # Load agents.md
    agents_path = Path("agents.md")
    if agents_path.exists():
        with open(agents_path, 'r', encoding="utf-8") as f:
            content = f.read()
        st.markdown(content)
    else:
        st.info("No agents.md file found")
    
    # Run metadata
    st.subheader("Run Metadata")
    
    for run_id in available_runs[:10]:  # Show last 10
        run_dir = runs_dir / run_id
        
        with st.expander(f"Run: {run_id}"):
            # Check for metadata files
            config_path = run_dir / "config.yaml"
            if config_path.exists():
                with open(config_path, 'r', encoding="utf-8-sig") as f:
                    st.code(f.read(), language="yaml")
            else:
                st.write("No config file")


# ============================================================================
# Main
# ============================================================================
if __name__ == "__main__":
    if page == "Dashboard":
        dashboard_page()
    elif page == "Run Compare":
        compare_page()
    elif page == "Replay":
        replay_page()
    elif page == "Research Log":
        research_log_page()
