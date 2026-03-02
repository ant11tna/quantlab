"""Lightweight Charts components.

Chart components using Plotly for financial data visualization.
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_equity_chart(
    equity_df: pd.DataFrame,
    title: str = "Portfolio Equity Curve",
    show_drawdown: bool = False
) -> go.Figure:
    """Create equity curve chart.
    
    Args:
        equity_df: DataFrame with ts, nav, cash columns
        title: Chart title
        show_drawdown: Whether to show drawdown subplot
        
    Returns:
        Plotly figure
    """
    if show_drawdown:
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            subplot_titles=(title, "Drawdown"),
            row_heights=[0.7, 0.3]
        )
    else:
        fig = go.Figure()
    
    # Ensure ts is datetime
    equity_df = equity_df.copy()
    equity_df["ts"] = pd.to_datetime(equity_df["ts"])
    
    # Main equity line
    nav_trace = go.Scatter(
        x=equity_df["ts"],
        y=equity_df["nav"],
        mode="lines",
        name="NAV",
        line=dict(color="#2196F3", width=2),
        fill="tozeroy",
        fillcolor="rgba(33, 150, 243, 0.1)"
    )
    
    if show_drawdown:
        fig.add_trace(nav_trace, row=1, col=1)
    else:
        fig.add_trace(nav_trace)
    
    # Cash line
    if "cash" in equity_df.columns:
        cash_trace = go.Scatter(
            x=equity_df["ts"],
            y=equity_df["cash"],
            mode="lines",
            name="Cash",
            line=dict(color="#4CAF50", width=1.5)
        )
        if show_drawdown:
            fig.add_trace(cash_trace, row=1, col=1)
        else:
            fig.add_trace(cash_trace)
    
    # Drawdown subplot
    if show_drawdown:
        cummax = equity_df["nav"].cummax()
        drawdown = (equity_df["nav"] - cummax) / cummax * 100
        
        dd_trace = go.Scatter(
            x=equity_df["ts"],
            y=drawdown,
            mode="lines",
            name="Drawdown %",
            line=dict(color="#F44336", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(244, 67, 54, 0.2)"
        )
        fig.add_trace(dd_trace, row=2, col=1)
        
        fig.update_yaxes(title_text="Drawdown %", row=2, col=1)
    
    # Layout
    fig.update_layout(
        title=title if not show_drawdown else None,
        xaxis_title="Date",
        yaxis_title="Value ($)" if not show_drawdown else None,
        hovermode="x unified",
        template="plotly_white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=60, r=30, t=80 if show_drawdown else 50, b=50)
    )
    
    return fig


def create_drawdown_chart(equity_df: pd.DataFrame) -> go.Figure:
    """Create standalone drawdown chart."""
    equity_df = equity_df.copy()
    equity_df["ts"] = pd.to_datetime(equity_df["ts"])
    
    cummax = equity_df["nav"].cummax()
    drawdown = (equity_df["nav"] - cummax) / cummax * 100
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=equity_df["ts"],
        y=drawdown,
        mode="lines",
        name="Drawdown",
        line=dict(color="#F44336", width=1.5),
        fill="tozeroy",
        fillcolor="rgba(244, 67, 54, 0.2)"
    ))
    
    fig.update_layout(
        title="Portfolio Drawdown",
        xaxis_title="Date",
        yaxis_title="Drawdown (%)",
        hovermode="x unified",
        template="plotly_white",
        margin=dict(l=60, r=30, t=50, b=50)
    )
    
    return fig


def create_ohlc_chart(
    bars_df: pd.DataFrame,
    title: str = "Price Chart",
    trades: Optional[List[Dict]] = None
) -> go.Figure:
    """Create OHLC candlestick chart.
    
    Args:
        bars_df: DataFrame with ts, open, high, low, close columns
        title: Chart title
        trades: Optional list of trade markers
        
    Returns:
        Plotly figure
    """
    bars_df = bars_df.copy()
    bars_df["ts"] = pd.to_datetime(bars_df["ts"])
    
    fig = go.Figure()
    
    # Candlestick
    fig.add_trace(go.Candlestick(
        x=bars_df["ts"],
        open=bars_df["open"],
        high=bars_df["high"],
        low=bars_df["low"],
        close=bars_df["close"],
        name="Price"
    ))
    
    # Volume bars (if available)
    if "volume" in bars_df.columns:
        fig.add_trace(go.Bar(
            x=bars_df["ts"],
            y=bars_df["volume"],
            name="Volume",
            marker_color="rgba(100, 100, 100, 0.3)",
            yaxis="y2"
        ))
    
    # Trade markers
    if trades:
        for trade in trades:
            color = "#4CAF50" if trade.get("side") == "BUY" else "#F44336"
            symbol = "triangle-up" if trade.get("side") == "BUY" else "triangle-down"
            
            fig.add_trace(go.Scatter(
                x=[pd.to_datetime(trade["ts"])],
                y=[trade["price"]],
                mode="markers",
                marker=dict(
                    symbol=symbol,
                    size=12,
                    color=color,
                    line=dict(width=1, color="white")
                ),
                name=f"{trade.get('side', 'Trade')} {trade.get('symbol', '')}",
                showlegend=False,
                hovertemplate=f"{trade.get('side', '')}<br>Price: {trade.get('price')}<br>Qty: {trade.get('qty')}"
            ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Price ($)",
        hovermode="x unified",
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        yaxis2=dict(
            overlaying="y",
            side="right",
            showgrid=False,
            title="Volume"
        ) if "volume" in bars_df.columns else None
    )
    
    return fig


def create_weights_stacked_area(
    weights_df: pd.DataFrame,
    title: str = "Portfolio Weights Over Time"
) -> go.Figure:
    """Create stacked area chart for weights.
    
    Args:
        weights_df: DataFrame with ts column and weight columns
        title: Chart title
        
    Returns:
        Plotly figure
    """
    weights_df = weights_df.copy()
    weights_df["ts"] = pd.to_datetime(weights_df["ts"])
    weights_df = weights_df.set_index("ts").fillna(0)
    
    fig = go.Figure()
    
    colors = [
        "#2196F3", "#4CAF50", "#FF9800", "#9C27B0",
        "#F44336", "#00BCD4", "#795548", "#607D8B"
    ]
    
    for i, col in enumerate(weights_df.columns):
        fig.add_trace(go.Scatter(
            x=weights_df.index,
            y=weights_df[col],
            mode="lines",
            name=col,
            stackgroup="one",
            fillcolor=colors[i % len(colors)],
            line=dict(width=0.5)
        ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Weight",
        hovermode="x unified",
        template="plotly_white",
        yaxis=dict(
            tickformat=".0%",
            range=[0, 1]
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig


def create_rolling_metrics_chart(
    returns: pd.Series,
    window: int = 63,
    title: str = "Rolling Metrics"
) -> go.Figure:
    """Create rolling metrics chart.
    
    Args:
        returns: Returns series
        window: Rolling window
        title: Chart title
        
    Returns:
        Plotly figure
    """
    rolling_vol = returns.rolling(window).std() * (252 ** 0.5) * 100
    rolling_sharpe = (
        returns.rolling(window).mean() /
        returns.rolling(window).std() *
        (252 ** 0.5)
    )
    
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Rolling Volatility", "Rolling Sharpe Ratio")
    )
    
    fig.add_trace(go.Scatter(
        x=rolling_vol.index,
        y=rolling_vol.values,
        mode="lines",
        name="Volatility",
        line=dict(color="#FF9800")
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=rolling_sharpe.index,
        y=rolling_sharpe.values,
        mode="lines",
        name="Sharpe",
        line=dict(color="#4CAF50")
    ), row=2, col=1)
    
    fig.update_layout(
        title=title,
        hovermode="x unified",
        template="plotly_white",
        showlegend=False
    )
    
    fig.update_yaxes(title_text="Volatility (%)", row=1, col=1)
    fig.update_yaxes(title_text="Sharpe Ratio", row=2, col=1)
    
    return fig


def create_comparison_chart(
    run_data: Dict[str, pd.DataFrame],
    normalize: bool = True,
    title: str = "Strategy Comparison"
) -> go.Figure:
    """Create comparison chart for multiple runs.
    
    Args:
        run_data: Dict of run_id -> equity DataFrame
        normalize: Whether to normalize all curves to start at 1
        title: Chart title
        
    Returns:
        Plotly figure
    """
    fig = go.Figure()
    
    colors = [
        "#2196F3", "#4CAF50", "#FF9800", "#9C27B0",
        "#F44336", "#00BCD4"
    ]
    
    for i, (run_id, df) in enumerate(run_data.items()):
        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"])
        
        y_values = df["nav"]
        if normalize:
            y_values = y_values / y_values.iloc[0]
        
        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=y_values,
            mode="lines",
            name=run_id,
            line=dict(color=colors[i % len(colors)], width=2)
        ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Normalized Value" if normalize else "NAV ($)",
        hovermode="x unified",
        template="plotly_white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig
