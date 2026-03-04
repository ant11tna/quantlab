# QuantLab UI

Streamlit-based web interface for visualizing backtest results.

## Structure

```
ui/
├── app.py              # Main entry point
├── data/
│   ├── __init__.py
│   └── loader.py       # Data loading layer (abstracts file system)
├── pages/
│   ├── 1_runs.py       # Runs list page
│   └── 2_run_detail.py # Run detail page (placeholder)
└── components/         # Shared UI components (future)
```

## Data Conventions

### File Locations

UI only reads from `runs/<run_id>/`:
- `config.yaml` - Run configuration
- `results/metrics.json` - Performance metrics
- `results/equity_curve.parquet` (or .csv) - NAV over time
- `results/positions.parquet` - Position history
- `results/fills.parquet` - Trade fills

### Time Formats

| Component | Format | Notes |
|-----------|--------|-------|
| Internal DataFrames | `datetime64` | Pandas native |
| ECharts output | `"YYYY-MM-DD"` | String format |
| Lightweight Charts | `int` (epoch seconds) | Integer timestamp |

## Data Loader API

```python
from ui.data.loader import list_runs, load_run, load_equity_curve

# List all runs
runs_df = list_runs()
# Columns: run_id, started_at, name, status, total_return, max_drawdown, ...

# Load specific run
run_data = load_run("20240228_120000__my_strategy__abc123")
# Returns: {config_text, config_dict, metrics_dict, paths_dict, status}

# Load equity curve
equity_df = load_equity_curve(run_id)
# Columns: ts, nav, cash, exposure

# Convert to ECharts format
from ui.data.loader import equity_curve_to_echarts
equity_json = equity_curve_to_echarts(equity_df, last_n=60)
# Returns: {dates: ["2026-01-01", ...], values: [1000000, ...]}
```

## Page 1: Runs

### Layout
- **Left (2/3)**: Runs table with search/sort
- **Right (1/3)**: Selected run metrics + charts

### Charts

#### 1. Equity Sparkline (ECharts)
```javascript
{
  xAxis: { type: "category", data: ["2026-01-01", ...], show: false },
  yAxis: { type: "value", scale: true },
  series: [{
    type: "line",
    data: [1000000, 1000100, ...],
    smooth: true,
    areaStyle: { ... }
  }]
}
```

#### 2. Cost Breakdown Bar (ECharts)
```javascript
{
  xAxis: { type: "category", data: ["Fees", "Impact", "Slippage"] },
  yAxis: { type: "value", name: "Cost ($)" },
  series: [{
    type: "bar",
    data: [100, 50, 30],
    itemStyle: { color: "..." }
  }]
}
```

## Running the UI

```bash
cd quantlab
streamlit run ui/app.py
```

Or directly open a page:
```bash
streamlit run ui/pages/1_runs.py
```

## Dependencies

```bash
pip install streamlit plotly pandas pyarrow loguru pyyaml
# Optional for better charts:
pip install streamlit-echarts
```
