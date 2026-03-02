"""Data module for quantlab.

Handles data sources, ingestion, and transformation.
"""

from quantlab.data.sources.base import DataSource, AbstractDataSource
from quantlab.data.sources.local_csv import LocalCSVDataSource, MockDataSource
from quantlab.data.ingest import DataIngestor
from quantlab.data.curate import (
    CuratedDataBuilder,
    build_curated_bars_from_csv_dir,
)
from quantlab.data.transforms import (
    apply_adjustments,
    align_symbols,
    calculate_returns,
    detect_outliers,
    fill_missing,
    resample_bars,
)
from quantlab.data.manifest import DataManifest

# Optional: AKShare for Chinese markets
try:
    from quantlab.data.sources.akshare_source import AKShareDataSource, INDEX_SYMBOLS, ETF_SYMBOLS
    _has_akshare = True
except ImportError:
    _has_akshare = False
    AKShareDataSource = None  # type: ignore
    INDEX_SYMBOLS = {}
    ETF_SYMBOLS = {}

__all__ = [
    # Sources
    "DataSource",
    "AbstractDataSource",
    "LocalCSVDataSource",
    "MockDataSource",
    # Ingestion
    "DataIngestor",
    # Curate
    "CuratedDataBuilder",
    "build_curated_bars_from_csv_dir",
    # Transforms
    "apply_adjustments",
    "align_symbols",
    "calculate_returns",
    "detect_outliers",
    "fill_missing",
    "resample_bars",
    # Manifest
    "DataManifest",
]

# Add AKShare if available
if _has_akshare:
    __all__.extend([
        "AKShareDataSource",
        "INDEX_SYMBOLS",
        "ETF_SYMBOLS",
    ])
