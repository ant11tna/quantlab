"""Data sources module."""
from quantlab.data.sources.base import DataSource, AbstractDataSource
from quantlab.data.sources.local_csv import LocalCSVDataSource, MockDataSource

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
    "DataSource",
    "AbstractDataSource", 
    "LocalCSVDataSource",
    "MockDataSource",
]

if _has_akshare:
    __all__.extend([
        "AKShareDataSource",
        "INDEX_SYMBOLS",
        "ETF_SYMBOLS",
    ])
