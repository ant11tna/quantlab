"""IO utilities with explicit UTF-8 encoding for cross-platform compatibility.

Windows defaults to GBK/CP936 which causes UnicodeDecodeError when reading
UTF-8 encoded YAML/JSON files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Union

import yaml


PathLike = Union[str, Path]


def load_yaml(path: PathLike) -> Any:
    """Load YAML file with UTF-8 encoding (with BOM support).
    
    Uses utf-8-sig to handle files with or without BOM.
    
    Args:
        path: Path to YAML file
        
    Returns:
        Parsed YAML content
        
    Example:
        >>> config = load_yaml("config/backtest.yaml")
    """
    path = Path(path)
    with open(path, "r", encoding="utf-8-sig") as f:
        return yaml.safe_load(f)


def load_json(path: PathLike) -> Any:
    """Load JSON file with UTF-8 encoding.
    
    Args:
        path: Path to JSON file
        
    Returns:
        Parsed JSON content
        
    Example:
        >>> manifest = load_json("data/manifest/etf.json")
    """
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_yaml(path: PathLike, data: Any) -> None:
    """Save data to YAML file with UTF-8 encoding.
    
    Args:
        path: Path to output YAML file
        data: Data to serialize
        
    Example:
        >>> save_yaml("config/output.yaml", {"key": "value"})
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def save_json(path: PathLike, data: Any, indent: int = 2) -> None:
    """Save data to JSON file with UTF-8 encoding.
    
    Args:
        path: Path to output JSON file
        data: Data to serialize
        indent: Indentation level for pretty printing
        
    Example:
        >>> save_json("data/manifest/etf.json", {"ETF:510300": "2024-01-01"})
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False, default=str)


def read_text(path: PathLike) -> str:
    """Read text file with UTF-8 encoding.
    
    Args:
        path: Path to text file
        
    Returns:
        File content as string
    """
    return Path(path).read_text(encoding="utf-8")


def write_text(path: PathLike, content: str) -> None:
    """Write text to file with UTF-8 encoding.
    
    Args:
        path: Path to output file
        content: Text content to write
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
