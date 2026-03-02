"""Data manifest and versioning.

Tracks data snapshots and versions for reproducibility.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger


class DataManifest:
    """Manages data version manifests."""
    
    def __init__(self, manifest_dir: str | Path) -> None:
        """Initialize manifest manager.
        
        Args:
            manifest_dir: Directory to store manifests
        """
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
    
    def compute_hash(self, df: pd.DataFrame) -> str:
        """Compute hash of DataFrame content."""
        # Use pandas hash for speed
        hashed = pd.util.hash_pandas_object(df)
        return hashlib.sha256(hashed.values.tobytes()).hexdigest()[:16]
    
    def create_manifest(
        self,
        name: str,
        df: pd.DataFrame,
        symbols: List[str],
        source: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create manifest for dataset.
        
        Args:
            name: Dataset name
            df: DataFrame
            symbols: List of symbols
            source: Data source name
            metadata: Additional metadata
            
        Returns:
            Manifest dictionary
        """
        manifest = {
            "name": name,
            "version": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "created_at": datetime.now().isoformat(),
            "hash": self.compute_hash(df),
            "row_count": len(df),
            "columns": list(df.columns),
            "symbols": sorted(symbols),
            "date_range": {
                "start": df["ts"].min().isoformat() if "ts" in df.columns else None,
                "end": df["ts"].max().isoformat() if "ts" in df.columns else None,
            },
            "source": source,
            "metadata": metadata or {},
        }
        
        return manifest
    
    def save_manifest(
        self,
        manifest: Dict[str, Any],
        filename: Optional[str] = None
    ) -> Path:
        """Save manifest to file.
        
        Args:
            manifest: Manifest dictionary
            filename: Optional filename (default: name_version.json)
            
        Returns:
            Path to saved manifest
        """
        if filename is None:
            filename = f"{manifest['name']}_{manifest['version']}.json"
        
        path = self.manifest_dir / filename
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Saved manifest to {path}")
        return path
    
    def load_manifest(self, filename: str) -> Dict[str, Any]:
        """Load manifest from file."""
        path = self.manifest_dir / filename
        
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def list_manifests(self) -> List[Path]:
        """List all manifest files."""
        return sorted(self.manifest_dir.glob("*.json"))
    
    def verify_data(
        self,
        df: pd.DataFrame,
        manifest: Dict[str, Any]
    ) -> bool:
        """Verify data matches manifest.
        
        Args:
            df: DataFrame to verify
            manifest: Expected manifest
            
        Returns:
            True if data matches
        """
        current_hash = self.compute_hash(df)
        expected_hash = manifest["hash"]
        
        if current_hash != expected_hash:
            logger.error(f"Hash mismatch: {current_hash} != {expected_hash}")
            return False
        
        return True
    
    def get_latest(self, name: str) -> Optional[Dict[str, Any]]:
        """Get latest manifest for dataset name."""
        manifests = []
        for path in self.manifest_dir.glob(f"{name}_*.json"):
            with open(path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
                manifests.append((manifest["created_at"], manifest))
        
        if not manifests:
            return None
        
        # Sort by created_at descending
        manifests.sort(reverse=True)
        return manifests[0][1]
