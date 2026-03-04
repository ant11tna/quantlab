from __future__ import annotations


def normalize_input(s: str) -> str:
    """Normalize user input for resolver/alias matching."""

    normalized = s.strip().upper()
    normalized = normalized.replace("。", ".").replace("．", ".")
    normalized = normalized.replace("：", ":")
    return normalized
