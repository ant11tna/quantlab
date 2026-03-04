from __future__ import annotations

import argparse

from quantlab.universe.resolver import confirm, resolve
from quantlab.universe.store import UniverseStore


DEFAULT_INPUTS = [
    "600519",
    "sh600519",
    "159915",
    "sh510300",
    "0700",
    "00700.HK",
    "AAPL",
    "BRK.B",
    "NASDAQ:AAPL",
    "NYSE:SPY",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Universe resolver/confirm demo")
    parser.add_argument("symbols", nargs="*", help="Symbols to resolve")
    args = parser.parse_args()

    inputs = args.symbols if args.symbols else DEFAULT_INPUTS
    store = UniverseStore(base_dir="data/universe")

    for raw in inputs:
        print(f"\n=== Query: {raw} ===")
        candidates = resolve(raw, store)
        if not candidates:
            print("No candidates")
            continue

        for i, cand in enumerate(candidates, start=1):
            print(
                f"{i}. listing_id={cand.listing_id} exchange={cand.exchange} "
                f"ticker={cand.ticker} confidence={cand.confidence} "
                f"name={cand.name!r} rationale={cand.rationale}"
            )

        try:
            confirm(raw, candidates[0], store)
            print("Confirmed first candidate")
        except ValueError as exc:
            print(f"Confirm conflict: {exc}")

        verify = resolve(raw, store)
        if verify:
            print(
                "Post-confirm resolve:",
                f"listing_id={verify[0].listing_id} confidence={verify[0].confidence} rationale={verify[0].rationale}",
            )

    print("\n=== Universe Summary ===")
    print(f"instruments={len(store.load_instruments())}")
    print(f"listings={len(store.load_listings())}")
    print(f"aliases={len(store.load_aliases())}")


if __name__ == "__main__":
    main()
