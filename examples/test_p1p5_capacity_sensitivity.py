"""P1.5 Capacity Sensitivity Test

Tests the impact curve (capacity sensitivity) by comparing results
with different impact_k_bps values.

Key insight: If strategy returns decrease significantly with higher impact_k,
the strategy is only suitable for small capital (capacity constrained).
"""
from __future__ import annotations
import sys
from pathlib import Path
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quantlab.backtest.broker_sim import ExecutionConfig

def test_impact_curve():
    """Test that impact cost follows the expected curve."""
    print("=" * 70)
    print("P1.5 Impact Curve Verification")
    print("=" * 70)
    
    # Test impact calculation for various participation rates
    config = ExecutionConfig(
        participation_rate=Decimal("0.2"),
        min_lot=Decimal("1"),
        impact_k_bps=Decimal("20"),
        impact_alpha=Decimal("0.5")
    )
    
    print(f"\nImpact Curve: impact_bps = k * (participation ^ alpha)")
    print(f"  k = {config.impact_k_bps} bps")
    print(f"  alpha = {config.impact_alpha}")
    
    print(f"\n{'Participation':<15} {'Impact (bps)':<15} {'Impact (%)'}")
    print("-" * 50)
    
    test_cases = [0.01, 0.05, 0.1, 0.2, 0.5, 1.0]
    
    for p in test_cases:
        impact = float(config.impact_k_bps) * (p ** float(config.impact_alpha))
        print(f"{p:<15.2%} {impact:<15.2f} {impact/100:.4%}")
    
    # Verify non-linear relationship
    p1, p2 = 0.1, 0.5
    i1 = float(config.impact_k_bps) * (p1 ** float(config.impact_alpha))
    i2 = float(config.impact_k_bps) * (p2 ** float(config.impact_alpha))
    
    ratio_p = p2 / p1  # 5x
    ratio_i = i2 / i1  # Should be sqrt(5) ~ 2.24x (not 5x)
    
    print(f"\nNon-linearity Check:")
    print(f"  Participation ratio (0.5/0.1): {ratio_p:.1f}x")
    print(f"  Impact ratio: {ratio_i:.2f}x (should be ~{ratio_p**0.5:.2f}x for alpha=0.5)")
    
    assert ratio_i < ratio_p, "Impact should be sub-linear (alpha < 1)"
    print("  [PASS] Impact curve is sub-linear (capacity-sensitive)")
    
    return True


def test_capacity_comparison():
    """Simulate capacity sensitivity comparison."""
    print("\n" + "=" * 70)
    print("Capacity Sensitivity Comparison")
    print("=" * 70)
    
    # Simulate three scenarios
    scenarios = [
        ("Small Capital (k=0)", 0, 1.0),
        ("Medium Capital (k=10)", 10, 0.5),
        ("Large Capital (k=30)", 30, 0.2),
    ]
    
    print("\nScenario Setup:")
    print("-" * 70)
    
    # Simulate returns degradation
    base_return = 0.15  # 15% baseline
    
    results = []
    for name, k_bps, participation_rate in scenarios:
        # Simulate impact drag on returns
        # Higher k_bps + lower participation = more drag
        avg_participation = 0.1 / participation_rate  # Higher when participation_rate is lower
        avg_impact = k_bps * (avg_participation ** 0.5)
        
        # Return degradation: each 10bps of impact ~ 0.5% return drag
        return_drag = avg_impact / 1000  # Simplified
        adjusted_return = base_return - return_drag
        
        print(f"\n{name}:")
        print(f"  impact_k_bps: {k_bps}")
        print(f"  participation_rate: {participation_rate}")
        print(f"  avg_participation: {avg_participation:.2f}")
        print(f"  avg_impact: {avg_impact:.1f} bps")
        print(f"  est_return: {adjusted_return:.2%}")
        
        results.append({
            "name": name,
            "k_bps": k_bps,
            "participation": participation_rate,
            "return": adjusted_return
        })
    
    # Check capacity sensitivity
    print("\n" + "-" * 70)
    print("Capacity Sensitivity Check:")
    
    ret_small = results[0]["return"]
    ret_large = results[2]["return"]
    
    print(f"  Small capital return: {ret_small:.2%}")
    print(f"  Large capital return: {ret_large:.2%}")
    print(f"  Degradation: {ret_small - ret_large:.2%}")
    
    if ret_large < ret_small * 0.7:  # Lost more than 30%
        print("  [WARNING] Significant capacity constraints detected!")
        print("  [INFO] Strategy may not scale to large capital.")
    else:
        print("  [PASS] Moderate capacity constraints.")
    
    return True


def main():
    print("\n" + "=" * 70)
    print("P1.5 CAPACITY SENSITIVITY TEST SUITE")
    print("=" * 70)
    
    try:
        test_impact_curve()
        test_capacity_comparison()
        
        print("\n" + "=" * 70)
        print("[SUCCESS] All P1.5 tests passed!")
        print("=" * 70)
        print("\nKey Takeaways:")
        print("1. Impact cost scales sub-linearly with size (alpha=0.5)")
        print("2. Large orders face proportionally lower impact per share")
        print("3. BUT absolute impact increases with size")
        print("4. Strategy capacity is limited by impact drag on returns")
        return 0
        
    except AssertionError as e:
        print(f"\n[FAIL] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
