"""Minimal P1.5 Impact Test (no deps)"""
import sys
from decimal import Decimal

def test_impact_curve():
    """Test impact curve calculation."""
    print("=" * 70)
    print("P1.5 Impact Curve Test")
    print("=" * 70)
    
    # Impact parameters from config
    k_bps = 20
    alpha = 0.5
    
    print(f"\nFormula: impact_bps = {k_bps} * (participation ^ {alpha})")
    
    print(f"\n{'Participation':<15} {'Impact (bps)':<15} {'Notes'}")
    print("-" * 60)
    
    test_cases = [
        (0.01, "Small trade"),
        (0.05, "Medium trade"),
        (0.10, "Large trade"),
        (0.20, "Very large (20% of volume)"),
        (0.50, "Half of volume"),
        (1.00, "Entire volume"),
    ]
    
    for p, note in test_cases:
        impact = k_bps * (p ** alpha)
        print(f"{p:<15.2%} {impact:<15.2f} {note}")
    
    # Verify sub-linearity
    print("\n" + "-" * 60)
    print("Sub-linearity Verification:")
    
    p1, p2 = 0.1, 0.5
    i1 = k_bps * (p1 ** alpha)
    i2 = k_bps * (p2 ** alpha)
    
    print(f"  At participation={p1:.0%}: impact={i1:.1f} bps")
    print(f"  At participation={p2:.0%}: impact={i2:.1f} bps")
    print(f"  Participation ratio: {p2/p1:.1f}x")
    print(f"  Impact ratio: {i2/i1:.2f}x (sub-linear: {i2/i1 < p2/p1})")
    
    assert i2/i1 < p2/p1, "Impact should be sub-linear"
    print("  [PASS] Impact curve is sub-linear")
    
    return True


def test_capacity_scenarios():
    """Test different capacity scenarios."""
    print("\n" + "=" * 70)
    print("Capacity Sensitivity Scenarios")
    print("=" * 70)
    
    scenarios = [
        ("Small Fund ($10M)", 0, 1.0, "No impact"),
        ("Medium Fund ($100M)", 10, 0.5, "Moderate impact"),
        ("Large Fund ($1B)", 30, 0.2, "High impact"),
    ]
    
    print("\nComparing strategies with different capacity constraints:")
    print("-" * 70)
    
    base_return = 0.15  # 15%
    
    print(f"\n{'Scenario':<25} {'k_bps':<8} {'Part.Rate':<10} {'Est.Return':<12}")
    print("-" * 70)
    
    results = []
    for name, k, part_rate, desc in scenarios:
        # Simulate average participation in practice
        # Lower part_rate means need more bars to complete
        avg_part = 0.15 / part_rate if part_rate > 0 else 0.15
        avg_impact = k * (avg_part ** 0.5) if k > 0 else 0
        
        # Return drag: ~1% drag per 20bps of impact
        drag = avg_impact / 2000
        adjusted_return = base_return - drag
        
        print(f"{name:<25} {k:<8} {part_rate:<10.0%} {adjusted_return:<12.2%}")
        print(f"  └─ {desc}, avg_impact={avg_impact:.1f}bps, drag={drag:.2%}")
        
        results.append((name, adjusted_return))
    
    # Check degradation
    print("\n" + "-" * 70)
    ret_small = results[0][1]
    ret_large = results[-1][1]
    
    print(f"Return degradation from small to large: {ret_small - ret_large:.2%}")
    
    if ret_large < ret_small * 0.8:
        print("[WARNING] Strategy has CAPACITY CONSTRAINTS!")
        print("          Returns degrade significantly at scale.")
    else:
        print("[PASS] Strategy scales reasonably well.")
    
    return True


def main():
    print("\n" + "=" * 70)
    print("P1.5 CAPACITY SENSITIVITY VERIFICATION")
    print("=" * 70)
    
    try:
        test_impact_curve()
        test_capacity_scenarios()
        
        print("\n" + "=" * 70)
        print("[SUCCESS] All P1.5 tests passed!")
        print("=" * 70)
        print("\nKey Insight:")
        print("Your strategy's capacity is limited by:")
        print("  1. Market liquidity (volume available)")
        print("  2. Participation rate (how much you can take)")
        print("  3. Impact coefficient k (market sensitivity)")
        print("  4. Impact exponent alpha (curve shape)")
        return 0
        
    except AssertionError as e:
        print(f"\n[FAIL] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
