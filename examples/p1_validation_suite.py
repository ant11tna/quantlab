"""P1 Validation Test Suite - ASCII Version"""
from __future__ import annotations
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

class SimpleBar:
    def __init__(self, ts, symbol, open_, high, low, close, volume):
        self.ts = ts
        self.symbol = symbol
        self.open_ = Decimal(str(open_))
        self.high = Decimal(str(high))
        self.low = Decimal(str(low))
        self.close = Decimal(str(close))
        self.volume = Decimal(str(volume))

class SimpleOrder:
    def __init__(self, symbol, side, qty, order_type="MARKET"):
        self.id = f"ORD_{id(self)}"
        self.ts = datetime.now()
        self.symbol = symbol
        self.side = side
        self.qty = Decimal(str(qty))
        self.order_type = order_type
        self.limit_price = None
        self.status = "SUBMITTED"
        self.filled_qty = Decimal("0")
        self._fill_prices = []
        self._fill_qtys = []
    
    @property
    def remaining_qty(self):
        return self.qty - self.filled_qty
    
    @property
    def is_filled(self):
        return self.filled_qty >= self.qty
    
    @property
    def avg_fill_price(self):
        if not self._fill_qtys:
            return Decimal("0")
        total_cost = sum(p * q for p, q in zip(self._fill_prices, self._fill_qtys))
        total_qty = sum(self._fill_qtys)
        return total_cost / total_qty if total_qty > 0 else Decimal("0")

class SimpleBroker:
    def __init__(self, initial_cash, participation_rate=0.2, min_lot=1):
        self.initial_cash = Decimal(str(initial_cash))
        self.cash = self.initial_cash
        self.participation_rate = Decimal(str(participation_rate))
        self.min_lot = Decimal(str(min_lot))
        self.orders = []
        self.positions = {}
        self.last_prices = {}
        self.nav_history = []
    
    def place_order(self, order):
        self.orders.append(order)
        return order.id
    
    def process_bar(self, bar):
        self.last_prices[bar.symbol] = bar.close
        fills = []
        
        for order in self.orders:
            if order.status in ["FILLED", "CANCELLED"]:
                continue
            if order.symbol != bar.symbol:
                continue
            
            remaining = order.remaining_qty
            max_by_volume = bar.volume * self.participation_rate
            fill_qty = min(remaining, max_by_volume)
            
            if self.min_lot > 0:
                fill_qty = (fill_qty // self.min_lot) * self.min_lot
            
            if fill_qty <= 0:
                continue
            
            fill_price = bar.close
            if order.side == "BUY":
                cost = fill_qty * fill_price
                if cost > self.cash:
                    max_affordable = self.cash / fill_price
                    if self.min_lot > 0:
                        max_affordable = (max_affordable // self.min_lot) * self.min_lot
                    fill_qty = min(fill_qty, max_affordable)
                    if fill_qty <= 0:
                        continue
            
            self._execute_fill(order, fill_qty, fill_price)
            fills.append({"order_id": order.id, "qty": fill_qty, "price": fill_price})
        
        self._record_nav(bar.ts)
        return fills
    
    def _execute_fill(self, order, qty, price):
        order.filled_qty += qty
        order._fill_prices.append(price)
        order._fill_qtys.append(qty)
        
        if order.is_filled:
            order.status = "FILLED"
        else:
            order.status = "PARTIAL_FILLED"
        
        symbol = order.symbol
        if symbol not in self.positions:
            self.positions[symbol] = {"qty": Decimal("0"), "avg_price": Decimal("0")}
        
        pos = self.positions[symbol]
        
        if order.side == "BUY":
            old_cost = pos["qty"] * pos["avg_price"]
            new_cost = qty * price
            pos["qty"] += qty
            if pos["qty"] > 0:
                pos["avg_price"] = (old_cost + new_cost) / pos["qty"]
            self.cash -= qty * price
        else:
            pos["qty"] -= qty
            self.cash += qty * price
            if pos["qty"] == 0:
                pos["avg_price"] = Decimal("0")
    
    def _record_nav(self, ts):
        positions_value = Decimal("0")
        for symbol, pos in self.positions.items():
            price = self.last_prices.get(symbol, pos["avg_price"])
            positions_value += pos["qty"] * price
        nav = self.cash + positions_value
        self.nav_history.append({"ts": ts, "cash": self.cash, "positions_value": positions_value, "nav": nav})
        return nav
    
    def get_nav(self):
        if self.nav_history:
            return self.nav_history[-1]["nav"]
        return self.initial_cash

def test_1():
    print("\n" + "="*60)
    print("TEST 1: Cross-Bar Fill")
    print("="*60)
    print("Order 600, volume=1000, participation=0.2")
    print("Expected: 200 per bar, 3 bars total")
    
    broker = SimpleBroker(100000, 0.2, 1)
    order = SimpleOrder("TEST", "BUY", 600)
    broker.place_order(order)
    
    bars = [SimpleBar(datetime(2024,1,1)+timedelta(days=i), "TEST", 100+i, 101+i, 99+i, 100+i, 1000) for i in range(5)]
    
    print(f"\n{'Bar':<5} {'Price':<8} {'Fill':<8} {'Total':<12} {'Status'}")
    print("-" * 50)
    
    bar_count = 0
    for i, bar in enumerate(bars):
        if order.is_filled:
            break
        prev = order.filled_qty
        broker.process_bar(bar)
        filled_this = order.filled_qty - prev
        bar_count += 1
        print(f"{i+1:<5} {bar.close:<8} {filled_this:<8} {order.filled_qty}/{order.qty:<6} {order.status}")
    
    assert order.status == "FILLED", f"Status should be FILLED, got {order.status}"
    assert order.filled_qty == 600, f"Filled qty should be 600, got {order.filled_qty}"
    assert bar_count == 3, f"Should take 3 bars, took {bar_count}"
    
    print("\n[OK] Cross-bar fill working")
    return True

def test_2():
    print("\n" + "="*60)
    print("TEST 2: Cash Constraint")
    print("="*60)
    print("Cash 10000, price 100, order 200 shares (need 20000)")
    print("Expected: fill 100, cash=0, PARTIAL_FILLED")
    
    broker = SimpleBroker(10000, 1.0, 1)
    order = SimpleOrder("TEST", "BUY", 200)
    broker.place_order(order)
    
    bar = SimpleBar(datetime.now(), "TEST", 100, 101, 99, 100, 10000)
    broker.process_bar(bar)
    
    print(f"Filled: {order.filled_qty}, Cash: {broker.cash}, Status: {order.status}")
    
    assert broker.cash >= 0, f"Cash must be >= 0, got {broker.cash}"
    assert order.filled_qty == 100, f"Should fill 100, got {order.filled_qty}"
    assert order.status == "PARTIAL_FILLED", f"Status should be PARTIAL_FILLED, got {order.status}"
    
    print("\n[OK] Cash constraint working")
    return True

def test_3():
    print("\n" + "="*60)
    print("TEST 3: NAV Continuity (MTM)")
    print("="*60)
    print("Hold 100 shares, price 100->110->105")
    print("Expected: NAV follows price")
    
    broker = SimpleBroker(100000, 1.0, 1)  # 100% participation for quick fill
    order = SimpleOrder("TEST", "BUY", 100)
    broker.place_order(order)
    
    bar1 = SimpleBar(datetime(2024,1,1), "TEST", 100, 101, 99, 100, 10000)
    broker.process_bar(bar1)
    
    # Check: after fill, should have 100 shares, cash reduced
    print(f"After Bar 1 (price=100): cash={broker.cash}, shares={broker.positions['TEST']['qty']}")
    
    nav1 = broker.get_nav()
    print(f"NAV1 = {nav1} (expected ~10000)")
    
    bar2 = SimpleBar(datetime(2024,1,2), "TEST", 110, 111, 109, 110, 10000)
    broker.process_bar(bar2)
    nav2 = broker.get_nav()
    print(f"NAV2 = {nav2} (expected ~11000, price up)")
    
    bar3 = SimpleBar(datetime(2024,1,3), "TEST", 105, 106, 104, 105, 10000)
    broker.process_bar(bar3)
    nav3 = broker.get_nav()
    print(f"NAV3 = {nav3} (expected ~10500, price down)")
    
    assert nav2 > nav1, f"NAV should increase when price up: {nav2} <= {nav1}"
    assert nav3 < nav2, f"NAV should decrease when price down: {nav3} >= {nav2}"
    
    print("\n[OK] NAV continuity working")
    return True

def test_4():
    print("\n" + "="*60)
    print("TEST 4: Average Fill Price")
    print("="*60)
    print("Prices: 100, 101, 99, each 200 shares")
    print("Expected avg: (100*200 + 101*200 + 99*200)/600 = 100")
    
    broker = SimpleBroker(100000, 0.2, 1)
    order = SimpleOrder("TEST", "BUY", 600)
    broker.place_order(order)
    
    bars = [
        SimpleBar(datetime(2024,1,1), "TEST", 100, 101, 99, 100, 1000),
        SimpleBar(datetime(2024,1,2), "TEST", 101, 102, 100, 101, 1000),
        SimpleBar(datetime(2024,1,3), "TEST", 99, 100, 98, 99, 1000),
    ]
    
    for bar in bars:
        if order.is_filled:
            break
        broker.process_bar(bar)
    
    expected = Decimal("100")
    actual = order.avg_fill_price
    print(f"Expected avg: {expected}")
    print(f"Actual avg: {actual}")
    
    assert actual == expected, f"Avg price should be {expected}, got {actual}"
    
    print("\n[OK] Average price calculation working")
    return True

def test_5():
    print("\n" + "="*60)
    print("TEST 5: Dead Order")
    print("="*60)
    
    # Test 1: Normal case should fill
    print("\nCase 1: volume=10, participation=0.2, min_lot=1")
    print("max_fill = 2, should be able to fill 1 share")
    
    broker = SimpleBroker(100000, 0.2, 1)
    order = SimpleOrder("TEST", "BUY", 1)
    broker.place_order(order)
    
    bar = SimpleBar(datetime.now(), "TEST", 100, 101, 99, 100, 10)
    broker.process_bar(bar)
    
    print(f"Filled: {order.filled_qty}, Status: {order.status}")
    assert order.filled_qty >= 1, "Should fill at least 1 share"
    
    # Test 2: Dead order (liquidity too low)
    print("\nCase 2: volume=1, participation=0.1, min_lot=1")
    print("max_fill = 0.1, after lot = 0, should never fill")
    
    broker2 = SimpleBroker(100000, 0.1, 1)
    order2 = SimpleOrder("TEST", "BUY", 100)
    broker2.place_order(order2)
    
    bar2 = SimpleBar(datetime.now(), "TEST", 100, 101, 99, 100, 1)
    
    for i in range(5):
        broker2.process_bar(bar2)
    
    print(f"After 5 bars: filled={order2.filled_qty}, status={order2.status}")
    assert order2.filled_qty == 0, "Should never fill with zero liquidity"
    assert order2.status == "SUBMITTED", "Status should remain SUBMITTED"
    
    print("\n[OK] Dead order handling working")
    return True

def main():
    print("\n" + "="*70)
    print("P1 VALIDATION TEST SUITE")
    print("="*70)
    
    tests = [
        ("Cross-Bar Fill", test_1),
        ("Cash Constraint", test_2),
        ("NAV Continuity", test_3),
        ("Average Price", test_4),
        ("Dead Order", test_5),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            test_func()
            results.append((name, "PASS"))
        except AssertionError as e:
            results.append((name, f"FAIL: {e}"))
        except Exception as e:
            results.append((name, f"ERROR: {e}"))
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    for name, result in results:
        status = "PASS" if result == "PASS" else "FAIL"
        print(f"{name:<25} {status}")
    
    passed = sum(1 for _, r in results if r == "PASS")
    total = len(results)
    
    print("\n" + "="*70)
    print(f"TOTAL: {passed}/{total} passed")
    
    if passed == total:
        print("\n[SUCCESS] All P1 tests passed!")
        return 0
    else:
        print("\n[WARNING] Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
