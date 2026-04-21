from fxbot.atr_exits import ATRExitPlan, compute_atr_exits, pip_size_for


def test_basic_atr_scaling():
    plan = compute_atr_exits(atr_pips=10.0, spread_pips=0.5)
    assert isinstance(plan, ATRExitPlan)
    # SL = max(10*1.2, 0.5*3) = 12.0, driven by ATR.
    assert plan.sl_pips == 12.0
    assert plan.sl_driver == "atr"
    # TP = max(10*2.4, 12*1.8) = 24 vs 21.6 → 24, driven by ATR.
    assert plan.tp_pips == 24.0
    assert plan.tp_driver == "atr"
    assert abs(plan.expected_rr - 2.0) < 1e-9


def test_spread_floor_expands_sl():
    # Very tight ATR but wide spread → SL pinned to spread floor.
    plan = compute_atr_exits(atr_pips=1.0, spread_pips=2.0)
    # SL_atr = 1.2, SL_spread_floor = 6.0 → 6.0 wins.
    assert plan.sl_pips == 6.0
    assert plan.sl_driver == "spread_floor"
    # TP must respect min_rr = 1.8 → 6 * 1.8 = 10.8 vs 1*2.4 = 2.4.
    assert plan.tp_pips == 10.8
    assert plan.tp_driver == "min_rr"


def test_no_maximum_cap_on_trending_day():
    """120-pip ATR day: TP should NOT be capped at 25."""
    plan = compute_atr_exits(atr_pips=120.0, spread_pips=0.3)
    assert plan.tp_pips > 100.0
    assert plan.sl_pips > 100.0


def test_zero_atr_still_valid():
    plan = compute_atr_exits(atr_pips=0.0, spread_pips=1.0)
    # SL = max(0, 3.0) = 3.0.
    assert plan.sl_pips == 3.0
    # TP = max(0, 3.0 * 1.8) = 5.4.
    assert plan.tp_pips == 5.4


def test_min_rr_enforced():
    plan = compute_atr_exits(
        atr_pips=10.0, spread_pips=0.5, tp_atr_mult=1.0, min_rr=2.0
    )
    # TP_atr = 10, TP_rr = 12 * 2 = 24 → rr wins.
    assert plan.tp_pips == 24.0
    assert plan.tp_driver == "min_rr"
    assert abs(plan.expected_rr - 2.0) < 1e-9


def test_negative_inputs_clamped():
    plan = compute_atr_exits(atr_pips=-5.0, spread_pips=-1.0)
    assert plan.atr_pips == 0.0
    assert plan.spread_pips == 0.0
    # SL, TP both still non-negative (and consistent).
    assert plan.sl_pips >= 0.0
    assert plan.tp_pips >= plan.sl_pips


def test_pip_size():
    assert pip_size_for("EUR_USD") == 0.0001
    assert pip_size_for("USD_JPY") == 0.01
    assert pip_size_for("GBP_JPY") == 0.01
    assert pip_size_for("AUD_USD") == 0.0001
