from fxbot.kill_switch import KillDecision, evaluate_drawdown_kill, format_kill_snapshot


def test_empty_history_is_within_limits():
    d = evaluate_drawdown_kill(daily_pnl_pct=[])
    assert isinstance(d, KillDecision)
    assert d.hard_halt is False
    assert d.soft_cut is False
    assert d.reason == "within_limits"


def test_flat_history_is_within_limits():
    d = evaluate_drawdown_kill(daily_pnl_pct=[0.0] * 100)
    assert d.hard_halt is False
    assert d.soft_cut is False


def test_small_drawdown_no_trigger():
    # ~-3% over 30 days — below 6% soft threshold.
    daily = [-0.001] * 30
    d = evaluate_drawdown_kill(daily_pnl_pct=daily)
    assert d.hard_halt is False
    assert d.soft_cut is False


def test_soft_cut_fires_at_30d_drawdown():
    # Compound drawdown of ~-8% over 30 days.
    daily = [-0.003] * 30
    d = evaluate_drawdown_kill(daily_pnl_pct=daily)
    assert d.soft_cut is True
    assert d.hard_halt is False
    assert d.risk_per_trade_override is not None
    assert 0.0 < d.risk_per_trade_override < 1.0


def test_hard_halt_fires_at_90d_drawdown():
    # ~-12% over 90 days.
    daily = [-0.0015] * 90
    d = evaluate_drawdown_kill(daily_pnl_pct=daily)
    assert d.hard_halt is True
    # Hard halt also implies soft cut.
    assert d.soft_cut is True
    assert d.risk_per_trade_override == 0.0


def test_hard_halt_takes_precedence_over_soft():
    # Large drawdown concentrated in last 30d still breaches 90d threshold.
    daily = [0.0] * 60 + [-0.005] * 30
    d = evaluate_drawdown_kill(daily_pnl_pct=daily)
    assert d.hard_halt is True


def test_recovery_offsets_drawdown():
    # Dip then recover — peak-to-trough still large but current cum
    # return is back near zero. Drawdown metric is peak-relative so
    # the dip should still register.
    daily = [-0.05] + [0.01] * 29  # -5% day then small recovery.
    d = evaluate_drawdown_kill(daily_pnl_pct=daily, soft_cut_threshold_pct=0.03)
    assert d.soft_cut_pct > 0.03


def test_format_kill_snapshot_shape():
    d = evaluate_drawdown_kill(daily_pnl_pct=[-0.003] * 30)
    snap = format_kill_snapshot(d)
    assert "as_of" in snap
    assert snap["soft_cut"] in (True, False)
    assert "reason" in snap
