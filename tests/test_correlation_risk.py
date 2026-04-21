from fxbot.correlation_risk import (
    CORE_PAIRS,
    compute_portfolio_vol_pct,
    default_correlation_matrix,
    would_breach_portfolio_cap,
)


def _trade(instrument, direction, risk_pct):
    return {"instrument": instrument, "direction": direction, "risk_pct": risk_pct}


def test_default_matrix_is_symmetric_and_diag_one():
    corr = default_correlation_matrix()
    for pair in CORE_PAIRS:
        assert corr[(pair, pair)] == 1.0
    for (a, b), rho in corr.items():
        assert corr[(b, a)] == rho


def test_empty_book_has_zero_vol():
    vol, weights = compute_portfolio_vol_pct([])
    assert vol == 0.0
    assert weights == {}


def test_single_trade_vol_equals_abs_risk():
    vol, weights = compute_portfolio_vol_pct([_trade("EUR_USD", "LONG", 0.015)])
    assert abs(vol - 0.015) < 1e-9
    assert weights == {"EUR_USD": 0.015}


def test_two_correlated_long_trades_increase_vol():
    """Long EUR/USD + long GBP/USD: correlated 0.75 → combined vol > single."""
    single_vol, _ = compute_portfolio_vol_pct([_trade("EUR_USD", "LONG", 0.015)])
    combined_vol, _ = compute_portfolio_vol_pct([
        _trade("EUR_USD", "LONG", 0.015),
        _trade("GBP_USD", "LONG", 0.015),
    ])
    assert combined_vol > single_vol
    # With rho=0.75, combined vol should be about sqrt(2 * 0.015^2 * (1 + 0.75))
    assert combined_vol < 0.030  # but much less than 2x naive sum


def test_opposite_correlated_trades_cancel():
    """Long EUR/USD + long USD/CHF (rho -0.85 → LONG USD/CHF is -EUR/USD-like) should net down."""
    naive = 0.03
    combined_vol, _ = compute_portfolio_vol_pct([
        _trade("EUR_USD", "LONG", 0.015),
        _trade("USD_CHF", "LONG", 0.015),
    ])
    assert combined_vol < naive


def test_would_breach_accepts_small_new_trade():
    open_trades = [_trade("EUR_USD", "LONG", 0.010)]
    d = would_breach_portfolio_cap(
        open_trades=open_trades,
        candidate_instrument="USD_JPY",
        candidate_direction="LONG",
        candidate_risk_pct=0.010,
        cap_pct=0.03,
    )
    assert d.allowed is True
    assert d.portfolio_vol_after > d.portfolio_vol_before


def test_would_breach_rejects_correlated_stack():
    open_trades = [
        _trade("EUR_USD", "LONG", 0.015),
        _trade("GBP_USD", "LONG", 0.015),
        _trade("AUD_USD", "LONG", 0.015),
    ]
    d = would_breach_portfolio_cap(
        open_trades=open_trades,
        candidate_instrument="NZD_USD",
        candidate_direction="LONG",
        candidate_risk_pct=0.015,
        cap_pct=0.03,
    )
    assert d.allowed is False
    assert "would_breach_cap" in d.reason


def test_cross_pair_decomposes_into_legs():
    """EUR_JPY LONG expands to EUR_USD + USD_JPY, so a short EUR_USD hedges partially."""
    book_a, _ = compute_portfolio_vol_pct([_trade("EUR_JPY", "LONG", 0.015)])
    book_b, weights = compute_portfolio_vol_pct([
        _trade("EUR_JPY", "LONG", 0.015),
        _trade("EUR_USD", "SHORT", 0.015),
    ])
    # EUR_USD leg cancels in the EUR_JPY decomposition; USD_JPY leg remains.
    assert "USD_JPY" in weights
    assert abs(weights["EUR_USD"]) < 1e-9


def test_malformed_instrument_decomposes_to_empty():
    d = would_breach_portfolio_cap(
        open_trades=[],
        candidate_instrument="GARBAGE",
        candidate_direction="LONG",
        candidate_risk_pct=0.015,
        cap_pct=0.03,
    )
    # Empty decomposition → zero vol → allowed.
    assert d.allowed is True
    assert d.portfolio_vol_after == 0.0
