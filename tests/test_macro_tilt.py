from fxbot.macro_tilt import (
    MacroTilt,
    compute_macro_tilt,
    pair_tilt_multiplier,
)


def test_empty_inputs_return_zero():
    t = compute_macro_tilt()
    assert isinstance(t, MacroTilt)
    assert t.score == 0.0
    assert t.contributing_weight == 0.0
    assert t.components == {}


def test_strong_dxy_positive_tilts_usd_long():
    t = compute_macro_tilt(dxy_z=+3.0)
    assert t.score > 0.5
    assert "dxy_tilt" in t.components


def test_strong_dxy_negative_tilts_usd_short():
    t = compute_macro_tilt(dxy_z=-3.0)
    assert t.score < -0.5


def test_rate_spread_positive_favours_usd():
    t = compute_macro_tilt(us_minus_other_2y_bp=+150.0)
    assert t.score > 0
    assert t.components["rate_spread_z"] > 0


def test_commodity_strength_softens_usd():
    t = compute_macro_tilt(commodity_20d_return_pct=+10.0)
    assert t.score < 0   # commodities up → USD softer


def test_vix_high_percentile_usd_bid():
    t = compute_macro_tilt(vix_percentile_60d=90.0)
    assert t.score > 0


def test_pair_multiplier_sign_flips_for_quote_usd():
    # Positive USD tilt → EUR_USD should bias SHORT.
    assert pair_tilt_multiplier("EUR_USD", +0.5) < 0
    # Same tilt → USD_JPY should bias LONG.
    assert pair_tilt_multiplier("USD_JPY", +0.5) > 0


def test_pair_multiplier_zero_for_cross():
    assert pair_tilt_multiplier("EUR_GBP", +0.8) == 0.0
    assert pair_tilt_multiplier("AUD_NZD", -0.8) == 0.0


def test_score_clamped_to_unit_range():
    t = compute_macro_tilt(
        dxy_z=100.0,
        us_minus_other_2y_bp=10_000.0,
        us_esi_z=50.0,
        commodity_20d_return_pct=-100.0,
        vix_percentile_60d=100.0,
    )
    assert -1.0 <= t.score <= 1.0


def test_single_component_renormalises():
    t = compute_macro_tilt(dxy_z=+1.5)
    # Only one component contributes; aggregate = component value.
    assert abs(t.score - t.components["dxy_tilt"]) < 1e-9
