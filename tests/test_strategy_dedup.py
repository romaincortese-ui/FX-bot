from fxbot.strategy_dedup import apply_per_instrument_dedup, select_best_per_instrument


def _cand(instrument, strategy, direction, score, **extra):
    return {"instrument": instrument, "strategy": strategy, "direction": direction, "score": score, **extra}


def test_empty_returns_empty():
    assert select_best_per_instrument([]) == []


def test_single_candidate_passes_through():
    c = _cand("EUR_USD", "SCALPER", "LONG", 72.5)
    assert select_best_per_instrument([c]) == [c]


def test_same_direction_top_wins():
    a = _cand("EUR_USD", "SCALPER", "LONG", 70.0)
    b = _cand("EUR_USD", "REVERSAL", "LONG", 80.0)
    out = select_best_per_instrument([a, b])
    assert len(out) == 1
    assert out[0]["strategy"] == "REVERSAL"


def test_opposite_directions_within_threshold_mutes_both():
    a = _cand("EUR_USD", "SCALPER", "LONG", 72.0)
    b = _cand("EUR_USD", "REVERSAL", "SHORT", 70.0)
    out = select_best_per_instrument([a, b], indeterminate_threshold=5.0)
    assert out == []


def test_opposite_directions_outside_threshold_top_wins():
    a = _cand("EUR_USD", "SCALPER", "LONG", 85.0)
    b = _cand("EUR_USD", "REVERSAL", "SHORT", 60.0)
    out = select_best_per_instrument([a, b], indeterminate_threshold=5.0)
    assert len(out) == 1
    assert out[0]["direction"] == "LONG"
    assert out[0]["score"] == 85.0


def test_multiple_instruments_independent():
    out = select_best_per_instrument([
        _cand("EUR_USD", "SCALPER", "LONG", 70.0),
        _cand("GBP_USD", "TREND", "SHORT", 82.0),
        _cand("EUR_USD", "TREND", "LONG", 60.0),
    ])
    instruments = {c["instrument"] for c in out}
    assert instruments == {"EUR_USD", "GBP_USD"}


def test_apply_disabled_is_identity():
    cands = [
        _cand("EUR_USD", "SCALPER", "LONG", 72.0),
        _cand("EUR_USD", "REVERSAL", "SHORT", 70.0),
    ]
    out = apply_per_instrument_dedup(cands, enabled=False)
    assert len(out) == 2


def test_apply_enabled_mutes():
    cands = [
        _cand("EUR_USD", "SCALPER", "LONG", 72.0),
        _cand("EUR_USD", "REVERSAL", "SHORT", 70.0),
    ]
    assert apply_per_instrument_dedup(cands, enabled=True) == []


def test_candidates_without_instrument_are_dropped():
    cands = [
        {"strategy": "SCALPER", "direction": "LONG", "score": 70.0},
        _cand("EUR_USD", "SCALPER", "LONG", 72.0),
    ]
    out = select_best_per_instrument(cands)
    assert len(out) == 1
    assert out[0]["instrument"] == "EUR_USD"
