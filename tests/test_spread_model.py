from datetime import datetime, timezone

from fxbot.spread_model import (
    NEWS_SPREAD_MULTIPLIER,
    Session,
    estimate_spread_pips,
    estimate_stop_slippage_pips,
    session_for_datetime,
    session_for_hour,
)


def _dt(hour: int) -> datetime:
    return datetime(2026, 4, 21, hour, 0, tzinfo=timezone.utc)


def test_session_mapping():
    assert session_for_hour(0) is Session.TOKYO
    assert session_for_hour(5) is Session.TOKYO
    assert session_for_hour(7) is Session.LONDON
    assert session_for_hour(11) is Session.LONDON
    assert session_for_hour(12) is Session.LONDON_NY_OVERLAP
    assert session_for_hour(15) is Session.LONDON_NY_OVERLAP
    assert session_for_hour(16) is Session.NY
    assert session_for_hour(20) is Session.NY
    assert session_for_hour(21) is Session.LATE_NY
    assert session_for_hour(23) is Session.TOKYO


def test_session_for_naive_datetime_assumed_utc():
    naive = datetime(2026, 4, 21, 14, 0)
    assert session_for_datetime(naive) is Session.LONDON_NY_OVERLAP


def test_eur_usd_overlap_tightest():
    overlap = estimate_spread_pips(instrument="EUR_USD", dt_utc=_dt(14))
    tokyo = estimate_spread_pips(instrument="EUR_USD", dt_utc=_dt(3))
    assert overlap.spread_pips < tokyo.spread_pips
    assert overlap.source == "table"


def test_gbp_usd_tokyo_wider_than_overlap():
    tokyo = estimate_spread_pips(instrument="GBP_USD", dt_utc=_dt(3))
    overlap = estimate_spread_pips(instrument="GBP_USD", dt_utc=_dt(14))
    assert tokyo.spread_pips > overlap.spread_pips


def test_usd_jpy_has_table_entry():
    est = estimate_spread_pips(instrument="USD_JPY", dt_utc=_dt(14))
    assert est.source == "table"
    assert est.spread_pips == 0.5


def test_nzd_jpy_cross_uses_table_or_cross_fallback():
    est = estimate_spread_pips(instrument="NZD_JPY", dt_utc=_dt(3))
    assert est.spread_pips >= 2.0
    assert est.source in {"table", "fallback_jpy_cross"}


def test_unmeasured_major_uses_major_fallback():
    # AUD_USD isn't in the table but is a major.
    est = estimate_spread_pips(instrument="AUD_USD", dt_utc=_dt(14))
    assert est.source == "fallback_major"
    assert 0.0 < est.spread_pips < 1.0


def test_unmeasured_jpy_cross_uses_cross_fallback():
    est = estimate_spread_pips(instrument="EUR_JPY", dt_utc=_dt(3))
    assert est.source == "fallback_jpy_cross"


def test_unmeasured_other_cross_uses_other_fallback():
    est = estimate_spread_pips(instrument="EUR_GBP", dt_utc=_dt(14))
    assert est.source == "fallback_other_cross"


def test_news_window_multiplies_spread():
    base = estimate_spread_pips(
        instrument="EUR_USD", dt_utc=_dt(14), inside_tier1_news=False
    )
    news = estimate_spread_pips(
        instrument="EUR_USD", dt_utc=_dt(14), inside_tier1_news=True
    )
    assert abs(news.spread_pips - base.spread_pips * NEWS_SPREAD_MULTIPLIER) < 1e-9
    assert news.inside_tier1_news is True


def test_stop_slippage_increases_during_news():
    regular = estimate_stop_slippage_pips(
        instrument="EUR_USD", dt_utc=_dt(14), inside_tier1_news=False
    )
    news = estimate_stop_slippage_pips(
        instrument="EUR_USD", dt_utc=_dt(14), inside_tier1_news=True
    )
    assert news > regular * 4.0
