from fxbot.news_impact import NewsImpact, classify_news_impact


def test_nfp_on_eur_usd_blocks():
    d = classify_news_impact(
        event_title="Non-Farm Employment Change",
        event_currency="USD",
        instrument="EUR_USD",
    )
    assert d.impact is NewsImpact.BLOCK


def test_nfp_on_aud_nzd_reduces():
    d = classify_news_impact(
        event_title="Non-Farm Employment Change",
        event_currency="USD",
        instrument="AUD_NZD",
    )
    assert d.impact is NewsImpact.REDUCE


def test_nfp_on_eur_gbp_passes():
    d = classify_news_impact(
        event_title="Non-Farm Employment Change",
        event_currency="USD",
        instrument="EUR_GBP",
    )
    assert d.impact is NewsImpact.PASS


def test_ecb_on_eur_usd_blocks():
    d = classify_news_impact(
        event_title="ECB Main Refinancing Rate",
        event_currency="EUR",
        instrument="EUR_USD",
    )
    assert d.impact is NewsImpact.BLOCK


def test_ecb_on_gbp_usd_reduces():
    d = classify_news_impact(
        event_title="ECB Main Refinancing Rate",
        event_currency="EUR",
        instrument="GBP_USD",
    )
    assert d.impact is NewsImpact.REDUCE


def test_boj_on_usd_jpy_blocks():
    d = classify_news_impact(
        event_title="BoJ Policy Rate",
        event_currency="JPY",
        instrument="USD_JPY",
    )
    assert d.impact is NewsImpact.BLOCK


def test_boj_on_eur_usd_passes():
    d = classify_news_impact(
        event_title="BoJ Policy Rate",
        event_currency="JPY",
        instrument="EUR_USD",
    )
    assert d.impact is NewsImpact.PASS


def test_rba_on_aud_usd_blocks():
    d = classify_news_impact(
        event_title="RBA Cash Rate",
        event_currency="AUD",
        instrument="AUD_USD",
    )
    assert d.impact is NewsImpact.BLOCK


def test_rba_on_nzd_usd_reduces():
    d = classify_news_impact(
        event_title="RBA Cash Rate",
        event_currency="AUD",
        instrument="NZD_USD",
    )
    assert d.impact is NewsImpact.REDUCE


def test_unknown_event_on_currency_leg_blocks():
    d = classify_news_impact(
        event_title="Some Minor Release",
        event_currency="USD",
        instrument="EUR_USD",
    )
    assert d.impact is NewsImpact.BLOCK


def test_unknown_event_unrelated_passes():
    d = classify_news_impact(
        event_title="Some Minor Release",
        event_currency="USD",
        instrument="EUR_GBP",
    )
    assert d.impact is NewsImpact.PASS


def test_malformed_instrument_passes():
    d = classify_news_impact(
        event_title="NFP",
        event_currency="USD",
        instrument="EURUSD",
    )
    assert d.impact is NewsImpact.PASS
