import importlib


def test_wide_watchlist_spreads_do_not_block_pair_health(monkeypatch):
    main = importlib.import_module("main")
    main._pair_health.clear()
    main._unsupported_instruments.clear()
    main._last_spread_gate_signature = None

    monkeypatch.setattr(main, "PAPER_TRADE", False, raising=False)
    monkeypatch.setattr(main, "OANDA_API_KEY", "test-key", raising=False)
    monkeypatch.setattr(main, "OANDA_ACCOUNT_ID", "test-account", raising=False)
    monkeypatch.setattr(main, "STATIC_CORE_PAIRS", ["EUR_USD"], raising=False)
    monkeypatch.setattr(main, "STATIC_EXTENDED_PAIRS", [], raising=False)
    monkeypatch.setattr(main, "STATIC_ALL_PAIRS", ["EUR_USD"], raising=False)
    monkeypatch.setattr(main, "WATCHLIST_ALLOWLIST_ONLY", True, raising=False)
    monkeypatch.setattr(main, "filter_supported_pairs", lambda pairs, context="": list(pairs), raising=False)
    monkeypatch.setattr(main, "get_supported_currency_pairs", lambda force=False: {"EUR_USD"}, raising=False)
    monkeypatch.setattr(main, "get_daily_atr", lambda pair: (0.0, 0.01), raising=False)
    monkeypatch.setattr(
        main,
        "_fetch_pricing_chunk",
        lambda chunk: [
            {
                "instrument": "EUR_USD",
                "closeoutBid": "1.10000",
                "closeoutAsk": "1.10060",
            }
        ],
        raising=False,
    )

    for _ in range(12):
        watchlist = main.build_dynamic_watchlist(top_n=1, max_spread_pips=2.5)

    assert watchlist == ["EUR_USD"]
    health = main._ensure_pair_health("EUR_USD")
    assert health["status"] == "healthy"
    assert health["consecutive_spread_failures"] == 0
    assert main.is_pair_tradeable("EUR_USD") is True