from fxbot.lane_filter import parse_trade_lanes, trade_lane_block_reason


def test_trade_lane_allowlist_accepts_matching_direction():
    allowlist = parse_trade_lanes("SCALPER:AUD_USD:SHORT,SCALPER:EUR_USD:LONG")

    assert trade_lane_block_reason("SCALPER", "AUD_USD", "SHORT", allowlist) is None
    assert trade_lane_block_reason("SCALPER", "AUD_USD", "LONG", allowlist) == "trade lane not allowlisted"


def test_trade_lane_blocklist_overrides_allowlist():
    allowlist = parse_trade_lanes("SCALPER:*:*")
    blocklist = parse_trade_lanes("SCALPER:USD_CHF:SHORT")

    assert trade_lane_block_reason("SCALPER", "USD_CHF", "SHORT", allowlist, blocklist) == "trade lane blocked"
    assert trade_lane_block_reason("SCALPER", "USD_CHF", "LONG", allowlist, blocklist) is None
