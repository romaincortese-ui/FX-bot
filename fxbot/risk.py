def would_breach_correlation_limit(
    open_trades: list[dict],
    instrument: str,
    direction: str,
    max_correlated_trades: int,
) -> tuple[bool, int, int]:
    base, quote = instrument.split("_")
    usd_long = 0
    usd_short = 0

    for trade in open_trades:
        trade_base, trade_quote = trade["instrument"].split("_")
        trade_direction = trade["direction"]
        if trade_base == "USD":
            if trade_direction == "LONG":
                usd_long += 1
            else:
                usd_short += 1
        elif trade_quote == "USD":
            if trade_direction == "LONG":
                usd_short += 1
            else:
                usd_long += 1

    if quote == "USD":
        if direction == "LONG":
            usd_short += 1
        else:
            usd_long += 1
    elif base == "USD":
        if direction == "LONG":
            usd_long += 1
        else:
            usd_short += 1

    return max(usd_long, usd_short) > max_correlated_trades, usd_long, usd_short
