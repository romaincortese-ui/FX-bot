from datetime import datetime, timezone

from run_daily_calibration import build_rolling_window


def test_build_rolling_window_anchors_to_utc_midnight():
    now = datetime(2026, 4, 4, 12, 34, 56, tzinfo=timezone.utc)

    start, end = build_rolling_window(now, rolling_days=180, end_offset_days=0)

    assert end == datetime(2026, 4, 4, 0, 0, tzinfo=timezone.utc)
    assert start == datetime(2025, 10, 6, 0, 0, tzinfo=timezone.utc)


def test_build_rolling_window_respects_end_offset_days():
    now = datetime(2026, 4, 4, 0, 5, 0, tzinfo=timezone.utc)

    start, end = build_rolling_window(now, rolling_days=90, end_offset_days=1)

    assert end == datetime(2026, 4, 3, 0, 0, tzinfo=timezone.utc)
    assert start == datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc)