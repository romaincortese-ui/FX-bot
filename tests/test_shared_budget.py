import json
import os
import threading

from fxbot.shared_budget import (
    atomic_release,
    atomic_reserve,
    read_state,
    total_reserved,
)


def test_reserve_accepts_under_cap(tmp_path):
    ledger = tmp_path / "budget.json"
    r = atomic_reserve(
        path=str(ledger),
        key="fx:eur_usd",
        amount=0.01,
        max_total=0.05,
    )
    assert r.accepted is True
    assert r.reservation_id
    assert r.total_after == 0.01
    state = read_state(str(ledger))
    assert len(state["reservations"]) == 1


def test_reserve_rejects_when_would_exceed(tmp_path):
    ledger = tmp_path / "budget.json"
    assert atomic_reserve(path=str(ledger), key="a", amount=0.04, max_total=0.05).accepted is True
    r = atomic_reserve(path=str(ledger), key="b", amount=0.02, max_total=0.05)
    assert r.accepted is False
    assert r.reason.startswith("would_exceed_cap")
    # Rejected reservation does NOT mutate ledger.
    assert total_reserved(str(ledger)) == 0.04


def test_reserve_allows_exact_cap(tmp_path):
    ledger = tmp_path / "budget.json"
    r = atomic_reserve(path=str(ledger), key="a", amount=0.05, max_total=0.05)
    assert r.accepted is True


def test_release_removes_reservation(tmp_path):
    ledger = tmp_path / "budget.json"
    r1 = atomic_reserve(path=str(ledger), key="a", amount=0.01, max_total=0.05)
    r2 = atomic_reserve(path=str(ledger), key="b", amount=0.02, max_total=0.05)
    assert total_reserved(str(ledger)) == 0.03
    assert atomic_release(path=str(ledger), reservation_id=r1.reservation_id) is True
    assert total_reserved(str(ledger)) == 0.02
    # Double release is a no-op.
    assert atomic_release(path=str(ledger), reservation_id=r1.reservation_id) is False


def test_release_unknown_id_is_false(tmp_path):
    ledger = tmp_path / "budget.json"
    atomic_reserve(path=str(ledger), key="a", amount=0.01, max_total=0.05)
    assert atomic_release(path=str(ledger), reservation_id="not-a-real-id") is False


def test_non_positive_amount_rejected(tmp_path):
    ledger = tmp_path / "budget.json"
    r = atomic_reserve(path=str(ledger), key="a", amount=0.0, max_total=0.05)
    assert r.accepted is False
    assert r.reason == "non_positive_amount"


def test_read_missing_file_returns_empty(tmp_path):
    ledger = tmp_path / "does_not_exist.json"
    state = read_state(str(ledger))
    assert state["reservations"] == []


def test_concurrent_reserves_respect_cap(tmp_path):
    """Run many threads racing against a tight cap; total reserved must
    never exceed the cap, and the count of accepted reservations must
    equal ``cap / amount``.
    """
    ledger = tmp_path / "budget.json"
    cap = 0.10
    per_amount = 0.01  # expect exactly 10 accepted
    results: list[bool] = []
    lock_for_list = threading.Lock()

    def worker():
        r = atomic_reserve(
            path=str(ledger),
            key="x",
            amount=per_amount,
            max_total=cap,
        )
        with lock_for_list:
            results.append(r.accepted)

    threads = [threading.Thread(target=worker) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = sum(1 for r in results if r)
    # Under a correct lock, exactly 10 reservations should have been
    # accepted (floor(cap / amount)).
    assert accepted == 10
    assert total_reserved(str(ledger)) <= cap + 1e-9


def test_state_persists_on_disk(tmp_path):
    ledger = tmp_path / "budget.json"
    r = atomic_reserve(path=str(ledger), key="a", amount=0.01, max_total=0.05)
    assert r.accepted
    raw = json.loads((ledger).read_text(encoding="utf-8"))
    assert "reservations" in raw
    assert raw["reservations"][0]["amount"] == 0.01
    assert "last_updated" in raw
