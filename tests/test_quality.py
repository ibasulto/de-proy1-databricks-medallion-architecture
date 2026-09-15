import pytest

from utils.quality import assert_balances


def test_assert_balances_pass():
    results = assert_balances(
        [{"name": "net", "expected": 1000, "actual": 1001, "tolerance_pct": 0.05}]
    )
    assert results[0]["passed"] is True


def test_assert_balances_fail_over_tolerance():
    results = assert_balances(
        [{"name": "net", "expected": 1000, "actual": 2000, "tolerance_pct": 0.05}]
    )
    assert results[0]["passed"] is False
    assert results[0]["diff_pct"] == pytest.approx(100.0)


def test_assert_balances_zero_expected_no_divzero():
    results = assert_balances(
        [{"name": "empty", "expected": 0, "actual": 0, "tolerance_pct": 0.01}]
    )
    assert results[0]["passed"] is True
