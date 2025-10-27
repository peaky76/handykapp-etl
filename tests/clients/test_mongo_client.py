from pendulum import parse

from clients.mongo_client import apply_newmarket_workaround


def test_apply_newmarket_workaround_for_early_rowley():
    assert apply_newmarket_workaround(parse("2023-05-01")) == "Newmarket Rowley"


def test_apply_newmarket_workaround_for_early_july():
    assert apply_newmarket_workaround(parse("2023-06-11")) == "Newmarket July"


def test_apply_newmarket_workaround_for_late_july():
    assert apply_newmarket_workaround(parse("2023-08-15")) == "Newmarket July"


def test_apply_newmarket_workaround_for_late_rowley():
    assert apply_newmarket_workaround(parse("2023-09-01")) == "Newmarket Rowley"
