from processors.utils import compact


def test_compact_removes_empty_values():
    assert compact({"a": 1, "b": None, "c": 0}) == {"a": 1, "c": 0}
