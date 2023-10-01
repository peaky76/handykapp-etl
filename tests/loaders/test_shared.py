from loaders.shared import select_set


def test_select_set():
    data = [
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER2"},
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER3"},
    ]
    assert ["TRAINER1", "TRAINER2", "TRAINER3"] == select_set(data, "trainer")
