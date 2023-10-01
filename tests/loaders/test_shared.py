from loaders.shared import convert_person, select_set


def test_convert_person():
    expected = {
        "title": "Mr.",
        "first": "John",
        "middle": "",
        "last": "Smith",
        "suffix": "",
        "nickname": "",
        "display_name": {"BHA": "Mr. John Smith"},
    }
    assert expected == convert_person("Mr. John Smith", "BHA")


def test_select_set():
    data = [
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER2"},
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER3"},
    ]
    assert ["TRAINER1", "TRAINER2", "TRAINER3"] == select_set(data, "trainer")
