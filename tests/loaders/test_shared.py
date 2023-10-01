from loaders.shared import convert_person, decondensed_title, select_set


def test_convert_person_with_bha():
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


def test_decondensed_title_with_title():
    assert decondensed_title("J Smith") == "J Smith"


def test_decondensed_title_with_two_capitals():
    assert decondensed_title("JSmith") == "J Smith"


def test_decondensed_title_with_three_capitals():
    assert decondensed_title("JFSmith") == "J F Smith"


def test_decondensed_title_with_four_capitals():
    assert decondensed_title("JFTSmith") == "J F T Smith"


def test_decondensed_title_with_apostrophe():
    assert decondensed_title("JO'Smith") == "J O'Smith"


def test_decondensed_title_on_all_initials():
    assert decondensed_title("JOFS") == "J O F S"
