import pytest

from processors.formdata_processors.race_builder import check_race_complete


@pytest.fixture
def race(mocker):
    race = mocker.patch("models.FormdataRace")
    race.number_of_runners = 6
    return race


def test_check_race_complete_when_not_enough_runners(race):
    runners = []
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_correct_number_of_runners_in_valid_rank(mocker, race):
    runners = [
        mocker.patch("models.FormdataRunner", position="5"),
        mocker.patch("models.FormdataRunner", position="3"),
        mocker.patch("models.FormdataRunner", position="6"),
        mocker.patch("models.FormdataRunner", position="1"),
        mocker.patch("models.FormdataRunner", position="2"),
        mocker.patch("models.FormdataRunner", position="4"),
    ]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_correct_number_of_runners_in_invalid_rank(
    mocker, race
):
    runners = [
        mocker.patch("models.FormdataRunner", position="5"),
        mocker.patch("models.FormdataRunner", position="3"),
        mocker.patch("models.FormdataRunner", position="7"),
        mocker.patch("models.FormdataRunner", position="1"),
        mocker.patch("models.FormdataRunner", position="2"),
        mocker.patch("models.FormdataRunner", position="4"),
    ]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_correct_number_of_runners_in_valid_rank_and_some_equal(
    mocker, race
):
    runners = [
        mocker.patch("models.FormdataRunner", position="5"),
        mocker.patch("models.FormdataRunner", position="=3"),
        mocker.patch("models.FormdataRunner", position="=3"),
        mocker.patch("models.FormdataRunner", position="1"),
        mocker.patch("models.FormdataRunner", position="2"),
        mocker.patch("models.FormdataRunner", position="6"),
    ]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_correct_number_of_runners_in_invalid_rank_and_some_equal(
    mocker, race
):
    runners = [
        mocker.patch("models.FormdataRunner", position="5"),
        mocker.patch("models.FormdataRunner", position="=3"),
        mocker.patch("models.FormdataRunner", position="=3"),
        mocker.patch("models.FormdataRunner", position="1"),
        mocker.patch("models.FormdataRunner", position="2"),
        mocker.patch("models.FormdataRunner", position="7"),
    ]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_correct_number_of_runners_in_valid_rank_with_non_completion(
    mocker, race
):
    runners = [
        mocker.patch("models.FormdataRunner", position="5"),
        mocker.patch("models.FormdataRunner", position="3"),
        mocker.patch("models.FormdataRunner", position="P"),
        mocker.patch("models.FormdataRunner", position="1"),
        mocker.patch("models.FormdataRunner", position="2"),
        mocker.patch("models.FormdataRunner", position="4"),
    ]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_correct_number_of_runners_in_invalid_rank_with_non_completion(
    mocker, race
):
    runners = [
        mocker.patch("models.FormdataRunner", position="5"),
        mocker.patch("models.FormdataRunner", position="3"),
        mocker.patch("models.FormdataRunner", position="6"),
        mocker.patch("models.FormdataRunner", position="1"),
        mocker.patch("models.FormdataRunner", position="2"),
        mocker.patch("models.FormdataRunner", position="P"),
    ]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners
