import pytest

from processors.formdata_processors.race_builder import check_race_complete


@pytest.fixture
def race(mocker):
    race = mocker.patch("models.FormdataRace")
    race.number_of_runners = 6
    return race


@pytest.fixture
def div_one_runners(mocker):
    # Based on https://www.racingpost.com/results/6/beverley/2024-08-31/873840
    data = [
        ["1", -0.2, "9-10", 80],
        ["2", 0.2, "9-7", 74],
        ["3", 0.8, "9-12", 77],
        ["4", 1.5, "8-11", 62],
        ["5", 1.75, "10-2", 78],
        ["6", 2.75, "9-8", 71],
    ]
    return [
        mocker.patch(
            "models.FormdataRunner",
            position=datum[0],
            beaten_distance=datum[1],
            weight=datum[2],
            form_rating=datum[3],
        )
        for datum in data
    ]


@pytest.fixture
def div_two_runners(mocker):
    # Based on https://www.racingpost.com/results/6/beverley/2024-08-31/876000
    data = [
        ["1", -0.5, "9-7", 78],
        ["2", 0.5, "9-6", 77],
        ["3", 1.0, "9-8", 77],
        ["4", 1.5, "9-2", 72],
        ["5", 1.75, "9-10", 76],
        ["6", 3.25, "9-10", 69],
    ]
    return [
        mocker.patch(
            "models.FormdataRunner",
            position=datum[0],
            beaten_distance=datum[1],
            weight=datum[2],
            form_rating=datum[3],
        )
        for datum in data
    ]


def test_check_race_complete_when_not_enough_runners(race):
    runners = []
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank(mocker, race):
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


def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank(mocker, race):
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


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank_and_some_equal(
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


def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank_and_some_equal(
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


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank_with_non_completion(
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


def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank_with_non_completion(
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


def test_check_race_complete_when_exact_number_of_runners_but_mixed_divs(
    race, div_one_runners, div_two_runners
):
    runners = [*div_one_runners[:3], *div_two_runners[3:]]
    actual = check_race_complete(race, runners)

    assert len(runners) == 6
    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_extra_runners_and_not_enough_from_one_div(
    race, div_one_runners, div_two_runners
):
    runners = [*div_one_runners[:3], *div_two_runners[2:]]
    actual = check_race_complete(race, runners)

    assert len(runners) == 7
    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_extra_runners_but_one_complete_div(
    race, div_one_runners, div_two_runners
):
    runners = [div_one_runners[2], *div_two_runners]
    actual = check_race_complete(race, runners)

    assert len(runners) == 7
    assert actual["complete"] == div_two_runners
    assert actual["todo"] == [div_one_runners[2]]
