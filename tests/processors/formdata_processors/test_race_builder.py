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
        ["1", -0.2, "9-10", 0, 80],  # -56
        ["2", 0.2, "9-7", 0, 74],  # -59
        ["3", 0.8, "9-12", 0, 77],  # -61
        ["4", 1.5, "8-11", 3, 62],  # -64
        ["5", 1.75, "10-2", 0, 78],  # -64
        ["6", 2.75, "9-8", 5, 71],  # -63
    ]
    return [
        mocker.patch(
            "models.FormdataRunner",
            position=datum[0],
            beaten_distance=datum[1],
            weight=datum[2],
            allowance=datum[3],
            form_rating=datum[4],
        )
        for datum in data
    ]


@pytest.fixture
def div_two_runners(mocker):
    # Based on https://www.racingpost.com/results/6/beverley/2024-08-31/876000
    data = [
        ["1", -0.5, "9-7", 0, 78],  # -55
        ["2", 0.5, "9-6", 0, 77],  # -55
        ["3", 1.0, "9-8", 0, 77],  # -57
        ["4", 1.5, "9-2", 3, 72],  # -59
        ["5", 1.75, "9-10", 0, 76],  # -60
        ["6", 3.25, "9-10", 0, 69],  # -67
    ]
    return [
        mocker.patch(
            "models.FormdataRunner",
            position=datum[0],
            beaten_distance=datum[1],
            weight=datum[2],
            allowance=datum[3],
            form_rating=datum[4],
        )
        for datum in data
    ]


def test_check_race_complete_when_not_enough_runners(race):
    runners = []
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank(
    race, div_one_runners
):
    runners = div_one_runners
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank(
    race, div_one_runners
):
    div_one_runners[0].position = "7"
    runners = div_one_runners
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank_and_some_equal(
    race, div_one_runners
):
    div_one_runners[3].position = "=4"
    div_one_runners[4].position = "=4"
    div_one_runners[4].beaten_distance = 1.5

    runners = div_one_runners
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank_and_some_equal(
    race, div_one_runners
):
    div_one_runners[0].position = "7"
    div_one_runners[3].position = "=4"
    div_one_runners[4].position = "=4"
    div_one_runners[4].beaten_distance = 1.5

    runners = div_one_runners
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank_with_non_completion(
    race, div_one_runners
):
    div_one_runners[5].position = "P"
    runners = div_one_runners
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank_with_non_completion(
    race, div_one_runners
):
    div_one_runners[3].position = "P"
    runners = div_one_runners
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


def test_check_race_complete_with_mixed_divs_with_potentially_conflicting_non_equal_ranks(
    race, div_one_runners, div_two_runners
):
    runners = [*div_one_runners[:4], div_two_runners[3], div_two_runners[5]]
    actual = check_race_complete(race, runners)

    assert len(runners) == 6
    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_with_mixed_divs_which_would_fit_ranks_but_not_ratings(
    race, div_one_runners, div_two_runners
):
    runners = [*div_one_runners[:3], *div_two_runners[3:]]
    actual = check_race_complete(race, runners)

    assert len(runners) == 6
    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_with_mixed_divs_which_would_fit_ranks_and_ratings(
    race, div_one_runners, div_two_runners
):
    runners = [*div_one_runners[:5], div_two_runners[5]]
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
    runners = [div_one_runners[3], *div_two_runners]
    actual = check_race_complete(race, runners)

    assert len(runners) == 7
    assert actual["complete"] == div_two_runners
    assert actual["todo"] == [div_one_runners[3]]
