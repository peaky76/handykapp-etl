import pytest

from processors.formdata_processors.race_builder import (
    all_duplicate_positions_have_equals,
    check_race_complete,
    validate_positions,
)

# Based on https://www.racingpost.com/results/6/beverley/2024-08-31/873840
BEV_DIV_ONE_DATA = [
    ["1", -0.3, "9-10", 0, 80],  # -56
    ["2", 0.3, "9-7", 0, 74],  # -59
    ["3", 0.8, "9-12", 0, 77],  # -61
    ["4", 1.5, "8-11", 3, 62],  # -64
    ["5", 1.75, "10-2", 0, 78],  # -64
    ["6", 2.75, "9-8", 5, 71],  # -68
]

# Based on https://www.racingpost.com/results/6/beverley/2024-08-31/876000
BEV_DIV_TWO_DATA = [
    ["1", -0.5, "9-7", 0, 78],  # -55
    ["2", 0.5, "9-6", 0, 77],  # -55
    ["3", 1.0, "9-8", 0, 77],  # -57
    ["4", 1.5, "9-2", 3, 72],  # -59
    ["5", 1.75, "9-10", 0, 76],  # -60
    ["6", 3.25, "9-10", 0, 69],  # -67
]


def build_mock_runner(
    mocker, position, beaten_distance, weight, allowance, form_rating
):
    return mocker.patch(
        "models.FormdataRunner",
        position=position,
        beaten_distance=beaten_distance,
        weight=weight,
        allowance=allowance,
        form_rating=form_rating,
    )


@pytest.fixture
def race(mocker):
    race = mocker.patch("models.FormdataRace")
    race.number_of_runners = 6
    return race


@pytest.fixture
def div_one_runners(mocker):
    return [build_mock_runner(mocker, *datum) for datum in BEV_DIV_ONE_DATA]


@pytest.fixture
def div_two_runners(mocker):
    return [build_mock_runner(mocker, *datum) for datum in BEV_DIV_TWO_DATA]


@pytest.fixture
def non_finisher(mocker):
    return build_mock_runner(mocker, "P", 0, "9-10", 0, None)


@pytest.fixture
def runners_with_equal_positions(mocker, div_one_runners):
    tied_fourth_one = build_mock_runner(mocker, *BEV_DIV_ONE_DATA[3])
    tied_fourth_one.position = "=4"
    tied_fourth_two = build_mock_runner(mocker, "=4", 1.5, "10-2", 0, 78)
    sixth = build_mock_runner(mocker, *BEV_DIV_ONE_DATA[5])
    sixth.beaten_distance -= 0.25
    return [*div_one_runners[:3], tied_fourth_one, tied_fourth_two, sixth]


@pytest.fixture
def runners_with_invalid_rank(mocker, div_one_runners):
    seventh = build_mock_runner(mocker, "7", 0, "9-10", 0, 80)
    return [*div_one_runners[:5], seventh]


def test_all_duplicate_positions_have_equals_when_they_do(mocker):
    runner_1 = build_mock_runner(mocker, "=1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "=1", 0, "9-7", 0, 74)
    runner_3 = build_mock_runner(mocker, "3", 0, "9-7", 0, 74)

    assert all_duplicate_positions_have_equals([runner_1, runner_2, runner_3]) is True


def test_all_duplicate_positions_have_equals_when_they_do_not(mocker):
    runner_1 = build_mock_runner(mocker, "=1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "1", 0, "9-7", 0, 74)
    runner_3 = build_mock_runner(mocker, "3", 0, "9-7", 0, 74)

    assert all_duplicate_positions_have_equals([runner_1, runner_2, runner_3]) is False


def test_validate_positions_when_clearly_consecutive(mocker):
    runner_1 = build_mock_runner(mocker, "1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "2", 0, "9-7", 0, 74)
    assert validate_positions([runner_1, runner_2]) is True


def test_validate_positions_when_identical_with_equal(mocker):
    runner_1 = build_mock_runner(mocker, "=1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "=1", 0, "9-7", 0, 74)
    assert validate_positions([runner_1, runner_2]) is True


def test_validate_positions_when_identical_without_equal(mocker):
    runner_1 = build_mock_runner(mocker, "1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "1", 0, "9-7", 0, 74)
    assert validate_positions([runner_1, runner_2]) is False


def test_validate_positions_when_identical_with_mixed_equals(mocker):
    runner_1 = build_mock_runner(mocker, "=1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "1", 0, "9-7", 0, 74)
    assert validate_positions([runner_1, runner_2]) is False


def test_validate_positions_when_non_consecutive(mocker):
    runner_1 = build_mock_runner(mocker, "1", 0, "9-10", 0, 80)
    runner_2 = build_mock_runner(mocker, "3", 0, "9-7", 0, 74)
    assert validate_positions([runner_1, runner_2]) is False


# Single races
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
    race, runners_with_invalid_rank
):
    runners = runners_with_invalid_rank
    actual = check_race_complete(race, runners)

    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank_and_some_equal(
    race, runners_with_equal_positions
):
    runners = runners_with_equal_positions
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


def test_check_race_complete_when_exact_number_of_runners_in_valid_rank_with_non_completion(
    race, div_one_runners, non_finisher
):
    runners = [*div_one_runners[:5], non_finisher]
    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []


# Multiple races
def test_check_race_complete_when_exact_number_of_runners_in_invalid_rank_with_non_completion(
    race, div_one_runners, non_finisher
):
    runners = [*div_one_runners[:3], non_finisher, *div_one_runners[4:]]
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


def test_check_race_complete_when_indeterminate_non_finisher(
    race, div_one_runners, div_two_runners, non_finisher
):
    runners = [*div_two_runners[:5], *div_one_runners[:5], non_finisher]
    actual = check_race_complete(race, runners)

    assert len(runners) == 11
    assert actual["complete"] == []
    assert actual["todo"] == runners


def test_check_race_complete_when_no_ratings(race, div_one_runners):
    runners = div_one_runners
    for runner in runners:
        runner.form_rating = None

    actual = check_race_complete(race, runners)

    assert actual["complete"] == runners
    assert actual["todo"] == []
