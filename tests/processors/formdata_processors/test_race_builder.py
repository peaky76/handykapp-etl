import pytest

from processors.formdata_processors.race_builder import (
    all_duplicate_positions_have_equals,
    build_record,
    calculate_adjusted_ratings,
    check_consecutive,
    check_race_complete,
    get_consecutive_tied_count,
    get_position_num,
    get_valid_combinations,
    is_finisher,
    is_monotonically_decreasing_or_equal,
    validate_positions,
    validate_ratings_vs_distances,
    validate_ratings_vs_positions,
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


def test_get_position_num_with_numeric_position(mocker):
    assert get_position_num("3") == 3


def test_get_position_num_with_equal_position(mocker):
    assert get_position_num("=2") == 2


def test_get_position_num_with_steward_changed_position(mocker):
    assert get_position_num("2p3") == 2


def test_get_position_num_with_non_completion(mocker):
    with pytest.raises(ValueError):
        get_position_num("P")


def test_is_finisher_with_numeric_position(mocker):
    runner = mocker.Mock(position="3")
    assert is_finisher(runner) is True


def test_is_finisher_with_equal_position(mocker):
    runner = mocker.Mock(position="=2")
    assert is_finisher(runner) is True


def test_is_finisher_with_non_numeric_position(mocker):
    runner = mocker.Mock(position="P")
    assert is_finisher(runner) is False


def test_is_monotonically_decreasing_or_equal_with_decreasing_sequence():
    assert is_monotonically_decreasing_or_equal((5, 4, 3, 2, 1)) is True


def test_is_monotonically_decreasing_or_equal_with_equal_elements():
    assert is_monotonically_decreasing_or_equal((5, 5, 4, 4, 3)) is True


def test_is_monotonically_decreasing_or_equal_with_increasing_sequence():
    assert is_monotonically_decreasing_or_equal((1, 2, 3, 4, 5)) is False


def test_is_monotonically_decreasing_or_equal_with_mixed_sequence():
    assert is_monotonically_decreasing_or_equal((5, 4, 6, 3, 2)) is False


def test_check_consecutive_with_consecutive_positions():
    assert check_consecutive("1", "2") is True


def test_check_consecutive_with_non_consecutive_positions():
    assert check_consecutive("1", "3") is False


def test_check_consecutive_with_equal_positions():
    assert check_consecutive("=1", "=1") is True


def test_check_consecutive_with_equal_and_numeric_positions():
    assert check_consecutive("=1", "1") is False


def test_check_consecutive_with_subsequent_equal_positions():
    assert check_consecutive("1", "=2") is True


def test_check_consecutive_with_equal_position_followed_by_unfeasible():
    assert check_consecutive("=1", "2") is False


def test_check_consecutive_when_run_of_ties_would_exceeds_next_value():
    assert check_consecutive("=1", "3", 3) is False


def test_check_consecutive_when_run_of_ties_reaches_next_value():
    assert check_consecutive("=1", "4", 3) is True


def test_check_consecutive_when_run_of_ties_does_not_reach_next_value():
    assert check_consecutive("=1", "5", 3) is False


def test_get_consecutive_tied_count_when_zero(mocker):
    r1 = mocker.Mock(position="1")
    r2 = mocker.Mock(position="2")
    r3 = mocker.Mock(position="3")

    assert get_consecutive_tied_count([r1, r2, r3]) == 0


def test_get_consecutive_tied_count_when_one_tie(mocker):
    r1 = mocker.Mock(position="1")
    r2 = mocker.Mock(position="2")
    r3 = mocker.Mock(position="=3")

    assert get_consecutive_tied_count([r1, r2, r3]) == 1


def test_get_consecutive_tied_count_when_multiple_ties(mocker):
    r1 = mocker.Mock(position="1")
    r2 = mocker.Mock(position="=2")
    r3 = mocker.Mock(position="=2")

    assert get_consecutive_tied_count([r1, r2, r3]) == 2


def test_get_valid_combinations_with_no_runners():
    actual = get_valid_combinations([], 4)
    expected = []

    assert actual == expected


def test_get_valid_combinations_with_one_runner(mocker):
    r1 = mocker.Mock(position="1")
    r2 = mocker.Mock(position="1")
    actual = get_valid_combinations([r1, r2], 1)
    expected = [[r1], [r2]]

    assert actual == expected


def test_get_valid_combinations_when_fewer_runners_than_required(mocker):
    r1 = mocker.Mock(position="1")
    r2 = mocker.Mock(position="2")
    actual = get_valid_combinations([r1, r2], 3)
    expected = []

    assert actual == expected


def test_get_valid_combinations_in_base_case(mocker):
    r1a = mocker.Mock(position="1")
    r1b = mocker.Mock(position="1")
    r2a = mocker.Mock(position="2")
    r2b = mocker.Mock(position="2")
    r3a = mocker.Mock(position="3")
    r3b = mocker.Mock(position="3")
    r4a = mocker.Mock(position="4")
    r4b = mocker.Mock(position="4")
    actual = get_valid_combinations([r1a, r1b, r2a, r2b, r3a, r3b, r4a, r4b], 4)
    expected = [
        [r1a, r2a, r3a, r4a],
        [r1b, r2a, r3a, r4a],
        [r1a, r2b, r3a, r4a],
        [r1b, r2b, r3a, r4a],
        [r1a, r2a, r3b, r4a],
        [r1b, r2a, r3b, r4a],
        [r1a, r2a, r3a, r4b],
        [r1b, r2a, r3a, r4b],
        [r1a, r2b, r3b, r4a],
        [r1b, r2b, r3b, r4a],
        [r1a, r2b, r3a, r4b],
        [r1b, r2b, r3a, r4b],
        [r1a, r2a, r3b, r4b],
        [r1b, r2a, r3b, r4b],
        [r1a, r2b, r3b, r4b],
        [r1b, r2b, r3b, r4b],
    ]

    assert len(actual) == len(expected)
    for combo in expected:
        assert combo in actual, (
            f"Expected combination {combo} not found in actual combinations"
        )


def test_get_valid_combinations_with_a_tied_position(mocker):
    r1a = mocker.Mock(position="1")
    r1b = mocker.Mock(position="1")
    r2a = mocker.Mock(position="2")
    r2b = mocker.Mock(position="=2")
    r2c = mocker.Mock(position="=2")
    r3 = mocker.Mock(position="3")
    r4a = mocker.Mock(position="4")
    r4b = mocker.Mock(position="4")
    actual = get_valid_combinations([r1a, r1b, r2a, r2b, r2c, r3, r4a, r4b], 4)
    expected = [
        [r1a, r2a, r3, r4a],
        [r1b, r2a, r3, r4a],
        [r1a, r2b, r2c, r4a],
        [r1b, r2b, r2c, r4a],
        [r1a, r2a, r3, r4b],
        [r1b, r2a, r3, r4b],
        [r1a, r2b, r2c, r4b],
        [r1b, r2b, r2c, r4b],
    ]

    assert len(actual) == len(expected)
    for combo in expected:
        assert combo in actual, (
            f"Expected combination {combo} not found in actual combinations"
        )


def test_get_valid_combinations_with_non_finisher(mocker):
    r1a = mocker.Mock(position="1")
    r1b = mocker.Mock(position="1")
    r2a = mocker.Mock(position="2")
    r2b = mocker.Mock(position="2")
    r3a = mocker.Mock(position="3")
    r3b = mocker.Mock(position="3")
    r4 = mocker.Mock(position="4")
    r0 = mocker.Mock(position="P")
    actual = get_valid_combinations([r1a, r1b, r2a, r2b, r3a, r3b, r4, r0], 4)
    expected = [
        [r1a, r2a, r3a, r4],
        [r1b, r2a, r3a, r4],
        [r1a, r2b, r3a, r4],
        [r1b, r2b, r3a, r4],
        [r1a, r2a, r3b, r4],
        [r1b, r2a, r3b, r4],
        [r1a, r2a, r3a, r0],
        [r1b, r2a, r3a, r0],
        [r1a, r2b, r3b, r4],
        [r1b, r2b, r3b, r4],
        [r1a, r2b, r3a, r0],
        [r1b, r2b, r3a, r0],
        [r1a, r2a, r3b, r0],
        [r1b, r2a, r3b, r0],
        [r1a, r2b, r3b, r0],
        [r1b, r2b, r3b, r0],
    ]

    assert len(actual) == len(expected)
    for combo in expected:
        assert combo in actual, (
            f"Expected combination {combo} not found in actual combinations"
        )


def test_calculate_adjusted_ratings():
    weights = ("9-10", "9-7", "9-2")
    allowances = (0, 3, 5)
    form_ratings = (80, 75, None)

    expected = [80 - (136 + 0), 75 - (133 + 3)]
    result = calculate_adjusted_ratings(weights, allowances, form_ratings)

    assert result == expected


# def test_validate_ratings_vs_positions_with_valid_ratings(mocker):
#     runner1 = mocker.Mock(weight="9-10", allowance=0, form_rating=80)
#     runner2 = mocker.Mock(weight="9-7", allowance=0, form_rating=74)

#     is_valid, ratings = validate_ratings_vs_positions([runner1, runner2])

#     assert is_valid is True
#     assert ratings == [-56, -59]


# def test_validate_ratings_vs_positions_with_invalid_ratings(mocker):
#     runner1 = mocker.Mock(weight="9-10", allowance=0, form_rating=70)  # Adjusted: -66
#     runner2 = mocker.Mock(weight="9-7", allowance=0, form_rating=74)  # Adjusted: -59

#     is_valid, ratings = validate_ratings_vs_positions([runner1, runner2])

#     assert is_valid is False
#     assert ratings == [-66, -59]


# def test_validate_ratings_vs_distances_valid_case(mocker):
#     finishers = [
#         mocker.Mock(beaten_distance=0),
#         mocker.Mock(beaten_distance=1.5),
#         mocker.Mock(beaten_distance=3.0),
#     ]
#     adjusted_ratings = [-56, -59, -62]

#     assert validate_ratings_vs_distances(finishers, adjusted_ratings) is True


# def test_validate_ratings_vs_distances_with_inconsistent_ratios(mocker):
#     finishers = [
#         mocker.Mock(beaten_distance=0),
#         mocker.Mock(beaten_distance=1.0),
#         mocker.Mock(beaten_distance=10.0),
#     ]
#     adjusted_ratings = [-56, -58, -60]  # Inconsistent ratio between gaps

#     assert validate_ratings_vs_distances(finishers, adjusted_ratings) is False


# def test_validate_ratings_vs_distances_with_insufficient_data(mocker):
#     finishers = [mocker.Mock(beaten_distance=0), mocker.Mock(beaten_distance=1.5)]
#     adjusted_ratings = [-56, -59]

#     # Should return True when we don't have enough data to validate
#     assert validate_ratings_vs_distances(finishers, adjusted_ratings) is True


# def test_build_record(mocker):
#     race = mocker.Mock(model_dump=lambda: {"date": "2024-08-31", "course": "Beverley"})
#     runners = [
#         mocker.Mock(model_dump=lambda: {"name": "Horse1", "position": "1"}),
#         mocker.Mock(model_dump=lambda: {"name": "Horse2", "position": "2"}),
#     ]

#     record = build_record(race, runners)

#     assert record.date == "2024-08-31"
#     assert record.course == "Beverley"
#     assert len(record.runners) == 2
#     assert record.runners[0]["name"] == "Horse1"
#     assert record.runners[1]["position"] == "2"


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
