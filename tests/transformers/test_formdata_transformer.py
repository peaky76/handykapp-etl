import pendulum
import pytest
from horsetalk import RacingCode
from models import FormdataRun, Race
from transformers.formdata_transformer import (
    convert_symbol_to_going_description,
    extract_age_from_formdata_run,
    extract_dist_going,
    extract_grade_from_formdata_run,
    extract_middle_details,
    extract_obstacle_from_formdata_run,
    extract_prize,
    extract_race_from_formdata_run,
    extract_rating,
    extract_weight,
    get_formdata_date,
    get_formdatas,
    is_horse,
    is_race_date,
)

FORMDATA_FETCH = "transformers.formdata_transformer.get_files"
FORMDATA_FILENAMES = [
    "formdata_190630.pdf",
    "formdata_200628.pdf",
    "formdata_200705.pdf",
    "formdata_210530.pdf",
    "formdata_220529.pdf",
    "formdata_230528.pdf",
    "formdata_230604.pdf",
    "formdata_230611.pdf",
    "formdata_230618.pdf",
    "formdata_230625.pdf",
    "formdata_230702.pdf",
    "formdata_nh_230611.pdf",
    "formdata_nh_230618.pdf",
    "formdata_nh_230625.pdf",
    "formdata_nh_230702.pdf",
]

@pytest.fixture()
def partial_run():
    return {
        "date": pendulum.parse("2020-01-01"), 
        "race_type": "3H", 
        "win_prize": "10", 
        "racecourse": "Asc", 
        "number_of_runners": 10, 
        "weight": "9-13", 
        "jockey": "AGambler", 
        "position": "3", 
        "distance": 5, 
        "going": "G",
    }


def test_extract_obstacle_type_from_formdata_run_when_flat(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "3F", "racecourse": "Asc", "distance": 5 }))
    assert extract_obstacle_from_formdata_run(run) is None


def test_extract_obstacle_type_from_formdata_run_when_hurdle(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "Hh", "racecourse": "Chm", "distance": 24 })) 
    assert extract_obstacle_from_formdata_run(run) == "Hurdle"


def test_extract_obstacle_type_from_formdata_run_when_chase(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "Hc", "racecourse": "Chm", "distance": 24 })) 
    assert extract_obstacle_from_formdata_run(run) == "Chase"


def test_extract_obstacle_type_from_formdata_run_when_cross_country(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "Hc", "racecourse": "Chm", "distance": 29 })) 
    assert extract_obstacle_from_formdata_run(run) == "Cross-Country"


def test_extract_grade_from_formdata_run_when_g1(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "3FG1"}))
    assert extract_grade_from_formdata_run(run) == "G1"


def test_extract_grade_from_formdata_run_when_g2(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "3FG2"}))
    assert extract_grade_from_formdata_run(run) == "G2"


def test_extract_grade_from_formdata_run_when_g3(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "3FG3"}))
    assert extract_grade_from_formdata_run(run) == "G3"


def test_extract_grade_from_formdata_run_when_not_graded(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "3F"}))
    assert extract_grade_from_formdata_run(run) is None


def test_extract_age_from_formdata_run_when_age_2(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "2F"}))
    assert extract_age_from_formdata_run(run) == 2


def test_extract_age_from_formdata_run_when_age_3(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "3F"}))
    assert extract_age_from_formdata_run(run) == 3


def test_extract_age_from_formdata_run_when_age_4(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "4F"}))
    assert extract_age_from_formdata_run(run) == 4


def test_extract_age_from_formdata_run_when_no_age(partial_run):
    run = FormdataRun(**(partial_run | {"race_type": "F"}))
    assert extract_age_from_formdata_run(run) is None


def test_convert_symbol_to_going_description():
    assert convert_symbol_to_going_description("H") == "Hard"
    assert convert_symbol_to_going_description("F") == "Firm"
    assert convert_symbol_to_going_description("M") == "Good to Firm"
    assert convert_symbol_to_going_description("G") == "Good"
    assert convert_symbol_to_going_description("D") == "Good to Soft"
    assert convert_symbol_to_going_description("S") == "Soft"
    assert convert_symbol_to_going_description("V") == "Heavy"
    assert convert_symbol_to_going_description("f") == "Fast"
    assert convert_symbol_to_going_description("m") == "Standard to Fast"
    assert convert_symbol_to_going_description("g") == "Standard"
    assert convert_symbol_to_going_description("d") == "Standard to Slow"
    assert convert_symbol_to_going_description("s") == "Slow"


def test_extract_race_from_formdata_run(partial_run):
    run = FormdataRun(**partial_run)
    race = extract_race_from_formdata_run(run)
    assert isinstance(race, Race)
    assert race.racecourse.name == "Asc"
    assert race.racecourse.country == "GB"
    assert race.racecourse.surface == "Turf"
    assert race.racecourse.obstacle is None
    assert race.racecourse.references["racing_research"] == "Asc"
    assert race.datetime == pendulum.parse("2020-01-01")
    assert race.title == ""
    assert race.is_handicap is True
    assert race.is_cancelled is False
    assert race.distance_description == "5f"
    assert race.race_class is None
    assert race.age_restriction.minimum == 3
    assert race.age_restriction.maximum == 3
    assert race.prize == "£10000"
    assert race.going_description == "Good"
    assert race.number_of_runners == 10
    assert race.references["racing_research"] == ""
    assert race.source == "racing_research"


def test_extract_dist_going_for_turf_going():
    assert extract_dist_going("5G") == (float("5"), "G")


def test_extract_dist_going_for_aw_going():
    assert extract_dist_going("5d") == (float("5"), "d")


def test_extract_dist_going_for_decimal_dist():
    assert extract_dist_going("9.1G") == (float("9.1"), "G")


def test_extract_middle_details_when_jockey_and_single_digit_position():
    expected = {
        "headgear": None,
        "allowance": 0,
        "jockey": "JFanning",
        "position": "3",
    }

    assert extract_middle_details("JFanning3") == expected


def test_extract_middle_details_when_jockey_and_double_digit_position():
    expected = {
        "headgear": None,
        "allowance": 0,
        "jockey": "JFanning",
        "position": "12",
    }

    assert extract_middle_details("JFanning12") == expected


def test_extract_middle_details_when_headgear_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": 0,
        "jockey": "JFanning",
        "position": "12",
    }

    assert extract_middle_details("tJFanning12") == expected


def test_extract_middle_details_when_allowance_jockey_position():
    expected = {
        "headgear": None,
        "allowance": 3,
        "jockey": "HDavies",
        "position": "12",
    }

    assert extract_middle_details("3HDavies12") == expected


def test_extract_middle_details_when_headgear_allowance_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "12",
    }

    assert extract_middle_details("t3HDavies12") == expected


def test_extract_middle_details_when_position_equal():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "=12",
    }

    assert extract_middle_details("t3HDavies=12") == expected


def test_extract_middle_details_when_position_reordered():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "2p3",
    }

    assert extract_middle_details("t3HDavies2p3") == expected


def test_extract_middle_details_when_position_is_disaster():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "p",
    }

    assert extract_middle_details("t3HDaviesp") == expected


def test_extract_middle_details_when_position_includes_disqulification():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "3d",
    }

    assert extract_middle_details("t3HDavies3d") == expected


def test_extract_prize():
    assert extract_prize("2CG1156") == ("2CG", "1156")


def test_extract_rating_when_rating_only():
    assert extract_rating("115") == 115


def test_extract_rating_when_hurdle_rating():
    assert extract_rating("115h") == 115


def test_extract_rating_when_chase_rating():
    assert extract_rating("115c") == 115


def test_extract_rating_when_disaster():
    assert extract_rating("p19c") is None


def test_extract_rating_when_no_rating():
    assert extract_rating("-") is None


def test_extract_rating_when_no_rating_over_jumps():
    assert extract_rating("-c") is None


def test_extract_rating_when_less_than_zero():
    assert extract_rating("-2") is None


def test_extract_weight():
    assert extract_weight("9-13t3RyanSexton") == ("9-13", "t3RyanSexton")


def test_get_formdata_date():
    assert get_formdata_date(FORMDATA_FILENAMES[0]) == pendulum.date(2019, 6, 30)


def test_get_formdatas_default(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    assert get_formdatas() == FORMDATA_FILENAMES


def test_get_formdatas_with_code(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_nh_230611.pdf",
        "formdata_nh_230618.pdf",
        "formdata_nh_230625.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert get_formdatas(code=RacingCode.NH) == expected


def test_get_formdatas_with_after_year(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_230528.pdf",
        "formdata_230604.pdf",
        "formdata_230611.pdf",
        "formdata_230618.pdf",
        "formdata_230625.pdf",
        "formdata_230702.pdf",
        "formdata_nh_230611.pdf",
        "formdata_nh_230618.pdf",
        "formdata_nh_230625.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert get_formdatas(after_year=22) == expected


def test_get_formdatas_with_for_refresh(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_190630.pdf",
        "formdata_200628.pdf",
        "formdata_210530.pdf",
        "formdata_220529.pdf",
        "formdata_230528.pdf",
        "formdata_230702.pdf",
        "formdata_nh_230611.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert get_formdatas(for_refresh=True) == expected


def test_is_horse_true_without_country():
    assert is_horse("AADDEEY")


def test_is_horse_true_with_country():
    assert is_horse("AADDEEY (IRE)")


def test_is_horse_true_with_multiple_word_name():
    assert is_horse("HURRICANE LANE")


def test_is_horse_not_true_for_dist_going():
    assert not is_horse("5G")


def test_is_horse_not_true_for_title():
    assert not is_horse("FORMDATA")


def test_is_horse_not_true_for_trainer():
    assert not is_horse("D B O'Meara")


def test_is_horse_not_true_for_trainer_initials():
    assert not is_horse("D B O")


def test_is_horse_not_true_for_date_range():
    assert not is_horse("21 MAY 22 - 21 MAY 23")


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_false_with_non_date():
    assert not is_race_date("JMitchell")
