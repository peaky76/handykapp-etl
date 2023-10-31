import pendulum
from horsetalk import RacingCode
from transformers.formdata_transformer import (
    extract_dist_going,
    extract_middle_details,
    extract_prize,
    extract_weight,
    get_formdata_date,
    get_formdatas,
    is_horse,
    is_race_date,
    transform_horse_data,
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


def test_extract_dist_going_for_turf_going():
    assert (float("5"), "G") == extract_dist_going("5G")


def test_extract_dist_going_for_aw_going():
    assert (float("5"), "d") == extract_dist_going("5d")


def test_extract_dist_going_for_decimal_dist():
    assert (float("9.1"), "G") == extract_dist_going("9.1G")


def test_extract_middle_details_when_jockey_and_single_digit_position():
    expected = {
        "headgear": None,
        "allowance": 0,
        "jockey": "JFanning",
        "position": "3",
    }

    assert expected == extract_middle_details("JFanning3")


def test_extract_middle_details_when_jockey_and_double_digit_position():
    expected = {
        "headgear": None,
        "allowance": 0,
        "jockey": "JFanning",
        "position": "12",
    }

    assert expected == extract_middle_details("JFanning12")


def test_extract_middle_details_when_headgear_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": 0,
        "jockey": "JFanning",
        "position": "12",
    }

    assert expected == extract_middle_details("tJFanning12")


def test_extract_middle_details_when_allowance_jockey_position():
    expected = {
        "headgear": None,
        "allowance": 3,
        "jockey": "HDavies",
        "position": "12",
    }

    assert expected == extract_middle_details("3HDavies12")


def test_extract_middle_details_when_headgear_allowance_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "12",
    }

    assert expected == extract_middle_details("t3HDavies12")


def test_extract_middle_details_when_position_equal():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "=12",
    }

    assert expected == extract_middle_details("t3HDavies=12")


def test_extract_middle_details_when_position_reordered():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "2p3",
    }

    assert expected == extract_middle_details("t3HDavies2p3")


def test_extract_middle_details_when_position_is_disaster():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "p",
    }

    assert expected == extract_middle_details("t3HDaviesp")


def test_extract_middle_details_when_position_includes_disqulification():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "3d",
    }

    assert expected == extract_middle_details("t3HDavies3d")


def test_extract_prize():
    assert ("2CG", "1156") == extract_prize("2CG1156")


def test_extract_weight():
    assert ("9-13", "t3RyanSexton") == extract_weight("9-13t3RyanSexton")


def test_get_formdata_date():
    assert pendulum.date(2019, 6, 30) == get_formdata_date(FORMDATA_FILENAMES[0])


def test_get_formdatas_default(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    assert FORMDATA_FILENAMES == get_formdatas()


def test_get_formdatas_with_code(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_nh_230611.pdf",
        "formdata_nh_230618.pdf",
        "formdata_nh_230625.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert expected == get_formdatas(code=RacingCode.NH)


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
    assert expected == get_formdatas(after_year=22)


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
    assert expected == get_formdatas(for_refresh=True)


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


def test_is_horse_not_true_for_trainer():
    assert not is_horse("D B O'Meara")


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_false_with_non_date():
    assert not is_race_date("JMitchell")


def test_transform_horse_data():
    data = [
        {
            "name": "AADDEEY",
            "country": "GB",
            "year": 2018,
            "trainer": "D B O'Meara",
            "trainer_form": "F2",
            "prize_money": "£2000",
            "runs": [
                {
                    "date": "2020-06-04",
                    "type": "3G3",
                    "win_prize": "21",
                    "course": "Ncl",
                    "number_of_runners": "13",
                    "weight": "9-5",
                    "headgear": None,
                    "allowance": 0,
                    "jockey": "SDeSousa",
                    "position": "8",
                    "beaten_distance": 6.5,
                    "time_rating": 76,
                    "distance": 6,
                    "going": "g",
                    "form_rating": 99,
                }
            ],
        }
    ]
    expected = {
        "name": "AADDEEY (GB)",
        "prize_money": "£2000",
        "trainer": "D B O'Meara",
        "year": 2018,
    }

    assert expected == list(transform_horse_data.fn(data))[0]
