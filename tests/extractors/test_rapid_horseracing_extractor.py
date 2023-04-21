import pendulum
from src.extractors.rapid_horseracing_extractor import (
    LIMITS,
    RACECARDS_DESTINATION,
    RESULTS_DESTINATION,
    SOURCE,
    extract_racecards,
    extract_result,
    get_file_date,
    get_headers,
    get_next_racecard_date,
    get_unfetched_race_ids,
)


def test_rapid_horseracing_source():
    expected = "https://horse-racing.p.rapidapi.com/"
    assert expected == SOURCE


def test_rapid_horseracing_racecards_destination():
    expected = "handykapp/rapid_horseracing/racecards/"
    assert expected == RACECARDS_DESTINATION


def test_rapid_horseracing_results_destination():
    expected = "handykapp/rapid_horseracing/results/"
    assert expected == RESULTS_DESTINATION


def test_rapid_horseracing_has_limits():
    expected = ["day", "minute"]
    assert expected == list(LIMITS.keys())


def test_get_file_date():
    filename = "rapid_api_racecards_20200101.json"
    expected = "20200101"
    assert expected == get_file_date(filename)


def test_get_headers(mocker):
    url = "https://<host>/rest_of_url"
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.Secret.load"
    ).return_value.get.return_value = "<key>"
    headers = get_headers(url)
    assert "<host>" == headers["x-rapidapi-host"]
    assert "<key>" == headers["x-rapidapi-key"]


def test_get_unfetched_race_ids(mocker):
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.get_files"
    ).return_value = ["file1", "file2"]
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.read_file"
    ).return_value = [{"id_race": 999}]
    expected = [999, 999]
    assert expected == get_unfetched_race_ids.fn("2020-01-01")


def test_extract_result(mocker):
    write_file = mocker.patch("src.extractors.rapid_horseracing_extractor.write_file")
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.fetch_content"
    ).return_value = "foobar"
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.RESULTS_DESTINATION", "results/"
    )

    extract_result.fn("12345")

    assert 1 == write_file.call_count
    assert (
        mocker.call("foobar", "results/rapid_api_result_12345.json")
        == write_file.call_args
    )


def test_extract_racecards(mocker):
    write_file = mocker.patch("src.extractors.rapid_horseracing_extractor.write_file")
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.fetch_content"
    ).return_value = "foobar"
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.RACECARDS_DESTINATION",
        "racecards/",
    )

    extract_racecards.fn("2020-01-01")

    assert 1 == write_file.call_count
    assert (
        mocker.call("foobar", "racecards/rapid_api_racecards_20200101.json")
        == write_file.call_args
    )


def test_get_next_racecard_date_when_date_available(mocker):
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.get_files"
    ).return_value = [
        "rapid_api_racecards_20200101.json",
        "rapid_api_racecards_20200102.json",
        "rapid_api_racecards_20200103.json",
    ]
    mocker.patch("pendulum.now").return_value = pendulum.parse("2020-01-05")

    assert "2020-01-04" == get_next_racecard_date.fn()


def test_get_next_racecard_date_when_no_dates_left(mocker):
    mocker.patch(
        "src.extractors.rapid_horseracing_extractor.get_files"
    ).return_value = [
        "rapid_api_racecards_20200101.json",
        "rapid_api_racecards_20200102.json",
        "rapid_api_racecards_20200103.json",
    ]
    mocker.patch("pendulum.now").return_value = pendulum.parse("2020-01-03")

    assert None is get_next_racecard_date.fn()
