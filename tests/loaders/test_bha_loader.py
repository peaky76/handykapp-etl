from loaders.bha_loader import (
    convert_person,
    load_horses,
    load_people,
    select_set,
)
from pymongo import ASCENDING as ASC


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


def test_load_people(mocker):
    mocker.patch("prefect.context.TaskRunContext")
    mocker.patch("prefect.get_run_logger")
    mocker.patch(
        "pymongo.collection.Collection.insert_one"
    ).return_value.inserted_id = 1
    people = ["TRAINER1", "TRAINER2", "TRAINER1"]
    assert {"TRAINER1": 1, "TRAINER2": 1} == load_people.fn(people, "a_source")


def test_load_horses(mocker):
    mocker.patch("prefect.context.TaskRunContext")
    mocker.patch("prefect.get_run_logger")
    insert_one = mocker.patch("pymongo.collection.Collection.insert_one")
    data = [
        {
            "name": "HORSE1 (IRE)",
            "sex": "M",
            "year": 2008,
            "dam": "DAM1 (IRE)",
            "sire": "SIRE1 (IRE)",
            "trainer": "TRAINER1",
            "operations": None,
            "ratings": {
                "flat": 0,
                "aw": 0,
                "chase": 0,
                "hurdle": 0,
            },
        },
        {
            "name": "HORSE2 (GB)",
            "sex": "M",
            "year": 2008,
            "dam": "DAM2 (GB)",
            "sire": "SIRE2 (GB)",
            "trainer": "TRAINER2",
            "operations": [{"type": "gelding", "date": None}],
            "ratings": {
                "flat": 0,
                "aw": 0,
                "chase": 0,
                "hurdle": 0,
            },
        },
    ]
    load_horses.fn(data)
    assert 2 == insert_one.call_count


def test_select_set():
    data = [
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER2"},
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER3"},
    ]
    assert ["TRAINER1", "TRAINER2", "TRAINER3"] == select_set(data, "trainer")
