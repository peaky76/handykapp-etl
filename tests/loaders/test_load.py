from loaders.load import (
    convert_person,
    drop_database,
    load_horses,
    load_people,
    load_races,
    load_ratings,
    select_set,
    spec_database,
)
from pymongo import ASCENDING as ASC


def test_drop_database(mocker):
    client = mocker.patch("src.clients.mongo_client")
    drop_database.fn()
    assert client.drop_database.called_once_with("racing")


def test_spec_database_adds_horse_unique_index(mocker):
    create_index = mocker.patch("pymongo.collection.Collection.create_index")
    spec_database.fn()
    assert create_index.called_once_with(
        [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
    )


def test_spec_database_adds_horse_name_index(mocker):
    create_index = mocker.patch("pymongo.collection.Collection.create_index")
    spec_database.fn()
    assert create_index.called_once_with("name")


def test_spec_database_adds_people_name_index(mocker):
    create_index = mocker.patch("pymongo.collection.Collection.create_index")
    spec_database.fn()
    assert create_index.called_once_with("name", unique=True)


def test_spec_database_adds_racecourse_index(mocker):
    create_index = mocker.patch("pymongo.collection.Collection.create_index")
    spec_database.fn()
    assert create_index.called_once_with([("name", ASC), ("country", ASC)], unique=True)


def test_spec_database_adds_races_index(mocker):
    create_index = mocker.patch("pymongo.collection.Collection.create_index")
    spec_database.fn()
    assert create_index.called_once_with(
        [("racecourse", ASC), ("datetime", ASC)], unique=True
    )


def test_spec_database_adds_races_result_horse_index(mocker):
    create_index = mocker.patch("pymongo.collection.Collection.create_index")
    spec_database.fn()
    assert create_index.called_once_with("result.horse")


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


def test_load_races(mocker):
    insert_one = mocker.patch("pymongo.collection.Collection.insert_one")
    races = [
        {"foo": "bar", "result": {}},
    ]
    load_races.fn(races)
    assert insert_one.called_once_with({"foo": "bar"})


def test_load_ratings():
    assert load_ratings.fn() is None


def test_select_set():
    data = [
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER2"},
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER3"},
    ]
    assert ["TRAINER1", "TRAINER2", "TRAINER3"] == select_set.fn(data, "trainer")
