from loaders.load import (
    drop_database,
    load_horse_detail,
    load_parents,
    load_people,
    load_races,
    load_ratings,
    select_dams,
    select_sires,
    select_trainers,
    spec_database,
)
from pymongo import ASCENDING as ASC


def test_drop_database(mocker):
    client = mocker.patch("src.clients.mongo_client")
    # client = mocker.patch("src.loaders.load.client")
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


def test_load_people(mocker):
    mocker.patch(
        "pymongo.collection.Collection.insert_one"
    ).return_value.inserted_id = 1
    people = ["TRAINER1", "TRAINER2", "TRAINER1"]
    assert {"TRAINER1": 1, "TRAINER2": 1} == load_people.fn(people, "a_source")


def test_load_parents(mocker):
    mocker.patch(
        "pymongo.collection.Collection.insert_one"
    ).return_value.inserted_id = 1
    horses = [
        {"name": "HORSE1", "country": "IRE"},
        {"name": "HORSE2", "country": "GB"},
        {"name": "HORSE2", "country": "IRE"},
    ]
    assert {
        ("HORSE1", "IRE"): 1,
        ("HORSE2", "GB"): 1,
        ("HORSE2", "IRE"): 1,
    } == load_parents.fn(horses, "M")


def test_load_horse_detail(mocker):
    insert_one = mocker.patch("pymongo.collection.Collection.insert_one")
    mocker.patch("prefect.context.TaskRunContext")
    mocker.patch("prefect.get_run_logger")
    data = [
        {
            "name": "HORSE1",
            "country": "IRE",
            "sex": "M",
            "year": 2008,
            "dam": "DAM1",
            "dam_country": "IRE",
            "sire": "SIRE1",
            "sire_country": "IRE",
            "trainer": "TRAINER1",
            "flat": 0,
            "aw": 0,
            "chase": 0,
            "hurdle": 0,
            "is_gelded": False,
        },
        {
            "name": "HORSE2",
            "country": "GB",
            "sex": "M",
            "year": 2008,
            "dam": "DAM2",
            "dam_country": "GB",
            "sire": "SIRE2",
            "sire_country": "GB",
            "trainer": "TRAINER2",
            "flat": 0,
            "aw": 0,
            "chase": 0,
            "hurdle": 0,
            "is_gelded": True,
        },
    ]
    sires_ids = {("SIRE1", "IRE"): 1, ("SIRE2", "GB"): 2}
    dams_ids = {("DAM1", "IRE"): 1, ("DAM2", "GB"): 2}
    trainer_ids = {"TRAINER1": 1, "TRAINER2": 2}
    load_horse_detail.fn(data, sires_ids, dams_ids, trainer_ids)
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


def test_select_dams():
    data = [
        {"dam": "DAM1", "dam_country": "IRE"},
        {"dam": "DAM2", "dam_country": "GB"},
        {"dam": "DAM2", "dam_country": "IRE"},
        {"dam": "DAM1", "dam_country": "IRE"},
        {"dam": "DAM3", "dam_country": "IRE"},
    ]
    assert [
        {"name": "DAM1", "country": "IRE"},
        {"name": "DAM2", "country": "GB"},
        {"name": "DAM2", "country": "IRE"},
        {"name": "DAM3", "country": "IRE"},
    ] == select_dams.fn(data)


def test_select_sires():
    data = [
        {"sire": "SIRE1", "sire_country": "IRE"},
        {"sire": "SIRE2", "sire_country": "GB"},
        {"sire": "SIRE2", "sire_country": "IRE"},
        {"sire": "SIRE1", "sire_country": "IRE"},
        {"sire": "SIRE3", "sire_country": "IRE"},
    ]
    assert [
        {"name": "SIRE1", "country": "IRE"},
        {"name": "SIRE2", "country": "GB"},
        {"name": "SIRE2", "country": "IRE"},
        {"name": "SIRE3", "country": "IRE"},
    ] == select_sires.fn(data)


def test_select_trainers():
    data = [
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER2"},
        {"trainer": "TRAINER1"},
        {"trainer": "TRAINER3"},
    ]
    assert ["TRAINER1", "TRAINER2", "TRAINER3"] == select_trainers.fn(data)
