from loaders.load import (
    drop_database,
    load_parents,
    load_people,
    select_dams,
    select_sires,
)


def test_drop_database(mocker):
    client = mocker.patch("src.loaders.load.client")
    drop_database.fn()
    assert client.drop_database.called_once_with("racing")


def test_load_people(mocker):
    mocker.patch(
        "pymongo.collection.Collection.insert_one"
    ).return_value.inserted_id = 1
    people = ["TRAINER1", "TRAINER2", "TRAINER1"]
    assert {"TRAINER1": 1, "TRAINER2": 1} == load_people.fn(people)


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
