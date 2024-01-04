from loaders.bha_loader import (
    load_horses,
    load_people,
)


def test_load_people(mocker):
    mocker.patch("prefect.context.TaskRunContext")
    mocker.patch("prefect.get_run_logger")
    mocker.patch(
        "pymongo.collection.Collection.insert_one"
    ).return_value.inserted_id = 1
    people = ["TRAINER1", "TRAINER2", "TRAINER1"]
    assert load_people.fn(people, "a_source") == {"TRAINER1": 1, "TRAINER2": 1}


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
    assert insert_one.call_count == 2
