import pytest
from bson import ObjectId
from pydantic import ValidationError

from models.mongo_runner import MongoRunner
from models.py_object_id import PyObjectId


def test_mongo_runner_init_with_sufficient_fields():
    assert MongoRunner(horse=PyObjectId(ObjectId()))


def test_mongo_runner_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        MongoRunner()


def test_mongo_runner_init_with_optional_fields():
    horse_id = PyObjectId(ObjectId())
    assert MongoRunner(
        horse=horse_id,
        lbs_carried=120,
        allowance=3,
        saddlecloth=1,
        draw=5,
        headgear="blinkers",
        official_rating=85,
        position="1",
        beaten_distance="0.5",
    )
