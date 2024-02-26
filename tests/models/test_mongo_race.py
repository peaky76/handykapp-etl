import pytest
from models.mongo_race import MongoRace
from pydantic import ValidationError


def test_mongo_race_init_with_sufficient_fields():
    assert MongoRace(racecourse="5f4a3b3e4c4a3b3e4c4a3b3e", datetime="2021-01-01T12:00:00", title="THE GREAT BIG HANDICAP", distance_description="1m 4f")


def test_mongo_race_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        MongoRace(racecourse="5f4a3b3e4c4a3b3e4c4a3b3e")


def test_mongo_race_init_with_incorrect_fields():
    with pytest.raises(ValidationError):
        MongoRace(racecourse="5f4a3b3e4c4a3b3e4c4a3b3e", datetime="2021-01-01T12:00:00", title=1234, distance_description="1m 4f")

