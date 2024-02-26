import pytest
from models.mongo_horse import MongoHorse
from pydantic import ValidationError


def test_mongo_horse_init_with_sufficient_fields():
    assert MongoHorse(name="Dobbin", country="GB", year=2020, sex="M", breed="Thoroughbred")


def test_mongo_horse_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        MongoHorse(name="Dobbin", sex="M", colour="Bay")


def test_mongo_horse_init_with_incorrect_fields():
    with pytest.raises(ValidationError):
        MongoHorse(name="Dobbin", sex="Y")