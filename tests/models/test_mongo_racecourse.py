import pytest
from models.mongo_racecourse import MongoRacecourse
from pydantic import ValidationError


def test_mongo_racecourse_init_with_sufficient_fields():
    assert MongoRacecourse(name="Newbury", obstacle=None)


def test_mongo_racecourse_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        MongoRacecourse(name="Newbury")


def test_mongo_racecourse_init_with_incorrect_fields():
    with pytest.raises(ValidationError):
        MongoRacecourse(name="Newbury", obstacle=1234)

