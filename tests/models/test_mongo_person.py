import pytest
from models.mongo_person import MongoPerson
from pydantic import ValidationError


def test_mongo_person_init_with_sufficient_fields():
    assert MongoPerson(first="John", last="Doe")


def test_mongo_person_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        MongoPerson(first="John")


def test_mongo_person_init_with_incorrect_fields():
    with pytest.raises(ValidationError):
        MongoPerson(first="John", last=1234)