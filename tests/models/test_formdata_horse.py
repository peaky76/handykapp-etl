import pytest
from pydantic import ValidationError

from models.formdata_horse import FormdataHorse


def test_formdata_horse_init_with_sufficient_fields():
    assert FormdataHorse(
        name="Dobbin",
        country="GB",
        year=2020,
        trainer="John Doe",
        trainer_form="F1",
        prize_money="£1000",
    )


def test_formdata_horse_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        FormdataHorse(name="Dobbin", country="GB", year=2020, trainer="John Doe")


def test_formdata_horse_init_with_incorrect_fields():
    with pytest.raises(ValidationError):
        FormdataHorse(
            name="Do",
            country="GB",
            year=2020,
            trainer="John Doe",
            trainer_form="F1",
            prize_money="£1000",
        )


def test_formdata_horse_trainer_form_validation():
    with pytest.raises(ValidationError):
        FormdataHorse(
            name="Dobbin",
            country="GB",
            year=2020,
            trainer="John Doe",
            trainer_form="X",
            prize_money="£1000",
        )
    assert FormdataHorse(
        name="Dobbin",
        country="GB",
        year=2020,
        trainer="John Doe",
        trainer_form="F1",
        prize_money="£1000",
    )


def test_formdata_horse_prize_money_validation():
    with pytest.raises(ValidationError):
        FormdataHorse(
            name="Dobbin",
            country="GB",
            year=2020,
            trainer="John Doe",
            trainer_form="F1",
            prize_money="1000",
        )
    assert FormdataHorse(
        name="Dobbin",
        country="GB",
        year=2020,
        trainer="John Doe",
        trainer_form="F1",
        prize_money="£1000",
    )
    assert FormdataHorse(
        name="Dobbin",
        country="GB",
        year=2020,
        trainer="John Doe",
        trainer_form="F1",
        prize_money="£-",
    )
