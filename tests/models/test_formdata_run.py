import pytest
from pydantic import ValidationError

from models.formdata_run import FormdataRun


def test_formdata_run_init_with_sufficient_fields():
    assert FormdataRun(
        date="2023-10-01",
        race_type="Flat",
        win_prize="£1000",
        course="Asc",
        number_of_runners=10,
        weight="9-2",
        jockey="WBuick",
        position="2",
        beaten_distance=0.8,
        time_rating=65,
        distance=9.9,
        going="G",
        form_rating=87,
    )


def test_formdata_run_init_with_insufficient_fields():
    with pytest.raises(ValidationError):
        FormdataRun(
            date="2023-10-01",
            race_type="Flat",
            win_prize="£1000",
            course="Asc",
            number_of_runners=10,
            jockey="WBuick",
            position="2",
        )


def test_formdata_run_init_with_incorrect_date_format():
    with pytest.raises(ValidationError):
        FormdataRun(
            date="01-10-2023",
            race_type="Flat",
            win_prize="£1000",
            course="Asc",
            number_of_runners=10,
            weight="9-2",
            jockey="WBuick",
            position="2",
            beaten_distance=0.8,
            time_rating=65,
            distance=9.9,
            going="G",
            form_rating=87,
        )


def test_formdata_run_init_with_incorrect_weight_format():
    with pytest.raises(ValidationError):
        FormdataRun(
            date="2023-10-01",
            race_type="Flat",
            win_prize="£1000",
            course="Asc",
            number_of_runners=10,
            weight="92",
            jockey="WBuick",
            position="2",
            beaten_distance=0.8,
            time_rating=65,
            distance=9.9,
            going="G",
            form_rating=87,
        )
