import petl
import pytest
from transformers.core_racecourses_transformer import (
    transform_racecourses_data,
    validate_racecourses_data,
)


@pytest.fixture()
def valid_racecourses():
    return petl.fromdicts([
        {
            "Name": "Peaky Park",
            "Formal Name": "Peaky Park Downs",
            "Surface": "Turf",
            "Obstacle": "Hurdle",
            "Grade": "1",
            "Straight": "1 mile",
            "Shape": "Oval",
            "Direction": "Right",
            "Speed": "Stiff",
            "Contour": "Flat",
            "Location": "Peaky City",
            "Country": "GB",
            "RR Abbr": "Pea",
        },
        {
            "Name": "Robsville",
            "Formal Name": "Robsville",
            "Surface": "Polytrack",
            "Obstacle": "",
            "Grade": "2",
            "Straight": "5 furlongs",
            "Shape": "Horseshoe",
            "Direction": "Left",
            "Speed": "Galloping",
            "Contour": "Undulating",
            "Location": "County Rob",
            "Country": "IRE",
            "RR Abbr": "Rob",
        },
    ])

@pytest.fixture()
def invalid_racecourses():
    return petl.fromdicts([
        {
            "Name": "Peaky Park",
            "Formal Name": "Peaky Park Downs",
            "Surface": "Turf",
            "Obstacle": "Traffic Cones",
            "Grade": "1",
            "Straight": "No, I'm gay",
            "Shape": "Square",
            "Direction": "Upside Down",
            "Speed": "Light",
            "Contour": "Wooooah Bodyform",
            "Location": "Peakshire",
            "Country": "GB",
            "RR Abbr": "RRA",
        },
        {
            "Name": "Robsville",
            "Formal Name": "Robsville",
            "Surface": "Invalid Surface",
            "Obstacle": "Hurdle",
            "Grade": "Michael",
            "Straight": "No chaser",
            "Shape": "Square",
            "Direction": "Upside Down",
            "Speed": "Light",
            "Contour": "Wooooah Bodyform",
            "Location": "County Rob",
            "Country": "IRE",
            "RR Abbr": "Rob",
        },
    ])

@pytest.fixture()
def transformed_data():
    return [
        {
            "name": "Peaky Park",
            "formal_name": "Peaky Park Downs",
            "code": "NH",
            "surface": "Turf",
            "obstacle": "Hurdle",
            "shape": "Oval",
            "handedness": "Right",
            "style": "Stiff",
            "contour": "Flat",
            "country": "GB",
            "references": {
                "racing_research": "Pea"
            },
            "source": "core"
        },
        {
            "name": "Robsville",
            "formal_name": "Robsville",
            "code": "Flat",
            "surface": "All Weather",
            "obstacle": None,
            "shape": "Horseshoe",
            "handedness": "Left",
            "style": "Sharp",
            "contour": "Undulating",
            "country": "IRE",
            "references": {
                "racing_research": "Rob"
            },
            "source": "core"
        }
    ]

def test_transform_racecourses_data_returns_correct_racecourse_object(valid_racecourses, transformed_data):
    actual = transform_racecourses_data.fn(valid_racecourses)[0].model_dump()
    expected = transformed_data[0]
    assert actual == expected

def test_validate_racecourses_data_returns_no_problems_for_valid_data(valid_racecourses):
    problems = validate_racecourses_data.fn(valid_racecourses)
    assert len(problems.dicts()) == 0

def test_validate_racecourses_data_returns_list_of_problems_for_invalid_data(invalid_racecourses):
    problems = validate_racecourses_data.fn(invalid_racecourses)
    assert len(problems.dicts()) > 0
