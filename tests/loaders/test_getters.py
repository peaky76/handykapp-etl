from loaders.getters import get_racecourse_id
from mongomock import MongoClient

# Create a MongoDB mock client
db = MongoClient().db

def test_get_racecourse_id_returns_cached_racecourse_id():
    lookup = {("Robertsbridge", "Turf", "National Hunt", "Hurdle"): "987654321"}
    course = "Robertsbridge"
    surface = "Turf"
    code = "National Hunt"
    obstacle = "Hurdle"

    racecourse_id, updated_lookup = get_racecourse_id(db, lookup, course, surface, code, obstacle)

    assert racecourse_id == "987654321"
    assert updated_lookup == lookup

def test_get_racecourse_id_returns_stored_racecourse_id():
    db.racecourses.insert_one({"name": "Robertsbridge", "surface": "Turf", "code": "National Hunt", "obstacle": "Hurdle", "_id": "987654321"})
    lookup = {}
    course = "Robertsbridge"
    surface = "Turf"
    code = "National Hunt"
    obstacle = "Hurdle"

    racecourse_id, updated_lookup = get_racecourse_id(db, lookup, course, surface, code, obstacle)

    assert racecourse_id == "987654321"
    assert updated_lookup == {("Robertsbridge", "Turf", "National Hunt", "Hurdle"): "987654321"}

def test_get_racecourse_id_returns_none_when_racecourse_not_found():
    lookup = {}
    course = "Jamesville"
    surface = "Turf"
    code = "Flat"
    obstacle = None

    racecourse_id, updated_lookup = get_racecourse_id(db, lookup, course, surface, code, obstacle)

    assert racecourse_id is None
    assert updated_lookup == {}