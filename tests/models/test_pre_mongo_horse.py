from models import PreMongoHorse


def test_pre_mongo_horse_str():
    horse = PreMongoHorse(
        name="Dobbin", country="GB", year=2020, sex="M", breed="Thoroughbred"
    )
    assert str(horse) == "Dobbin (GB) 2020 (M)"
