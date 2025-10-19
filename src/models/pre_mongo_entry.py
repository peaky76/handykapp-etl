from .pre_mongo_horse import PreMongoHorse


class PreMongoEntry(PreMongoHorse):
    lbs_carried: int | None = None
    allowance: int | None = None
    saddlecloth: int | None = None
    draw: int | None = None
    headgear: str | None = None
    official_rating: int | None = None
    jockey: str | None = None
