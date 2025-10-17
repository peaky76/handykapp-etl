from typing import Optional

from .pre_mongo_horse import PreMongoHorse


class PreMongoEntry(PreMongoHorse):
    lbs_carried: Optional[int] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    official_rating: Optional[int] = None
    jockey: Optional[str] = None
