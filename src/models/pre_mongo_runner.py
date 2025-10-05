from typing import Optional

from pydantic_extra_types.pendulum_dt import Duration

from .pre_mongo_horse import PreMongoHorse


class PreMongoRunner(PreMongoHorse):
    lbs_carried: Optional[int] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    official_rating: Optional[int] = None
    jockey: Optional[str] = None
    damsire: Optional[str] = None
    finishing_position: Optional[str] = None
    official_position: Optional[str] = None
    beaten_distance: Optional[float] = None
    time: Optional[Duration] = None
    sp: Optional[str] = None
