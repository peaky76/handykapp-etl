from typing import Optional

from pydantic_extra_types.pendulum_dt import Duration

from .pre_mongo_entry import PreMongoEntry


class PreMongoRunner(PreMongoEntry):
    finishing_position: Optional[str] = None
    official_position: Optional[str] = None
    beaten_distance: Optional[float] = None
    time: Optional[Duration] = None
    sp: Optional[str] = None
