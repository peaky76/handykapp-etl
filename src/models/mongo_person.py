from typing import Dict, Optional

from .jockey_rating import JockeyRating
from .mongo_base_model import MongoBaseModel
from .references import References
from .sex import Sex
from .year import Year


class MongoPerson(MongoBaseModel):
    title: Optional[str] = None
    first: str
    middle: Optional[str] = None
    last: str
    suffix: Optional[str] = None
    nickname: Optional[str] = None
    sex: Optional[Sex] = None
    role: Optional[str] = None
    ratings: Optional[Dict[Year, JockeyRating]] = None
    references: Optional[References] = None