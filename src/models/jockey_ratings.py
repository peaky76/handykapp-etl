from typing import Dict

from pydantic import BaseModel

from .jockey_rating import JockeyRating
from .year import Year


class JockeyRatings(BaseModel):
    name: str
    ratings: Dict[Year, JockeyRating]
