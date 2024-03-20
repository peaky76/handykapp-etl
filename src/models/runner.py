from typing import Optional

from .horse import Horse
from .person import Person


class Runner(Horse):
    jockey: Optional[Person] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    lbs_carried: Optional[int] = None
    official_rating: Optional[int] = None
