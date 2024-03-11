from typing import Optional

from .py_object_id import PyObjectId
from .transformed_horse import TransformedHorse


class TransformedRunner(TransformedHorse):
    race_id: PyObjectId
    jockey: Optional[str] = None
    trainer: Optional[str] = None    
    owner: Optional[str] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    lbs_carried: Optional[int] = None 
    official_rating: Optional[int] = None
    position: Optional[int | str] = None
    distance_beaten: Optional[int | str] = None
    sp: Optional[str] = None