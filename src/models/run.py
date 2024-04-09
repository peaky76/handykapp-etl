from typing import Optional

from .entry import Entry


class Run(Entry):
    finishing_position: Optional[int | str] = None
    beaten_distance: Optional[int | str] = None
    sp: Optional[str] = None
