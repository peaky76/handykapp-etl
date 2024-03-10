from pydantic import BaseModel


class HashableBaseModel(BaseModel):
    def __hash__(self):
        values = tuple(tuple(v) if isinstance(v, list) else v for v in self.__dict__.values())
        return hash((type(self), *values))

    def __eq__(self, other):
        if isinstance(other, HashableBaseModel):
            return self.__dict__ == other.__dict__
        return False