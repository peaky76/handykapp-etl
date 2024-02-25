def compact(dictionary: dict):
    return {k: v for k, v in dictionary.items() if v is not None}
