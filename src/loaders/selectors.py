def select_set(data, key):
    return sorted(list(set([x[key] for x in data])))
