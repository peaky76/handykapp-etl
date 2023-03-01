from loaders.load import select_dams, select_sires


def test_select_dams():
    data = [
        {"dam": "DAM1", "dam_country": "IRE"},
        {"dam": "DAM2", "dam_country": "GB"},
        {"dam": "DAM2", "dam_country": "IRE"},
        {"dam": "DAM1", "dam_country": "IRE"},
        {"dam": "DAM3", "dam_country": "IRE"},
    ]
    assert [
        {"name": "DAM1", "country": "IRE"},
        {"name": "DAM2", "country": "GB"},
        {"name": "DAM2", "country": "IRE"},
        {"name": "DAM3", "country": "IRE"},
    ] == select_dams.fn(data)


def test_select_sires():
    data = [
        {"sire": "SIRE1", "sire_country": "IRE"},
        {"sire": "SIRE2", "sire_country": "GB"},
        {"sire": "SIRE2", "sire_country": "IRE"},
        {"sire": "SIRE1", "sire_country": "IRE"},
        {"sire": "SIRE3", "sire_country": "IRE"},
    ]
    assert [
        {"name": "SIRE1", "country": "IRE"},
        {"name": "SIRE2", "country": "GB"},
        {"name": "SIRE2", "country": "IRE"},
        {"name": "SIRE3", "country": "IRE"},
    ] == select_sires.fn(data)
