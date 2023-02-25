from flows.bha import SOURCE, DESTINATION


def test_bha_source():
    expected = "https://www.britishhorseracing.com/feeds/v4/ratings/csv/"
    assert expected == SOURCE


def test_bha_destination():
    expected = "handykapp/bha/"
    assert expected == DESTINATION
