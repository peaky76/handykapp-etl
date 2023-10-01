from loaders.formdata_loader import adjust_rr_name


def test_formdata_loader_adjust_rr_name_on_basic_name():
    assert adjust_rr_name("JSmith") == "J Smith"


def test_formdata_loader_adjust_rr_name_when_first_name_spelt_out():
    assert adjust_rr_name("JohnSmith") == "John Smith"


def test_formdata_loader_adjust_rr_name_when_middle_initial_given():
    assert adjust_rr_name("JFSmith") == "J F Smith"


def test_formdata_loader_adjust_rr_name_when_country_given():
    assert adjust_rr_name("JSmith (IRE)") == "J Smith (IRE)"


def test_formdata_loader_adjust_rr_name_when_title_given():
    assert adjust_rr_name("MsJSmith") == "Ms J Smith"


def test_formdata_loader_adjust_rr_name_when_irish():
    assert adjust_rr_name("JO'Smith") == "J O'Smith"


def test_formdata_loader_adjust_rr_name_when_scottish():
    assert adjust_rr_name("JMcSmith") == "J McSmith"
