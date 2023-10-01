# from loaders.main_loader import drop_database, spec_database
# from pymongo import ASCENDING as ASC
# #

# def test_drop_database(mocker):
#     client = mocker.patch("src.clients.mongo_client")
#     drop_database.fn()
#     assert client.drop_database.called_once_with("handykapp")


# def test_spec_database_adds_horse_unique_index(mocker):
#     create_index = mocker.patch("pymongo.collection.Collection.create_index")
#     spec_database.fn()
#     assert create_index.called_once_with(
#         [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
#     )


# def test_spec_database_adds_horse_name_index(mocker):
#     create_index = mocker.patch("pymongo.collection.Collection.create_index")
#     spec_database.fn()
#     assert create_index.called_once_with("name")


# def test_spec_database_adds_people_name_index(mocker):
#     create_index = mocker.patch("pymongo.collection.Collection.create_index")
#     spec_database.fn()
#     assert create_index.called_once_with("name", unique=True)


# def test_spec_database_adds_racecourse_index(mocker):
#     create_index = mocker.patch("pymongo.collection.Collection.create_index")
#     spec_database.fn()
#     assert create_index.called_once_with([("name", ASC), ("country", ASC)], unique=True)


# def test_spec_database_adds_races_index(mocker):
#     create_index = mocker.patch("pymongo.collection.Collection.create_index")
#     spec_database.fn()
#     assert create_index.called_once_with(
#         [("racecourse", ASC), ("datetime", ASC)], unique=True
#     )


# def test_spec_database_adds_races_result_horse_index(mocker):
#     create_index = mocker.patch("pymongo.collection.Collection.create_index")
#     spec_database.fn()
#     assert create_index.called_once_with("result.horse")
