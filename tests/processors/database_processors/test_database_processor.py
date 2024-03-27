# import pytest
# from processors.database_processor import DatabaseProcessor
from processors.database_processors.database_processor import dot_flatten_nested


def test_dot_flatten_nested_leaves_one_level_dict_unchanged():
    input_dict = {"a": 1, "b": 2}
    expected = {"a": 1, "b": 2}
    assert dot_flatten_nested(input_dict) == expected

def test_dot_flatted_nested_works_on_two_level_dict():
    input_dict = {"a": {"c": 1}, "b": 2}
    expected = {"a.c": 1, "b": 2}
    assert dot_flatten_nested(input_dict) == expected

def test_dot_flatten_nested_works_on_three_level_dict():
    input_dict = {"a": {"c": {"d": 1} }, "b": 2}
    expected = {"a.c.d": 1, "b": 2}
    assert dot_flatten_nested(input_dict) == expected

# @pytest.fixture()
# def processor(mocker):
#     processor = DatabaseProcessor()
#     processor._descriptor = "Foobar"
#     processor._next_processor = None
#     processor.post_process = mocker.Mock()
#     return processor

# def test_processor_update():
#     processor = DatabaseProcessor()
#     with pytest.raises(NotImplementedError):
#         processor.update(None)

# def test_processor_process_when_update_made(mocker, processor):
#     mocker.patch("prefect.context.TaskRunContext")
#     logger_mock = mocker.patch("processors.processor.get_run_logger")

#     mock_update = mocker.Mock()
#     mock_update.return_value = {"_id": 1}
#     mock_insert = mocker.Mock()
    
#     processor.update = mock_update
#     processor.insert = mock_insert

#     generator = processor
#     next(generator)

#     generator.send("item")

#     assert processor.update.called_once_with("item", 1)
#     assert not processor.insert.called
#     assert processor.post_process.called_once_with("item", 1)

# def test_processor_process_when_update_not_made(mocker, processor):
#     mocker.patch("prefect.context.TaskRunContext")
#     logger_mock = mocker.patch("processors.processor.get_run_logger")

    
#     mock_update = mocker.Mock()
#     mock_update.return_value = None
#     mock_insert = mocker.Mock()
#     mock_insert.return_value = {"_id": 1}
    
#     processor.update = mock_update
#     processor.insert = mock_insert

#     generator = processor
#     next(generator)

#     generator.send("item")

#     assert processor.update.called_once_with("item", 1)
#     assert processor.insert.called_once_with("item")
#     assert processor.post_process.called_once_with("item", 1)