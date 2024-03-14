# import pytest
# from processors.database_processor import DatabaseProcessor


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

#     generator = processor.process_wrapper()
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

#     generator = processor.process_wrapper()
#     next(generator)

#     generator.send("item")

#     assert processor.update.called_once_with("item", 1)
#     assert processor.insert.called_once_with("item")
#     assert processor.post_process.called_once_with("item", 1)