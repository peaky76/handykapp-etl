import pytest
from processors.processor import Processor


@pytest.fixture()
def processor(mocker):
    processor = Processor()
    processor._descriptor = "Foobar"
    processor._next_processor = None
    processor.post_process = mocker.Mock()
    return processor

def test_processor_update():
    processor = Processor()
    with pytest.raises(NotImplementedError):
        processor.update(None, None)

def test_processor_insert():
    processor = Processor()
    with pytest.raises(NotImplementedError):
        processor.insert(None, None)

def test_processor_process_when_update_made(mocker, processor):
    mocker.patch("prefect.context.TaskRunContext")
    logger_mock = mocker.patch("processors.processor.get_run_logger")

    mock_update = mocker.Mock()
    mock_update.return_value = {"_id": 1}
    mock_insert = mocker.Mock()
    
    processor.update = mock_update
    processor.insert = mock_insert

    generator = processor.process()
    next(generator)

    generator.send(("item", "source"))

    assert processor.update.called_once_with("item", "source")
    assert not processor.insert.called
    assert processor.post_process.called_once_with("item", 1, "source", logger_mock)

def test_processor_process_when_update_not_made(mocker, processor):
    mocker.patch("prefect.context.TaskRunContext")
    logger_mock = mocker.patch("processors.processor.get_run_logger")

    
    mock_update = mocker.Mock()
    mock_update.return_value = None
    mock_insert = mocker.Mock()
    mock_insert.return_value = {"_id": 1}
    
    processor.update = mock_update
    processor.insert = mock_insert

    generator = processor.process()
    next(generator)

    generator.send(("item", "source"))

    assert processor.update.called_once_with("item", "source")
    assert processor.insert.called_once_with("item", "source")
    assert processor.post_process.called_once_with("item", 1, "source", logger_mock)