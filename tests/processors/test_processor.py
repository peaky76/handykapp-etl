import pytest
from processors.processor import Processor


@pytest.fixture()
def mock_processor(mocker):
    mocker.patch("prefect.context.TaskRunContext")
    logger = mocker.patch("prefect.get_run_logger")
    process_func = mocker.Mock()
    process_func.return_value = (0, 0, 0)
    next_processor = mocker.patch('processors.processor.Processor')
    p = Processor("foobar", process_func, next_processor).process
    return p, logger, process_func, next_processor

def test_processor_process_calls_logger_info(mock_processor):
    p, logger, _, _ = mock_processor
    x = p()
    next(x)
    assert logger.info.called_with("Starting foobar processor")

def test_processor_process_calls_next_processor(mock_processor):
    p, _, _, next_processor = mock_processor
    x = p()
    next(x)
    assert next_processor.called

def test_processor_process_calls_process_func_with_item_and_source(mock_processor):
    p, _, process_func, _ = mock_processor
    x = p()
    next(x)
    assert process_func.called_with(None, None)

def test_processor_process_calls_logger_info_on_exit(mock_processor):
    p, logger, _, _ = mock_processor
    x = p()
    x.close()
    assert logger.info.called_with("Finished processing foobar. Updated 0, added 0, skipped 0")