from transformers.transformer import log_validation_problem


def test_log_validation_problem(mocker):
    logger = mocker.patch("transformers.transformer.get_run_logger")
    problem = {
        "name": "name_str",
        "field": "year",
        "row": "1",
        "value": "foobar",
        "error": "ValueError",
    }
    log_validation_problem(problem)
    assert logger().warning.called