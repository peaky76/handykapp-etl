import fitz  # type: ignore
from prefect import get_run_logger

from helpers import stream_file
from transformers.formdata_transformer import get_formdata_date

from .page_processor import page_processor


def file_processor():
    logger = get_run_logger()
    logger.info("Starting file processor")
    page_count = 0

    p = page_processor()
    next(p)

    try:
        while True:
            file = yield
            logger.info(f"Processing {file}")

            date = get_formdata_date(file)
            doc = fitz.open("pdf", stream_file(file))
            for page in doc:
                p.send((page, date))
                page_count += 1

    except GeneratorExit:
        logger.info(f"Processed {page_count} pages")
        p.close()
