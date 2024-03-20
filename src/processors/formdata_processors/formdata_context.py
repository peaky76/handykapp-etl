import contextvars

formdata_year = contextvars.ContextVar("formdata_year", default=None)
