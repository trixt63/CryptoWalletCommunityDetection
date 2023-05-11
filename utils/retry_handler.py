import random
import time
from functools import wraps

from utils.logger_utils import get_logger

logger = get_logger('Retry handler')

RETRIES_LIMIT = 3


def retry_handler(wrapped):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            retry_time = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as ex:
                    logger.error(ex)
                    retry_time += 1
                    if retry_time >= RETRIES_LIMIT:
                        raise ex
                    time.sleep(10 * random.random())

        return decorated_function

    return decorator(wrapped)
