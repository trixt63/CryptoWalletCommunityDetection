import time
from functools import wraps

from src.constants.time_constants import SLEEP_DURATION
from src.utils.logger_utils import get_logger
from src.utils.time_utils import round_timestamp

logger = get_logger('Repeat handler')


def repeat_handler(wrapped):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            """Schedule function execution after every fixed interval.

            :key interval: (int) Fixed interval.
            :key end_timestamp: (int) Timestamp to finish execute. Default: None.
            :key from_start: (bool) Calculate next run time from start time or not. Default: False.
            """
            interval = kwargs.get('interval')
            end_timestamp = kwargs.get('end_timestamp')
            while True:
                try:
                    next_synced_timestamp = call_function(f, *args, **kwargs)

                    # Check if not repeat
                    if interval is None:
                        return

                    if (end_timestamp is not None) and (next_synced_timestamp > end_timestamp):
                        return

                    # Sleep to next synced time
                    time_sleep = next_synced_timestamp - time.time()
                    if time_sleep > 0:
                        logger.info(f'Sleep {round(time_sleep, 3)} seconds')
                        time.sleep(time_sleep)
                except Exception as ex:
                    logger.exception(ex)
                    logger.warning(f'Something went wrong!!! Try again after {SLEEP_DURATION} seconds ...')
                    time.sleep(SLEEP_DURATION)

        return decorated_function

    return decorator(wrapped)


def call_function(f, *args, **kwargs):
    interval = kwargs.get('interval')
    from_start = kwargs.get('from_start', False)
    next_synced_timestamp = None
    if from_start:
        if interval:
            next_synced_timestamp = round_timestamp(int(time.time()), round_time=interval) + interval
        f(*args, **kwargs)
    else:
        f(*args, **kwargs)
        if interval:
            next_synced_timestamp = round_timestamp(int(time.time()), round_time=interval) + interval
    return next_synced_timestamp
