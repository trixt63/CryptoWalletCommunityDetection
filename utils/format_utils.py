import datetime
import re

from utils.logger_utils import get_logger

logger = get_logger('Format Utils')


def convert_tvl(tvl_str: str, type_=int):
    if not tvl_str:
        return None

exchange    try:
        tvl_str = tvl_str.replace(',', '').lower().strip()
        if tvl_str.startswith('$'):
            tvl_str = tvl_str[1:]

        if tvl_str.startswith('<'):
            return 0

        decimals_map = {
            'b': 9,
            'm': 6,
            'k': 3
        }
        decimals = 0
        for k, decimals_ in decimals_map.items():
            if tvl_str.endswith(k):
                tvl_str = tvl_str[:-1]
                decimals = decimals_
                break

        tvl_str = float(tvl_str) * round(10 ** decimals)
        return type_(tvl_str)
    except Exception as ex:
        logger.exception(ex)
        return None


def convert_tx_timestamp(time_str):
    time_format = '%b-%d-%Y %I:%M:%S %p'
    time = datetime.datetime.strptime(time_str, time_format)
    return int(time.timestamp())


def remove_special_characters(string: str, specific_characters: str = None):
    if specific_characters is None:
        specific_characters = '!@#$%^&*()+=[]\\|{}:;\'"<>?/.,~`'
    return string.translate(str.maketrans('', '', specific_characters))


def filter_string(string: str, allowable_characters_regex: str = None):
    if allowable_characters_regex is None:
        allowable_characters_regex = '[a-zA-Z0-9_\-:.@()+,=;$!*\'%]'
    chars = re.findall(allowable_characters_regex, string)
    return ''.join(chars)


def convert_percentage(p_str: str):
    p_str = p_str.strip().replace(',', '')
    try:
        if p_str.endswith('%'):
            p_str = p_str.replace('%', '')
            p = round(float(p_str) / 100, 4)
        else:
            p = float(p_str)
    except (TypeError, ValueError):
        logger.warning(f'Cannot format percentage of {p_str}')
        p = 0
    except Exception as ex:
        logger.exception(ex)
        p = 0
    return p


def format_cmc_number_data(text, handler_func=int, exception='--', default_value=None):
    if text != exception:
        try:
            return handler_func(text)
        except ValueError:
            return default_value

    return default_value


def format_cmc_launched_at(text):
    return int(datetime.datetime.strptime(text, '%b %Y').timestamp())


def snake_to_pascal(text: str):
    return "".join(x.capitalize() for x in text.lower().split("_"))


def snake_to_lower_camel(text: str):
    camel_string = snake_to_pascal(text)
    return text[0].lower() + camel_string[1:]
