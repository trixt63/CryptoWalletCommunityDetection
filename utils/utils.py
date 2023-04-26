import copy
import time

from src.utils.logger_utils import get_logger

_LOGGER = get_logger(__name__)


def token_change_logs_to_usd(token_change_logs, timestamp=None):
    value_in_usd = 0
    for token, change_logs in token_change_logs.items():
        sorted_change_logs = sorted(change_logs.items(), key=lambda x: x[0])
        if sorted_change_logs:
            if timestamp is None:
                value_in_usd += sorted_change_logs[-1][1]['valueInUSD']
            else:
                value = None
                for t, v in sorted_change_logs:
                    if t > timestamp:
                        break
                    if v is None:
                        continue
                    value = v.copy()

                if value:
                    value_in_usd += value['valueInUSD']

    return value_in_usd


def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def calculate_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        returned_value = func(*args, **kwargs)
        end_time = time.time()
        _LOGGER.debug(f"Processing time: {end_time - start_time}")
        return returned_value

    return wrapper


def aggregate_logs_by_timestamp(current_aggregated_dict, adding_dict, single_value=True):
    new_aggregated_list = {}
    CURRENT_DICT = "CURRENT_DICT"
    ADDING_DICT = "ADDING_DICT"
    if len(current_aggregated_dict) == 0:
        return adding_dict

    elif len(adding_dict) == 0:
        return current_aggregated_dict

    else:
        current_list_tagged = []
        adding_list_tagged = []

        for timestamp in current_aggregated_dict.keys():
            current_list_tagged.append({
                "timestamp": timestamp,
                "tag": CURRENT_DICT
            })
        for timestamp in adding_dict.keys():
            adding_list_tagged.append({
                "timestamp": timestamp,
                "tag": ADDING_DICT
            })

        combined_list = current_list_tagged + adding_list_tagged
        sorted_combined_list = sorted(combined_list, key=lambda k: k["timestamp"])

        last_current_dict_index = None
        last_adding_dict_index = None

        index = 0
        while index < len(sorted_combined_list):
            if sorted_combined_list[index]["tag"] == CURRENT_DICT:
                last_current_dict_index = index
            else:
                last_adding_dict_index = index

            if single_value is True:
                init_value = 0
            else:
                init_value = {
                    "amount": 0,
                    "valueInUSD": 0
                }

            if last_adding_dict_index is None:
                adding_value = init_value
            else:
                timestamp = sorted_combined_list[last_adding_dict_index]["timestamp"]
                adding_value = adding_dict[timestamp]

            if last_current_dict_index is None:
                current_value = init_value
            else:
                timestamp = sorted_combined_list[last_current_dict_index]["timestamp"]
                current_value = current_aggregated_dict[timestamp]

            if single_value is True:
                new_aggregated_list[sorted_combined_list[index]['timestamp']] = sum_single_value(
                    current_value=current_value,
                    adding_value=adding_value
                )
            else:
                new_aggregated_list[sorted_combined_list[index]['timestamp']] = sum_token_change_log(
                    current_value=current_value,
                    adding_value=adding_value
                )

            index = index + 1

    return new_aggregated_list


def aggregate_separated_logs(current_merged_logs: dict, adding_logs: dict, chain_id=None):
    merged_logs = copy.deepcopy(current_merged_logs)
    for adding_key, adding_value in adding_logs.items():
        key = f'{chain_id}_{adding_key}' if chain_id is not None else adding_key
        if adding_key in merged_logs.keys():
            merged_logs[key] += adding_value
        else:
            merged_logs[key] = adding_value
    return merged_logs


def aggregate_token_change_logs(current_merged_logs: dict, adding_logs: dict, chain_id=None):
    merged_logs = copy.deepcopy(current_merged_logs)
    for token, log in adding_logs.items():
        key = f'{chain_id}_{token}' if chain_id is not None else token
        if token not in merged_logs.keys():
            merged_logs.update({key: log})
        else:
            merged_logs.update(
                {key: aggregate_logs_by_timestamp(merged_logs[token], adding_logs[token], single_value=False)}
            )
    return merged_logs


def update_token_change_logs(current_merged_logs: dict, adding_logs: dict):
    merged_logs = copy.deepcopy(current_merged_logs)
    for token, log in adding_logs.items():
        if token not in merged_logs.keys():
            merged_logs[token] = log
        else:
            merged_log = merged_logs[token]
            for timestamp, value in log.items():
                if merged_log.get(timestamp) is None:
                    merged_log[timestamp] = value
                else:
                    merged_log[timestamp].update(value)

    return merged_logs


def token_change_logs_to_usd(token_change_logs, timestamp=None):
    value_in_usd = 0
    for token, change_logs in token_change_logs.items():
        sorted_change_logs = sorted(change_logs.items(), key=lambda x: x[0])
        if sorted_change_logs:
            if timestamp is None:
                value_in_usd += sorted_change_logs[-1][1]['valueInUSD']
            else:
                value = None
                for t, v in sorted_change_logs:
                    if t > timestamp:
                        break
                    if v is None:
                        continue
                    value = v.copy()

                if value:
                    value_in_usd += value['valueInUSD']

    return value_in_usd


def sum_single_value(current_value, adding_value):
    return current_value + adding_value


def sum_token_change_log(current_value, adding_value):
    return {
        "amount": current_value["amount"] + adding_value["amount"],
        "valueInUSD": current_value["valueInUSD"] + adding_value["valueInUSD"]
    }


def concat_chain_id(token_dict: dict, chain_id):
    concat_dict = {}
    for token_address, value in token_dict.items():
        concat_dict[f"{chain_id}_{token_address}"] = value
    return concat_dict


def change_logs_integer_timestamp(change_logs):
    return {int(t): v for t, v in change_logs.items()}


def token_change_logs_integer_timestamp(token_change_logs):
    result = {}
    for token, change_logs in token_change_logs.items():
        result[token] = change_logs_integer_timestamp(change_logs)
    return result


def round_timestamp(timestamp, round_time):
    return int(timestamp / round_time) * round_time


def check_in_round_time(t1, t2, round_time):
    return round_timestamp(t1, round_time) == round_timestamp(t2, round_time)


def add_prefix_to_key_of_dict(dict_object: dict, prefix):
    new_dict = dict()
    for k in dict_object:
        new_dict[f"{prefix}_{k}"] = dict_object[k]
    return new_dict


def set_or_add_to_dict(updated_dict: dict, origin_dict: dict):
    for k, v in updated_dict.items():
        if origin_dict.get(k):
            origin_dict[k] += v
        else:
            origin_dict[k] = v


def get_previous_key_value_in_sorted_dict(sorted_dict: dict, key):
    if not sorted_dict:
        return None, None
    keys = sorted_dict.keys()
    try:
        for k in reversed(keys):
            if int(k) < int(key):
                return k, sorted_dict[k]
    except Exception as e:
        _LOGGER.warning(f"get_previous_key_value_in_sorted_dict err {e}")
    return None, None
