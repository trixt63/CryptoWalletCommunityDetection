import json
from web3 import Web3
from web3.middleware import geth_poa_middleware

from src.services.blockchain.eth.eth_services import EthService
from src.constants.time_constants import TimeConstants
from src.constants.network_constants import Networks


class BlockUtils:
    w3 = Web3(Web3.HTTPProvider(Networks.providers[Networks.bsc]))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    eth_service = EthService(w3)

    @staticmethod
    def get_block_range_in_days(start_block, end_block):
        """
        The get_block_range_in_days function takes a start block and an end block as arguments.
        It returns a dictionary with the following structure:
        {
            start_timestamp: [first_block, last_block]
        }
        Where first_block is the first block in that day and last_block is the last block in that day.
        The function assumes that all blocks are ordered by timestamp from oldest to newest.

        Args:
            start_block: Specify the first block for which you want to get the range
            end_block: Specify the last block that should be included in the range

        Returns:
            A dictionary where the keys are the start of each day and the values are tuples containing two numbers:
            first block of the day and the last one.

        Doc Author:
            Trelent
        """
        start_time = json.loads(Web3.toJSON(BlockUtils.w3.eth.get_block(start_block)))['timestamp']
        end_time = json.loads(Web3.toJSON(BlockUtils.w3.eth.get_block(end_block)))['timestamp']

        start_day = start_time - start_time % TimeConstants.A_DAY \
            if start_time % TimeConstants.A_DAY != 0 \
            else start_time
        end_day = end_time - end_time % TimeConstants.A_DAY \
            if end_time % TimeConstants.A_DAY != 0 \
            else end_time
        number_of_days = int((end_day - start_day) / TimeConstants.A_DAY) + 1

        res = {}
        start_day_timestamp = start_day
        for _ in range(number_of_days):
            end_day_timestamp = start_day_timestamp + TimeConstants.A_DAY - 1
            (first_block, last_block) = BlockUtils.eth_service.get_block_range_for_timestamps(start_day_timestamp,
                                                                                              end_day_timestamp)
            res[start_day_timestamp] = [first_block, last_block]
            start_day_timestamp = end_day_timestamp + 1
        return res

    @staticmethod
    def get_day_first_block(timestamp: int):
        if timestamp % TimeConstants.A_DAY != 0:
            start_day = timestamp - timestamp % TimeConstants.A_DAY
            end_day = timestamp
        else:
            start_day = timestamp
            end_day = timestamp + TimeConstants.MINUTES_5

        (first_block, _) = BlockUtils.eth_service.get_block_range_for_timestamps(start_day, end_day)
        return first_block


if __name__ == '__main__':
    a = BlockUtils.get_day_first_block(1675555200)
    b = BlockUtils.get_day_first_block(1675987200)
    print(b-1)
    print(a)
