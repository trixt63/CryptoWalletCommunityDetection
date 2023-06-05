import copy
import gc
import os
import time
from typing import List, Dict
from web3 import HTTPProvider
from web3 import Web3
from web3.middleware import geth_poa_middleware
from query_state_lib.base.utils.encoder import encode_eth_call_data
from query_state_lib.base.mappers.eth_call_mapper import EthCall
from query_state_lib.client.client_querier import ClientQuerier
from cli_scheduler.scheduler_job import SchedulerJob

from constants.lp_constants import lp_factory_mapping
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from utils import file_utils
from utils.logger_utils import get_logger

BATCH_CALLS = 1000
ARCHIVE_MAPPING = {
    '0x38': 'https://rpc.ankr.com/bsc',
    '0xfa': 'https://rpc.ankr.com/fantom',
    '0x1': 'https://rpc.ankr.com/eth'
}

PAIR_ID_BATCH_SIZE = 1000

_LAST_SYNCED_FILE = '.data/export_lp_contracts_job.txt'

logger = get_logger('Export lp tokens')


class ExportLPContractsJob(SchedulerJob):
    def __init__(self, interval,
                 chain_id, factory_address, factory_abi, lp_abi,
                 start_pair_id=0, end_pair_id=None):
        super().__init__(scheduler=f'^true@{interval}#true')

        self.mongo_entity = MongoDBEntity()
        self._exporter = MongoDB()
        self.chain_id = chain_id

        self._start_pair_id = start_pair_id
        self._end_pair_id = end_pair_id or float('inf')

        archive = ARCHIVE_MAPPING[chain_id]
        self.web3 = Web3(HTTPProvider(archive))
        self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.client_querier = ClientQuerier(provider_url=archive)

        self.factory_abi = factory_abi
        self.lp_abi = lp_abi
        self.factory_address = self.web3.toChecksumAddress(factory_address)
        self.factory_contract = self.web3.eth.contract(abi=factory_abi, address=self.factory_address)

    def _pre_start(self):
        # init the file to save last crawled pair id
        if self._start_pair_id or (not os.path.isfile(_LAST_SYNCED_FILE)):
            file_utils.init_last_synced_file(self._start_pair_id, _LAST_SYNCED_FILE)

    def _start(self):
        # get the last crawled pair_id & the latest pair id on the blockchain
        self._start_pair_id = file_utils.read_last_synced_file(_LAST_SYNCED_FILE)
        self._latest_pair_id = self.factory_contract.functions.allPairsLength().call() - 1

    def _end(self):
        gc.collect()

    # override
    def _check_finish(self):
        # Check if not repeat
        if self.interval is None:
            return True

        # Check if over end timestamp
        if (self.end_timestamp is not None) and (self.next_synced_timestamp > self.end_timestamp):
            return True

        # Check if over end pair id
        if (self._end_pair_id is not None) and (self._start_pair_id > self._end_pair_id):
            return True

        return False

    def _execute(self):
        logger.info(f"Exporting details from LP pair {self._start_pair_id} to LP pair {self._end_pair_id or self._latest_pair_id} "
                    f"created by factory {self.factory_address} on chain {self.chain_id}")

        # get all tokens
        logger.info(f"Getting CoinGecko tokens on chain {self.chain_id}")
        tokens_cursor = self.mongo_entity.get_listed_tokens(chain_id=self.chain_id)
        self.listed_tokens = {token['address']: token for token in tokens_cursor}
        logger.info(f"Number of listed tokens: {len(self.listed_tokens)}")

        batch_1st_pair = self._start_pair_id
        while True:
            batch_last_pair = min(batch_1st_pair + PAIR_ID_BATCH_SIZE - 1,
                                  self._latest_pair_id,
                                  self._end_pair_id or float('inf'))
            self._export_batch_lp_contracts(pair_ids_batch=list(range(batch_1st_pair, batch_last_pair + 1)))
            batch_1st_pair = batch_last_pair + 1
            file_utils.write_last_synced_file(_LAST_SYNCED_FILE, batch_1st_pair)

            if batch_1st_pair > self._latest_pair_id or batch_1st_pair > self._end_pair_id:
                break

    def _export_batch_lp_contracts(self, pair_ids_batch: List):
        _start_time = time.time()

        # get all lp contracts
        lp_contracts = _get_lp_from_pair_ids(factory_address=self.factory_address,
                                             chain_id=self.chain_id,
                                             pair_ids=pair_ids_batch,
                                             factory_abi=self.factory_abi,
                                             lp_abi=self.lp_abi,
                                             client_querier=self.client_querier)

        # get addresses of 2 tokens in each pair
        lps_pairs_addresses = _get_pairs_addresses(lp_addresses=list(lp_contracts.keys()),
                                                   lp_abi=self.lp_abi,
                                                   client_querier=self.client_querier)
        # then filter
        for lp_address, pair_addresses in lps_pairs_addresses.items():
            if pair_addresses['token0'] not in self.listed_tokens.keys() or \
                    pair_addresses['token1'] not in self.listed_tokens.keys():
                lp_contracts.pop(lp_address)
            else:
                lp_contracts[lp_address].update(lps_pairs_addresses.get(lp_address))

        # get reserves, then balances of 2 token
        lps_pairs_reserves = _get_pairs_reserves(lp_addresses=list(lp_contracts.keys()),
                                                 lp_abi=self.lp_abi,
                                                 client_querier=self.client_querier)

        for lp_address, pair_reserves in lps_pairs_reserves.items():
            try:
                _balances_in_usd = dict()
                for i in ['0', '1']:
                    token_address = lp_contracts[lp_address][f'token{i}']

                    token_price = self.listed_tokens[token_address]['price']
                    token_decimal = self.listed_tokens[token_address].get('decimals', 18)
                    token_reserve = pair_reserves[f'reserve{i}']
                    _balances_in_usd[f'token{i}'] = token_reserve / (10 ** token_decimal) * token_price

                lp_contracts[lp_address]['pair_balances_in_usd'] = copy.deepcopy(_balances_in_usd)
            except Exception as ex:
                logger.warning(f"Error at LP {lp_address}: {ex}")
                continue

        # export
        self._exporter.upsert_lp_tokens(data=list(lp_contracts.values()))
        logger.info(
            f"Processed pair id {pair_ids_batch[0]} to {pair_ids_batch[-1]}; "
            f"Exported {len(lp_contracts)}; Took {time.time() - _start_time:.2f}s")


def _get_lp_from_pair_ids(factory_address, chain_id, pair_ids: List, factory_abi, lp_abi, client_querier) -> dict:
    lp_contracts = dict()
    rpc_calls = list()

    # get list of lps
    for _pair_id in pair_ids:
        encoded = encode_eth_call_data(
            abi=factory_abi,
            fn_name="allPairs",
            args=[_pair_id]
        )
        rpc_calls.append(EthCall(
            to=factory_address,
            abi=factory_abi,
            fn_name="allPairs",
            block_number="latest",
            data=encoded,
            id=_pair_id))

    calls_results = client_querier.sent_batch_to_provider(
        list_json_rpc=rpc_calls,
        batch_size=50,
        max_workers=8
    )

    for _id, result in calls_results.items():
        _address = result.decode_result()[0].lower()
        lp_contracts[_address] = {
            'chain_id': chain_id,
            'address': _address,
            'pair_id': _id,
            'factory': factory_address.lower(),
            'dex': lp_factory_mapping.get(factory_address.lower()).get('name')
        }

    # # get addresses of 2 tokens in pair
    # lps_pairs_addresses = _get_pairs_addresses(lp_addresses=list(lp_contracts.keys()),
    #                                            lp_abi=lp_abi,
    #                                            client_querier=client_querier)
    # # then filter
    # for lp_address, pair_addresses in lps_pairs_addresses.items():
    #     if pair_addresses['token0'] not in tokens_in_pairs.keys() or \
    #             pair_addresses['token1'] not in tokens_in_pairs.keys():
    #         lp_contracts.pop(lp_address)
    #     else:
    #         lp_contracts[lp_address].update(lps_pairs_addresses.get(lp_address))

    return lp_contracts


def _get_pairs_addresses(lp_addresses, lp_abi, client_querier) -> Dict:
    lps_pairs_addresses = {lp_address: {'token0': 0, 'token1': 0}
                           for lp_address in lp_addresses}

    for fn_name in ['token0', 'token1']:
        rpc_calls = list()
        for lp_address in lp_addresses:
            encoded = encode_eth_call_data(
                abi=lp_abi,
                fn_name=fn_name,
                args=[]
            )
            rpc_calls.append(EthCall(
                to=lp_address,
                abi=lp_abi,
                fn_name=fn_name,
                block_number="latest",
                data=encoded,
                id=str(lp_address)))

        calls_results = client_querier.sent_batch_to_provider(
            list_json_rpc=rpc_calls,
            batch_size=100,
            max_workers=8
        )

        for lp_address, result in calls_results.items():
            lps_pairs_addresses[lp_address][fn_name] = result.decode_result()[0].lower()

    return lps_pairs_addresses


def _get_pairs_reserves(lp_addresses, lp_abi, client_querier):
    lps_pairs_reserves = {lp_address: {'reserve0': 0, 'reserve1': 0, 'decimals': 18}
                          for lp_address in lp_addresses}

    rpc_calls = list()
    for lp_address in lp_addresses:
        encoded = encode_eth_call_data(
            abi=lp_abi,
            fn_name='getReserves',
            args=[]
        )
        rpc_calls.append(EthCall(
            to=lp_address,
            abi=lp_abi,
            fn_name='getReserves',
            block_number="latest",
            data=encoded,
            id=str(lp_address)))

    calls_results = client_querier.sent_batch_to_provider(
        list_json_rpc=rpc_calls,
        batch_size=100,
        max_workers=8
    )

    for lp_address, result in calls_results.items():
        lps_pairs_reserves[lp_address]['reserve0'] = result.decode_result()[0]
        lps_pairs_reserves[lp_address]['reserve1'] = result.decode_result()[1]

    return lps_pairs_reserves


def _get_pairs_decimals(lp_addresses, lp_abi, client_querier):
    lps_pairs_decimals = {lp_address: {'decimals': 18}
                          for lp_address in lp_addresses}
    rpc_calls = list()

    for lp_address in lp_addresses:
        encoded = encode_eth_call_data(
            abi=lp_abi,
            fn_name='decimals',
            args=[]
        )
        rpc_calls.append(EthCall(
            to=lp_address,
            abi=lp_abi,
            fn_name='decimals',
            block_number="latest",
            data=encoded,
            id=str(lp_address)))

    calls_results = client_querier.sent_batch_to_provider(
        list_json_rpc=rpc_calls,
        batch_size=100,
        max_workers=8
    )

    for lp_address, result in calls_results.items():
        lps_pairs_decimals[lp_address]['decimals'] = result.decode_result()[0]

    return lps_pairs_decimals
