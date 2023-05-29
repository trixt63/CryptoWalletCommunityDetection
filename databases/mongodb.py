from typing import List

import pymongo
from pymongo import MongoClient, UpdateOne

from config import MongoDBConfig
# from constants.mongodb_constants import WALLETS_COL, CreatedPairEventsCollection
from utils.logger_utils import get_logger
from utils.format_utils import snake_to_lower_camel

logger = get_logger('MongoDB')
WALLETS_COL = 'test_lpTraders'


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]
        self.lp_tokens_col = self._db['lpTokens']
        self.wallets_col = self._db[WALLETS_COL]

        self._create_index()

    def _create_index(self):
        if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
            self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    def update_wallets(self, wallets: List[dict]):
        try:
            wallet_updates_bulk = []
            for wallet in wallets:
                wallet['_id'] = wallet['address']

                # pop all information besides data about lendings/dex/deposit/...
                wallet_base_data = {
                    '_id': wallet.pop('_id'),
                    'address': wallet.pop('address'),
                }
                tags = wallet.pop('tags')

                # process query to update nested documents for data about lendings/dex/deposit/...
                field_name = list(wallet.keys())[0]  # 'lendingPools' or 'tradedLPs' or 'exchangesDeposited'...
                project_names = list(wallet[field_name].keys())  # project id
                # update nested documents
                _mongo_add_to_set_query = {f"{field_name}.{project_name}": {"$each": wallet[field_name][project_name]}
                                           for project_name in project_names}
                _mongo_add_to_set_query["tags"] = {'$each': tags}

                # add update query into bulk
                _filter = {'_id': wallet_base_data['_id']}
                _update = {
                    '$set': wallet_base_data,
                    '$addToSet': _mongo_add_to_set_query
                }
                wallet_updates_bulk.append(UpdateOne(filter=_filter, update=_update, upsert=True))

            self.wallets_col.bulk_write(wallet_updates_bulk)
        except Exception as ex:
            logger.exception(ex)

    # The next 3 functions are for analysis purpose ###
    def count_wallets(self, _filter):
        _count = self.wallets_col.count_documents(_filter)
        return _count

    def count_wallets_each_chain(self, field_id, project_id, chain_id='0x38'):
        """Count number of wallets of each project on each chain"""
        _filter = {f"{field_id}.{project_id}": {"$exists": 1}}
        _projection = {f"{field_id}.{project_id}": 1}
        deployments = self.wallets_col.find(_filter, _projection)
        _count = 0
        for _depl in deployments:
            for project in _depl[field_id][project_id]:
                if project['chainId'] == chain_id:
                    _count += 1
                    continue
        return _count

    def count_exchange_deposit_wallets_each_chain(self, field_id, project_id, chain_id='0x38'):
        """Each CEX project stores a list of chain_ids, instead a list of objects like other type of project,
        so I need a separate function to handle this"""
        _filter = {f"{field_id}.{project_id}": chain_id}
        _count = self.wallets_col.count_documents(_filter)
        return _count
    # end analysis #############

    # for LP pair
    def get_latest_pair_id(self, chain_id: str):
        filter_ = {'chainId': chain_id}
        latest_pair = self.lp_tokens_col.find_one(filter_, sort=[("pairId", pymongo.DESCENDING)])
        return latest_pair['pairId']

    def get_lps_by_pair_ids(self, chain_id, start_pair_id, end_pair_id):
        filter_ = {
            'chainId': chain_id,
            'pairId': {
                '$gte': start_pair_id,
                '$lt': end_pair_id
            }
        }
        cursor = self.lp_tokens_col.find(filter_)
        return cursor

    def get_pair_by_balance_range(self, chain_id, upper=None, lower=0):
        _filter = {'chainId': chain_id}
        _balance_filter = {}
        if upper:
            _balance_filter['$lt'] = upper / 2
        if lower:
            _balance_filter['$gte'] = lower / 2
        if _balance_filter:
            _filter.update({"pairBalancesInUSD.token0": _balance_filter})
        return self.lp_tokens_col.find(filter=_filter)

    def get_pair_created_event(self, chain_id, address):
        _chains_mapping = {
            '0x38': 'bsc',
            '0x1': 'ethereum',
            '0xfa': 'fantom'
        }
        pair_created_col = self._db[f'pair_created_events_{_chains_mapping[chain_id]}']
        filter_ = {'pair': address}
        cursor = pair_created_col.find_one(filter_)
        return cursor
