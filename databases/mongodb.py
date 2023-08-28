from typing import List, Dict

import pymongo
from pymongo import MongoClient, UpdateOne

from config import MongoDBConfig
# from constants.mongodb_constants import WALLETS_COL, CreatedPairEventsCollection
from utils.logger_utils import get_logger

logger = get_logger('MongoDB')
WALLETS_COL = 'test_lpTraders'


class MongoDB:
    def __init__(self, connection_url=None, wallet_col=WALLETS_COL):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]
        self.lp_tokens_col = self._db['lpTokens']
        self.wallets_col = self._db[wallet_col]

        self._deposit_wallets_col = self._db['depositWallets']

        self._deposit_connections_col = self._db['deposit_connections']
        self._users_social_col = self._db['users']
        self._transfer_events_col = self._db['transferEvents']

        self._lp_deployers_col = self._db['lpDeployers']
        self._lp_traders_col = self._db['lpTraders']

        self._lending_wallets_col = self._db['lendingWallets']

        self._groups_col = self._db['groups']

        self._create_index()

    def _create_index(self):
        if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
            self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    #######################
    #   Aggregate groups  #
    #######################

    def _get_min(self, col_name, field_name, filter_={}):
        cursor = self._db[col_name].find(filter_).sort(field_name, 1).limit(1)
        return cursor[0][field_name]

    def _get_max(self, col_name, field_name, filter_={}):
        cursor = self._db[col_name].find(filter_).sort(field_name, -1).limit(1)
        return cursor[0][field_name]

    def get_transfers_by_blocks_range(self, start_block: str, end_block: str):
        cursor = self._transfer_events_col.find({'block_number': {'$gte': start_block,
                                                                  '$lte': end_block}})
        return cursor

    #######################
    #        Update       #
    #######################

    def upsert_lp_tokens(self, data: List[dict]):
        exported_data = [{
            '_id': f"{datum['chain_id']}_{datum['address']}",
            'address': datum['address'],
            'chainId': datum['chain_id'],
            'dex': datum['dex'],
            'pairId': datum['pair_id'],
            'factory': datum['factory'],
            'token0': datum['token0'],
            'token1': datum['token1'],
            'pairBalancesInUSD': datum['pair_balances_in_usd'],
        } for datum in data]

        bulk_operation = [UpdateOne({'_id': datum['_id']},
                                    {"$set": datum},
                                    upsert=True)
                          for datum in exported_data
        ]

        try:
            self.lp_tokens_col.bulk_write(bulk_operation)
        except Exception as ex:
            logger.exception(ex)

    def update_wallets(self, wallets: List[dict]):
        try:
            wallet_updates_bulk = []
            for wallet in wallets:
                wallet['_id'] = wallet['address']

                # pop all information besides data about lendings/dex/deposit/...
                wallet_base_data = {
                    '_id': wallet.pop('_id'),
                    'address': wallet.pop('address'),
                    'lastUpdatedAt': wallet.pop('lastUpdatedAt')
                }
                tags = wallet.pop('tags')

                # process query to update nested documents for data about lendings/dex/deposit/...
                field_name = list(wallet.keys())[0]  # 'lendingPools' or 'tradedLPs' or 'exchangesDeposited'...
                project_names = list(wallet[field_name].keys())  # project id (pancakeswap or something)
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

    def update_transactions(self, chain_id, data: List[Dict]):
        bulk_updates = [
            UpdateOne({'_id': datum['_id']}, {'$set': datum}, upsert=True)
            for datum in data
        ]
        try:
            self._db[f"{chain_id}_transactions"].bulk_write(bulk_updates)
        except Exception as ex:
            logger.exception(ex)

    # the next 3 funcs are fore DEX-related threads
    def get_latest_pair_id(self, chain_id: str):
        filter_ = {'chainId': chain_id}
        try:
            latest_pair = self.lp_tokens_col.find_one(filter_, sort=[("pairId", pymongo.DESCENDING)])
            return latest_pair.get('pairId')
        except AttributeError as attr_e:
            logger.warning(f"Cannot get latest pairId from {chain_id}")
        return None

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

    #######################
    #      Analysis       #
    #######################
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

    def _get_duplicated_wallets(self, input_wallets: list, collection_name: str):
        col = self._db[collection_name]
        _filter = {
            '_id': {'$in': input_wallets}
        }
        _project = {
            'address': 1
        }
        duplicated_wallets = col.find(_filter, _project)
        return duplicated_wallets

    def _delete_wallets(self, collection_name: str,ids: list):
        _filter = {'_id': {'$in': ids}}
        col = self._db[collection_name]
        col.delete_many(_filter)

    #######################
    #      Odd jobs       #
    #######################
    def get_number_of_docs(self, collection_name):
        col = self._db[collection_name]
        return col.estimated_document_count()

    def get_groups_by_num_wallets(self, chain_id, num_user_cond: dict or int, num_depo_cond: dict or int):
        pipeline = [
            {
                '$match': {
                    'num_user': num_user_cond,
                    'num_depo': num_depo_cond,
                    'Chain': chain_id
                }
            },
            {
                '$sort': {
                    'num_user': -1
                }
            }
        ]
        result = self._groups_col.aggregate(pipeline)
        return result

    def fix_transfer_events(self, start_block: str, end_block: str):
        old_transfer_events_col = self._db['transferEvents']
        new_transfer_events_col = self._db['transferEvents_new']
        new_transfer_events: List[Dict] = list()
        bulk_operation = list()

        cursor = old_transfer_events_col.find({'block_number': {'$gte': start_block,
                                                                '$lte': end_block}})
        for transfer_event in cursor:
            chain_id = transfer_event['chainID']
            from_address = transfer_event['from_address']
            to_address = transfer_event['to_address']
            id = f"{chain_id}_{from_address}_{to_address}",
            new_transfer_event = {
                # '_id': f"{chain_id}_{from_address}_{to_address}",
                'chainId': chain_id,
                'contractAddress': transfer_event['contract_address'],
                'blockNumber': int(transfer_event['block_number']),
                'fromAddress': transfer_event['from_address'],
                'toAddress': transfer_event['to_address'],
                'value': float(transfer_event['value']),
            }
            bulk_operation.append(UpdateOne(filter={'_id': id},
                                            update={'$set': new_transfer_event},
                                            upsert=True))

        new_transfer_events_col.bulk_write(bulk_operation)


