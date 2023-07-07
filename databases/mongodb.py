from typing import List

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
        self._deposit_wallets_col_old = self._db['depositWallets_old']

        self._deposit_connections_col = self._db['deposit_connections']
        self._users_social_col = self._db['users']
        # self._user_social_deposit_col = self._db['userSocial_deposit']
        self._cex_wallets_col = self._db['cexUsers']

        self._lp_deployers_col = self._db['lpDeployers']
        self._lp_traders_col = self._db['lpTraders']

        self._lending_wallets_col = self._db['lendingWallets']

        self._create_index()

    def _create_index(self):
        if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
            self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

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
    # end analysis #############

    # for LP pair
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
    #     Artifacts       #
    #######################

    def migrate_deposit_wallets(self, start=0, pagination=1000):
        """Migrate depositWallets (from multichain wallets to single-chain"""
        _number_of_deposit_wallets = self._deposit_wallets_col_old.estimated_document_count()

        for i in range(start, _number_of_deposit_wallets, pagination):
            bulk_operation = list()
            deposit_wallets = self._deposit_wallets_col_old.find({}).skip(i).limit(pagination)

            for _deposit_wallet in deposit_wallets:
                address = _deposit_wallet['address']
                tags = _deposit_wallet['tags']
                exchanges_data = dict(_deposit_wallet['depositedExchanges'])

                for _exchange_name, _chains in exchanges_data.items():
                    for chain_id in _chains:
                        data_upsert = {
                            '_id': f"{chain_id}_{address}",
                            'address': address,
                            'lastUpdatedAt': _deposit_wallet['lastUpdatedAt']
                        }
                        data_add_to_set = {
                            "depositedExchanges": _exchange_name,
                            "tags": {"$each": tags},
                        }

                        _update_filter = {'_id': data_upsert['_id']}
                        _update_data = {'$set': data_upsert,
                                        '$addToSet': data_add_to_set}
                        bulk_operation.append(UpdateOne(filter=_update_filter, update=_update_data, upsert=True))

            self._deposit_wallets_col.bulk_write(bulk_operation)
            logger.info(f"Update {i + pagination} / {_number_of_deposit_wallets} deposit wallets")

    def update_cex_users(self):
        _pagination = 1000
        # number_of_deposit_wallets = self._deposit_wallets_col.estimated_document_count()
        number_of_deposit_wallets = 10000
        for i in range(0, number_of_deposit_wallets, _pagination):
            bulk_operation = list()
            _cursor = self._deposit_connections_col.find(filter={}).skip(i).limit(_pagination)
            for deposit_wallet in _cursor:
                cex_user = {
                    '_id': deposit_wallet['_id'],
                    'exchange': deposit_wallet['exchange chain'],
                    'depositAddress': deposit_wallet['to_address'],
                    'userAddresses': deposit_wallet['from_address']
                }
                for user_wallet in deposit_wallet['from_address']:
                    user_social = self._users_social_col.find_one(filter={'_id': user_wallet},
                                                                  projection={'twitter': 1, 'discord': 1})
                    if user_social:
                        cex_user['socialAccounts'] = {
                            'twitter': user_social['twitter'],
                            'discord': user_social['discord']
                        }

                _update_filter = {'_id': cex_user['_id']}
                _update_command = {'$set': cex_user}
                bulk_operation.append(UpdateOne(filter=_update_filter, update=_update_command, upsert=True))

            self._cex_wallets_col.bulk_write(bulk_operation)
            logger.info(f"Iterated {i + _pagination} / {number_of_deposit_wallets}")
