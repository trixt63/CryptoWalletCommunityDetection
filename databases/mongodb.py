from typing import List
from pymongo import MongoClient, UpdateOne

from config import MongoDBConfig
from utils.logger_utils import get_logger
from utils.format_utils import snake_to_lower_camel

logger = get_logger('MongoDB')
WALLETS_COL = 'dexTraders'


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]
        self.lp_tokens_col = self._db['elite_lp_tokens']
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

    # def get_lp_contract_addresses(self, chain_id):
    #     _filter = {'chainId': chain_id}
    #     tokens_cursors = self.lp_tokens_col.find(_filter).batch_size(1000)
    #     return tokens_cursors

    # def test_update(self):
    #     datum = {
    #       "_id": "0x305246818d727167f028f14b6f631e92a8e15795",
    #       "address": "0x305246818d727167f028f14b6f631e92a8e15795",
    #       "tags": [
    #         "dex_trader",
    #         "bot"
    #       ],
    #       "tradedlps": {
    #         "pancakeswap": [
    #           {
    #             "chainId": "0x38",
    #             "address": "0x2222"
    #           }
    #         ],
    #         "spookyswap": [
    #             {
    #                 "chainId": "0xfa",
    #                 "address": "0x1111"
    #             },
    #             {
    #                 "chainId": "0xfa",
    #                 "address": "0x2222"
    #             }
    #         ]
    #       }
    #     }
    #
    #     tags = datum.pop('tags')
    #     base_datum = {
    #         '_id': datum.pop('_id'),
    #         'address': datum.pop('address'),
    #     }
    #
    #     field_name = list(datum.keys())[0]
    #     platform_names = list(datum[field_name].keys())
    #
    #     _filter = {'_id': base_datum['_id']}
    #
    #     __add_to_set = {f"{field_name}.{platform_name}": {"$each": datum[field_name][platform_name]}
    #                     for platform_name in platform_names}
    #     __add_to_set["tags"] = {'$each': tags}
    #     _update = {
    #         '$set': base_datum,
    #         '$addToSet': __add_to_set
    #     }
    #     self.wallets_col.update_one(filter=_filter,
    #                                 update=_update,
    #                                 upsert=True)
    #
    #     return 0
