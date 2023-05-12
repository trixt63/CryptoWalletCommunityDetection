from typing import List
from pymongo import MongoClient, UpdateOne

from config import MongoDBConfig
from utils.logger_utils import get_logger
from utils.format_utils import snake_to_lower_camel

logger = get_logger('MongoDB')


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]
        self.wallets_col = self._db['lendingWallets']
        self.lp_tokens_col = self._db['elite_lp_tokens']

        self._create_index()

    def _create_index(self):
        if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
            self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    def update_wallets(self, wallets: List[dict]):

        try:
            wallets_update = []
            for wallet in wallets:
                wallet_mongo = {
                    snake_to_lower_camel(_key): _val
                    for _key, _val in wallet.items()
                }
                wallet_mongo['_id'] = wallet['address']
                tags = wallet_mongo.pop('tags')
                wallets_update.append(UpdateOne({'_id': wallet_mongo['_id']},
                                                {'$set': wallet_mongo, '$addToSet': {"tags": {'$each': tags}}},
                                                upsert=True))
            self.wallets_col.bulk_write(wallets_update)
        except Exception as ex:
            logger.exception(ex)

    def get_lp_contract_addresses(self, chain_id):
        _filter = {'chainId': chain_id}
        tokens_cursors = self.lp_tokens_col.find(_filter).batch_size(1000)
        return tokens_cursors
