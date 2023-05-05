import pymongo
from pymongo import MongoClient, UpdateOne

from config import MongoDBConfig
from utils.logger_utils import get_logger

logger = get_logger('MongoDB')


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]
        self.wallets_col = self._db['wallets']

        self._create_index()

    def _create_index(self):
        if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
            self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    def update_wallets(self, wallets: list):
        try:
            wallets_data = []
            for wallet in wallets:
                wallet_dict = wallet.to_dict()
                wallet_dict['_id'] = f"{wallet_dict['chainId']}_{wallet_dict['address']}"
                tags = wallet_dict.pop('tags')
                wallets_data.append(UpdateOne({'_id': wallet_dict['_id']},
                                              {'$set': wallet_dict, '$addToSet': {"tags": {'$each': tags}}},
                                              upsert=True))
            self.wallets_col.bulk_write(wallets_data)
        except Exception as ex:
            logger.exception(ex)
