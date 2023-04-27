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

        self._db = self.connection['TmpWalletDatabase']
        self.wallet_collection = self._db['wallets']

        self._create_index()

    def _create_index(self):
        if 'wallets_number_of_txs_index_1' not in self.wallet_collection.index_information():
            self.wallet_collection.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    def get_wallets(self, limit=50000):
        cursor = self.wallet_collection.find({}).sort('number_of_txs', pymongo.DESCENDING).limit(limit)
        return list(cursor)

    def update_wallets(self, wallets):
        try:
            data = []
            for doc in wallets:
                data.append(UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True))
            self.wallet_collection.bulk_write(data)
        except Exception as ex:
            logger.exception(ex)

    def remove_wallets(self, keys):
        try:
            filter_ = {'_id': {'$in': keys}}
            self.wallet_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def get_all_wallets(self, skip=0, limit=None):
        cursor = self.wallet_collection.find().sort('number_of_txs', pymongo.DESCENDING).skip(skip).limit(limit)
        return cursor

    def get_wallets_by_flag(self, flag_idx):
        cursor = self.wallet_collection.find({'flagged': flag_idx})
        return cursor
