from typing import List
from models.wallet.wallet import Wallet
from pymongo import MongoClient, UpdateOne

from config import MongoDBEntityConfig
from utils.logger_utils import get_logger

logger = get_logger('MongoDB Entity')


class MongoDBEntity:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBEntityConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBEntityConfig.DATABASE]
        self.config_col = self._db['configs']
        self.multichain_wallets_col = self._db['multichain_wallets']

        # self._create_index()

    # def _create_index(self):
    #     if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
    #         self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    def get_current_multichain_wallets_flagged_state(self):
        _filter = {'_id': 'multichain_wallets_flagged_state'}
        state = self.config_col.find(_filter)
        return state[0]['batch_idx']

    def get_multichain_wallets_lendings(self, flagged):
        _filter = {'flagged': flagged}
        _projection = {'address': 1, 'lendings': 1}
        data = self.multichain_wallets_col.find(_filter, _projection)
        return data
