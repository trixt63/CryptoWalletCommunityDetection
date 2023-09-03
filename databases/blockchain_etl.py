from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from config import BlockchainETLConfig
from constants.blockchain_etl_constants import BlockchainETLCollections, BlockchainETLIndexes
from constants.time_constants import TimeConstants
from utils.logger_utils import get_logger
from utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('Blockchain ETL')


class BlockchainETL:
    def __init__(self, connection_url=None, db_prefix=""):
        self._conn = None
        if not connection_url:
            connection_url = BlockchainETLConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        if db_prefix:
            db_name = db_prefix + "_" + BlockchainETLConfig.DATABASE
        else:
            db_name = BlockchainETLConfig.DATABASE

        self.mongo_db = self.connection[db_name]

        self.block_collection = self.mongo_db[BlockchainETLCollections.blocks]
        self.transaction_collection = self.mongo_db[BlockchainETLCollections.transactions]
        self.collector_collection = self.mongo_db[BlockchainETLCollections.collectors]

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_relate_to_address(self, address, from_block, to_block):
        filter_ = {
            "block_number": {"$gte": from_block, "$lt": to_block},
            "$or": [
                {"from_address": address},
                {"to_address": address}
            ]
        }
        cursor = self.transaction_collection.find(filter_).batch_size(1000)
        return cursor

    def get_transactions_relate_to_list_addresses(self, addresses, from_block, to_block):
        filter_ = {
            "block_number": {"$gte": from_block, "$lt": to_block},
            "$or": [
                {"from_address": {"$in": addresses}},
                {"to_address": {"$in": addresses}}
            ]
        }
        cursor = self.transaction_collection.find(filter_).batch_size(1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_to_addresses(self, to_addresses, from_block, to_block):
        """For getting transactions to a list addresses (such as hot wallets)"""
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lte": to_block}},
                {"to_address": {"$in": [address.lower() for address in to_addresses]}}
            ]
        }
        projection = ['from_address']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_native_transfer_txs(self, from_block, to_block):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lte": to_block}},
                {"input": "0x"},
                {"value": {"$ne": "0"}},
                {"receipt_status": 1}
            ]
        }
        projection = ['from_address', 'to_address', 'value', 'block_timestamp', 'hash']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_blocks_in_range(self, start_block, end_block):
        filter_ = {
            'number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        cursor = self.block_collection.find(filter_).batch_size(10000)
        return cursor

    def get_transactions_in_range(self, start_block, end_block, projection=None):
        filter_ = {
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        cursor = self.transaction_collection.find(filter_, projection).batch_size(10000)
        return cursor

    def get_number_calls_to_address(self, to_address, from_timestamp, to_timestamp):
        filter_ = {
            'to_address': to_address,
            'block_timestamp': {
                '$gte': from_timestamp,
                '$lte': to_timestamp
            }
        }
        count_ = self.transaction_collection.count_documents(filter_)
        return count_

    def get_transaction_by_hash(self, transaction_hash):
        filter_ = {'_id': f"transaction_{transaction_hash}"}
        cursor = self.transaction_collection.find_one(filter_)
        return cursor
