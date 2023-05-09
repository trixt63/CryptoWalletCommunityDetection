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

    def _create_index(self):
        # If blockchain_etl 30 days, create index
        # Collection: blocks
        # - number: -1
        #
        # Collection: transactions
        # - block_number: -1
        # - block_timestamp: 1
        # - from_address: 1, block_number: -1
        # - to_address: 1, block_number: -1
        # - from_address: 1, to_address: 1

        if BlockchainETLIndexes.ttl_blocks not in self.block_collection:
            self.block_collection.create_index([('item_timestamp', 1)], expireAfterSeconds=TimeConstants.DAYS_30,
                                               name=BlockchainETLIndexes.ttl_blocks)
        if BlockchainETLIndexes.ttl_transactions not in self.transaction_collection:
            self.transaction_collection.create_index([('item_timestamp', 1)], expireAfterSeconds=TimeConstants.DAYS_30,
                                                     name=BlockchainETLIndexes.ttl_transactions)

    def get_last_block_number(self, collector_id="streaming_collector"):
        """Get the last block number collected by collector"""
        last_block_number = self.collector_collection.find_one({"_id": collector_id})
        return last_block_number["last_updated_at_block_number"]

    def get_transactions_by_smart_contracts(self, from_block, to_block, contract_addresses: list):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lte": to_block}},
                {"to_address": {"$in": [address.lower() for address in contract_addresses]}},
                {"receipt_status": 1}
            ]
        }
        projection = ['from_address', 'to_address', 'input', 'block_timestamp', 'hash']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    # @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_to_addresses(self, to_addresses, from_block, to_block):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lte": to_block}},
                {"to_address": {"$in": [address.lower() for address in to_addresses]}}
            ]
        }
        projection = ['from_address']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_txs_in_range_timestamp(self, from_timestamp, to_timestamp):
        filter_ = {
            "$and": [
                {"block_timestamp": {"$gte": from_timestamp, "$lte": to_timestamp}},
                {"receipt_status": 1}
            ]
        }
        projection = ['from_address', 'to_address', 'gas_price', 'receipt_gas_used', 'value']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_sort_txs_in_range(self, start_timestamp, end_timestamp):
        filter_ = {
            'block_timestamp': {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }
        }
        projection = ["from_address", "to_address", "input"]
        cursor = self.transaction_collection.find(filter_, projection).batch_size(10000)
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

    def insert_transactions(self, transactions):
        try:
            self.transaction_collection.insert_many(transactions)
        except BulkWriteError:
            data = []
            for tx in transactions:
                data.append(UpdateOne({'_id': tx['_id']}, {'$set': tx}, upsert=True))
            self.transaction_collection.bulk_write(data)

    def insert_blocks(self, blocks):
        try:
            self.block_collection.insert_many(blocks)
        except BulkWriteError:
            data = []
            for block in blocks:
                data.append(UpdateOne({'_id': block['_id']}, {'$set': block}, upsert=True))
            self.block_collection.bulk_write(data)

    def delete_blocks(self, out_date_block):
        try:
            filter_ = {'number': {"$lte": out_date_block}}
            self.block_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def delete_transactions(self, out_date_block):
        try:
            filter_ = {'block_number': {"$lte": out_date_block}}
            self.transaction_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def get_collector(self, collector_id):
        try:
            collector = self.collector_collection.find_one({'_id': collector_id})
            return collector
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_collector(self, collector):
        try:
            self.collector_collection.update_one({'_id': collector['_id']}, {'$set': collector}, upsert=True)
        except Exception as ex:
            logger.exception(ex)

    def get_the_first_tx(self, address):
        filter_ = {
            "$or": [
                {"from_address": address},
                {"to_address": address}
            ]
        }
        projection = ['block_timestamp']
        cursor = self.transaction_collection.find(filter_, projection=projection).sort('block_number').limit(1)
        return list(cursor)
