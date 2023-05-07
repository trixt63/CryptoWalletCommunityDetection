import gc
import time

from multithread_processing.base_job import BaseJob

from constants.tag_constants import WalletTags
from databases.blockchain_etl import BlockchainETL
# from databases.postgresql import PostgresDB
# from exporters.arangodb_exporter import ArangoDBExporter
from models.blocks import Blocks
from utils.logger_utils import get_logger

logger = get_logger('Exchange Deposit Wallet Job')


class ExchangeDepositWalletsJob(BaseJob):
    def __init__(
            self, _db: PostgresDB, _blockchain_etl: BlockchainETL, _exporter: ArangoDBExporter, exchange_wallets, chain_id,
            start_timestamp, end_timestamp, period, batch_size, max_workers, sources=None
    ):
        self._db = _db
        self._blockchain_etl = _blockchain_etl
        self._exporter = _exporter

        self.exchange_wallets = exchange_wallets
        self.chain_id = chain_id

        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.period = period

        if sources is None:
            sources = ['mongo', 'postgres']
        self.sources = sources

        work_iterable = range(self.start_timestamp, self.end_timestamp, self.period)
        super().__init__(work_iterable, batch_size, max_workers)

    def _start(self):
        self._deposit_wallets = {}

    def _end(self):
        self.batch_executor.shutdown()
        self._export()

        del self._deposit_wallets
        gc.collect()

    def _execute_batch(self, works):
        start_time = int(time.time())
        start_timestamp = works[0]
        end_timestamp = min(start_timestamp + self.period, self.end_timestamp)

        block_range = Blocks().block_numbers(self.chain_id, [start_timestamp, end_timestamp])
        result = {}

        if 'postgres' in self.sources:
            # Get event transfer to exchange wallets, group by from_address
            items = self._db.get_event_transfer_by_to_addresses(
                self.exchange_wallets,
                block_range[start_timestamp],
                block_range[end_timestamp]
            )
            for item in items:
                from_address = item['from_address']
                if from_address not in self.exchange_wallets:
                    result[from_address] = True

        if 'mongo' in self.sources:
            # Get transaction to exchange wallets
            docs = self._blockchain_etl.get_transactions_to_addresses(
                self.exchange_wallets,
                block_range[start_timestamp],
                block_range[end_timestamp]
            )
            for item in docs:
                from_address = item['from_address']
                if from_address not in self.exchange_wallets:
                    result[from_address] = True

        self._combine(result)
        logger.info(f'Combine {len(self._deposit_wallets)} wallet addresses, took {round(time.time() - start_time)}s')

    def _combine(self, result: dict):
        # Combine result of batches
        self._deposit_wallets.update(result)

    def _export(self):
        # Export exchange deposit wallets with tag
        data = []
        for address in self._deposit_wallets:
            data.append({
                'address': address,
                'chainId': self.chain_id,
                'tags': {WalletTags.centralized_exchange_deposit_wallet: True}
            })
        self._exporter.export_wallets(data)
