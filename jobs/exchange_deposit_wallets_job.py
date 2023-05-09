import gc
import time

from multithread_processing.base_job import BaseJob

from constants.tag_constants import WalletTags
from databases.blockchain_etl import BlockchainETL
from databases.arangodb_klg import ArangoDB
from databases.postgresql import PostgresDB
from databases.mongodb import MongoDB
# from exporters.arangodb_exporter import ArangoDBExporter
from models.blocks import Blocks
from models.wallet import Wallet
from utils.logger_utils import get_logger

logger = get_logger('Exchange Deposit Wallet Job')


class ExchangeDepositWalletsJob(BaseJob):
    def __init__(
            self,
            _db: PostgresDB,
            _blockchain_etl: BlockchainETL,
            klg: ArangoDB,
            exchange_wallets: dict,
            chain_id,
            start_timestamp, end_timestamp, period,
            batch_size, max_workers,
            sources=None
    ):
        self._db = _db
        self._blockchain_etl = _blockchain_etl
        self._kgl = klg

        self._exporter = MongoDB()

        self.exchange_wallets = exchange_wallets
        self.wallets_groupby_exchanges = dict()
        for wallet_addr, exchange_id in exchange_wallets.items():
            self.wallets_groupby_exchanges.setdefault(exchange_id, []).append(wallet_addr)

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
        self._wallets_by_address = dict()  # {address: Wallet object}

    def _end(self):
        self.batch_executor.shutdown()
        self._export()

        del self._wallets_by_address
        gc.collect()

    def _execute_batch(self, works):
        start_time = int(time.time())
        start_timestamp = works[0]
        end_timestamp = min(start_timestamp + self.period, self.end_timestamp)
        block_range = Blocks().block_numbers(self.chain_id, [start_timestamp, end_timestamp])

        for source in self.sources:
            self._get_wallets_by_address_from_source(source, block_range[start_timestamp], block_range[end_timestamp])

    def _get_wallets_by_address_from_source(self, source, from_timestamp, to_timestamp):
        for exchange_id, wallet_addresses in self.wallets_groupby_exchanges.items():
            items = list()
            if source == 'postgres':
                items = self._db.get_event_transfer_by_to_addresses(
                    wallet_addresses,
                    from_timestamp,
                    to_timestamp
                )
            elif source == 'mongo':
                items = self._blockchain_etl.get_transactions_to_addresses(
                    self.exchange_wallets,
                    from_timestamp,
                    to_timestamp
                )
            else:
                logger.warning(f"Invalid source: {source}. Supported sources are: {['mongo', 'postgres']}")

            for item in items:
                from_address = item['from_address']
                if from_address in self._wallets_by_address.keys():
                    self._wallets_by_address[from_address].exchange_deposits.add(exchange_id)
                else:
                    new_deposit_wallet = Wallet(address=from_address)
                    new_deposit_wallet.add_tags(WalletTags.centralized_exchange_deposit_wallet)
                    self._wallets_by_address[from_address] = new_deposit_wallet
                    self._wallets_by_address[from_address].exchange_deposits.add(exchange_id)

    def _export(self):
        """Export exchange deposit wallets with tag"""
        wallets = list(self._wallets_by_address.values())
        self._exporter.update_wallets(wallets)
