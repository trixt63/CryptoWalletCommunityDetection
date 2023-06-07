import gc
import time
from typing import Dict

from multithread_processing.base_job import BaseJob

from databases.blockchain_etl import BlockchainETL
from databases.postgresql import PostgresDB
from databases.mongodb import MongoDB
from models.blocks import Blocks
from models.wallet.wallet_deposit_exchange import WalletDepositExchange
from models.protocol import Protocol
from utils.logger_utils import get_logger

logger = get_logger('Exchange Deposit Wallet Job')


class ExchangeDepositWalletsJob(BaseJob):
    """Multithread job to get export wallets that deposit into hot wallets during a time interval (usually 1 day)
    """
    def __init__(
            self,
            transfer_event_db: PostgresDB,
            blockchain_etl: BlockchainETL,
            exporter: MongoDB,
            exchange_wallets: dict,
            chain_id,
            start_timestamp, end_timestamp, period,
            batch_size, max_workers,
            sources=None
    ):
        """
        Args:
            self: Represent the instance of the class
            transfer_event_db: database with transfer events
            blockchain_etl: database with transactions data
            exchange_wallets: dict: Define the wallets that belong to an exchange
            chain_id: Specify the chain that we want to extract data from
            start_timestamp: Set the start_timestamp of the data to be extracted
            end_timestamp: Set the end time of the data to be extracted
            period: Determine the time interval for each worker
            batch_size: Set the number of work for each worker to process parallely
            max_workers: Limit the number of workers that can be used to process the work_iterable
            sources: Specify the source of the data
    """
        self._db = transfer_event_db
        self._blockchain_etl = blockchain_etl
        # self._kgl = klg

        self.exporter = exporter

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
        self._wallets_by_address: Dict[str, WalletDepositExchange] = dict()  # {address: Wallet object}

    def _end(self):
        self.batch_executor.shutdown()
        self._export_wallets()

        del self._wallets_by_address
        gc.collect()

    def _execute_batch(self, works):
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
                    wallet_addresses,
                    from_timestamp,
                    to_timestamp
                )
            else:
                logger.warning(f"Invalid source: {source}. Supported sources are: {['mongo', 'postgres']}")

            for item in items:
                from_address = item['from_address']
                if from_address in self._wallets_by_address:
                    # self._wallets_by_address[from_address].deposited_exchanges.add(exchange_id)
                    self._wallets_by_address[from_address].add_protocol(protocol_id=exchange_id,
                                                                        address=from_address,
                                                                        chain_id=self.chain_id)
                else:
                    new_deposit_wallet = WalletDepositExchange(address=from_address)
                    # new_deposit_wallet.add_tags(WalletTags.centralized_exchange_deposit_wallet)
                    self._wallets_by_address[from_address] = new_deposit_wallet
                    self._wallets_by_address[from_address].add_protocol(protocol_id=exchange_id,
                                                                        address=from_address,
                                                                        chain_id=self.chain_id)

    def _export_wallets(self):
        """Export exchange deposit wallets with tag"""
        wallets = list(self._wallets_by_address.values())
        wallets_data = [wallet.to_dict() for wallet in wallets]
        for datum in wallets_data:
            datum['lastUpdatedAt'] = int(time.time())
        self.exporter.update_wallets(wallets_data)
