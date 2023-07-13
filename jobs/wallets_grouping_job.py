import gc
import time
from typing import Dict
import pandas as pd

from multithread_processing.base_job import BaseJob

from databases.blockchain_etl import BlockchainETL
from databases.postgresql import PostgresDB
from databases.mongodb import MongoDB
from models.blocks import Blocks
from models.wallet.wallet_deposit_exchange import WalletDepositExchange
from utils.logger_utils import get_logger
from services.wallet_clustering import get_group, get_user_depo, get_group_full, get_groupp

logger = get_logger('Wallets Grouping job')


def main():
    db = MongoDB()
    min_block = int(db._get_min('transferEvents', 'block_number'))
    max_block = int(db._get_max('transferEvents', 'block_number'))
    transfer_events = list(db.get_transfers_by_blocks_range(min_block, max_block))
    transfer_events_df = pd.DataFrame.from_records(transfer_events)
    print(transfer_events_df.head(10))


# class ExchangeDepositWalletsJob(BaseJob):
#     """Multithread job to get export wallets that deposit into hot wallets during a time interval (usually 1 day)
#     """
#     def __init__(
#             self,
#             transfer_event_db: PostgresDB,
#             blockchain_etl: BlockchainETL,
#             exporter: MongoDB,
#             exchange_wallets: dict,
#             chain_id,
#             start_timestamp, end_timestamp, period,
#             batch_size, max_workers,
#             sources=None
#     ):
#         """
#         Args:
#             self: Represent the instance of the class
#             transfer_event_db: database with transfer events
#             blockchain_etl: database with transactions data
#             exchange_wallets: dict: Define the wallets that belong to an exchange
#             chain_id: Specify the chain that we want to extract data from
#             start_timestamp: Set the start_timestamp of the data to be extracted
#             end_timestamp: Set the end time of the data to be extracted
#             period: Determine the time interval for each worker
#             batch_size: Set the number of work for each worker to process parallely
#             max_workers: Limit the number of workers that can be used to process the work_iterable
#             sources: Specify the source of the data
#     """
#         self._db = transfer_event_db
#         self._blockchain_etl = blockchain_etl
#         # self._kgl = klg
#
#         self.exporter = exporter
#
#
#         self.start_timestamp = start_timestamp
#         self.end_timestamp = end_timestamp
#         self.period = period
#
#         work_iterable = range(self.start_timestamp, self.end_timestamp, self.period)
#         super().__init__(work_iterable, batch_size, max_workers)
#
#     def _start(self):
#         self._wallets_by_address: Dict[str, WalletDepositExchange] = dict()  # {address: Wallet object}
#
#     def _end(self):
#         self.batch_executor.shutdown()
#         self._export_wallets()
#
#         del self._wallets_by_address
#         gc.collect()
#
#     def _execute_batch(self, works):
#         pass
#
#     def _export_wallets(self):
#         """Export exchange deposit wallets with tag"""
#         wallets = list(self._wallets_by_address.values())
#         wallets_data = [wallet.to_dict() for wallet in wallets]
#         for datum in wallets_data:
#             datum['lastUpdatedAt'] = int(time.time())
#         self.exporter.update_wallets(wallets_data)

if __name__=='__main__':
    main()
