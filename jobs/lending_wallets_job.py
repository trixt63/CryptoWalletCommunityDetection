import time
from typing import List
from cli_scheduler.scheduler_job import SchedulerJob

from constants.lending.lending_pool_id_mapper import LendingPoolIdMapper
from constants.time_constants import TimeConstants
from databases.mongodb_entity import MongoDBEntity
from databases.mongodb import MongoDB
from models.wallet.wallet_lending import WalletLending
from utils.time_utils import round_timestamp
from utils.logger_utils import get_logger

logger = get_logger('Lending Wallets Exporter')


class LendingWalletsJob(SchedulerJob):
    def __init__(self, interval,
                 importer: MongoDBEntity, exporter: MongoDB,
                 n_days=30):
        super().__init__(f'^true@{interval}/$#true')

        self.n_days = n_days
        self._first_timestamp = round_timestamp(int(time.time()) - n_days*TimeConstants.A_DAY,
                                                TimeConstants.A_DAY)

        self.current_batch_id = None

        self.importer = importer
        self.exporter = exporter

    def _start(self):
        self.current_batch_id = None
        self.lending_pool_id_mapper = LendingPoolIdMapper().map()

    def _execute(self):
        logger.info('Getting lending wallet addresses from KLG')
        self.current_batch_id = self.importer.get_current_multichain_wallets_flagged_state()

        for flagged in range(1, self.current_batch_id+1):
            self._export_flagged_wallets(flagged)

    def _export_flagged_wallets(self, flagged: int):
        logger.info(f"Exporting wallet flag {flagged} / {self.current_batch_id}")
        batch_lending_wallets: List[WalletLending] = []
        wallets_data = self.importer.get_multichain_wallets_lendings(flagged)

        for wallet_addr_and_lendings in wallets_data:
            new_lending_wallet = WalletLending(address=wallet_addr_and_lendings['address'],
                                               last_updated_at=int(time.time()))
            wallet_all_lendings_log = wallet_addr_and_lendings['lendings']
            for chain_address, wallet_lending_log in wallet_all_lendings_log.items():
                try:
                    assert self._check_pool_used_within_ndays(wallet_lending_log)
                    pool_id = self.lending_pool_id_mapper[chain_address]
                    chain_id, address= chain_address.split('_')
                    new_lending_wallet.add_protocol(protocol_id=pool_id,
                                                    chain_id=chain_id,
                                                    address=address)
                except (AssertionError, KeyError):
                    continue

            if new_lending_wallet.not_empty():
                batch_lending_wallets.append(new_lending_wallet)

        logger.info(f"Flag {flagged}/{self.current_batch_id+1}: number of lending wallets: {len(batch_lending_wallets)}")
        self._export_wallets(batch_lending_wallets)

    def _check_pool_used_within_ndays(self, pool_deposit_borrow_log):
        """Check if wallet deposit/borrow to the pool within the last n days
        """
        deposit_logs = pool_deposit_borrow_log['depositChangeLogs']
        deposit_latest_timestamp = max(deposit_logs.keys())
        borrow_logs = pool_deposit_borrow_log['borrowChangeLogs']
        borrow_latest_timestamp = max(borrow_logs.keys())
        if (self._first_timestamp <= int(deposit_latest_timestamp) or
                self._first_timestamp <= int(borrow_latest_timestamp)):
            return True
        return False

    def _export_wallets(self, wallets: List[WalletLending]):
        wallets_data = []
        for wallet in wallets:
            wallet_dict = wallet.to_dict()
            wallet_dict['lastUpdatedAt'] = int(time.time())
            wallets_data.append(wallet_dict)
        self.exporter.update_wallets(wallets_data)

    def _end(self):
        del self.lending_pool_id_mapper
