from typing import List
from cli_scheduler.scheduler_job import SchedulerJob

from constants.lending.lending_address_to_pool_mapper import LendingAddressToPoolMapper
from databases.mongodb_entity import MongoDBEntity
from databases.mongodb import MongoDB
from models.wallet.wallet_lending import WalletLending
from utils.logger_utils import get_logger

logger = get_logger('Lending Wallets Exporter')


class LendingWalletsJob(SchedulerJob):
    def __init__(self, scheduler, n_days=30):
        super().__init__(scheduler)
        self.n_days = n_days

        # self._first_timestamp = None
        self.current_batch_id = None

    def _pre_start(self):
        self._klg = MongoDBEntity()
        self._mongodb = MongoDB()

    def _start(self):
        self.current_batch_id = None
        self.lending_address_to_pool_mapping = LendingAddressToPoolMapper().get_mapping()

    def _execute(self):
        logger.info('Getting lending wallet addresses from KLG')
        self.current_batch_id = self._klg.get_current_multichain_wallets_flagged_state()

        for flagged in range(1, self.current_batch_id+1):
            self._export_flagged_wallets(flagged)

    def _end(self):
        del self.lending_address_to_pool_mapping

    def _export_flagged_wallets(self, flagged: int):
        logger.info(f"Exporting wallet flag {flagged} / {self.current_batch_id}")
        batch_lending_wallets: List[WalletLending] = []
        wallets_data = self._klg.get_multichain_wallets_lendings(flagged)

        for wallet_addr_and_lendings in wallets_data:
            wallet_lending_pools = self._filter_wallet_lending_pools(wallet_addr_and_lendings.get('lendings'))

            if wallet_lending_pools:
                new_lending_wallet = WalletLending(address=wallet_addr_and_lendings['address'])

                for _pool_data in wallet_lending_pools:
                    new_lending_wallet.add_protocol(protocol_id=_pool_data['pool_id'],
                                                    chain_id=_pool_data['chain_id'],
                                                    address=_pool_data['address'])

                batch_lending_wallets.append(new_lending_wallet)

        logger.info(f"Flag {flagged}/{self.current_batch_id+1}: number of lending wallets: {len(batch_lending_wallets)}")
        self._export_wallets(batch_lending_wallets)

    def _filter_wallet_lending_pools(self, wallet_lendings_data: dict):
        if wallet_lendings_data:
            extracted_lending_pools = list()
            for lending_pool_key, lending_pool_data in wallet_lendings_data.items():
                # deposit_logs = lending_pool_data['depositChangeLogs']
                # deposit_latest_timestamp = max(deposit_logs.keys())
                # borrow_logs = lending_pool_data['borrowChangeLogs']
                # borrow_latest_timestamp = max(borrow_logs.keys())
                # if (self._first_timestamp <= int(deposit_latest_timestamp) or
                #         self._first_timestamp <= int(borrow_latest_timestamp)):
                pool_id = self.lending_address_to_pool_mapping.get(lending_pool_key, None)
                if pool_id:
                    extracted_lending_pools.append({
                        'chain_id': lending_pool_key.split('_')[0],
                        'address': lending_pool_key.split('_')[1],
                        'pool_id': pool_id
                    })

            return extracted_lending_pools
        else:
            return None

    def _export_wallets(self, wallets: List[WalletLending]):
        wallets_data = []
        for wallet in wallets:
            wallet_dict = wallet.to_dict()
            wallets_data.append(wallet_dict)
        self._mongodb.update_wallets(wallets_data)
