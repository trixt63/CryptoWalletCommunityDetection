import time

from databases.arangodb_klg import ArangoDB
from databases.mongodb import MongoDB
from constants.tag_constants import WalletTags
from constants.time_constants import TimeConstants
from models.wallet import Wallet
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp


logger = get_logger('Lending Wallets Exporter')


class LendingWalletsJob:
    def __init__(self, n_days=30):
        self.n_days = n_days
        self._klg = ArangoDB()
        self._mongodb = MongoDB()

        # self._first_timestamp = None
        self.current_batch_id = None

    def run(self):
        current_time = int(time.time())
        # self._first_timestamp = round_timestamp(current_time - self.n_days*TimeConstants.A_DAY, TimeConstants.A_DAY)
        self._get_and_export_lending_wallets()

    def _get_and_export_lending_wallets(self):
        logger.info('Getting lending wallet addresses from KLG')
        self.current_batch_id = self._klg.get_multichain_wallets_current_batch_idx()
        for flagged in range(1, self.current_batch_id+1):
            self._export_flagged_wallets(flagged)

    def _export_flagged_wallets(self, flagged: int):
        batch_lending_wallets = []
        _cursor = self._klg.get_wallet_addresses_and_lendings(flagged)
        data = list(_cursor)

        for wallet_addr_and_lendings in data:
            lending_pools = self._get_lending_pools(wallet_addr_and_lendings['lendings'])
            if lending_pools:
                new_lending_wallet = Wallet(address=wallet_addr_and_lendings['address'])
                new_lending_wallet.add_tags(WalletTags.lending_wallet)
                new_lending_wallet.lendings = lending_pools
                batch_lending_wallets.append(new_lending_wallet)

        logger.info(f"Flag {flagged}/{self.current_batch_id}: number of lending wallets: {len(batch_lending_wallets)}")
        self._export_lending_wallets(batch_lending_wallets)

    def _get_lending_pools(self, wallet_lendings_data: dict):
        recent_lending_pools = list()
        for lending_pool_key, lending_pool_data in wallet_lendings_data.items():
            # deposit_logs = lending_pool_data['depositChangeLogs']
            # deposit_latest_timestamp = max(deposit_logs.keys())
            # borrow_logs = lending_pool_data['borrowChangeLogs']
            # borrow_latest_timestamp = max(borrow_logs.keys())
            # if (self._first_timestamp <= int(deposit_latest_timestamp) or
            #         self._first_timestamp <= int(borrow_latest_timestamp)):
            recent_lending_pools.append({
                'chain_id': lending_pool_key.split('_')[0],
                'address': lending_pool_key.split('_')[1],
                'name': lending_pool_data['name']
            })
        return recent_lending_pools

    def _export_lending_wallets(self, lending_wallets: list):
        self._mongodb.update_wallets(lending_wallets)
