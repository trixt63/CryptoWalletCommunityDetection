import time

# from databases.arangodb_klg import ArangoDB
from databases.mongodb_entity import MongoDBEntity
from databases.mongodb import MongoDB
from constants.tag_constants import WalletTags
# from models.wallet.wallet import Wallet
from models.wallet.wallet_lending import WalletLending
from models.project import Project
from utils.logger_utils import get_logger

logger = get_logger('Lending Wallets Exporter')


class LendingWalletsJob:
    def __init__(self, n_days=30):
        self.n_days = n_days
        self._klg = MongoDBEntity()
        self._mongodb = MongoDB()

        # self._first_timestamp = None
        self.current_batch_id = None

    def run(self):
        # current_time = int(time.time())
        # self._first_timestamp = round_timestamp(current_time - self.n_days*TimeConstants.A_DAY, TimeConstants.A_DAY)
        self._export_lending_wallets()

    def _export_lending_wallets(self):
        logger.info('Getting lending wallet addresses from KLG')
        self.current_batch_id = self._klg.get_current_multichain_wallets_flagged_state()
        for flagged in range(1, self.current_batch_id+1):
            self._export_flagged_wallets(flagged)

    def _export_flagged_wallets(self, flagged: int):
        batch_lending_wallets = []
        _cursor = self._klg.get_multichain_wallets_lendings(flagged)
        wallets_data = list(_cursor)

        for wallet_addr_and_lendings in wallets_data:
            wallet_addr = wallet_addr_and_lendings['address']
            lending_pools_data = self._extract_lending_pools(wallet_addr_and_lendings.get('lendings'))

            if lending_pools_data:
                new_lending_wallet = WalletLending(address=wallet_addr)
                new_lending_wallet.add_tags(WalletTags.lending_wallet)

                for _pool_data in lending_pools_data:
                    chain_id = _pool_data['chain_id']
                    pool_address = _pool_data['address']
                    pool_id = _pool_data['name']

                    lending_pool = Project(project_id=pool_id,
                                           chain_id=chain_id,
                                           address=pool_address)
                    new_lending_wallet.add_project(lending_pool)

                batch_lending_wallets.append(new_lending_wallet)

        logger.info(f"Flag {flagged}/{self.current_batch_id+1}: number of lending wallets: {len(batch_lending_wallets)}")
        self._export_wallets(batch_lending_wallets)

    def _extract_lending_pools(self, wallet_lendings_data: dict):
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

    def _export_wallets(self, wallets: list):
        wallets_data = []
        for wallet in wallets:
            wallet_dict = wallet.to_dict()
            wallets_data.append(wallet_dict)
        self._mongodb.update_wallets(wallets_data)
