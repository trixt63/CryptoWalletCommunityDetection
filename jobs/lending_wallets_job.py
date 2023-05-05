from databases.arangodb_klg import ArangoDB
from databases.mongodb import MongoDB
from constants.tag_constants import WalletTags
from models.wallet import Wallet
from utils.logger_utils import get_logger


logger = get_logger('Lending Wallets Exporter')


class LendingWalletsJob:
    def __init__(self, chain_id):
        self.chain_id = chain_id
        self._klg = ArangoDB()
        self._mongodb = MongoDB()
        self._lending_wallets = list()

    def run(self):
        self._get_lending_wallets()
        self._export_lending_wallets()

    def _get_lending_wallets(self):
        logger.info('Getting lending wallet addresses')
        _cursor = self._klg.get_wallet_addresses_and_lendings(self.chain_id, timestamp=1682467200)
        # for _data in _cursor:
        #     wallet_address = _data['address']
        #
        #     lendings_list = [lending_addr for lending_addr in _data['lendings'].keys()]
        #     lendings_list = list(set(lendings_list))
        #
        #     new_lending_wallet = Wallet(chain_id=self.chain_id, address=wallet_address)
        #     new_lending_wallet.add_tags(WalletTags.lending_wallet, lendings_list)
        #     new_lending_wallet.lendings = lendings_list
        #
        #     self._lending_wallets.append(new_lending_wallet)
        while True:
            data = list(_cursor.batch())
            _cursor.batch().clear()
            for wallet_addr_and_lending in data:
                wallet_address = wallet_addr_and_lending['address']
                lending_pools_list = [wallet_addr_and_lending['lendings'].keys()]
                lending_pools_list = list(set(lending_pools_list))

                new_lending_wallet = Wallet(chain_id=self.chain_id, address=wallet_address)
                new_lending_wallet.add_tags(WalletTags.lending_wallet)
                new_lending_wallet.lendings = lending_pools_list

                self._lending_wallets.append(new_lending_wallet)

            if _cursor.has_more():
                _cursor.fetch()
            else:
                break

        logger.info(f"Got {len(self._lending_wallets)} lending wallet addresses")

    def _export_lending_wallets(self):
        logger.info('Export lending wallets to Mongo')
        self._mongodb.update_wallets(self._lending_wallets)
