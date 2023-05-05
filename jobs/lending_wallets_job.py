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
        wallet_addr_and_lendings = self._klg.get_wallet_addresses_and_lendings(self.chain_id, timestamp=1682467200)
        for _data in wallet_addr_and_lendings:
            wallet_address = _data['address']

            lending_names = [lending['name'] for lending in _data['lendings'].values()]
            lending_names = list(set(lending_names))

            new_lending_wallet = Wallet(chain_id=self.chain_id, address=wallet_address)
            new_lending_wallet.add_tags(WalletTags.lending_wallet, lending_names)
            new_lending_wallet.lendings = lending_names

            self._lending_wallets.append(new_lending_wallet)

        logger.info(f"Got {len(self._lending_wallets)} lending wallet addresses")

    def _export_lending_wallets(self):
        logger.info('Export lending wallets to Mongo')
        self._mongodb.update_wallets(self._lending_wallets)
