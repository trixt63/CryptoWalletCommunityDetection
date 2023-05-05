import json

from databases.arangodb_klg import ArangoDB
from databases.mongodb import MongoDB
from constants.tag_constants import WalletTags
from crawlers.dextools_crawler import DEXToolsCrawler
from models.wallet import Wallet
from utils.logger_utils import get_logger


logger = get_logger('Lending Wallets Exporter')


class DexWalletsJob:
    def __init__(self, chain_id):
        self.chain_id = chain_id
        self._klg = ArangoDB()
        self._mongodb = MongoDB()
        self._dex_contract_addresses = list()
        self._wallets = list()
        self._crawler = DEXToolsCrawler()

    def run(self):
        self._execute()

    def _execute(self):
        for _count, contract_addr in enumerate(self._dex_contract_addresses):
            transactions = self._crawler.get_exchanges(chain_id=self.chain_id,
                                                       contract_address=contract_addr)
            self._export(transactions, contract_address=contract_addr)
            logger.info(f"Crawled transactions of {_count+1}/{len(self._dex_contract_addresses)} "
                        f"swap contract")
        pass

    def _export(self, data, contract_address):
        logger.info('Export DEX wallets to Mongo')
        with open(f'.data/{self.chain_id}_{contract_address}_transaction.json', 'w') as f:
            json.dump(data, f, indent=2)
        # self._mongodb.update_wallets(self._wallets)
