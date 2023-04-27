from databases.blockchain_etl import BlockchainETL
from databases.arangodb_klg import ArangoDB


class LendingWalletsExporter:
    def __init__(self, chain_id):
        self.chain_id = chain_id
        self._klg =  ArangoDB()
        self._blockchain_etl = BlockchainETL()
        self._lending_wallet_addresses = list()

    def run(self):
        self._get_lending_wallet_addresses()

    def _get_lending_wallet_addresses(self):
        self._lending_wallet_addresses = self._klg.get_lending_wallet_addresses(self.chain_id)
