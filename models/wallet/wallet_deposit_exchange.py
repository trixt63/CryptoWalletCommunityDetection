from typing import Dict

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet
from models.protocol import Protocol


class WalletDepositExchange(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.exchanges_deposited: Dict[str, Protocol] = dict()
        self.add_tags(WalletTags.centralized_exchange_deposit_wallet)

    def add_project(self, project: Protocol):
        if project.protocol_id in self.exchanges_deposited:
            self.exchanges_deposited[project.protocol_id].add_deployments(project.deployments)
        else:
            self.exchanges_deposited[project.protocol_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.exchanges_deposited):
            deposited_exchanges = {cex_id: chains.to_deployed_chains_list()
                                   for cex_id, chains in self.exchanges_deposited.items()}
            returned_dict['depositedExchanges'] = deposited_exchanges

        return returned_dict
