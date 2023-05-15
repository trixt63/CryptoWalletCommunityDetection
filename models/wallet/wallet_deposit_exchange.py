from typing import Dict

from constants.tag_constants import WalletTags
from models.wallet.wallet import Wallet
from models.project import Project


class WalletDepositExchange(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.deposited_exchanges: Dict[str, Project] = dict()
        self.add_tags(WalletTags.centralized_exchange_deposit_wallet)

    def add_project(self, project: Project):
        if project.project_id in self.deposited_exchanges:
            self.deposited_exchanges[project.project_id].add_deployments(project.deployments)
        else:
            self.deposited_exchanges[project.project_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.deposited_exchanges):
            deposited_exchanges = {pool_id: pool_obj.to_list(chain_only=True)
                                   for pool_id, pool_obj in self.deposited_exchanges.items()}
            returned_dict['depositedExchanges'] = deposited_exchanges

        return returned_dict
