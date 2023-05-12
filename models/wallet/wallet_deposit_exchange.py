from typing import Dict


from models.wallet.wallet import Wallet
from models.project import Project


class WalletDepositExchange(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.deposited_exchanges: Dict[str, Project] = dict()

    def add_project(self, project: Project):
        if project.project_id in self.deposited_exchanges:
            self.deposited_exchanges[project.project_id].add_deployments(project.deployments)
        else:
            self.deposited_exchanges[project.project_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.deposited_exchanges):
            returned_dict['deposited_exchanges'] = self.deposited_exchanges
