from typing import Dict


from models.wallet.wallet import Wallet
from models.project import Project


class WalletLending(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.lending_pools: Dict[str, Project] = dict()

    def add_project(self, project: Project):
        if project.project_id in self.lending_pools:
            self.lending_pools[project.project_id].add_deployments(project.get_deployment())
        else:
            self.lending_pools[project.project_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.lending_pools):
            returned_dict['lending_pools'] = self.lending_pools
