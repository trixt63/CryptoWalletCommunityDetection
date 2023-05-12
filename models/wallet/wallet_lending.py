from typing import Dict

from models.wallet.wallet import Wallet
from models.project import Project


class WalletLending(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.lending_pools: Dict[str, Project] = dict()

    def add_project(self, project: Project):
        if project.project_id in self.lending_pools:
            self.lending_pools[project.project_id].add_deployments(project.deployments)
        else:
            self.lending_pools[project.project_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.lending_pools):
            lending_pools_dict = {pool_id: pool_obj.to_list()
                                  for pool_id, pool_obj in self.lending_pools.items()}
            returned_dict['lendingPools'] = lending_pools_dict

        return returned_dict
