from typing import Dict


from models.wallet.wallet import Wallet
from models.project import Project


class WalletTradeLP(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.traded_lps: Dict[str, Project] = dict()

    def add_project(self, project: Project):
        if project.project_id in self.traded_lps:
            self.traded_lps[project.project_id].add_deployments(project.deployments)
        else:
            self.traded_lps[project.project_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.traded_lps):
            traded_lps = {dex_id: lp_obj.to_list()
                          for dex_id, lp_obj in self.traded_lps.items()}
            returned_dict['tradedLPs'] = traded_lps

        return returned_dict
