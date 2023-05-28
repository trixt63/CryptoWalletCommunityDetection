from typing import Dict
from overrides import override

from models.wallet.wallet import Wallet
from models.project import Project
from constants.tag_constants import WalletTags


class WalletOwnLP(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.owned_lps: Dict[str, Project] = dict()
        self.add_tags(WalletTags.lp_owner)

    # @override
    def add_project(self, project_id, chain_id, address):
        project = Project(project_id, chain_id, address)
        if project.project_id in self.owned_lps:
            self.owned_lps[project.project_id].add_deployments(project.deployments)
        else:
            self.owned_lps[project.project_id] = project

    @override
    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.owned_lps):
            owned_lps = {dex_id: lp_obj.to_deployments_list()
                         for dex_id, lp_obj in self.owned_lps.items()}
            returned_dict['ownedLPs'] = owned_lps

        return returned_dict
