from typing import Dict

from models.wallet.wallet import Wallet
from models.protocol import Protocol
from constants.tag_constants import WalletTags


class WalletDeployLP(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.add_tags(WalletTags.lp_deployer)
        self.deployed_lps: Dict[str, Protocol] = dict()

    def add_protocol(self, protocol_id, chain_id, address):
        project = Protocol(protocol_id, chain_id, address)
        if project.protocol_id in self.deployed_lps:
            self.deployed_lps[project.protocol_id].add_deployments(project.deployments)
        else:
            self.deployed_lps[project.protocol_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.deployed_lps):
            deployed_lps = {dex_id: lp_obj.to_deployments_list()
                         for dex_id, lp_obj in self.deployed_lps.items()}
            returned_dict['deployedLPs'] = deployed_lps

        return returned_dict
