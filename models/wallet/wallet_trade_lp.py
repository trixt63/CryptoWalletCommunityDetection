from typing import Dict

from models.wallet.wallet import Wallet
from models.protocol import Protocol
from constants.tag_constants import WalletTags


class WalletTradeLP(Wallet):
    def __init__(self, address):
        super().__init__(address)
        self.traded_lps: Dict[str, Protocol] = dict()
        self.add_tags(WalletTags.lp_trader)

    def add_project(self, project_id, chain_id, address):
        project = Protocol(project_id, chain_id, address)
        if project.protocol_id in self.traded_lps:
            self.traded_lps[project.protocol_id].add_deployments(project.deployments)
        else:
            self.traded_lps[project.protocol_id] = project

    def to_dict(self):
        returned_dict = super().to_dict()

        if len(self.traded_lps):
            traded_lps = {dex_id: lp_obj.to_deployments_list()
                          for dex_id, lp_obj in self.traded_lps.items()}
            returned_dict['tradedLPs'] = traded_lps

        return returned_dict
