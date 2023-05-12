from typing import Dict, List, Set

from constants.tag_constants import WalletTags
from models.project import Project


class Wallet:
    def __init__(self, address):
        self.address = address
        self.tags = list()

        self.lps_traded: Dict[str, Project] = dict()

    def add_tags(self, new_tag: str):
        if new_tag not in WalletTags.all_wallet_tags:
            print(f"{new_tag} not in supported wallet tags")
            return None
        if new_tag not in self.tags:
            self.tags.append(new_tag)

    def add_project(self, project: Project):
        pass

    def to_dict(self):
        returned_dict = {
            'address': self.address,
            'tags': self.tags
        }

        # if len(self.lending_pools):
        #     returned_dict['lendings'] = self.lending_pools
        #
        # if len(self.exchanges_deposited):
        #     returned_dict['exchanges_deposited'] = list(self.exchanges_deposited)
        #
        # if len(self.lps_traded):
        #     returned_dict['lps_traded'] = list(self.lps_traded)
        #
        # return returned_dict

    def __eq__(self, other):
        return self.address == other.address

    def __hash__(self):
        return hash(self.address)
