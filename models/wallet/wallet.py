from constants.tag_constants import WalletTags
from models.project import Project


class Wallet:
    def __init__(self, address):
        self.address = address
        self.tags = list()

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
        return returned_dict

    def __eq__(self, other):
        return self.address == other.address

    def __hash__(self):
        return hash(self.address)
