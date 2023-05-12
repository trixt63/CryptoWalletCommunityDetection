from typing import Set


class Project:
    def __init__(self, project_id: str,
                 chain_id: str = None,
                 address: str = None):
        self.project_id = project_id

        self.deployments: Set[_Deployment] = set()

        if (chain_id is not None) and (address is not None):
            self.deployments.add(_Deployment(chain_id, address))

    def add_deployments(self, added_deployments: set):
        self.deployments.union(added_deployments)

    def to_list(self):
        return [{
            # self.project_id: {_depl.chain_id: _depl.address for _depl in self.deployments}
            'chainId': _depl.chain_id,
            'address': _depl.address
        } for _depl in self.deployments]

    def __eq__(self, other):
        return self.project_id == other.project_id

    def __hash__(self):
        return hash(self.project_id)


class _Deployment:
    def __init__(self, chain_id, address):
        self.chain_id = chain_id
        self.address = address

    def __eq__(self, other):
        return (self.address == other.address
                and self.chain_id == other.chain_id)

    def __hash__(self):
        return hash(f"{self.chain_id}_{self.address}")
