from typing import Set, List


class Protocol:
    """A DeFi project. Can be CEX, DEX, Lending pool, NFT, etc.,
    """
    def __init__(self, protocol_id: str,
                 chain_id: str = None,
                 address: str = None):
        self.protocol_id = protocol_id
        self.deployments: Set[_Deployment] = set()

        if (chain_id is not None) and (address is not None):  # automatically add first deployment
            self.deployments.add(_Deployment(chain_id, address))

    def add_deployments(self, added_deployments: set):
        self.deployments.union(added_deployments)

    def to_deployed_chains_list(self) -> List:
        """return a list of chain ids from deployments"""
        return list({_depl.chain_id for _depl in self.deployments})

    def to_deployments_list(self):
        """return a list of both chainIds and deployed addresses"""
        return [{
            'chainId': _depl.chain_id,
            'address': _depl.address
        } for _depl in self.deployments]

    def __eq__(self, other):
        return self.protocol_id == other.protocol_id

    def __hash__(self):
        return hash(self.protocol_id)


class _Deployment:
    """ChainId & the corresponding address of the project's deployment"""
    def __init__(self, chain_id, address):
        self.chain_id = chain_id
        self.address = address

    def __eq__(self, other):
        return (self.address == other.address
                and self.chain_id == other.chain_id)

    def __hash__(self):
        return hash(f"{self.chain_id}_{self.address}")
