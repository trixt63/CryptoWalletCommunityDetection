from web3 import Web3
from web3.middleware import geth_poa_middleware
from query_state_lib.client.client_querier import ClientQuerier

from constants.lending.contract_address import ContractAddresses


class CompoundForkService:
    """
    A class for getting reserves (a.k.a. cTokens) of a Compound-forked lending pools,
    e.g.: Venus, Cream
    """
    def __init__(self, provider_uri):
        self._w3 = Web3(Web3.HTTPProvider(provider_uri))
        self.client_querier = ClientQuerier(provider_url=provider_uri)
        self._w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    def get_all_markets(self, comptroller: str,
                        comptroller_abi: list,
                        block_number: int = 'latest'):
        comptroller_contract = self._w3.eth.contract(
            address=self._w3.toChecksumAddress(comptroller), abi=comptroller_abi)
        tokens = []
        for token in comptroller_contract.functions.getAllMarkets().call(block_identifier=block_number):
            if token in [ContractAddresses.LUNA.lower(), ContractAddresses.UST.lower(), ContractAddresses.LUNA,
                         ContractAddresses.UST]:
                continue
            tokens.append(token)
        return tokens


if __name__ == '__main__':
    import json
    from constants.lending.lending_pools_info.ethereum.compound_eth import COMPOUND_ETH
    from artifacts.abis.lending_pool.cream_comptroller_abi import CREAM_COMPTROLLER_ABI

    service = CompoundForkService(provider_uri="https://rpc.ankr.com/eth")
    ctokens = service.get_all_markets(
        comptroller=COMPOUND_ETH["comptrollerAddress"],
        comptroller_abi=CREAM_COMPTROLLER_ABI
    )
    with open("compound_bsc.json", "w") as f:
        f.write(json.dumps(ctokens, indent=2))
