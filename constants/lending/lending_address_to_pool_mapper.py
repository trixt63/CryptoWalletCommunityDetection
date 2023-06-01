from artifacts.abis.lending_pool.cream_comptroller_abi import CREAM_COMPTROLLER_ABI
from constants import network_constants
from constants.network_constants import Chains, Networks
from services.blockchain.compound_fork_service import CompoundForkService


class LendingAddressToPoolMapper:
    # mapping
    def get_mapping(self):
        address_to_pool_mapping = dict()

        # Aave:
        address_to_pool_mapping[f"{Chains.ethereum}_{network_constants.ETHEREUM_AAVE_ADDRESS.lower()}"] = 'aave'
        address_to_pool_mapping[f"{Chains.polygon}_{network_constants.POLYGON_AAVE_ADDRESS.lower()}"] = 'aave'

        # Geist:
        address_to_pool_mapping[f"{Chains.fantom}_{network_constants.FTM_GEIST_ADDRESS.lower()}"] = 'geist'

        # Trava:
        address_to_pool_mapping[f"{Chains.bsc}_{network_constants.BSC_TRAVA_ADDRESS.lower()}"] = 'trava'
        address_to_pool_mapping[f"{Chains.ethereum}_{network_constants.ETH_TRAVA_ADDRESS.lower()}"] = 'trava'
        address_to_pool_mapping[f"{Chains.fantom}_{network_constants.FTM_TRAVA_ADDRESS.lower()}"] = 'trava'

        # Valas:
        address_to_pool_mapping[f"{Chains.bsc}_{network_constants.BSC_VALAS_ADDRESS.lower()}"] = 'valas'

        # Compound
        address_to_pool_mapping.update(self._map_ctokens(Chains.ethereum,
                                                         'compound',
                                                         network_constants.ETH_COMPOUND_ADDRESS))

        # Cream
        address_to_pool_mapping.update(self._map_ctokens(Chains.bsc,
                                                         'cream',
                                                         network_constants.BSC_CREAM_ADDRESS))
        address_to_pool_mapping.update(self._map_ctokens(Chains.ethereum,
                                                         'cream',
                                                         network_constants.ETH_CREAM_ADDRESS))

        # Venus
        address_to_pool_mapping.update(self._map_ctokens(Chains.bsc,
                                                         'venus',
                                                         network_constants.BSC_VENUS_ADDRESS))

        return address_to_pool_mapping

    @staticmethod
    def _map_ctokens(chain_id, pool_id, comptroller):
        # init the providers
        bsc_provider = Networks.providers[network_constants.Networks.bsc]
        bsc_lending_service = CompoundForkService(provider_uri=bsc_provider)

        eth_provider = Networks.providers[network_constants.Networks.ethereum]
        eth_lending_service = CompoundForkService(provider_uri=eth_provider)

        ftm_provider = Networks.providers[network_constants.Networks.fantom]
        ftm_lending_service = CompoundForkService(provider_uri=ftm_provider)

        _lending_services = {
            Chains.bsc: bsc_lending_service,
            Chains.ethereum: eth_lending_service,
            Chains.fantom: ftm_lending_service
        }

        # get mapping
        _address_to_pool_mapping = dict()
        for c_token in _lending_services[chain_id].get_all_markets(
                comptroller=comptroller,
                comptroller_abi=CREAM_COMPTROLLER_ABI
        ):
            _address_to_pool_mapping[f"{Chains.ethereum}_{c_token.lower()}"] = pool_id
        return _address_to_pool_mapping


if __name__ == '__main__':
    mapper = LendingAddressToPoolMapper()
    result = mapper.get_mapping()
    print(result)
