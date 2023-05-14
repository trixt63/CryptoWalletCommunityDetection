from constants.network_constants import Chains


class LPConstants:
    CHAIN_DEX_MAPPINGS = {
        Chains.bsc: ["PancakePair"],
        Chains.fantom: ["Spooky LP"]
    }

    LP_NAME_ID_MAPPINGS = {
        "PancakePair": "pancakeswap",
        "Spooky LP": "spookyswap"
    }
