class Tags:
    address = 'address'
    contract = 'contract'
    dapp = 'dapp'
    lending = 'lending'
    vault = 'vault'
    dex = 'dex'
    token = 'token'
    pair = 'pair'

    all = [contract, dapp, lending, vault, dex, token, pair]


class WalletTags:
    centralized_exchange_deposit_wallet = 'centralized_exchange_deposit_wallet'
    centralized_exchange_wallet = 'centralized_exchange_wallet'
    lending_wallet = 'lending_wallet'


class RelationshipType:
    # Wallet
    deposit = 'deposit'  # => Lending
    borrow = 'borrow'  # => Lending
    swap = 'swap'  # => Vault
    hold = 'hold'  # => Token
    transfer = 'transfer'  # => Wallet
    liquidate = 'liquidate'  # => Wallet
    call_contract = 'call_contract'  # => Contract

    # Project
    release = 'release'  # => Token
    subproject = 'subproject'  # => Project
    has_contract = 'has_contract'  # => Contract

    # Contract
    forked_from = 'forked_from'  # => Contract
    addon_contract = 'addon_contract'  # => Contract
    support = 'support'  # => Token
    reward = 'reward'  # => Token
    include = 'include'  # => Token
    exchange = 'exchange'  # => Project


class TransferFunc:
    transfer = 'transfer'
    transfer_from = 'transferFrom'
    transfer_native = 'transferNative'


class DappName:
    VENUS = 'pool_venus'
    AAVE = 'pool_aave'
    CREAM = 'pool_cream'
    TRAVA = 'pool_trava'
    COMPOUND = 'pool_compound'
    GEIST = 'pool_geist'
    ALPACA = 'pool_alpaca'
    VALAS = 'pool_valas'
