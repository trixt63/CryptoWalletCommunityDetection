class WalletTags:
    # cex wallets
    centralized_exchange_deposit_wallet = 'centralized_exchange_deposit_wallet'
    centralized_exchange_wallet = 'centralized_exchange_wallet'

    # lending wallet
    lending_wallet = 'lending_wallet'

    # dex wallets
    lp_owner: 'lp_owner'
    lp_holder: 'lp_holder'
    dex_traders: 'dex_traders'
    bot: 'bot'

    # nft wallets
    nft_wallet = 'nft_wallet'

    all_wallet_tags = {centralized_exchange_wallet, centralized_exchange_deposit_wallet, lending_wallet, nft_wallet}
