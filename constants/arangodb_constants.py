class ArangoDBCollections:
    wallets = 'wallets'
    projects = 'projects'
    smart_contracts = 'smart_contracts'
    relationships = 'relationships'
    call_smart_contracts = 'call_smart_contracts'
    abi = 'abi'
    configs = 'configs'
    credit_score_configs = 'credit_score_configs'
    has_merged_wallets = 'has_merged_wallets'
    merged_wallets = 'merged_wallets'
    merged_wallets_credit_scores = 'merged_wallets_credit_scores'
    multichain_wallets = 'multichain_wallets'
    multichain_wallets_credit_scores = 'multichain_wallets_credit_scores'
    multichain_wallets_size = 'multichain_wallets_credit_size'
    wallets_credit_scores = 'wallets_credit_scores'

    is_part_ofs = 'is_part_ofs'


class ArangoDBKeys:
    project_overview = 'project_overview'


class KnowledgeGraphModel:
    edge_definitions = [
        {
            'edge_collection': ArangoDBCollections.relationships,
            'from_vertex_collections': [
                ArangoDBCollections.wallets,
                ArangoDBCollections.projects,
                ArangoDBCollections.smart_contracts
            ],
            'to_vertex_collections': [
                ArangoDBCollections.wallets,
                ArangoDBCollections.projects,
                ArangoDBCollections.smart_contracts
            ],
        }
    ]


class ArangoDBIndex:
    smart_contract_tags = 'smart_contract_tags'
    smart_contract_address = 'smart_contract_address'
    smart_contract_chain = 'smart_contract_chain'
    relationship_type = 'relationship_type'
    relationship_from_type = 'relationship_from_type'
    relationship_ttl = 'relationship_ttl'


class WalletLabels:
    elite = 'elite'
    new_elite = 'newElite'
    target = 'target'
    new_target = 'newTarget'
