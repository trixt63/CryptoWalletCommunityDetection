from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker

from config import PostgresDBConfig
from constants.network_constants import NATIVE_TOKEN
from constants.postgres_constants import TransferEvent, WALLET_TABLES, TF_EVENTS_VIEW, AmountInOut
from utils.logger_utils import get_logger
from utils.time_execute_decorator import TimeExeTag, sync_log_time_exe

logger = get_logger('PostgreSQL')


class PostgresDB:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresDBConfig.CONNECTION_URL
        self.engine = create_engine(connection_url)

        # Create a session to manage the database transactions
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        self.session.close()

    ###################################
    #      Wallet Address Table       #
    ###################################
    def reset_wallets_table(self, table_class) -> None:
        inspector: Inspector = inspect(self.engine)
        table = table_class.__table__
        table_name = table_class.__tablename__

        if not inspector.has_table(table_name=table_name, schema=PostgresDBConfig.SCHEMA):
            # "tables" must be specified because EliteWallet and Base both share the metadata.
            # In other words, Base.metadata.create_all(bind=self.engine, tables=[table_name]) is the same as follows.
            table_class.metadata.create_all(bind=self.engine, tables=[table])
        self.session.execute(f'TRUNCATE TABLE {PostgresDBConfig.SCHEMA}.{table_name}')
        self.session.commit()

    def update_wallets_table(self, label: str, wallets: list):
        rows = [{'address': address} for address in set(wallets)]
        table_class = WALLET_TABLES.get(label)
        try:
            self.reset_wallets_table(table_class)
            self.session.bulk_insert_mappings(table_class, rows)
            self.session.commit()
        except Exception as e:
            logger.exception(e)

    # @sync_log_time_exe(tag=TimeExeTag.database)
    def get_event_transfer_by_to_addresses(self, to_addresses, from_block, to_block):
        query = f"""
            SELECT from_address 
            FROM {PostgresDBConfig.SCHEMA}.{PostgresDBConfig.TRANSFER_EVENT_TABLE}
            WHERE to_address = ANY (ARRAY{to_addresses})
            AND block_number BETWEEN {from_block} AND {to_block}
            GROUP BY from_address
        """

        event_transfer = self.session.execute(query).all()
        return event_transfer

    ####################
    #      Views       #
    ####################
    def drop_view(self, view_name: str):
        try:
            self.session.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
            self.session.commit()
        except Exception as e:
            logger.exception(e)

    def drop_table(self, table_name: str):
        try:
            self.session.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.session.commit()
        except Exception as e:
            logger.exception(e)

    def truncate_table(self, table_name: str):
        try:
            self.session.execute(f"TRUNCATE TABLE {table_name}")
            self.session.commit()
        except Exception as e:
            logger.exception(e)

    def create_tf_events_view(self, label: str, min_block: int, max_block: int):
        events_table = f'{PostgresDBConfig.SCHEMA}.{TransferEvent.__tablename__}'
        view_name = f'{PostgresDBConfig.SCHEMA}.{TF_EVENTS_VIEW[label]}'
        create_view_query = f"""
                        CREATE MATERIALIZED VIEW {view_name} AS
                            SELECT DISTINCT contract_address, log_index, block_number, from_address, to_address, value
                            FROM {events_table}
                            WHERE block_number >= {min_block}
                            AND block_number <= {max_block};
                    """
        self.drop_view(view_name=view_name)
        self.session.execute(create_view_query)

        logger.info('View created. Indexing. . .')
        self.session.execute(f"CREATE INDEX from_address_index_{label} ON {view_name}(from_address)")
        self.session.execute(f"CREATE INDEX to_address_index_{label} ON {view_name}(to_address)")
        self.session.execute(f"CREATE INDEX block_number_index_{label} ON {view_name}(block_number)")
        self.session.commit()
        return view_name

    def get_view_block_range(self, view_name: str):
        return self.session.execute(f'SELECT min(block_number), max(block_number) FROM {view_name}').one()

    def get_transfer_tokens_list(self, view_name: str):
        query_result = self.session.execute(f'SELECT DISTINCT contract_address FROM {view_name}').all()
        return [item._asdict()['contract_address'] for item in query_result]

    def get_incoming_token_amount(self, view_name: str, from_block: int, to_block: int, wallets=None, tokens=None):
        """
        The get_incoming_token_amount function returns a dictionary of dictionaries.
        The outer dictionary is keyed by the wallet address, and each inner dictionary is keyed by contract address.
        Each inner dictionary contains the total value received for that wallet/contract combination.

        Args:
            self: Access the class variables
            view_name:str: Specify the view that is queried
            from_block:int: Specify the block from which to start counting
            to_block:int: Specify the block to stop at
            wallets:list=None: Filter the results

        Returns:
            A dictionary with the wallet address as key and a dictionary of contract addresses as value
            {
                "0xwallet1": {
                    "0xtoken1": 12123,
                    "0xtoken2": 234234
                },
                "0xwallet2": {}
            }

        Doc Author:
            Minh
        """
        if not wallets:
            query = f'SELECT to_address AS wallet_address, ' \
                    f'SUM(value) AS total_value, ' \
                    f"COUNT(*) AS number_of_tx, " \
                    f'contract_address ' \
                    f'FROM {view_name} ' \
                    f'WHERE block_number >= {from_block} ' \
                    f'AND block_number <= {to_block} ' \
                    f'GROUP BY to_address, contract_address'
        else:
            wallets = ', '.join([f"'{wallet}'" for wallet in wallets])
            query = f"SELECT to_address AS wallet_address, " \
                    f"SUM(value) AS total_value, " \
                    f"COUNT(*) AS number_of_tx, " \
                    f"contract_address " \
                    f"FROM {view_name} " \
                    f"WHERE block_number >= {from_block} " \
                    f"AND block_number <= {to_block} " \
                    f"AND to_address IN ({wallets}) " \
                    f"GROUP BY to_address, contract_address"
        incoming = self.session.execute(query).all()
        total_value = {x.wallet_address: {} for x in incoming}
        total_tx = {x.wallet_address: 0 for x in incoming}
        for item in incoming:
            if tokens and item.contract_address not in tokens:
                continue
            total_value[item.wallet_address][item.contract_address] = item.total_value
            total_tx[item.wallet_address] += item.number_of_tx

        return total_value, total_tx

    def get_outgoing_token_amount(self, view_name: str, from_block: int, to_block: int, wallets=None, tokens=None):
        """
        Same as above method but in reversed
        """
        if not wallets:
            query = f'SELECT from_address AS wallet_address, ' \
                    f'SUM(value) AS total_value, ' \
                    f"COUNT(*) AS number_of_tx, " \
                    f'contract_address ' \
                    f'FROM {view_name} ' \
                    f'WHERE block_number >= {from_block} ' \
                    f'AND block_number <= {to_block} ' \
                    f'GROUP BY from_address, contract_address'
        else:
            wallets = ', '.join([f"'{wallet}'" for wallet in wallets])
            query = f"SELECT from_address AS wallet_address, " \
                    f"SUM(value) AS total_value, " \
                    f"COUNT(*) AS number_of_tx, " \
                    f"contract_address " \
                    f"FROM {view_name} " \
                    f"WHERE block_number >= {from_block} " \
                    f"AND block_number <= {to_block} " \
                    f"AND from_address IN ({wallets}) " \
                    f"GROUP BY from_address, contract_address"
        outgoing = self.session.execute(query).all()
        total_value = {x.wallet_address: {} for x in outgoing}
        total_tx = {x.wallet_address: 0 for x in outgoing}
        for item in outgoing:
            if tokens and item.contract_address not in tokens:
                continue
            total_value[item.wallet_address][item.contract_address] = item.total_value
            total_tx[item.wallet_address] += item.number_of_tx

        return total_value, total_tx

    def get_event_transfer(self, view_name: str, from_block: int, to_block: int, wallets=None, tokens=None):
        query = f'SELECT * FROM {view_name} ' \
                f'WHERE block_number >= {from_block} ' \
                f'AND block_number <= {to_block} '
        if wallets:
            _wallets = ', '.join([f"'{wallet}'" for wallet in wallets])
            query += f"AND (from_address IN ({_wallets}) or to_address IN ({_wallets}))"
        if tokens:
            _tokens = ', '.join([f"'{token}'" for token in tokens])
            query += f"AND contract_address IN ({_tokens})"
        event_transfer = self.session.execute(query).all()
        return event_transfer

    def get_event_transfer_by_contract(self, from_block=None, to_block=None):
        query = f"""
            SELECT contract_address, COUNT(*) as number_of_transfers 
            FROM {PostgresDBConfig.SCHEMA}.{PostgresDBConfig.TRANSFER_EVENT_TABLE}
            WHERE block_number BETWEEN {from_block} AND {to_block}
            GROUP BY contract_address
        """

        event_transfer = self.session.execute(query).all()
        return event_transfer

    def create_fantom_amount_in_out_table(self, schema, from_block, to_block):
        self.truncate_table(f"{schema}.amount_in_out")
        create_view_query = f"""
                INSERT INTO {schema}.amount_in_out SELECT * FROM (
                SELECT from_address AS address, contract_address AS token, -SUM(value) AS value, 0 AS income, count(*) AS number_tx
                FROM {schema}.token_transfer
                WHERE block_number BETWEEN {from_block} AND {to_block}
                GROUP BY address, token
                UNION ALL
                SELECT to_address AS address, contract_address AS token, SUM(value) AS value, SUM(value) AS income, 0 AS number_tx
                FROM {schema}.token_transfer
                WHERE block_number BETWEEN {from_block} AND {to_block}
                GROUP BY address, token
                ) AS S
                """
        self.session.execute(create_view_query)
        self.session.commit()

    def create_amount_in_out_table(self, schema, from_block, to_block):
        self.truncate_table(f"{schema}.amount_in_out")
        create_view_query = f"""
        INSERT INTO {schema}.amount_in_out SELECT * FROM (
        SELECT from_address AS address, contract_address AS token, -SUM(value) AS value, 0 AS income, count(*) AS number_tx
        FROM {schema}.token_transfer
        WHERE block_number BETWEEN {from_block} AND {to_block}
        GROUP BY address, token
        UNION ALL
        SELECT to_address AS address, contract_address AS token, SUM(value) AS value, SUM(value) AS income, 0 AS number_tx
        FROM {schema}.token_transfer
        WHERE block_number BETWEEN {from_block} AND {to_block}
        GROUP BY address, token
        union all
        select address, contract_address as token, sum(value) as value, 0 as number_tx, 0 as income from {schema}.wrapped_token
        where block_number between {from_block} and {to_block}
        and event_type = 'DEPOSIT'
        group by address, token
        union all
        select address, contract_address as token, -sum(value) as value, 0 as number_tx, 0 as income from {schema}.wrapped_token
        where block_number between {from_block} and {to_block}
        and event_type = 'WITHDRAWAL'
        group by address, token
        ) AS S
        """
        self.session.execute(create_view_query)
        self.session.commit()

    def create_balance_change_table(self, schema, timestamp):
        create_balance_change_view = f"""
        CREATE TABLE IF NOT EXISTS {schema}.balance_change_{timestamp} AS(
        SELECT address, token, SUM(value) AS value, SUM(income) AS income, SUM(number_tx) as number_tx
        FROM {schema}.amount_in_out
        where address!='{NATIVE_TOKEN}'
        GROUP BY address, token
        )
        """
        self.session.execute(create_balance_change_view)
        create_primary_key = f"""
        ALTER TABLE {schema}.balance_change_{timestamp}
        ADD PRIMARY KEY (address, token)
        """
        self.session.execute(create_primary_key)
        self.session.execute(
            f"CREATE INDEX address_index_{timestamp} ON {schema}.balance_change_{timestamp}(address)")
        self.session.execute(
            f"CREATE INDEX token_index_{timestamp} ON {schema}.balance_change_{timestamp}(token)")
        self.session.commit()

    def create_update_balance_change_amount_in_out_table(self, schema, from_block, to_block, timestamp):
        self.truncate_table(f"{schema}.amount_in_out")
        create_view_query = f"""
        INSERT INTO {schema}.amount_in_out SELECT * FROM(
        SELECT address, token, sum(value) as value, sum(income) as income, sum(number_tx) as number_tx
        FROM (
        select address, contract_address as token, sum(value) as value, 0.0 as income, 0 as number_tx from {schema}.wrapped_token
        where block_number between {from_block} and {to_block}
        and event_type = 'DEPOSIT'
        group by address, token
        union all
        select address, contract_address as token, -sum(value) as value,  0.0 as income, 0 as number_tx from {schema}.wrapped_token
        where block_number between {from_block} and {to_block}
        and event_type = 'WITHDRAWAL'
        group by address, token
        union all
        select address, token, value, income, number_tx from {schema}.balance_change_{timestamp}
        where address!='{NATIVE_TOKEN}'
        ) AS s
        group by address, token) AS S
        """
        self.session.execute(create_view_query)
        self.session.commit()

    def insert_amount_in_out_to_balance_change_table(self, schema, timestamp):
        query = f"""
            INSERT INTO {schema}.balance_change_{timestamp} SELECT * FROM {schema}.amount_in_out
        """
        self.session.execute(query)
        self.session.commit()

    def calculate_balance_native_token_change(self, schema, from_block, to_block):
        self.truncate_table(f"{schema}.amount_in_out")
        create_view_query = f"""
        INSERT INTO {schema}.amount_in_out SELECT * FROM (
        SELECT address, token, sum(value) as value, sum(income) as income, sum(number_tx) as number_tx
        FROM(
        select address, '{NATIVE_TOKEN}' as token, -sum(value) as value, 0 as number_tx, 0 as income from {schema}.wrapped_token
        where block_number between {from_block} and {to_block}
        and event_type = 'DEPOSIT'
        group by address, token
        union all
        select address, '{NATIVE_TOKEN}' as token, sum(value) as value, 0 as number_tx, 0 as income from {schema}.wrapped_token
        where block_number between {from_block} and {to_block}
        and event_type = 'WITHDRAWAL'
        group by address, token
        union all
        select address, token, value, number_tx, income from {schema}.native_token_amount_in_out
        ) AS s 
        group by address, token
        ) AS S
        """
        self.session.execute(create_view_query)
        self.session.commit()

    def insert_amount_in_out_table(self, data: list):
        try:
            self.reset_wallets_table(AmountInOut)
            self.session.bulk_insert_mappings(AmountInOut, data)
            self.session.commit()
        except Exception as e:
            logger.exception(e)

    def get_wallet_balance_change(self, table, wallets, tokens):
        query = f"""
                    SELECT *
                    FROM {table}
                """
        if wallets:
            _wallets = ', '.join([f"'{wallet}'" for wallet in wallets])
            query += f"WHERE address IN ({_wallets})"

        if tokens:
            _tokens = ', '.join([f"'{token}'" for token in tokens])
            query += f"AND token IN ({_tokens})"

        data = self.session.execute(query).all()
        result = {}
        for item in data:
            if item.address not in result:
                result[item.address] = {"dailyNumberOfTransactions": 0}
            result[item.address][item.token] = {
                "amount": float(item.value),
                "dailyTransactionAmounts": float(item.income)
            }
            result[item.address]['dailyNumberOfTransactions'] += int(item.number_tx)
        return result

    def get_wallet_balance_change_all(self, schema, wallets, tokens, timestamps):
        _wallets = ', '.join([f"'{wallet}'" for wallet in wallets])
        _token = ', '.join([f"'{token}'" for token in tokens])
        query = f"""
                SELECT * FROM (
                SELECT *, {timestamps[0]} AS timestamp FROM {schema}.balance_change_{timestamps[0]}
                WHERE address in ({_wallets}) AND token in ({_token})
                """
        for timestamp in timestamps[1:]:
            query += f"""
                    UNION ALL
                    SELECT *, {timestamp} AS timestamp FROM {schema}.balance_change_{timestamp}
                    WHERE address in ({_wallets}) AND token in ({_token})
                    """
        query += ") AS S"
        data = self.session.execute(query).all()
        result = {}
        transfer_tokens = {}
        for item in data:
            # if item.token not in tokens:
            #     continue
            if item.address not in result:
                result[str(item.address)] = {}
                transfer_tokens[str(item.address)] = []
            if int(item.timestamp) not in result[str(item.address)]:
                result[str(item.address)][int(item.timestamp)] = {}
            result[str(item.address)][int(item.timestamp)].update({str(item.token): {
                "dailyTransactionAmounts": float(item.income),
                "dailyNumberOfTransactions": int(item.number_tx),
                "amount": float(item.value)
            }})
            if str(item.token) not in transfer_tokens[str(item.address)]:
                transfer_tokens[str(item.address)].append(str(item.token))

        return result, transfer_tokens

    def get_wallet_balance_change_token(self, token: str, chain_id: str, timestamp: int):
        schema = f"chain_{chain_id}"
        query = f"""
        select * from {schema}.balance_change_{timestamp}
        where token = '{token}'
        """
        data = self.session.execute(query).all()
        addresses = []
        wallet_data = {}
        for item in data:
            address = str(item.address)
            addresses.append(address)
            wallet_data[address] = float(item.value)

        return addresses, wallet_data

    def get_transfer_event_native_token_with_wrapped_token(self, chain_id, wrapped_token, start_block, end_block):
        schema = f"chain_{chain_id}"
        query = f"""
        select address, value from
        (select to_address as address, value from {schema}.token_transfer
        where contract_address = '{wrapped_token}'
        and from_address = '{NATIVE_TOKEN}'
        and block_number between {start_block} and {end_block}
        union all
        select from_address as address, -value as value from {schema}.token_transfer
        where contract_address = '{wrapped_token}'
        and to_address = '{NATIVE_TOKEN}'
        and block_number between {start_block} and {end_block}
        ) as s
        """
        data = self.session.execute(query).all()
        result = {}
        for item in data:
            result[str(item.address)] = float(item.value)

        return result
