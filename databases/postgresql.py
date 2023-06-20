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
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        pass

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
        event_transfer = session.execute(query).all()
        self.session.commit()
        return event_transfer
