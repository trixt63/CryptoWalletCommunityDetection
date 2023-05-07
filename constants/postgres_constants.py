from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, PrimaryKeyConstraint, UniqueConstraint, Index, MetaData
from config import PostgresDBConfig

from constants.arangodb_constants import WalletLabels

metadata_obj = MetaData(schema=PostgresDBConfig.SCHEMA)
Base = declarative_base(metadata=metadata_obj)


class TransferEvent(Base):
    __tablename__ = PostgresDBConfig.TRANSFER_EVENT_TABLE
    block_number = Column(Integer)
    log_index = Column(Integer)
    contract_address = Column(String)
    transaction_hash = Column(String)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(Numeric)
    __table_args__ = (PrimaryKeyConstraint('block_number', 'log_index'),)

    def to_dict(self):
        return {
            'block_number': self.block_number,
            'log_index': self.log_index,
            'contract_address': self.contract_address,
            'transaction_hash': self.transaction_hash,
            'from_address': self.from_address,
            'to_address': self.to_address,
            'value': self.value
        }


class EliteWallet(Base):
    __tablename__ = 'elite_wallet'
    address = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('address', name='uq_elite_wallet_address'), PrimaryKeyConstraint('address'))


class TargetWallet(Base):
    __tablename__ = 'target_wallet'
    address = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('address', name='uq_target_wallet_address'), PrimaryKeyConstraint('address'))


class NewEliteWallet(Base):
    __tablename__ = 'new_elite_wallet'
    address = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('address', name='uq_new_elite_wallet_address'), PrimaryKeyConstraint('address'))


class NewTargetWallet(Base):
    __tablename__ = 'new_target_wallet'
    address = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('address', name='uq_new_target_wallet_address'), PrimaryKeyConstraint('address'))


class AmountInOut(Base):
    __tablename__ = 'amount_in_out'
    address = Column(String, nullable=False)
    token = Column(String, nullable=False)
    value = Column(Numeric)
    income = Column(Numeric)
    number_tx = Column(Numeric)
    __table_args__ = (PrimaryKeyConstraint('address'),)


WALLET_TABLES = {
    # Give me a label, and I'll give you the corresponding Table
    WalletLabels.elite: EliteWallet,
    WalletLabels.target: TargetWallet,
    WalletLabels.new_elite: NewEliteWallet,
    WalletLabels.new_target: NewTargetWallet
}

TF_EVENTS_VIEW = {
    # Give me a label, and I'll give you the corresponding View name
    WalletLabels.elite: 'elite_events',
    WalletLabels.target: 'target_events',
    WalletLabels.new_elite: 'new_elite_events',
    WalletLabels.new_target: 'new_target_events'
}
