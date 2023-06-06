import click

from constants.time_constants import TimeConstants
from jobs.lending_wallets_job import LendingWalletsJob
from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
def lending_wallets(interval):
    mongodb = MongoDB(wallet_col='lendingWallets')
    mongo_entity = MongoDBEntity()
    job = LendingWalletsJob(interval=interval,
                            importer=mongo_entity,
                            exporter=mongodb)
    job.run()
