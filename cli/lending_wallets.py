import click

from constants.time_constants import TimeConstants

from jobs.lending_wallets_job import LendingWalletsJob


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
def lending_wallets(interval):
    job = LendingWalletsJob()
    job.run()
