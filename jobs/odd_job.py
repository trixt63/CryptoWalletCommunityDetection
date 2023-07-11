import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))

from databases.mongodb import MongoDB
from utils.logger_utils import get_logger
from multithread_processing.base_job import BaseJob

logger = get_logger('Odd job')


class MultithreadOddJob(BaseJob):
    def __init__(self):
        self.mongodb = MongoDB()
        self.pagination = 10000
        # self._number_of_docs = self.mongodb.get_number_of_docs('depositWallets')
        self._number_of_docs = 12333

        super().__init__(
            work_iterable=range(self._number_of_docs),
            max_workers=8,
            batch_size=self.pagination
        )

    def _start(self):
        logger.info(f"Number of docs: {self._number_of_docs}")

    def _execute_batch(self, works):
        self.mongodb.add_chain_id_for_deposit_wallets(works[0], works[-1])
        logger.info(f"Add chainId for deposit wallet {works[0]} to {works[-1]}")


def main():
    job = MultithreadOddJob()
    job.run()


if __name__ == "__main__":
    main()
