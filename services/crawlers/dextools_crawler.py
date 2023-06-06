import os
import time
from datetime import datetime
from typing import Set

from bs4 import BeautifulSoup as soup
from selenium.webdriver.common.by import By
import selenium.common.exceptions

from services.crawlers.base_crawler import BaseCrawler
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler
from models.lp_transaction import LPTransaction

logger = get_logger('DEXTools Crawler')
logger.setLevel(20)  # only console logging INFO or above
EARLIEST_TX_FILE_PATH = '.data/dextools_oldest_transactions.txt'


class DEXToolsCrawler:
    def __init__(self, page_number_limit=60):
        self.crawler = BaseCrawler()
        self.page_number_limit = page_number_limit
        # delete log file
        if os.path.exists(EARLIEST_TX_FILE_PATH):
            os.remove(EARLIEST_TX_FILE_PATH)

    @retry_handler
    def get_lp_transactions(self, chain_id, lp_address) -> Set[LPTransaction]:
        logger.info(f"Crawling transactions of LP contract {lp_address} on chain {chain_id} from DEXTools")
        _url = self._create_dextools_url(chain_id, lp_address)

        return self.crawler.use_chrome_driver(_url, self._handler_exchanges, contract_address=lp_address)

    def _handler_exchanges(self, driver, contract_address):
        time.sleep(5)
        driver.maximize_window()
        page_number = 1
        crawled_transactions = set()

        while True:
            page_soup = soup(driver.page_source, 'html.parser')
            table = page_soup.find('app-trade-history')
            rows = table.findAll('datatable-body-row')
            newly_crawled_transactions = set()
            oldest_trade_date = ''

            for row in rows:
                cols = row.find('div', {'class': 'datatable-row-center'}).findAll('datatable-body-cell')

                trade_date = cols[0].find('div', {'class': 'datatable-body-cell-label'}).contents[3]
                maker_link = cols[-2].find('a').get('href')
                tx_link = cols[-1].findAll('a')[0].get('href')
                is_bot = True if cols[-1].find('fa-icon') else False

                newly_crawled_transactions.add(LPTransaction(
                    maker_address=maker_link.split('/')[-1].lower(),
                    transaction_hash=tx_link.split('/')[-1].lower(),
                    is_bot=is_bot,
                    # timestamp=int(datetime.strptime(trade_date, ' %b %d %H:%M:%S ').timestamp())
                ))
                oldest_trade_date = trade_date

            if newly_crawled_transactions.issubset(crawled_transactions):  # break if there are no new transactions
                self._write_log_file(contract_address, oldest_trade_date)
                break
            crawled_transactions = crawled_transactions.union(newly_crawled_transactions)

            # Click to next page
            logger.debug(f"Page: {page_number}. Number of tx crawled: {len(crawled_transactions)}")
            next_page_button = self._get_next_page_button(driver)
            driver.execute_script("arguments[0].click();", next_page_button)
            time.sleep(1)
            page_number += 1
            if page_number > self.page_number_limit:
                self._write_log_file(contract_address, oldest_trade_date)
                break

        return crawled_transactions

    @staticmethod
    def _create_dextools_url(chain_id, contract_address):
        _chain_url_mapping = {
            '0x38': 'bnb',
            '0xfa': 'fantom',
            '0x89': 'polygon',
            '0x1': 'ether'
        }
        return f"https://www.dextools.io/app/en/{_chain_url_mapping[chain_id]}/pair-explorer/{contract_address}"

    @staticmethod
    def _get_next_page_button(driver):
        """Get the next page button"""
        li_nth_child = 8
        next_button_selenium = None
        while li_nth_child >= 0:
            try:
                css_selector = f'.pager > li:nth-child({str(li_nth_child)}) > a:nth-child(1)'
                next_button_selenium = driver.find_element(By.CSS_SELECTOR, css_selector)
            except selenium.common.exceptions.NoSuchElementException:
                li_nth_child -= 1

        logger.exception("Cannot find Next page button")
        return next_button_selenium

    @staticmethod
    def _write_log_file(contract_address: str, oldest_time: str):
        log_file = open(EARLIEST_TX_FILE_PATH, "a+")
        log_file.write(f"{contract_address}: {oldest_time}\n")
        log_file.close()
