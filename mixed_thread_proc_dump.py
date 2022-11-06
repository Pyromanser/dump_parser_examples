import os
import re
import time
import datetime
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from itertools import chain
from more_itertools import grouper
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from logger import get_logger

SITE_BASE_URL = 'https://translatedby.com/'
TAG = 'GURPS'
TAG_URL = urljoin(SITE_BASE_URL, f"you/tags/{TAG}/")
TIMESTAMP = datetime.datetime.now().strftime('%Y-%m-%d')
DUMP_DIR_NAME = f"{TAG}_{TIMESTAMP}_mixed_tp"

logger = get_logger(__name__)


def get_response_with_retry(url, retry=5, sleep=1):  # TODO: replace with Retry from urllib3 or with backoff
    """
    Just simple solution to avoid one-time bad server response
    can be replaced with `requests.get(url)`
    """
    for _ in range(retry):
        response = requests.get(url)
        if response.status_code != requests.codes.ok:
            logger.warning(f"{url} returned {response.status_code}, retry")
        else:
            return response
        time.sleep(sleep)
    raise Exception("Too many retries")


def get_response_content(url):
    logger.debug(f"Requesting {url}")
    return get_response_with_retry(url).content


def checks(site_url, dir_name):
    """Dummy checks"""
    response = requests.get(site_url)
    if response.status_code != requests.codes.ok:
        logger.error(f"Site {site_url} is down, try later")
        return False
    if os.path.isdir(dir_name):
        logger.error(f"Directory {dir_name} already exists")
        return False
    return True


def do_book(book_dir, content):
    """Dumps book info and book translation"""
    logger.debug(f"Dumping {book_dir}")
    about_page_content, book_file_content = content[0], content[1]
    about_soup = BeautifulSoup(about_page_content, 'html.parser')
    blockquote = about_soup.find(id="about-translation").blockquote
    about = blockquote.string.strip() if blockquote else ''

    with open(os.path.join(book_dir, 'about.txt'), 'wt', encoding='utf-8') as f_about, open(os.path.join(book_dir, 'result.txt'), 'wb') as f_book:
        f_about.write(about)
        f_book.write(book_file_content)


def parse_page(response):
    book_names, book_urls = [], []
    book_dt_elems = BeautifulSoup(response.text, 'html.parser').find('dl', {'class': 'translations-list'}).find_all('dt')
    for book_dt_elem in book_dt_elems:
        book_names.append(book_dt_elem.a.string.replace('\n', ' '))
        book_urls.append(urljoin(SITE_BASE_URL, re.sub('/trans/$', '/', book_dt_elem.a.get('href'))))
    return book_names, book_urls


def main():
    if not checks(SITE_BASE_URL, DUMP_DIR_NAME):
        return
    logger.debug('Checks passed')

    os.mkdir(os.path.join(DUMP_DIR_NAME))
    logger.debug('Dump directory created')

    response_main_page = get_response_with_retry(TAG_URL)
    soup = BeautifulSoup(response_main_page.text, 'html.parser')
    pages = int(soup.find('div', {'class': 'spager'}).find_all('a', href=True)[-1].string)
    logger.debug(f'Found {pages} pages')

    with ThreadPoolExecutor() as thread_executor, ProcessPoolExecutor() as process_executor:
        pages_responses = thread_executor.map(
            get_response_with_retry,
            chain(
                (TAG_URL,),
                (urljoin(SITE_BASE_URL, a["href"]) for a in
                 soup.find('div', {'class': 'spager'}).find_all('a', href=True)),
            )
        )
        logger.debug(f'Got all responses')

        books_names, books_base_urls = [], []
        for names, urls in process_executor.map(parse_page, pages_responses):
            logger.debug(f'Parsing page')
            books_names.extend(names)
            books_base_urls.extend(urls)
        logger.debug(f'Pages parsed')

        logger.debug(f'Creating folders')
        book_dirs = [os.path.join(DUMP_DIR_NAME, book_name) for book_name in books_names]
        process_executor.map(os.mkdir, book_dirs)
        logger.debug(f'Folders created')

        books_urls = [
            [urljoin(book_base_url, "stats/"), urljoin(book_base_url, ".txt")] for book_base_url in books_base_urls
        ]

        logger.debug(f'Requesting books')
        books_responses = thread_executor.map(
            get_response_content,
            chain(*books_urls)
        )
        logger.debug(f'Requests done')

        process_executor.map(do_book, book_dirs, grouper(books_responses, 2, incomplete='strict'))


if __name__ == "__main__":
    start = datetime.datetime.now()
    logger.info('Start')
    main()
    logger.info(f"Done in {datetime.datetime.now() - start}")
