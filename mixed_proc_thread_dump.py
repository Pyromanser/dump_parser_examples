import os
import re
import time
import datetime
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from itertools import chain
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from logger import get_logger

SITE_BASE_URL = 'https://translatedby.com/'
TAG = 'GURPS'
TAG_URL = urljoin(SITE_BASE_URL, f"you/tags/{TAG}/")
TIMESTAMP = datetime.datetime.now().strftime('%Y-%m-%d')
DUMP_DIR_NAME = f"{TAG}_{TIMESTAMP}_mixed_pt"

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


def parse_book(book_url, book_dir):
    """Dumps book info and book translation"""
    logger.debug(f"Dumping {book_url}")
    about_page_url = urljoin(book_url, "stats/")
    book_file_url = urljoin(book_url, ".txt")

    about_response = get_response_with_retry(about_page_url)
    book_response = get_response_with_retry(book_file_url)

    soup = BeautifulSoup(about_response.text, 'html.parser')
    blockquote = soup.find(id="about-translation").blockquote
    about = blockquote.string.strip() if blockquote else ''

    with open(os.path.join(book_dir, 'about.txt'), 'wt', encoding='utf-8') as f_about, open(os.path.join(book_dir, 'result.txt'), 'wb') as f_book:
        f_about.write('URL - {url}\n'.format(url=book_url))
        f_about.write(about)
        f_book.write(book_response.content)


def process_page(page_url):
    logger.debug(f'Processing {page_url}')
    response = get_response_with_retry(page_url)
    book_names, book_urls = [], []
    book_dt_elems = BeautifulSoup(response.text, 'html.parser').find('dl', {'class': 'translations-list'}).find_all('dt')
    for book_dt_elem in book_dt_elems:
        book_names.append(book_dt_elem.a.string.replace('\n', ' '))
        book_urls.append(urljoin(SITE_BASE_URL, re.sub('/trans/$', '/', book_dt_elem.a.get('href'))))

    book_dirs = []
    for book_name in book_names:
        book_dir = os.path.join(DUMP_DIR_NAME, book_name)
        os.mkdir(book_dir)
        book_dirs.append(book_dir)

    with ThreadPoolExecutor() as executor:
        executor.map(parse_book, book_urls, book_dirs)


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

    with ProcessPoolExecutor() as process_executor:
        process_executor.map(
            process_page,
            chain(
                (TAG_URL,),
                (urljoin(SITE_BASE_URL, a["href"]) for a in
                 soup.find('div', {'class': 'spager'}).find_all('a', href=True)),
            )
        )

if __name__ == "__main__":
    start = datetime.datetime.now()
    logger.info('Start')
    main()
    logger.info(f"Done in {datetime.datetime.now() - start}")
