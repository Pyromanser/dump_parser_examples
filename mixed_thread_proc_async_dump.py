import os
import re
import time
import aiohttp
import asyncio
import datetime
import requests
from aiofile import async_open
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from itertools import chain
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from logger import get_logger

SITE_BASE_URL = 'https://translatedby.com/'
TAG = 'GURPS'
TAG_URL = urljoin(SITE_BASE_URL, f"you/tags/{TAG}/")
TIMESTAMP = datetime.datetime.now().strftime('%Y-%m-%d')
DUMP_DIR_NAME = f"{TAG}_{TIMESTAMP}_mixed_tpa"

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


async def async_get_response_with_retry(url, session, retry=5, sleep=1):  # TODO: replace with backoff or aiohttp_retry
    """
    Just simple solution to avoid one-time bad server response
    can be replaced with `requests.get(url)`
    """
    for _ in range(retry):
        response = await session.get(url)
        if response.status != 200:
            logger.warning(f"{url} returned {response.status}, retry")
        else:
            return response
        await asyncio.sleep(sleep)
    raise Exception("Too many retries")


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


async def async_parse_book(book_url, book_name):
    """Dumps book info and book translation"""
    logger.debug(f"Dumping {book_url}")
    async with aiohttp.ClientSession() as session:
        about_page_url = urljoin(book_url, "stats/")
        book_file_url = urljoin(book_url, ".txt")

        book_dir = os.path.join(DUMP_DIR_NAME, book_name)
        os.mkdir(book_dir)

        about_response, file_response = await asyncio.gather(
            async_get_response_with_retry(about_page_url, session),
            async_get_response_with_retry(book_file_url, session)
        )

        soup = BeautifulSoup(await about_response.text(), 'html.parser')
        blockquote = soup.find(id="about-translation").blockquote
        about = blockquote.string.strip() if blockquote else ''

        async with (
            async_open(os.path.join(book_dir, 'about.txt'), 'wt', encoding='utf-8') as about_file,
            async_open(os.path.join(book_dir, 'result.txt'), 'wb') as result_file
        ):
            await asyncio.gather(
                about_file.write('URL - {url}\n'.format(url=book_url) + about),
                result_file.write(await file_response.read())
            )


def parse_book(book_url, book_name):
    """Dumps book info and book translation"""
    asyncio.run(async_parse_book(book_url, book_name))


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

        process_executor.map(parse_book, books_base_urls, books_names)


if __name__ == "__main__":
    start = datetime.datetime.now()
    logger.info('Start')
    main()
    logger.info(f"Done in {datetime.datetime.now() - start}")
