import os
import re
import aiohttp
import asyncio
import datetime
from aiofile import async_open
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from itertools import chain
from more_itertools import grouper
from concurrent.futures import ProcessPoolExecutor

from logger import get_logger

SITE_BASE_URL = 'https://translatedby.com/'
TAG = 'GURPS'
TAG_URL = urljoin(SITE_BASE_URL, f"you/tags/{TAG}/")
TIMESTAMP = datetime.datetime.now().strftime('%Y-%m-%d')
DUMP_DIR_NAME = f"{TAG}_{TIMESTAMP}_mixed_pa"

logger = get_logger(__name__)


async def get_response_with_retry(url, session, retry=5, sleep=1):  # TODO: replace with backoff or aiohttp_retry
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


async def get_response_text(url, session):
    logger.debug(f"Requesting {url}")
    response = await get_response_with_retry(url, session)
    return await response.text()


async def get_response_content(url, session):
    logger.debug(f"Requesting {url}")
    response = await get_response_with_retry(url, session)
    return await response.read()


async def get_response_about_and_dump(url, session, book_dir, process_executor):
    text = await get_response_text(url, session)
    return process_executor.submit(dump_about, text, book_dir)


def dump_about(about_page_content, book_dir):
    logger.debug(f'Dumping about {book_dir}')
    about_soup = BeautifulSoup(about_page_content, 'html.parser')
    blockquote = about_soup.find(id="about-translation").blockquote
    about = blockquote.string.strip() if blockquote else ''

    with open(os.path.join(book_dir, 'about.txt'), 'wt', encoding='utf-8') as f_about:
        f_about.write(about)


async def get_response_book_and_dump(url, session, book_dir, process_executor):
    content = await get_response_content(url, session)
    return process_executor.submit(dump_book, content, book_dir)


def dump_book(book_file_content, book_dir):
    logger.debug(f'Dumping file {book_dir}')
    with open(os.path.join(book_dir, 'result.txt'), 'wb') as f_book:
        f_book.write(book_file_content)


async def checks(site_url, dir_name, session):
    """Dummy checks"""
    response = await session.get(site_url)
    if response.status != 200:
        logger.error(f"Site {site_url} is down, try later")
        return False
    if os.path.isdir(dir_name):
        logger.error(f"Directory {dir_name} already exists")
        return False
    return True


def parse_page(response_text):
    logger.debug(f'Parsing page')
    book_names, book_urls = [], []
    book_dt_elems = BeautifulSoup(response_text, 'html.parser').find('dl', {'class': 'translations-list'}).find_all('dt')
    for book_dt_elem in book_dt_elems:
        book_names.append(book_dt_elem.a.string.replace('\n', ' '))
        book_urls.append(urljoin(SITE_BASE_URL, re.sub('/trans/$', '/', book_dt_elem.a.get('href'))))
    return book_names, book_urls


async def main():
    async with aiohttp.ClientSession() as session:
        if not await checks(SITE_BASE_URL, DUMP_DIR_NAME, session):
            return
        logger.debug('Checks passed')

        os.mkdir(os.path.join(DUMP_DIR_NAME))
        logger.debug('Dump directory created')

        response = await get_response_with_retry(TAG_URL, session)
        soup = BeautifulSoup(await response.text(), 'html.parser')
        pages = int(soup.find('div', {'class': 'spager'}).find_all('a', href=True)[-1].string)
        logger.debug(f'Found {pages} pages')

        pages_responses = await asyncio.gather(*[
            get_response_text(page_url, session) for page_url in chain(
                (TAG_URL, ),
                (urljoin(SITE_BASE_URL, a["href"]) for a in soup.find('div', {'class': 'spager'}).find_all('a', href=True)),
            )
        ])
        logger.debug(f'Got all responses')

        books_names, books_base_urls = [], []
        with ProcessPoolExecutor() as process_executor:
            for names, urls in process_executor.map(parse_page, pages_responses):
                books_names.extend(names)
                books_base_urls.extend(urls)
            logger.debug(f'Pages parsed')

            book_dirs = [os.path.join(DUMP_DIR_NAME, book_name) for book_name in books_names]
            process_executor.map(os.mkdir, book_dirs)
            logger.debug(f'Folders created')

            logger.debug(f'Processing books')
            results = await asyncio.gather(*chain(*[
                [
                    get_response_about_and_dump(urljoin(book_base_url, "stats/"), session, book_dir, process_executor),
                    get_response_book_and_dump(urljoin(book_base_url, ".txt"), session, book_dir, process_executor)
                ] for book_dir, book_base_url in zip(book_dirs, books_base_urls)
            ]))
            for result in results:
                result.result()



if __name__ == "__main__":
    start = datetime.datetime.now()
    logger.info('Start')
    asyncio.run(main())
    logger.info(f"Done in {datetime.datetime.now() - start}")
