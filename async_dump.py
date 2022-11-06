import os
import re
import aiohttp
import asyncio
import datetime
from aiofile import async_open
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from itertools import chain

from logger import get_logger

SITE_BASE_URL = 'https://translatedby.com/'
TAG = 'GURPS'
TAG_URL = urljoin(SITE_BASE_URL, f"you/tags/{TAG}/")
TIMESTAMP = datetime.datetime.now().strftime('%Y-%m-%d')
DUMP_DIR_NAME = f"{TAG}_{TIMESTAMP}_async"

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


async def parse_book(book_url, book_name, session):
    """Dumps book info and book translation"""
    logger.debug(f"Dumping {book_url}")
    about_page_url = urljoin(book_url, "stats/")
    book_file_url = urljoin(book_url, ".txt")

    book_dir = os.path.join(DUMP_DIR_NAME, book_name)
    os.mkdir(book_dir)

    about_response, file_response = await asyncio.gather(
        get_response_with_retry(about_page_url, session),
        get_response_with_retry(book_file_url, session)
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


async def parse_page(page_url, session):
    """Parse page and run parse book page for each book"""
    logger.debug(f'Parsing {page_url} page')
    response = await get_response_with_retry(page_url, session)
    soup = BeautifulSoup(await response.text(), 'html.parser')
    book_names, book_urls = [], []
    for book_dt_elem in soup.find('dl', {'class': 'translations-list'}).find_all('dt'):
        book_names.append(book_dt_elem.a.string.replace('\n', ' '))
        book_urls.append(urljoin(SITE_BASE_URL, re.sub('/trans/$', '/', book_dt_elem.a.get('href'))))

    await asyncio.gather(*[
        parse_book(book_url, book_name, session) for book_url, book_name in zip(book_urls, book_names)
    ])


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

        await asyncio.gather(*[
            parse_page(page_url, session) for page_url in chain(
                (TAG_URL, ),
                (urljoin(SITE_BASE_URL, a["href"]) for a in soup.find('div', {'class': 'spager'}).find_all('a', href=True)),
            )
        ])


if __name__ == "__main__":
    start = datetime.datetime.now()
    logger.info('Start')
    asyncio.run(main())
    logger.info(f"Done in {datetime.datetime.now() - start}")
