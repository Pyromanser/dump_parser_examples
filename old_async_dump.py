import aiohttp
import asyncio
import os
import re
import datetime
import requests

from aiofile import async_open
from bs4 import BeautifulSoup
from termcolor import colored


SITE = 'https://translatedby.com/'
TAG = 'GURPS'


async def scan_article(article_url, article_name, base_dir_name, session):
    # print("scanning article - {url} - {status}".format(**{'url': colored(article_url, "blue"), "status": colored("START", "blue")}))
    status_url = f"{article_url}stats/"
    response = await session.request(method='GET', url=status_url)
    body = await response.text()

    soup = BeautifulSoup(body, 'html.parser')
    about = soup.find(id="about-translation").blockquote
    about = about.string.strip() if about else ''

    DIR = os.path.join(base_dir_name, article_name)
    os.mkdir(DIR)
    # print("scanning article - {url} - {status}".format(**{'url': colored(article_url, "cyan"), "status": colored("SAVING IN PROCESS", "cyan")}))

    async with async_open(os.path.join(DIR, 'about.txt'), 'wt', encoding='utf-8') as f:
        await f.write('URL - {url}\n'.format(url=article_url))
        await f.write(about)

    async with async_open(os.path.join(DIR, 'result.txt'), 'wb', encoding='utf-8') as f:
        response = await session.request(method='GET', url=article_url + '.txt')
        await f.write(await response.read())
    # print("scanning article - {url} - {status}".format(**{'url': colored(article_url, "green"), "status": colored("DONE", "green")}))


async def scan_page(page_url, site_url, base_dir_name, session):
    # print("scanning page - {url} - {status}".format(**{'url': colored(page_url, "blue"), "status": colored("START", "blue")}))
    response = await session.request(method='GET', url=page_url)
    body = await response.text()
    soup = BeautifulSoup(body, 'html.parser')
    articles = [
        {
            'article_name': dt.a.string.replace('\n', ' '),
            'article_url': f"{site_url}{re.sub('/trans/$', '/', dt.a.get('href'))[1:]}",
        } for dt in soup.find('dl', {'class': 'translations-list'}).find_all('dt')
    ]
    # print("scanning page - {url} - {status}".format(**{'url': colored(page_url, "green"), "status": colored("DONE", "green")}))
    await asyncio.gather(*[scan_article(session=session, base_dir_name=base_dir_name, **article) for article in articles])


async def main(site_url, tag, base_dir_name):
    response = requests.get(site_url)
    if response.status_code != requests.codes.ok:  # Check site is online
        print("{site} status - {status_code}".format(**{'site': SITE, 'status_code': colored(str(response.status_code), 'red')}))
        return

    print("{site} status - {status_code}".format(**{'site': SITE, 'status_code': colored(str(response.status_code), 'green')}))

    if os.path.isdir(base_dir_name):
        print('Dump exists. Aborting.')
        return

    print('Create dump directory.')
    os.mkdir(os.path.join(base_dir_name))

    print('Check pages count')
    resource_url = f'{site_url}you/tags/{tag}/'
    response = requests.get(resource_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    pages = int(soup.find('div', {'class': 'spager'}).find_all('a')[-1].string)
    print(f'Found {pages} pages')

    print('Start aggregation and downloading...')
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[scan_page(page_url, site_url, base_dir_name, session) for page_url in (
                f'{resource_url}?page={i}' for i in range(1, pages+1)
            )]
        )


if __name__ == "__main__":
    start = datetime.datetime.now()
    print('Start')
    asyncio.run(main(SITE, TAG, datetime.datetime.now().strftime('%Y-%m-%d')))
    print("Done")
    print(datetime.datetime.now() - start)
