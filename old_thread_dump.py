import requests
import datetime
import os
import re
from itertools import chain, repeat
from termcolor import colored
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

SITE = 'https://translatedby.com/'
TAG = 'GURPS'


def scan_article(article_url, article_name, base_dir_name):
    status_url = f"{article_url}stats/"
    response = requests.get(status_url)
    body = response.text

    soup = BeautifulSoup(body, 'html.parser')
    about = soup.find(id="about-translation").blockquote
    about = about.string.strip() if about else ''

    DIR = os.path.join(base_dir_name, article_name)
    os.mkdir(DIR)

    with open(os.path.join(DIR, 'about.txt'), 'wt', encoding='utf-8') as f:
        f.write('URL - {url}\n'.format(url=article_url))
        f.write(about)

    with open(os.path.join(DIR, 'result.txt'), 'wb') as f:
        r = requests.get(article_url + '.txt')
        f.write(r.content)


def scan_page(page_url, site_url):
    response = requests.get(page_url)
    body = response.text
    soup = BeautifulSoup(body, 'html.parser')
    articles = [
        {
            'article_name': dt.a.string.replace('\n', ' '),
            'article_url': f"{site_url}{re.sub('/trans/$', '/', dt.a.get('href'))[1:]}",
        } for dt in soup.find('dl', {'class': 'translations-list'}).find_all('dt')
    ]
    return articles


def main(site_url, tag, base_dir_name):
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
    with ThreadPoolExecutor() as executor:
        articles = list(chain(*executor.map(scan_page, (f'{resource_url}?page={i}' for i in range(1, pages+1)), repeat(site_url))))
        executor.map(scan_article, (i["article_url"] for i in articles), (j["article_name"] for j in articles), repeat(base_dir_name))


if __name__ == "__main__":
    start = datetime.datetime.now()
    print('Start')
    main(SITE, TAG, datetime.datetime.now().strftime('%Y-%m-%d'))
    print("Done")
    print(datetime.datetime.now() - start)
