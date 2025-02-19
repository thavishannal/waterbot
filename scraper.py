import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
URL = os.getenv('RUNNING_URL')
SUBPAGES = int(os.getenv('RUNNING_SUBPAGES'))
FILENAME = os.getenv('RUNNING_QUOTES_FILENAME')


def scrape(url):
    '''Returns HTML code for cloudflare page.'''
    options = Options()
    options.headless = True
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(8)
    page_source = driver.page_source
    driver.quit()
    soup = BeautifulSoup(page_source, 'html.parser')
    return soup

def quotes_per_page(soup): 
    '''Parses HTML code for quotes and authors based on their tags'''   
    author_tags = soup.find_all('a', class_ = "bq-aut")
    authors = [tag.text for tag in author_tags]
    quote_tags = soup.find_all('div', {'style': 'display: flex;justify-content: space-between'})
    quotes = [tag.text.strip() for tag in quote_tags]
    return authors, quotes

def brainyquote_urls(main_url, num_subpages):
    url_list = [main_url]
    for subpage in range(2,num_subpages+1):
        url_list.append(f'{main_url}_{subpage}')
    return url_list

def store_quotes(url_list, filename):
    author_list = []
    quote_list = []
    for i, url in enumerate(url_list):
        page_source = scrape(url)
        new_authors, new_quotes = quotes_per_page(page_source)
        if len(new_authors) == len(new_quotes):
            author_list.extend(new_authors)
            quote_list.extend(new_quotes)
            print(f"Page {i+1}'s {len(new_quotes)} quotes have been scraped.")
            time.sleep(3)
        else:
            print(f"The author and quote lists scraped from page {i+1} don't match")
    dict = {'Author': author_list, 'Quote': quote_list}
    df = pd.DataFrame(dict)
    df.to_csv(filename, index=False)


url_list = brainyquote_urls(URL, SUBPAGES)
store_quotes(url_list, FILENAME)