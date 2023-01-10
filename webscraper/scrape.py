import requests
import re
import time
import warnings
import pandas as pd
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse
from multiprocessing import Pool
from fuzzywuzzy import fuzz

warnings.filterwarnings('ignore')

input_csv = 'sks-backend/webscraper/data/web_scraper_urls'


def clean_scraper_output(df):
    cleaned = df[
        (df['status_code'] == 200) &
        (df['page_text'].notna()) &
        (df['partial_ratio'] > 50)].rename(columns={'page_text': 'website_text'})[['BN', 'website_text']]
    cleaned['website_text'] = cleaned['website_text'].apply(
        lambda x: x.lower()).str.replace('menu', '').str.replace('home', '')

    return cleaned

# function to create domain based off website


def create_do(web):

    if type(web) == str and web != 'none':
        if '&' in web:
            web = web.split('&')[0].strip()
        web = web.lower().replace('https://', '').replace('http://', '').replace('htt:', '').replace('www.', '').strip()
        if '/' in web:
            web = web.split('/')[0].replace(' ', '').strip()
        if '?' in web:
            web = web.split('?')[0].replace(' ', '').strip()
        if '@' in web or '.' not in web or ' ' in web:
            return None

        return web
    else:
        return None

# create website using http:// + domain


def create_website_1(domain_clean):

    if domain_clean:
        website = 'http://' + domain_clean
    else:
        website = None

    return website

# create website using https:// + domain


def create_website_2(domain_clean):

    if domain_clean:
        website = 'https://' + domain_clean
    else:
        website = None

    return website

# create website using https://www. + domain


def create_website_3(domain_clean):

    if domain_clean:
        if 'ww' not in domain_clean:
            website = 'https://www.' + domain_clean
        else:
            website = None
    else:
        website = None

    return website

# clean web page text


def clean_text(html_text):
    try:
        clean_text = re.sub(r"\s+", " ", html_text).strip()
        return clean_text
    except:
        return None

# find meta descriptions from webpage


def get_meta(soup_object):

    if soup_object:
        try:
            meta1 = soup_object.find("meta", {"property": "og:description"})[
                'content'].replace('\n', '').replace('\t', '').strip()
        except:
            meta1 = None

        try:
            meta2 = soup_object.find("meta", {"name": "description"})[
                'content'].replace('\n', '').replace('\t', '').strip()
        except:
            meta2 = None

        # if both exists, choose the meta description with longest length
        if meta1 and meta2:

            if len(meta1) >= len(meta2):

                return meta1
            else:
                return meta2

        elif meta1:
            return meta1

        elif meta2:
            return meta2

        else:
            return None

    else:
        return None

# find keywords from meta tag


def get_keywords(soup_object):

    if soup_object:
        try:
            keywords = soup_object.find("meta", {"name": "keywords"})['content'].replace(
                '\n', '').replace('\t', '').lower().strip()
        except:
            keywords = None

        return keywords
    else:
        return None

# main scraping function


def get_data(company_website):

    try:

        domain = create_do(company_website)

        # first try to scrape the website
        webs = create_website_1(domain)
        r = requests.get(webs,
                         timeout=18.0,
                         allow_redirects=True,
                         verify=False)
        soup = bs(r.text)

        if len(soup.text) < 30:
            # second try to scrape the website
            webs = create_website_2(domain)
            r = requests.get(webs,
                             timeout=18.0,
                             allow_redirects=True,
                             verify=False)
            soup = bs(r.text)

            if len(soup.text) < 30:
                # third try to scrape the website
                webs = create_website_3(domain)
                r = requests.get(webs,
                                 timeout=18.0,
                                 allow_redirects=True,
                                 verify=False)
                soup = bs(r.text)

    except:

        soup = None

    if soup:

        status = r.status_code
        returned_web = r.url

        # create domain of the redirected website
        try:
            returned_dom = create_do(returned_web)
        except:
            returned_dom = ''

        # if redirected domain is in the for_sale list, indicate website is not valid
        try:
            website_sale = 0
            for sale in for_sale:
                if sale in returned_dom:
                    website_sale = 1
                    break
        except:
            website_sale = 0

        # find fuzzy match ratios of initial domain and redirected domain
        try:
            ratio = fuzz.ratio(domain, returned_dom)
            ratio_part = fuzz.partial_ratio(domain, returned_dom)
        except:
            ratio = ''
            ratio_part = ''

        meta_description = get_meta(soup)
        keyword = get_keywords(soup)
        text = clean_text(soup.text)

        return {
            'website': company_website,
            'redirected_domain': returned_dom,
            'ratio': ratio,
            'partial_ratio': ratio_part,
            'status_code': status,
            'website_for_sale': website_sale,
            'meta_description': meta_description,
            'keywords': keyword,
            'page_text': text
        }


# import data (not including .csv)
import_path = input_csv
df = pd.read_csv(f'{import_path}.csv.zip',
                 index_col=False)

# indication of a website now for sale i.e if word appear in redirected domain --- >> sale
for_sale = ['forsale', 'godaddy', 'domain']

# set workers based on cpu
# scrape data from unique websites
workers = 10
with Pool(workers) as p:
    data = p.map(get_data, list(set(df['website'])))

# some times we have some Null values slipping through ---> skip over them
valid_returns = []
for d in list(data):
    if d:
        valid_returns.append(d)
result_df = pd.DataFrame(valid_returns)

# merge back
df1 = df.merge(result_df, on='website', how='left')

# export path (not including .csv)
export_path = '/Users/brittany/repos/sks-project/sks-backend/webscraper/data/'
df1.to_csv(f'{export_path}.csv.zip',
           index=False,
           quoting=2,
           compression='zip')
