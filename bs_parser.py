from datetime import datetime
from bs4 import BeautifulSoup
from retry import retry
import pandas as pd
import requests 
import logging
import os 


@retry(tries=3, delay=0.5)
def get_page(url, proxy):
    main_page = requests.get(url, proxies=proxy)
    if main_page.status_code == 200:
        return main_page.text


@retry(tries=3, delay=0.5)
def parse_single_page(url, proxy):
    page = get_page(url, proxy)
    soup = BeautifulSoup(page, 'lxml')
    date = soup.find('span', class_='news_date').text
    article = soup.find('div', id='initial_news_story')
    text = " ".join([p.text for p in article.findAll('p')])

    return date, text


@retry(tries=3, delay=0.5)
def parse_article_links(main_url, articles, proxy):

    data = {'date': [], 'title': [], 'text': [],
            'comments': [], 'link': []}

    for article in articles:        
        if article.a:

            try:
                data['title'].append(
                    article.a.text) # Append Title
            except:
                data['title'].append("")

            try:
                data['link'].append(
                    main_url + article.a['href']) # Append Link
            except:
                data['link'].append("")
            
            try:
                temp = parse_single_page(main_url + article.a['href'], proxy)
                data['date'].append(temp[0]) # Append Date
                data['text'].append(temp[1]) # Append Full text
            except:
                data['date'], data['text'] = ""

            try:
                data['comments'].append(
                    article.find('span', class_='comm_num').text if article.find('span', class_='comm_num') else 0) # Append Comments if not None else 0
            except:
                data['comments'].append("")

                
    return data


@retry(tries=3, delay=0.5)
def parse(main_url, proxy):

    # Get main html page and turn it into bs4 instance
    html_main = get_page(main_url, proxy)
    main_soup = BeautifulSoup(html_main, 'lxml')    
    logger.info('Retrieved main soup')

    # Get the divs of all the articles
    articles = main_soup.findAll('div', class_='cat_news_item')
    logger.info('Retrieved articles divs')

    # Parse every article link 
    parsed_data = parse_article_links(main_url, articles, proxy)
    logger.info('Data parsed')

    # Create CSV file and save it
    df = pd.DataFrame(parsed_data)
    df.to_csv('zkn_parsed_daily_articles')


if __name__ == "__main__":
    # Initialize logger 
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    
    # Use proxies in case of ip restrictions
    ip_addrs = ['88.119.193.254:51242', '148.77.34.196:39175', '88.87.90.107:8080']
    for ip in ip_addrs:
        try: 
            proxy = {'http': ip, 'https': ip}
            parse('https://www.zakon.kz/news', proxy)
        except Exception as e:
            logger.error(str(e))
        else:
            logger.info("Parsing finished")
            break
