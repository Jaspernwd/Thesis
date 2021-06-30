from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import time
from lxml.html import fromstring
from itertools import cycle
import traceback

def scrape_trustpilot(companies, proxy_set = {}):
    # The code cycled through proxies as sometimes we would get blocked by Trustpilot
    proxies = proxy_set
    print(f'{len(proxies)} Proxies available!')
    proxy_pool = cycle(proxies)
    proxy = next(proxy_pool)
    print(f'Currently on this proxy: {proxy}')

    total_dataframe = []
    for company in companies:
        page = company['url']
        last_visited = ['placeholder']

        results = []

        while page != last_visited[-1]:
            # Iterate through all pages with reviews
            try:
                # Parse raw HTML
                soup = BeautifulSoup(requests.get(page, proxies = {'http': proxy, 'https': proxy}).text, 'html.parser')
                print('New Page!')
                # Sleep for a short while
                time.sleep(1)
                # Find all reviews
                reviews = soup.find_all('div', class_ = 'review-content')

                if len(reviews) == 0:
                    # No reviews found may indicate that this IP is blocked by the server of Trustpilot
                    print('Switching proxy!')
                    proxy = next(proxy_pool)
                    print(f'Currently on this proxy: {proxy}')
                    soup = BeautifulSoup(requests.get(page, proxies = {'http': proxy, 'https': proxy}).text, 'html.parser')
                    time.sleep(1)
                    reviews = soup.find_all('div', class_='review-content')

                for review in reviews:
                    # Extract publish datetime
                    time_div = review.find("script", type="application/json")
                    d = json.loads(time_div.contents[0])

                    # Text not always present in review
                    # Find text
                    txt = review.find('p', class_ = 'review-content__text')
                    if txt:
                        txt = txt.text.strip()

                    # Store review info in dict
                    review_info = {
                        'datetime': d['publishedDate'],
                        'title': review.find('a', class_ = 'link link--large link--dark').text.strip(),
                        'stars': review.find('img')['alt'],
                        'text': txt,
                        'company': company['name']
                    }
                    results.append(review_info)

                last_visited.append(page)
                # Search for a next page button
                next_page_button = soup.find('a', class_ = 'button button--primary next-page')
                if next_page_button:
                    # If the button is found, we go to the next page
                    page = "https://nl.trustpilot.com" + "{}" .format(next_page_button['href'])

            except:
                # If we receive an error, we switch IP
                proxy = next(proxy_pool)
                print('Error in proxy, picking next.')
                print(f'Currently on this proxy: {proxy}')

        total_dataframe.extend(results)
        name = company['name']
        print(f'{name} done!')
        print(f'crawled {len(total_dataframe)} reviews so far.')

    return total_dataframe

if __name__ == '__main__':
    with open('/Users/jaspernieuwdorp/Downloads/proxies.txt') as f:
        proxies = f.readlines()

    proxies = {proxy.strip().split(':')[0] for proxy in proxies}

    print("Let's start scraping!")
    websites = [
        {'name': 'PostNL', 'url': 'https://nl.trustpilot.com/review/www.postnl.com'},
        {'name': 'DHL', 'url': 'https://nl.trustpilot.com/review/www.dhl.nl'},
        {'name': 'GLS', 'url': 'https://nl.trustpilot.com/review/www.gls-netherlands.com'},
        {'name': 'Ziggo','url': 'https://nl.trustpilot.com/review/www.ziggo.nl'},
        {'name': 'Trunkrs', 'url': 'https://nl.trustpilot.com/review/trunkrs.nl'},
        {'name': 'ParcelParcel', 'url': 'https://nl.trustpilot.com/review/www.parcelparcel.com'}
    ]
    print(f"We're scraping {len(websites)} websites!")

    results = pd.DataFrame(scrape_trustpilot(websites, proxies))
    results.to_csv('scraping-results.csv')
    print("Everything done and saved!")
    print(f'Crawled {len(results)} in total.')