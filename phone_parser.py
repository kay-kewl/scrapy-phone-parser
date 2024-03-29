import time
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urlparse, urlencode, urlunparse
import pandas as pd

COUNT = 100
TIMEOUT = 15
TIME_BETWEEN_REQUESTS = 3


class RotateUserAgentMiddleware(UserAgentMiddleware):
    def __init__(self, user_agent=''):
        self.user_agent = user_agent

    def process_request(self, request, spider):
        ua = UserAgent()
        request.headers.setdefault('User-Agent', ua.random)


def wait():
    time.sleep(TIME_BETWEEN_REQUESTS)


class OzonSpider(scrapy.Spider):
    name = 'ozon_spider'
    params = {'sorting': 'rating'}
    phone_count = 0

    custom_settings = {
        'ROBOTSTXT_OBEY': False,  # ignore the robots.txt file
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.RotateUserAgentMiddleware': 110
        },
        'LOG_LEVEL': 'WARNING',
        'FEED_FORMAT': 'json',  # set the output format to JSON
        'FEED_URI': 'results.json'  # set the output file to results.json
    }

    def __init__(self):
        options = Options()
        options.headless = True
        self.driver = webdriver.Edge(options=options)

    def start_requests(self):
        self.driver.get('https://www.ozon.ru')
        wait()

        self.navigate_to_products()

        # dummy request, since scrapy gets error 403 when trying to scrape ozon products
        yield scrapy.Request(url='https://google.com', callback=self.parse_product)

    def parse_product(self, response):
        while self.phone_count < COUNT:
            product_links = self.collect_product_links()

            for link in product_links:
                if self.phone_count == COUNT:
                    break

                self.open_url_and_switch(link.get_attribute('href'))
                if (os_version := self.find_os()) is not None:
                    yield {'OS': os_version}
                self.close_tab_and_switch_back()

            self.click_next_page()

    def navigate_to_products(self):
        self.click_refresh()
        self.click_electronics()
        self.click_smartphones()
        self.sort_by_rating()

    def collect_product_links(self):
        return self.driver.find_elements(By.CSS_SELECTOR, '#paginatorContent > div > div > div > div > div > a')

    def open_url_and_switch(self, url):
        self.driver.execute_script(f"window.open('{url}', '_blank')")
        self.driver.switch_to.window(self.driver.window_handles[1])

        wait()

    def find_os(self):
        # if os exists, it's a phone - yield os_version, otherwise it's not - skip them
        # p.s. some phones have OS, but the version is not specified, so we skip them
        #      since it's not clear what to yield
        try:
            section_characteristics = self.driver.find_element(By.CSS_SELECTOR, '#section-characteristics')
            section_characteristics_text = section_characteristics.text.split('\n')

            os = section_characteristics_text[section_characteristics_text.index('Операционная система') + 1]
            os_version = section_characteristics_text[section_characteristics_text.index(f'Версия {os}') + 1]

            self.phone_count += 1
            print(f'Operating system found for this product. {COUNT - self.phone_count} phones left.')

            return os_version
        except Exception:
            print('Operating system not found for this product. Skipping...')

    def close_tab_and_switch_back(self):
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])

        wait()

    def click_next_page(self):
        next_button = self.driver.find_element(
            By.CSS_SELECTOR,
            '#layoutPage > div.b2 > div.container.b6 > div:nth-child(2) > div:nth-child(2) > div.e2n > '
            'div.e3n > div > div > a.em3.b239-a0.b239-b6.b239-b1'

        )
        next_button.click()

        wait()

    def click_refresh(self):
        refresh_button = WebDriverWait(self.driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, '//button[contains(text(), "Обновить")]'))
        )
        self.driver.execute_script("arguments[0].target='_self';", refresh_button)
        ActionChains(self.driver).click(refresh_button).perform()

        wait()

    def click_electronics(self):
        electronics_button = WebDriverWait(self.driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "Электроника")]'))
        )
        self.driver.execute_script("arguments[0].target='_self';", electronics_button)
        ActionChains(self.driver).click(electronics_button).perform()

        wait()

    def click_smartphones(self):
        smartphones_button = WebDriverWait(self.driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "Смартфоны и смарт-часы")]'))
        )
        smartphones_button.click()

        wait()

    def sort_by_rating(self):
        url_parts = list(urlparse(self.driver.current_url))

        # add the sorting parameter to the URL, since buttons aren't clickable
        query = urlencode(self.params)
        url_parts[4] = query
        new_url = urlunparse(url_parts)
        self.driver.get(new_url)

        wait()

    def closed(self, reason):
        self.driver.quit()


process = CrawlerProcess()
process.crawl(OzonSpider)
process.start()

# load the data from the JSON file, count the occurrences of each OS version, sort in descending order
data = pd.read_json('results.json')
counts = data['OS'].value_counts()
distribution = counts.sort_values(ascending=False)

# save to a text file
with open('os_distribution.txt', 'w') as f:
    f.write('OS distribution:\n')
    for os_version, count in distribution.items():
        f.write(f"{os_version} — {count}\n")
