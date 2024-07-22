import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request, Response
import logging
import scrapy
import json


logging.basicConfig(
    filemode='a',
    filename='logger.log',
    format='[%(asctime)s] %(levelname)s | %(name)s => %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
    level=logging.INFO
)


class SiteMapSpider(scrapy.Spider):
    name = "sitemap"
    allowed_domains = ["www.talabat.com"]

    def start_requests(self):
        base_url = 'https://www.talabat.com/oman/sitemap'
        yield Request(base_url)

    def parse(self, res: Response):

        response: scrapy.Selector = res.copy()
        links = (
            response
            .css(
                '#__next > div:nth-child(4) > div.sc-667fe0db-0.cTJMQW > div > div:nth-child(9) a::attr(href)'
            )
            .getall()
        )
        for link in links:
            yield Request(
                url=res.urljoin(link),
                callback=self.parse_pagination
            )

    def parse_pagination(self, res: Response):

        response: scrapy.Selector = res.copy()
        last_page = response.css('ul[data-test="pagination"] li.-last a::attr(page)').get('')
        for idx in range(1, int(last_page) + 1):
            yield Request(
                url=res.url + f'?page={idx}',
                callback=self.parse_pages
            )

    def parse_pages(self, res: Response):

        response: scrapy.Selector = res.copy()
        next_data = response.css('#__NEXT_DATA__::text').get('')
        data: dict = json.loads(next_data)
        vendors: list[dict] = data['props']['pageProps']['data']['vendors']
        for vendor in vendors:
            link = vendor['menuUrl']
            yield Request(
                url=res.urljoin(link)+'?aid=5649',
                callback=self.parse_menu,
                cb_kwargs={'id': vendor['id']},
            )

    def parse_menu(self, res: Response, id: str):

        response: scrapy.Selector = res.copy()
        next_data = response.css('#__NEXT_DATA__::text').get('')
        data: dict = json.loads(next_data)
        facility: dict = json.dumps(data['props']['pageProps'])

        yield Request(
            url=f'https://www.talabat.com/nextApi/v1/restaurant/{id}/reviews',
            callback=self.parse_id,
            cb_kwargs={'facility': facility}
        )
        
    
    def parse_id(self, res: Response, facility: str):
        response: dict = json.loads(res.text)
        
        reviews: list[dict] = response['result']
        facility: dict = json.loads(facility)
        
        yield {
            **facility,
            'reviews': reviews,
        }