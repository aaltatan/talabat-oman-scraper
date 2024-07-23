import scrapy
from scrapy.http import FormRequest, Response

import json
import math


class ItemsSpider(scrapy.Spider):
    
    name = "items"
    allowed_domains = ["www.talabat.com"]
    start_urls = ["https://www.talabat.com/oman/all-areas/groceries"]


    def parse(self, res: Response):
        response: scrapy.Selector = res.copy()
        next_data = response.css('#__NEXT_DATA__::text').get('')
        data = json.loads(next_data)
        
        areas: dict = data['props']['pageProps']['areas']
        for area_lists in areas.values():
            
            for area in area_lists:
                id = str(area['id'])
                slug = area['slug']
            
                params = {
                    'countrySlug': 'oman',
                    'vertical': 'groceries',
                    'areaId': id,
                    'areaSlug': slug,
                }
                
                yield FormRequest(
                    url=f'https://www.talabat.com/_next/data/manifests/vertical/vertical-area.json',
                    formdata=params,
                    method='GET',
                    callback=self.parse_pagination,
                    cb_kwargs={'slug': slug, 'id': id}
                )
            
    def parse_pagination(self, res: Response, slug: str, id: str):
        response: dict = json.loads(res.text)
        total_vendors = response['pageProps']['metadata']['totalVendors']
        pages_count = math.ceil(total_vendors / 20)
        
        for idx in range(1, pages_count + 1):
            params = {
                'countrySlug': 'oman',
                'vertical': 'groceries',
                'areaId': id,
                'areaSlug': slug,
                'page': str(idx)
            }
            
            yield FormRequest(
                url=f'https://www.talabat.com/_next/data/manifests/vertical/vertical-area.json',
                formdata=params,
                method='GET',
                callback=self.parse_shops,
                cb_kwargs={'area_id': id}
            )
            
    def parse_shops(self, res: Response, area_id: str):
        response: dict = json.loads(res.text)
        shops = response['pageProps']['gtmEventData']['shops']
        for shop in shops:
            branch_id = shop['branchId']
            branch_slug = shop['branchSlug']
            
            params = {
                'aid': area_id,
                'countrySlug': 'oman',
                'vertical': 'grocery',
                'branchId': str(branch_id),
                'branchSlug': branch_slug,
            }
            
            url = f'https://www.talabat.com/_next/data/manifests/menu.json'
            
            request =  FormRequest(
                url=url,
                formdata=params,
                method='GET',
                cb_kwargs={
                    'branch_slug': branch_slug, 
                    'branch_id': branch_id,
                    'area_id': area_id,
                },
                callback=self.parse_categories
            )
            yield request
            
            
    def parse_categories(
        self, 
        res: Response, 
        branch_slug: str, 
        branch_id: str, 
        area_id: str
    ):
        
        response: dict = json.loads(res.text)
        page_props: dict = response['pageProps']
        
        if page_props.get('initialState'):
            
            categories = response['pageProps']['initialState']['categories']
            
            for cat in categories:
                
                category_slug = cat['slug']
                subcategories = cat['subCategories']
                
                for subcategory in subcategories:
                    
                    subcategory_slug = subcategory['slug']
                    
                    url = 'https://www.talabat.com/_next/data/manifests/grocery-items.json'
                    
                    params = {
                        'aid': area_id,
                        'countrySlug': 'oman',
                        'vertical': 'grocery',
                        'branchId': str(branch_id),
                        'branchSlug': branch_slug,
                        'categorySlug': category_slug,
                        'subCategorySlug': subcategory_slug,
                    }
                    
                    yield FormRequest(
                        url=url,
                        formdata=params,
                        method='GET',
                        cb_kwargs={
                            'branch_slug': branch_slug, 
                            'branch_id': branch_id,
                            'category_slug': category_slug,
                            'subcategory_slug': subcategory_slug,
                            'area_id': area_id,
                        },
                        callback=self.parse_items_pagination
                    )
    
    
    def parse_items_pagination(
        self, 
        res: Response,
        branch_slug: str,
        branch_id: str,
        category_slug: str,
        subcategory_slug: str,
        area_id: str,
    ):
        response: dict = json.loads(res.text)
        pages_count = response['pageProps']['initialState']['itemsData']['pageCount']
        
        for idx in range(1, pages_count + 1):
            
            url = 'https://www.talabat.com/_next/data/manifests/grocery-items.json'
            
            params = {
                'countrySlug': 'oman',
                'vertical': 'grocery',
                'branchId': str(branch_id),
                'branchSlug': branch_slug,
                'categorySlug': category_slug,
                'subCategorySlug': subcategory_slug,
                'page': str(idx),
                'aid': area_id,
            }
            
            yield FormRequest(
                url=url,
                formdata=params,
                method='GET',
                callback=self.parse_items
            )
            
    
    def parse_items(self, res: Response):
        response: dict = json.loads(res.text)
        store = response['pageProps']['initialState']['groceryStore']
        items = response['pageProps']['initialState']['itemsData']['items']
        
        for item in items:
            yield {
                'store': store,
                **item
            }