import scrapy
import json
from scrapy.selector import Selector

cats = ['%E5%89%A7%E6%83%85', '%E7%88%B1%E6%83%85', '%E5%96%9C%E5%89%A7', '%E7%A7%91%E5%B9%BB', '%E5%8A%A8%E4%BD%9C', '%E6%82%AC%E7%96%91',
        '%E7%8A%AF%E7%BD%AA', '%E6%81%90%E6%80%96', '%E9%9D%92%E6%98%A5', '%E5%8A%B1%E5%BF%97', '%E6%88%98%E4%BA%89', '%E6%96%87%E8%89%BA',
        '%E9%BB%91%E8%89%B2%E5%B9%BD%E9%BB%98', '%E4%BC%A0%E8%AE%B0', '%E6%83%85%E8%89%B2', '%E6%9A%B4%E5%8A%9B', '%E9%9F%B3%E4%B9%90', '%E5%AE%B6%E5%BA%AD']
limits = [2620, 1700, 1040, 100, 340, 440, 280, 40, 520, 240, 540, 120, 40, 140, 0, 0, 140, 660]
class doubanSpider(scrapy.Spider):
    name = 'serie3'
    allowed_domains = ["douban.com"]
    # start_urls = ['https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=%E7%94%B5%E8%A7%86%E5%89%A7,%E5%A4%A7%E9%99%86,{}&start={}'.format(x[0], i)
    #               for i in range(0, x[1] + 20, 20) for x in zip(cats, limits)] # 9979


    def start_requests(self):
        headers = {'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en,zh;q=0.8,fr;q=0.6',
                   'Connection': 'keep-alive',
                   'Host': 'movie.douban.com',
                   'Referer' : 'https://movie.douban.com/tv/',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}
        urls = []
        for c, l in zip(cats, limits):
            urls_sub = ['https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=%E7%94%B5%E8%A7%86%E5%89%A7,%E5%A4%A7%E9%99%86,{}&start={}'.format(c, i)
                        for i in range(0, l + 20, 20)]
            urls += urls_sub

        for url in urls:
            yield scrapy.Request(url=url, headers=headers, method='GET', callback=self.parse)

    def parse(self, response):
        menu = response.body.decode('utf-8')
        menuList = json.loads(menu)    # 字典
        for item in menuList['data']:
            serie = {'movieID': int(item['id']),
                     'title-zh': item['title'],
                     'url': item['url']
                    }
            yield serie
    