import json
import scrapy

class doubanSpider(scrapy.Spider):
    name = 'serie'
    allowed_domains = ["douban.com"]
    start_urls = ['https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=%E7%94%B5%E8%A7%86%E5%89%A7&start={}'.format(i)
                  for i in list(range(0, 9980, 20)) + [9979]] # 9979


    def start_requests(self):
        headers = {'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en,zh;q=0.8,fr;q=0.6',
                   'Connection': 'keep-alive',
                   'Host': 'movie.douban.com',
                   'Referer' : 'https://movie.douban.com/tv/',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=headers, method='GET', callback=self.parse)

    def parse(self, response):
        menu = response.body.decode('utf-8')
        menuList = json.loads(menu)    # 字典
        for item in menuList['data']:
            serie = {'movieID': int(item['id']),
                     'cover': item['cover'],
                     'title-zh': item['title'],
                     'url': item['url']
                    }
            yield serie

    