import scrapy
import json
from scrapy.selector import Selector

categories = ['%E7%83%AD%E9%97%A8', '%E5%9B%BD%E4%BA%A7%E5%89%A7', '%E7%BE%8E%E5%89%A7', '%E8%8B%B1%E5%89%A7', '%E9%9F%A9%E5%89%A7',
              '%E6%97%A5%E5%89%A7', '%E6%B8%AF%E5%89%A7', '%E6%97%A5%E6%9C%AC%E5%8A%A8%E7%94%BB', '%E7%BB%BC%E8%89%BA']
catName = ['', '国产剧', '美剧', '英剧', '韩剧', '日剧', '港剧', '日本动漫', '综艺']
tour = 8
class doubanSpider(scrapy.Spider):
    name = 'serie'
    allowed_domains = ["douban.com"]
    start_urls = ['https://movie.douban.com/j/search_subjects?type=tv&tag={}&sort={}&page_limit=20&page_start={}'.format(categories[tour], key, i)
                  for key in ['recommend', 'time', 'rank']
                  for i in range(0, 500, 20)]


    def start_requests(self):
        headers = {'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en,zh;q=0.8,fr;q=0.6',
                   'Connection': 'keep-alive',
                   'Host': 'movie.douban.com',
                   'Referer' : 'https://movie.douban.com/tv/',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=headers, method='GET', callback=self.parse_menu)

    def parse_menu(self, response):
        menu = response.body.decode('utf-8')
        menuList = json.loads(menu)    # 字典
        for item in menuList['subjects']:
            href = item['url']
            yield response.follow(href, self.parse_detail)

    def parse_detail(self, response):
        movie = {'分类': [catName[tour]]}

        url = response.url
        movie['URL'] = url
        hasmovieID = 'subject' in url
        movie['ID'] = int(url.split('/')[-2]) if hasmovieID else None


        title = response.css("h1 span::text").extract()
        if title:
            movie['片名'] = title[0].strip()
        if len(title) > 1:
            movie['年代'] = int(title[1].strip()[1:5])
        else:
            year = response.css("span.year::text").extract()
            if year:
                movie['年代'] = year[0].strip()[1:5]

        mainpic = response.css("div[id*=mainpic] a::attr(href)").extract()
        if mainpic:
            movie['封面'] = mainpic[0].strip()

        info = response.css("div[id*=info]")
        infostr = info.extract_first()
        infolist = infostr.replace('<br>', '').split('\n')
        infolist = [Selector(text=item.strip()) for item in infolist if item.strip()]

        punct = ", ; . / : ， 。 ： ； /".split()
        for item in infolist:
            detail = item.css("::text").extract()
            if detail:
                itemName = detail.pop(0).strip()
                if itemName in ['导演', '编剧', '主演']:  # need to link actors, directors and writors to personal ID
                    itemContents_dict = []
                    membersinfo = item.css("span.attrs a")
                    for m in membersinfo:
                        href = m.css("::attr(href)").extract()
                        name = m.css("::text").extract()
                        if href and name:
                            hasactorID = 'celebrity' in href[0]
                            actorID = int(href[0].split('/')[-2]) if hasactorID else None
                            itemContents_dict.append({"ID":actorID, "姓名": name[0].strip()})
                    if itemContents_dict:
                        movie[itemName] = itemContents_dict
                    continue    # go to next item

                if itemName[-1] in punct:
                    itemName = itemName[:-1]
                itemContents = [e.strip() for e in detail if e.strip() and e.strip() not in punct]
                if len(itemContents) == 1 and itemName != '类型':
                    itemContents = itemContents[0]

                if itemName == '集数':
                    numEpi = ''.join([e for e in itemContents if e.isdigit()])
                    if numEpi:
                        itemContents = int(numEpi)

                elif itemName == '季数':
                    if isinstance(itemContents, list):
                        itemContents = len(itemContents)
                    else:
                        itemContents = int(itemContents)

                elif itemName == '单集片长':
                    itemContents = itemContents.replace(' ', '').replace('min', '分钟')
                    if '分钟' not in itemContents:
                        itemContents += '分钟'

                movie[itemName] = itemContents




        rating_avg = response.css("strong.rating_num::text").extract()
        if rating_avg:
            movie['豆瓣评分'] = float(rating_avg[0])

        rating_num = response.css("div.rating_sum a span::text").extract()
        if rating_num:
            movie['评价人数'] = int(rating_num[0])

        weights = response.css("div.ratings-on-weight div.item span.rating_per::text").extract()
        rating_per = {str(5 - i)+"星": weight for i, weight in enumerate(weights)}
        movie['评分'] = rating_per

        yield movie

