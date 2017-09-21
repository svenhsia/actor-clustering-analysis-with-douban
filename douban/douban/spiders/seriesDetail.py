import scrapy
from scrapy.selector import Selector
from pymongo import MongoClient

class doubanSpider(scrapy.Spider):
    name = 'serieDetail'
    allowed_domains = ["douban.com"]


    def start_requests(self):
        headers = {'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en,zh;q=0.8,fr;q=0.6',
                   'Connection': 'keep-alive',
                   'Host': 'movie.douban.com',
                   'Referer' : 'https://movie.douban.com/tv/',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}
        db = MongoClient().Douban
        mongoCol = db.total
        serie_all = list(mongoCol.find({'request-code': 0}))
        movies = db.movies
        for s in serie_all:
            url = s['url']
            yield scrapy.Request(url=url, headers=headers, method='GET', callback=self.parse)


    def parse(self, response):
        movie = {}
        url = response.url
        movie['url'] = url
        movie['movieID'] = int(url.split('/')[-2])

        title = response.css("h1 span::text").extract()
        if title:
            movie['title'] = title[0].strip()
        if len(title) > 1:
            movie['year'] = int(title[1].strip()[1:5])
        else:
            year = response.css("span.year::text").extract()
            if year:
                movie['year'] = year[0].strip()[1:5]

        mainpic = response.css("div[id*=mainpic] a::attr(href)").extract()
        if mainpic:
            movie['cover'] = mainpic[0].strip()

        info = response.css("div[id*=info]")
        infostr = info.extract_first()
        infolist = infostr.replace('<br>', '').split('\n')
        infolist = [Selector(text=item.strip()) for item in infolist if item.strip()]

        itemNameDict = {"导演": 'director', "编剧": 'scripter', "主演": 'actor', "类型": 'subject', "集数": 'episode',
                        "季数": 'season', "单集片长": 'length', "片长": 'length', "官方网站": 'website', "制片国家/地区": "country/region",
                        "语言": 'language', "首播": 'date-published', "上映日期": 'date-published', "又名": 'other-name', "IMDb链接": 'IMDb'}
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
                            itemContents_dict.append({"personID":actorID, "name": name[0].strip()})
                    if itemContents_dict:
                        movie[itemNameDict[itemName]] = itemContents_dict
                    continue    # go to next item

                if itemName[-1] in punct:
                    itemName = itemName[:-1]
                itemContents = [e.strip() for e in detail if e.strip() and e.strip() not in punct]

                if itemName == '集数':
                    numEpi = ''.join([e for e in itemContents[0] if e.isdigit()])
                    if numEpi:
                        itemContents = int(numEpi)

                elif itemName == '季数':
                    if isinstance(itemContents[0], list):
                        itemContents = len(itemContents[0])
                    else:
                        itemContents = int(itemContents[0])

                elif itemName in ['单集片长', '片长']:
                    itemContents = itemContents[0].replace(' ', '').replace('min', '分钟')
                    if '分钟' not in itemContents:
                        itemContents += '分钟'

                elif itemName in ['制片国家/地区', '语言', '首播', '上映日期', '又名']:
                    if len(itemContents) == 1:
                        itemContents = [s.strip() for s in itemContents[0].split('/') if s.strip()]


                elif itemName not in itemNameDict:
                    continue

                movie[itemNameDict[itemName]] = itemContents

        rating_avg = response.css("strong.rating_num::text").extract()
        if rating_avg:
            movie['rating'] = float(rating_avg[0])

        rating_num = response.css("div.rating_sum a span::text").extract()
        if rating_num:
            movie['num-rates'] = int(rating_num[0])

        weights = response.css("div.ratings-on-weight div.item span.rating_per::text").extract()
        rating_per = {str(5 - i)+"星": weight for i, weight in enumerate(weights)}
        movie['stars'] = rating_per

        # yield movie

        db = MongoClient().Douban
        movies = db.movies
        total = db.total
        # movies.insert_one(movie)
        movieType = total.find_one({'movieID': movie['movieID']})['type']
        total.update_one({'movieID': movie['movieID']}, {'$set': {'request-code': 200}})
        movie['type'] = movieType


        recommendations = []
        related_list = response.css("div.recommendations-bd dl")
        for related_one in related_list:
            link_to = related_one.css("dt a::attr(href)").extract_first()
            if link_to:
                link_url = link_to.split('?')[0].strip()
                link_id = int(link_url.split('/')[-2])
                recommendations.append(link_id)
                exist = total.find_one({'movieID': link_id})
                if not exist:
                    link_title = related_one.css("dd a::text").extract_first()
                    total.insert_one({'movieID': link_id, 'url': link_url, 'title-zh': link_title, 'type': movieType, 'country/region': [], 'request-code': 0})
                else:
                    new_type = list(set(movieType + exist['type']))
                    total.update_one({'movieID': link_id}, {'$set': {'type': new_type}})

        movie['recommendation'] = recommendations
        yield movie
        movies.insert_one(movie)
