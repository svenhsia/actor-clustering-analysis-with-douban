import scrapy
from pymongo import MongoClient

class doubanSpider(scrapy.Spider):
    name = 'actor'
    allowed_domains = ["douban.com"]
    # start_urls = ["https://movie.douban.com/celebrity/1275025/",
    #               "https://movie.douban.com/celebrity/1042053/",
    #               "https://movie.douban.com/celebrity/1316721/"]

    def start_requests(self):
        headers = {'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en,zh;q=0.8,fr;q=0.6',
                   'Connection': 'keep-alive',
                   'Host': 'movie.douban.com',
                   'Referer' : 'https://movie.douban.com/tv/',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}

        collection = MongoClient().Douban.series
        urls = []
        serieList = list(collection.find())
        for serie in serieList:
            persons = []
            for key in ['编剧', '主演', '导演']:
                if key in serie:
                    persons += serie[key]
            for p in persons:
                if p["ID"] is not None and p["ID"] != 'None':
                    href = "https://movie.douban.com/celebrity/{}/".format(p["ID"])
                    urls.append(href)
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)



    def parse(self, response):
        person = {}

        url = response.url
        uniqueid = url.split('/')[-2]
        person['ID'] = int(uniqueid)

        name = response.css('h1::text').extract()
        if name:
            splitPos = name[0].index(' ')
            person['姓名'] = name[0][:splitPos].strip()
            person['外文名'] = name[0][splitPos:].strip()
        else:
            return

        pic = response.css("div[id*=headline] img::attr(src)").extract()
        if pic:
            person['头像'] = pic[0].strip()


        splitItems = ['职业', '更多中文名', '更多外文名', '家庭成员']
        infos = response.css("div.info ul li")
        for info in infos:
            infolist = info.css("::text").extract()
            if infolist:
                for index, ele in enumerate(infolist):
                    infolist[index] = ele.replace(':', '').replace('：', '')
                infolist = [s.strip() for s in infolist if s.strip()]
                if infolist[0] in splitItems:
                    itemlist = infolist[-1].split('/')
                    person[infolist[0]] = [item.strip() for item in itemlist if item.strip()]
                else:
                    person[infolist[0]] = infolist[-1]


        intro = response.css("div[id*=intro].mod div.bd span")
        if intro:
            paragraphs = intro[1].css("::text").extract()
            if paragraphs:
                person['简介'] = ' '.join([p.strip() for p in paragraphs if p.strip()])
        else:
            intro = response.css("div[id*=intro].mod div.bd::text").extract()
            if intro:
                person['简介'] = ' '.join([p.strip() for p in intro if p.strip()])


        recent = response.css("div[id*=recent_movies].mod div.bd")
        recentList = []
        mostrecent = recent.css("ul.list-s li") if recent else False
        if mostrecent:
            mostrecentList = []
            for item in mostrecent:
                itemInfo = item.css("div.info a")
                if itemInfo:
                    href = itemInfo.css("::attr(href)").extract()
                    title = itemInfo.css("::attr(title)").extract()
                    if href and title:
                        movieID = int(href[0].split('/')[-2])
                        mostrecentList.append({"ID": movieID, "片名": title[0].strip()})
            if mostrecentList:
                recentList += mostrecentList
        unrelease = recent.css("ul.unreleased li") if recent else False
        if unrelease:
            unreleaseList = []
            for item in unrelease:
                itemInfo = item.css("a")
                if itemInfo:
                    href = itemInfo.css("::attr(href)").extract()
                    title = item.css("::text").extract()
                    if title and href:
                        movieID = int(href[0].split('/')[-2])
                        title = [s.strip() for s in title if s.strip()]
                        unreleaseList.append({"ID": movieID, "片名": title[0]})
            if unreleaseList:
                recentList += recentList
        if recentList:
            person['最近作品'] = recentList


        best = response.css("div[id*=best_movies].mod div.bd")
        bestList = []
        bestrecent = best.css("ul.list-s li") if best else False
        if bestrecent:
            bestrecentList = []
            for item in bestrecent:
                itemInfo = item.css("div.info a")
                if itemInfo:
                    href = itemInfo.css("::attr(href)").extract()
                    title = itemInfo.css("::attr(title)").extract()
                    if href and title:
                        movieID = int(href[0].split('/')[-2])
                        bestrecentList.append({"ID": movieID, "片名": title[0].strip()})
            if bestrecentList:
                bestList += bestrecentList
        if bestList:
            person['最好作品'] = bestList


        partner = response.css("div[id*=partners].mod div.bd ul.list-s li")
        if partner:
            partnerList = []
            for item in partner:
                itemInfo = item.css("div.info a")
                if itemInfo:
                    href = itemInfo.css("::attr(href)").extract()
                    title = itemInfo.css("::attr(title)").extract()
                    if href and title:
                        actorID = int(href[0].split('/')[-2])
                        partnerList.append({"ID": actorID, "姓名": title[0].strip()})
            if partnerList:
                person['合作影人'] = partnerList

        yield person
