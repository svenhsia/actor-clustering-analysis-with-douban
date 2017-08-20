import logging
logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)
import time
import os
import requests
from pymongo import MongoClient

# SLEEP = 0.25



db = MongoClient().Douban
col = db.total

kind = ['', '%E7%94%B5%E5%BD%B1', '%E7%94%B5%E8%A7%86%E5%89%A7', '%E7%9F%AD%E7%89%87', '%E7%BB%BC%E8%89%BA', '%E5%8A%A8%E7%94%BB', '%E7%BA%AA%E5%BD%95%E7%89%87']
# kind = ['', '电影', '电视剧', '短片', '综艺', '动画', '纪录片']

category = ['', '%E5%89%A7%E6%83%85', '%E7%88%B1%E6%83%85', '%E5%96%9C%E5%89%A7', '%E7%A7%91%E5%B9%BB',
            '%E5%8A%A8%E4%BD%9C', '%E6%82%AC%E7%96%91', '%E7%8A%AF%E7%BD%AA', '%E6%81%90%E6%80%96',
            '%E9%9D%92%E6%98%A5', '%E5%8A%B1%E5%BF%97', '%E6%88%98%E4%BA%89', '%E6%96%87%E8%89%BA',
            '%E9%BB%91%E8%89%B2%E5%B9%BD%E9%BB%98', '%E4%BC%A0%E8%AE%B0', '%E6%83%85%E8%89%B2',
            '%E6%9A%B4%E5%8A%9B', '%E9%9F%B3%E4%B9%90', '%E5%AE%B6%E5%BA%AD']
# category = ['', '剧情', '爱情', '喜剧', '科幻', '动作', '悬疑', '犯罪', '恐怖', '青春', '励志', '战争',
#             '文艺', '黑色幽默', '传记', '情色', '暴力', '音乐', '家庭']

region = ['', '%E5%A4%A7%E9%99%86', '%E7%BE%8E%E5%9B%BD', '%E9%A6%99%E6%B8%AF', '%E5%8F%B0%E6%B9%BE',
          '%E6%97%A5%E6%9C%AC', '%E9%9F%A9%E5%9B%BD', '%E8%8B%B1%E5%9B%BD', '%E6%B3%95%E5%9B%BD',
          '%E5%BE%B7%E5%9B%BD', '%E6%84%8F%E5%A4%A7%E5%88%A9', '%E8%A5%BF%E7%8F%AD%E7%89%99',
          '%E5%8D%B0%E5%BA%A6', '%E6%B3%B0%E5%9B%BD', '%E4%BF%84%E7%BD%97%E6%96%AF',
          '%E4%BC%8A%E6%9C%97', '%E5%8A%A0%E6%8B%BF%E5%A4%A7', '%E6%BE%B3%E5%A4%A7%E5%88%A9%E4%BA%9A',
          '%E7%88%B1%E5%B0%94%E5%85%B0', '%E7%91%9E%E5%85%B8', '%E5%B7%B4%E8%A5%BF', '%E4%B8%B9%E9%BA%A6']
# region = ['', '大陆', '美国', '香港', '台湾', '日本', '韩国', '英国', '法国', '德国', '意大利', '西班牙', '印度',
#           '泰国', '俄罗斯', '伊朗', '加拿大', '澳大利亚', '爱尔兰', '瑞典', '巴西', '丹麦']

sort = ['R', 'S', 'T']




# https://movie.douban.com/j/new_search_subjects?sort={}&range=0,10&tags={}&start={}
def insertData(dataList, thisKind, thisRegion):
    for d in dataList:
        movieID = int(d['id'])
        exist = col.find_one({'movieID': movieID}) # check if already in
        if exist:   # already in
            need_update = False
            exist_kind = exist['type']
            if thisKind and thisKind not in exist_kind: # another type tag
                exist_kind.append(thisKind)
                need_update = True
            exist_region = exist['country/region']
            if thisRegion and thisRegion not in exist_region:   # another region tag
                exist_region.append(thisRegion)
                need_update = True
            if need_update: # need to update type and region
                col.update_one({'movieID': movieID}, {"$set": {'type': exist_kind, 'country/region': exist_region}}, upsert=False)
        else:   # first occurrence, insert directly
            col.insert_one({'movieID': int(d['id']), 'url': d['url'], 'title-zh': d['title'], 'type': [thisKind], 'country/region': [thisRegion]})

# run over all pages of this url: 0 ~ 9980
def requestALL(sortKey, tagStr, thisKind, thisRegion):
    for s in range(0, 10000, 20):
        url = 'https://movie.douban.com/j/new_search_subjects?sort={}&range=0,10&tags={}&start={}'.format(sortKey, tagStr, s)
        if url in already_req:
            logging.info('already request ' + url)
            continue
        r = requests.get(url, allow_redirects=False)
        # r = s.mount(url, DESAdapter())
        logging.info("requesting " + url)
        # time.sleep(SLEEP)
        data = r.json()['data']
        if data: # has data
            insertData(data, thisKind, thisRegion)
            dejavu.write(url + '\n')
        elif s == 9980: # no data, but in full situation, try 9979
            url = 'https://movie.douban.com/j/new_search_subjects?sort={}&range=0,10&tags={}&start={}'.format(sortKey, tagStr, 9979)
            if url in already_req:
                logging.info("already request " + url)
                return True
            r = requests.get(url, allow_redirects=False)
            # r = s.mount(url, DESAdapter())
            logging.info("requesting " + url)
            # time.sleep(SLEEP)
            data = r.json()['data']
            if data: # 9979 has data
                insertData(data, thisKind, thisRegion)
                dejavu.write(url + '\n')
                return True # return to mark as full
        else:
            return False
            # else, 9960 the last page, not full

already_req = []
if os.path.exists('already_request.txt'):
    with open('already_request.txt', 'r') as dejavu:
        for line in dejavu:
            already_req.append(line.strip())


with open('already_request.txt', 'a') as dejavu:
    for k in kind:
        for c in category:
            for r in region:
                tags = [k, c, r]
                tags = [t for t in tags if t]
                tag_str = ','.join(tags)
                full = requestALL('R', tag_str, k, r) # True if reaches 9980, need to try other sort keys
                if full:
                    requestALL('S', tag_str, k, r)
                    requestALL('T', tag_str, k, r)
                # else, no need to try other sort keys
