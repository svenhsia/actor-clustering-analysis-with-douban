import logging
logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)
from collections import defaultdict
from pymongo import MongoClient
from pprint import pprint

mongoConnection = MongoClient()
database = mongoConnection.Douban
collection = database.series

CATEGORY = '国产'
ROLE = '主演'

serieList = list(
    collection.aggregate([
        {'$match': {'种类': CATEGORY}}
    ])
)

# roleStat = defaultdict()
roleStat = {}
roleCount = defaultdict(int)

for s in serieList:
    if ROLE in s:
        nameList = s[ROLE]
        for name in nameList:
            roleCount[name] += 1
            if name not in roleStat:
                roleStat[name] = [s['豆瓣评分'] * s['评价人数'], s['评价人数']]
            else:
                roleStat[name][0] += s['豆瓣评分'] * s['评价人数']
                roleStat[name][1] += s['评价人数']

toDelete = []
for r, v in roleStat.items():
    if roleCount[r] < 3:
        toDelete.append(r)
    roleStat[r] = (v[0] / v[1], roleCount[r])
for r in toDelete:
    roleStat.pop(r)
sortRole = sorted(roleStat.items(), key=lambda t: t[1][0], reverse=True)
pprint(sortRole)
