import logging
logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)
import time
import os
from urllib.request import unquote
from bson.objectid import ObjectId
from pymongo import MongoClient

# SLEEP = 0.25



db = MongoClient().Douban
col = db.total

movieList = list(col.find())


lack_info = []
for movie in movieList:
    types = movie['type']
    country = movie['country/region']
    types_unquoted = [unquote(t) for t in types if t.strip()]
    country_unquoted = [unquote(c) for c in country if c.strip()]
    if not types_unquoted and not country_unquoted:
        lack_info.append(movie['movieID'])
    col.update_one({'_id': ObjectId(movie['_id'])}, {'$set': {'type': types_unquoted, 'country/region': country_unquoted}}, upsert=False)

with open('lack_info_list.txt', 'w') as f:
    for l in lack_info:
        f.write(str(l) + '\n')

print(len(lack_info))
