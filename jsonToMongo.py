import logging
logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)
import json
from pymongo import MongoClient

mongoConnection = MongoClient()
database = mongoConnection.Douban
collection = database.series

# files = ['guochan_pingjia.json', 'guochan_redu.json', 'guochan_shijian.json']
# files = ['meiju.json', 'yingju.json', 'guochan.json', 'riju.json', 'hanju.json', 'gangju.json', 'dongman.json', 'zongyi.json', 'remen.json']
files = ['serie_new.json']

count = 0
for file in files:
    with open('douban/' + file) as f:
        data = json.load(f)
        for d in data:
            uniqueid = d['movieID']
            if not list(collection.find({'movieID': uniqueid})): # not duplicate
                collection.insert_one(d)

            else:   # duplicate
                existant = collection.find_one({'movieID': uniqueid})
                print("existant: " + str(existant))
                print("new: " + str(d))
print("count: " + str(count))
