import logging
logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)
import json
from pymongo import MongoClient

mongoConnection = MongoClient()
database = mongoConnection.Douban
collection = database.series

# files = ['guochan_pingjia.json', 'guochan_redu.json', 'guochan_shijian.json']
files = ['meiju.json', 'yingju.json', 'guochan.json', 'riju.json', 'hanju.json', 'gangju.json', 'dongman.json', 'zongyi.json', 'remen.json']

count = 0
for file in files:
    with open(file) as f:
        data = json.load(f)
        for d in data:
            uniqueid = d['ID']
            if not list(collection.find({'ID': uniqueid})): # not duplicate
                if d['分类']:
                    d.update({'分类': [d['分类']]})
                collection.insert_one(d)

            else:   # duplicate
                existant = collection.find_one({'ID': uniqueid})
                if d['分类'] and d['分类'] not in existant['分类']:
                    count += 1
                    print("before: \t" + str(existant))
                    print("new: \t" + str(d))
                    existant['分类'].append(d['分类'])
                    collection.update_one({"ID": uniqueid}, {'$set': {'分类': existant['分类']}})
                    print("after: \t" + str(collection.find_one({'ID': uniqueid})))
print("count: " + str(count))
