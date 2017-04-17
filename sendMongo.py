from pymongo import MongoClient
import collections
import numpy as np
def countWords(body):
	freq = collections.Counter( np.array(body.split()) )
	return freq.most_common(1)[0]

class SendMongo:
	def __init__(self,url):
		self.url = url
		self.client = MongoClient(url)

	def saveTweet(self,dbname,collname,iddoc,doc):
		db = self.client[dbname]
		tweet = db[collname]
		doc["_id"] = iddoc
		word, rep = countWords(doc["text"])
		del doc["text"]
		doc["most_common"] = {"word":word,"rep":rep}
		tweet_id = tweet.insert_one(doc)