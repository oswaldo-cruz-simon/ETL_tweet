from kafka import KafkaConsumer
from ElasticSearchClient import ElasticSearchClient
from sendMongo import SendMongo
import collections,json
import numpy as np



class TweetConsumer:
	def __init__(self,url,topic,urles,urlMongo):
		self.url = url
		self.topic = topic
		self.consumer = KafkaConsumer(self.topic,bootstrap_servers=[self.url])
		self.urles = 'http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80'
		self.es = ElasticSearchClient(urles)
		self.urlMongo = urlMongo
		self.mongo = SendMongo(urlMongo)
	def receiveMessage(self):
		for message in self.consumer:
			tweet = json.loads(message.value.decode('utf8'))
			text = tweet["text"]
			
			iddoc = str( int(message.key))
			self.es.sendTweet('oswaldo','tweet',iddoc,{'text':text})
			self.mongo.saveTweet("oswaldo",'tweet',iddoc,tweet)

consumer = TweetConsumer('localhost:9092','tweet','localhost:9200','localhost:27017')
consumer.receiveMessage()