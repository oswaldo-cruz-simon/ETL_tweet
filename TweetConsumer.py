from kafka import KafkaConsumer
from ElasticSearchClient import ElasticSearchClient
from sendMongo import SendMongo
import collections,json
import numpy as np



class TweetConsumer:
	def __init__(self,urlKafka,topic,urles,urlMongo):
		self.urlKafka = urlKafka
		self.topic = topic
		self.consumer = KafkaConsumer(self.topic,bootstrap_servers=[self.urlKafka])
		self.urles = urles
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

urles = "http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80"
urlMongo = "54.174.5.92:27017"
urlKafka = "localhost:9092"
topic = "tweet"
consumer = TweetConsumer(urlKafka,topic,urles,urlMongo)
consumer.receiveMessage()