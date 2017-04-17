from kafka import KafkaConsumer
from ElasticSearchClient import ElasticSearchClient
import collections,json
import numpy as np

def countWords(body):
	freq = collections.Counter( np.array(body.split()) )
	return freq.most_common(1)[0]

class TweetConsumer:
	def __init__(self,url,topic,urles):
		self.url = url
		self.topic = topic
		self.consumer = KafkaConsumer(self.topic,bootstrap_servers=[self.url])
		self.urles = 'http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80'
		self.es = ElasticSearchClient(urles)
	def receiveMessage(self):
		for message in self.consumer:
			tweet = json.loads(message.value.decode('utf8'))
			text = tweet["text"]
			word, rep = countWords(text)
			iddoc = str( int(message.key)) 
			print('[%s] %s=>%d' %(iddoc,word,rep) )
			self.es.sendTweet('oswaldo','tweet',str(int(message.key)),{'text':text})


consumer = TweetConsumer('localhost:9092','tweet','localhost:9200')
consumer.receiveMessage()