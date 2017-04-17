from kafka import KafkaProducer
import time,json 

class TweetProducer:
	def __init__(self,url):
		self.url = url
		self.producer = KafkaProducer(bootstrap_servers=url)
		#self.producer = KafkaProducer(bootstrap_servers=url, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

	def sendTweet(self,topic,key,tweet):
		#self.producer.send(topic,key=str.encode(str(key)),value=str.encode(tweet) )
		strtweet = json.dumps(tweet, sort_keys=True,indent=4, separators=(',', ': ')) 
		self.producer.send(topic,key=str.encode(str(key)),value=str.encode(strtweet) )
