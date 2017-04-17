from elasticsearch import Elasticsearch,helpers
import time,json

class ElasticSearchClient:
	def __init__(self,url):
		self.url = url
		self.es = Elasticsearch([url])
	def sendTweet(self,index,doc_type,iddoc,doc):
		res = self.es.index(index=index, doc_type=doc_type, id=iddoc, body=doc)