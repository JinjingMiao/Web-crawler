import json
import socket
import requests
import urllib.request
import requests
import collections
import re
from elasticsearch import Elasticsearch, helpers 
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

from operator import itemgetter, attrgetter
import heapq
import json

def writeID(): 

	url = 'http://localhost:9200/crawler/_search?'
	# querystring = {"scroll":"1m"}
	payload = {
				"query": {"match_all":{}},

	}
	querystring = {"scroll": "1m"}
	headers = {
		"Content-Type": "application/json"
	}
	response = requests.request("POST", url, data=json.dumps(payload), headers=headers, params=querystring)
	response = json.loads(response.text)
	print(type(response))
	f = open('/Users/jinjingmiao/Documents/CS6200InformationRetrieval/hw3-JJ/id.txt', 'w')
	m = 0
	while True:
		print(len(response['hits']['hits']))
		print (m )
		m += 1
		if(len(response['hits']['hits']) ==0):
			break


		#print (type(response))
		# if('hits' not in response.keys()):
		# 	break

		for hit in response['hits']['hits']:
			fsn = hit['_id']
			f.write(fsn)
			f.write("\n")

		scroll_id = response['_scroll_id']
	    #print scroll_id

		payload = {
			"scroll_id": scroll_id,
			"scroll" : "1m"
		}

	    #print (payload)
		url = "http://localhost:9200/_search/scroll"
		response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
		response = json.loads(response.text)



def index_content(index, id):
	res = es.get(index=index, id=id)['_source']
	return res['url'], res['content'], res['outlinks']

def index_url_outlinks(index, id):
	res = es.get(index=index, id=id)['_source']
	return res['url'], res['outlinks']

def es_ids():
	#res=helpers.scan(es, index='crawler', query={"query":{"match_all": {}}}, scroll='5m', request_timeout=None, size=1000)#like others so far
	#hits = res['hits']['hits']
	#print(len(hits))

	#scroll = helpers.scan(es, query="{"fields":"_id"}", scroll='10s', index='crawler', request_timeout=300000)
	for res in scroll:
		print (res['_id'])
	#return [d['_id'] for d in res['hits']['hits']]
		 
	# return [aa['_id'] for aa in a['hits']['hits']]

def storer():
	with open('./inlink.json') as json_file:
		inlinks = json.load(json_file)["inlink_dict"]
	print('##################')
	index_count = 1
	output_count = 1
	index = 'crawler'
	save_path = './result/indexes/output' + str(output_count) + '.txt'
	f = open(save_path, 'a')
	idf = open('/Users/jinjingmiao/Documents/CS6200InformationRetrieval/hw3-JJ/id.txt', 'r')
	for es_id in idf:
		es_id = es_id.strip()
		print('es_id', es_id)
		url = es.get(index=index, id=es_id)['_source']['url']
		print('url', url)
		content =  es.get(index=index, id=es_id)['_source']['content']
		out_links =  es.get(index=index, id=es_id)['_source']['outlinks']
		id = '<URL>\n' + str(url) + '\n</URL>'
		body = '\n<CONTENT>\n' + str(content) + '\n</CONTENT>'
		if url in inlinks:
			inlink_list = '\n<INLINKS>\n' + str(inlinks[str(url)]) + '\n</INLINKS>\n'
		else:
			inlink_list = ''
		outlinks = '\n<OUTLINKS>\n' + str(out_links) + '\n</OUTLINKS>'
		res = '\n<OUTPUT>\n' + id + body + inlink_list + outlinks + '\n</OUTPUT>\n'
		f.write(res)
		output_count += 1
		if output_count == 200:
			f.close()
			print(output_count, end='  ')
			index_count+=1
			f = open('./result/indexes/output' + str(index_count) + '.txt', 'a')
			output_count = 0
	f.close()
#es_ids()
storer()
#writeID()

