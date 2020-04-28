import json
import socket
import requests
import urllib.request
import collections
import re
import pickle
import nltk
from nltk.tokenize import word_tokenize
from lxml import etree
# nltk.download()
from nltk.stem import SnowballStemmer
snowball = SnowballStemmer('english')
from elasticsearch import Elasticsearch, helpers 
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin,urlunparse,  urlsplit, urlunsplit
import urllib.robotparser as robotparser
import time, datetime

from operator import itemgetter, attrgetter
import heapq
import json

from canonicalizer import canonicalizer
from polite import polite

OUTPATH = '/Users/jinjingmiao/Documents/CS6200InformationRetrieval/hw3-JJ/output.txt'










# def is_new_link(link):
# 	if link in explored or link in [item[1] for item in queue]:
# 		return False
# 	return True

def title_tag(url):
	raw_data = requests.get(url)
	soup = BeautifulSoup(raw_data.text, 'lxml')
	return soup.title.string

def keywords_related(text):
	token_arr = []
	keywords = ['marine', 'list', 'disaters', 'maritime','sink','sewol','shipwreck','migrant','lampedusa']
	text = text.lower()
	tokens = word_tokenize(text)

	for token in tokens:
		stemmed = snowball.stem(token)
		token_arr.append(stemmed)

	for kw in keywords:
		if kw in token_arr:
			return True
	return False





def es_docs_count(index):
	es_count = es.count(index= crawler, body= {"query":{"match_all":{}}})['count']
	return es_count


def update_inlinks(curr_url, outlinks):
	global inlinks
	for outlink in outlinks:
		if outlink not in inlinks.keys():
			inlinks[outlink] = [curr_url]
		elif curr_url not in inlinks[outlink]:
			inlinks[outlink].append(curr_url)

inlinks = {}

seed_urls = ['http://en.wikipedia.org/wiki/List_of_maritime_disasters', 'http://en.wikipedia.org/wiki/Sinking_of_the_MV_Sewol','http://en.wikipedia.org/wiki/2013_Lampedusa_migrant_shipwreck']

dq = collections.deque()
unsorted_queue = []
explored_links = []
for seed in seed_urls:
	dq.appendleft(( seed, 0))
#print(dq)

wiki = "https://en.wikipedia.org"
text = ''
outlinks = []
count = 0
robotcheckers = {}
MAX_CRAWL = 40000


def web_crawl():
	#output = open(OUTPATH, "w")
	start = datetime.datetime.now() 
	print('start: %s' % datetime.datetime.now())
	while dq:
		q_start = datetime.datetime.now()
		outlinks.clear()
		time_used = datetime.datetime.now() - start 
		print('Time used: %s' % time_used)

		whole_text =''
		## url_tuple: [url, wave]
		url_tupple = dq.pop()
		#tempStr0 = str(url_tupple) + " "
		#output.write(tempStr0)
		#print (url_tupple, end=' ')

		html = requests.get(url_tupple[0])
		soup = BeautifulSoup(html.text, 'lxml')
		wave_no = url_tupple[1]
		content = soup.find('div', {'id': 'mw-content-text'}).find_all('a', {'href': re.compile("^/wiki")})
		#content =soup.find_all('a', {'href': re.compile("^https?://")})
		soup_time = datetime.datetime.now() - q_start

		print('soup time: %s' % soup_time)

		for paragragh in soup.find_all('p'):
			whole_text += paragragh.text

		if not polite(robotcheckers, url_tupple[0]):
			tempStr1 = '- not polite!'+'\n'
			#output.write(tempStr1) 
			time.sleep(0.5)
			continue

		if 'html' not in html.headers['content-type']:
			tempStr1 = '- not html type!' + '\n'
			#output.write(tempStr1) 
			time.sleep(0.5)
			continue

		if not keywords_related(whole_text):
			tempStr1 = " irrelevant page" + '\n'
			print(tempStr1) 
			continue
		canonical_start = datetime.datetime.now()
		for link in content:
			canonical = canonicalizer(urljoin(url_tupple[0], link.get('href')))
			if canonical in outlinks:
				break;
			else:
				outlinks.append(canonical)
		canonical_time = datetime.datetime.now() - canonical_start

		print('canonical time: %s' % canonical_time)


		if not len(unsorted_queue) > 1000000:

			for link in outlinks:
				if link in unsorted_queue or link in explored_links:
					continue
				else:
					unsorted_queue.append(link)
					dq.insert(0, (link, wave_no+1))
		inlink_start = datetime.datetime.now()


		inlink_start = datetime.datetime.now()

		for outlink in outlinks:
			if outlink in inlinks:
				inlinks[outlink].append(url_tupple[0])
			else:
				inlinks[outlink] = []
				inlinks[outlink].append(url_tupple[0])
		explored_links.append(url_tupple[0])
		inlink_time = datetime.datetime.now() - inlink_start
		print('construct inlink time: %s' % inlink_time)
		#update_inlinks(url_tupple[0], outlinks)

		tempStr = ' outlinks '+ str(len(outlinks))+ ' deque '+str(len(dq))+' queue '+ str(len(unsorted_queue))+' explored_links '+str(len(explored_links))
		print(tempStr, end='\n')
		output.write(tempStr) 


		if(len(dq) == 0):
			print('\n --Deque used up. Now adding new tuples and sorting\n')
			output.write('\n --Deque used up. Now adding new tuples and sorting\n') 
			sort_start = datetime.datetime.now()
			tmp_q = collections.deque
			for link in unsorted_queue:
				if link in tmp_q:
					continue;
				else:
					heapq.heappush(tmp_q, ([len(inlinks[link]), link]))
			while tmp_q:
				tmp_tupple = heapq.heappop(tmp_q)
				dq.append((tmp_tupple[1], wave_no+1))
			unsorted_queue.clear()
			sort_time = datetime.datetime.now() - sort_start
			print('sort time: %s' % sort_time)

		# print(soup.title.string)
		# print(inlinks)
		es_start = datetime.datetime.now()
		inlink_list = []
		if url_tupple[0] in inlinks.keys():
			inlink_list = inlinks[url_tupple[0]]

		res = es.index(index="crawler", id=soup.title.string, body={
			'url': url_tupple[0],
			'content': whole_text,
			'header': json.dumps(dict(html.headers)),
			'inlinks': inlink_list,
			'outlinks': outlinks,
			})
		es_time = datetime.datetime.now() - es_start
		print('es time: %s' % es_time)
		print('oulinks length', len(outlinks))
		if es_count  > MAX_CRAWL:
	
			# inlink_dict = {'inlink_dict': inlinks}
			# with open('./inlink.json', 'w') as file:
			# 	file.write(json.dumps(inlink_dict))
			break
		time.sleep(0.2)
	output.close()
web_crawl()



