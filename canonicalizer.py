import json
import socket
import requests
import urllib.request
import collections
import re
import pickle
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin,urlunparse,  urlsplit, urlunsplit
import urllib.robotparser as robotparser
import time


def canonicalizer(url):
	url = url.lower()
	if not url.startswith("http"): 
		url=urljoin("http://",url)
	if url.startswith("https") and url.endswith(":443"):
		url = url[:-4]
	# URL 3
	if url.startswith("http") and url.endswith(":80"):
		url = url[:-3]
	if '#' in url:
		url = url.split('#',1)[0]

	parsed = list(urlparse(url)) 
	parsed[2] = re.sub("/{2,}", "/", parsed[2])
	cleaned =urlunparse(parsed)
	cleaned=resolve_url(cleaned)

	idx = len(cleaned) - cleaned[::-1].index('/')
	res= cleaned[:idx].lower() + cleaned[idx:]
	print(res)
	return res

def resolve_url(url):
	partOne = list(urlsplit(url))
	segments = partOne[2].split('/')
	segments = [segment + '/' for segment in segments[:-1]] + [segments[-1]]
	resolved = []
	for segment in segments:
		if segment not in ('./', '.'):
			resolved.append(segment)
		elif segment in ('../', '..'):
			if resolved[1:]:
				resolved.pop()
			else :
				break
	partOne[2] = ''.join(resolved)
	return urlunsplit(partOne)








def main ():
	canonicalizer('http://www.example.com:80')


if __name__ == '__main__':
	main()


