from urllib.parse import urlparse, urljoin,urlunparse,  urlsplit, urlunsplit
import urllib.robotparser as robotparser


robotcheckers = {}
def polite(robotcheckers, url):
	host = urlparse(url).netloc
	try:
		rc = robotcheckers[host]
	except KeyError:
		rc = robotparser.RobotFileParser()
		rc.set_url('http://' + host + '/robots.txt')
		rc.read()
		robotcheckers[host] = rc
		print(url)
	return rc.can_fetch('*', url)


polite(robotcheckers, 'https://twitter.com/search/realtime')
