from bs4 import BeautifulSoup
from googlesearch import search
import threading
import requests
import urllib.robotparser
import sys
import heapq
import logging
import re
from functools import wraps
import errno
import os
import signal

def timeout(seconds=0.5, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wraps(func)(wrapper)
    return decorator

def fetchSeedPages(query):
	seed_pages = []
	for result in search(query, tld="com", num=10, stop=10, pause=1):
		seed_pages.append(result)
	return seed_pages

def relevance(link):
	alpha = 50
	beta = 1
	incoming = page_index[link]['incoming']
	site_visits = domain_visit_log[parent_domain(link)]
	novelty = 100//(site_visits+1)
	rank = alpha * novelty + beta * incoming
	return -rank

def is_highest_priority(link, Q):
	r = relevance(link)
	if r <= Q[0][0]:
		return True
	else:
		heapq.heappush(Q, (r, link))
		return False

def parse_html(data):
	return BeautifulSoup(data.text, 'html.parser')

def update_log(link, size):
	"""
	Outputs:
	1) Visited URLs in order of visit
	2) Page size
	3) Page depth in graph
	4) Page priority score
	5) Time of download
	"""
	logging.basicConfig(filename='crawl.log', level = logging.INFO, format='%(asctime)s \t %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
	logging.info("SIZE: " + str(size//1000) + "KB" + '\t' +\
				 "DEPTH: " + str(page_index[link]['depth']) + '\t' +\
				 "RANK: " + str(-page_index[link]['rank']) + '\t' +\
				 link)

def get_hyperlinks(data):
	links = []
	for link in data.find_all('a'):
		hyperlink = link.get('href')
		if hyperlink and "http" in hyperlink:
			links.append(hyperlink)
	return links

def parent_domain(link):
	urlparse = link.split('/')[2].split('.')
	if len(urlparse) >= 2:
		return urlparse[-2] + '.' + urlparse[-1]
	else:
		return urlparse[0]

def finer_parent_domain(link):
	urlparse = link.split('/')
	return urlparse[0] + "//" + urlparse[2]

def initialize_page(link, incoming = 1, depth = 0):

	if parent_domain(link) not in domain_visit_log:
		domain_visit_log[parent_domain(link)] = 0

	page_index[link] = {}
	page_index[link]['visited'] = 0
	page_index[link]['incoming'] = incoming
	page_index[link]['rank'] = relevance(link)
	page_index[link]['depth'] = depth

def is_valid(link):
	if link[0:4] != "http" or\
	   link.endswith("jpg") or\
	   link.endswith("webm") or\
	   link.endswith("mov") or\
	   link.endswith("png") or\
	   link.endswith("mp4") or\
	   link.endswith("pdf"):
		return False
	return True

def update_index(links, url):
	for link in links:
		if not is_valid(link): continue
		if link not in page_index:
			initialize_page(link, depth = page_index[url]['depth'] + 1)
		else:
			if page_index[link]['visited']: continue
			else:
				page_index[link]['incoming'] += 1
				page_index[link]['rank'] = relevance(link)

		heapq.heappush(Q, (page_index[link]['rank'], link))		

@timeout
def allowed(link):
	"""
	Checks for a robots.txt file to determine if crawling this page is allowed
	"""
	rp = urllib.robotparser.RobotFileParser()
	rp.set_url(finer_parent_domain(link) + "/robots.txt")
	try:
		rp.read()
	except:
		return True
	return rp.can_fetch("*", link)
	
def visit_page(url):

	if not allowed(url) or page_index[url]['visited']: return
	page_index[url]['visited'] = 1
	domain_visit_log[parent_domain(url)] += 1
	try:
		data = requests.get(url, timeout = 3)
		size = len(data.content)
		data = parse_html(data)
		links = get_hyperlinks(data)
		update_index(links, url)
		update_log(url, size)
	except:
		print("Error visiting page (timeout).  Moving on.")

def attempt(LOCK):
	while Q:
		if LOCK.locked(): LOCK.release()
		#=========================================
		LOCK.acquire()
		cur = heapq.heappop(Q)
		url = cur[1]
		if is_highest_priority(url, Q):
			if LOCK.locked(): LOCK.release()

			try:
				#=========================================
				# HOLDS NO LOCK while checking robots.txt
				#=========================================
				if not allowed(url): continue
				#=========================================

				LOCK.acquire()

				if page_index[url]['visited']:
					LOCK.release()
					continue

				try: print("Crawling page: ", url)
				except: continue

				page_index[url]['visited'] = 1
				domain_visit_log[parent_domain(url)] += 1

				LOCK.release()

				#=========================================
				# HOLDS NO LOCK while getting http request
				#=========================================
				try:
					if is_valid(url):
						data = requests.get(url, timeout = (0.5, 2))
					else:
						continue
				except:
					print("Error visiting page (timeout).  Moving on.")
					continue
				#=========================================

				LOCK.acquire()
				size = len(data.content)
				data = parse_html(data)
				links = get_hyperlinks(data)
				update_index(links, url)
				update_log(url, size)

				LOCK.release()

				#=========================================

			except:
				if LOCK.locked(): LOCK.release()

		else:
			if LOCK.locked(): LOCK.release()
		if LOCK.locked():
			LOCK.release()

	return

def main():

	# Get initial seed pages via google API and initialize Q and results
	seed_pages = fetchSeedPages(sys.argv[1])
	for i, each in enumerate(seed_pages):
		initialize_page(each, incoming = 0, depth = 0)
		visit_page(each)

	# Create threads
	threadPool = []
	LOCK = threading.Lock()
	NUM_THREADS = 128

	for i in range(NUM_THREADS):
		t = threading.Thread(target = attempt, args = (LOCK,))
		threadPool.append(t)
	for thread in threadPool:
		thread.start()
	for thread in threadPool:
		thread.join()


if __name__ == "__main__":

	domain_visit_log = {} 
	page_index = {}
	Q = []

	main()

