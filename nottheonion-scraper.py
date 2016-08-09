#!/usr/bin/env python3

'''
A script for getting a corpus of "Onion-like" albeit real news articles.
'''

import gzip
import io
import json
import logging
import os
import sys
import re
import requests

from collections import deque
from newspaper import Article
from newspaper import news_pool
from urllib.error import HTTPError
from urllib.parse import urlsplit

DEFAULT_REQUEST_CHARSET = "UTF-8"

URL_FILENAME_TRANSLATION_TABLE = {ord(':') : '-', ord('/') : os.path.sep, ord('\\') : '-', ord('*') : '-', ord('?') : '-', ord('"') : '\'', ord('<') : '-', ord('>') : '-', ord('|') : '-', ord('\0') : '0', ord('.') : os.path.sep}

def crawl_reddit(url):
#	req = create_request(url)
#	with urllib.request.urlopen(req) as response:
#		result = scrape_reddit_thing_urls_from_response(response)
#	return result
	r = requests.get(url, headers={
		"Accept-Charset" : DEFAULT_REQUEST_CHARSET,
		"Accept" : "application/json",
#		"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"
		"User-Agent" : "ubuntu:nottheonion-scraper:0.0.1 (by /u/errantlinguist)"
	})

#def create_request(url):
#	return urllib.request.Request(url, data=None, headers={
#		"Accept-Charset" : DEFAULT_REQUEST_CHARSET,
#		"Accept" : "application/json",
#		"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"
#	})

def create_url_filename(url_str):
	'''
	http://stackoverflow.com/a/7406369/1391325
	'''
	split_url = urlsplit(url_str)
	stripped_url_str = "".join(part for part in split_url[1:3])
	result = stripped_url_str.translate(URL_FILENAME_TRANSLATION_TABLE)
	if result.endswith(os.path.sep):
		result = result[:len(result) - len(os.path.sep)]
	return result

def save_pages(urls, outpath_prefix):
	url_attempt_queue = deque((url, 0) for url in urls)
	while url_attempt_queue:
		url, attempts = url_attempt_queue.popleft()
		outpath_infix = create_url_filename(url)
		outpath = os.path.join(outpath_prefix, outpath_infix)
		if os.path.exists(outpath):
			print("File path \"%s\" already exists; Skipping." % outpath, file=sys.stderr)
		else:
			print("Requesting article \"%s\"." % url, file=sys.stderr)
			req = create_request(url)
			with urllib.request.urlopen(req) as response:
				info = response.info()
				charset = info.get_content_charset()
				print("Charset: %s" % charset)
				if not charset:
					charset = DEFAULT_REQUEST_CHARSET
				data_str = response.read().decode(charset)
		
				# After getting the response data, write it to file
			
				outdir = os.path.dirname(outpath)
				if not os.path.exists(outdir):
					os.makedirs(outdir)
				with open(outpath, 'w') as outf:
					outf.write(data_str)
				print("%s > %s" %(url, outpath), file=sys.stderr)
					
					
		
def scrape_reddit_thing_urls_from_response(response):
	code = response.getcode()
	info = response.info()
	charset = info.get_content_charset()
	if not charset:
		charset = DEFAULT_REQUEST_CHARSET
	data_str = response.read().decode(charset)
	#data_str = response.readall().decode(charset)
	#print(data_str)
	json_objs = json.loads(data_str)
	with io.StringIO(data_str) as str_buffer:
		json_objs = json.load(str_buffer, encoding=charset)
		
	reddit_thing_urls = scrape_reddit_thing_urls(json_objs)
	last_thing_name = json_objs["data"]["after"]
	return reddit_thing_urls, last_thing_name

def scrape_reddit_thing_urls(json_objs):
	data = json_objs["data"]
	children = data["children"]
	for child in children:
		child_data = child["data"]
		child_name = child_data["name"]
		url_attr = "url"
		url = child_data.get(url_attr)
		if url:
			yield (child_name, url)
		else:
			print("Reddit thing named \"%s\" has no \"%s\" attribute." %(child_name, url_attr), file=sys.stderr)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: %s URL OUTDIR" % sys.argv[0], file=sys.stderr)
		sys.exit(64)
	else:
		papers = []
		get_method_root = "https://www.reddit.com/r/nottheonion/.json?limit=100"
		current_get_method = get_method_root
		while current_get_method:
			reddit_thing_urls, last_thing_name = crawl_reddit(current_get_method)
		
			urls = (url for name, url in reddit_thing_urls)
			articles = (Article(url=url, fetch_images=False, follow_meta_refresh=True) for url in urls)
			papers.extend(articles)
			count = len(papers)
			
			if not last_thing_name:
				current_get_method = None
			else:
				current_get_method = get_method_root + "&count=" + str(count) + "&after=" + last_thing_name
				
			print("%d articles to get." % len(papers))
#		outdir = sys.argv[1]
#		save_pages(urls, outdir)
		
		
