#!/usr/bin/env python3

'''
A script for getting a corpus of "Onion-like" albeit real news articles.
'''

import os
import sys
import newspaper
import random
import re
import requests
import requests.auth
import string
import time

from collections import deque
from urllib.parse import urlsplit

DEFAULT_REQUEST_CHARSET = "UTF-8"

URL_FILENAME_TRANSLATION_TABLE = {ord(':') : '-', ord('/') : os.path.sep, ord('\\') : '-', ord('*') : '-', ord('?') : '-', ord('"') : '\'', ord('<') : '-', ord('>') : '-', ord('|') : '-', ord('\0') : '0', ord('.') : os.path.sep}

__AUTHOR_REDDIT_USERNAME = "errantlinguist"
__CLIENT_ID = "nottheonion-scraper"
__VERSION = "0.0.1"
__WEBSITE = "https://github.com/errantlinguist/nottheonion-scraper"

__CRAWLING_REQUEST_HEADERS = {
	"Accept" : "text/html;application/xhtml+xml",
	"Accept-Charset" : DEFAULT_REQUEST_CHARSET,
	"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36"
}

__CANONICAL_OUTPATH_SUFFIX = ".html"
__OUTPATH_SUFFIX_VARIANT_PATTERN = re.compile("\.x?html?$")

'''
See: https://github.com/reddit/reddit/wiki/API#user-content-rules
'''
__REDDIT_USER_AGENT_STR = "%(platform)s:%(app_id)s:%(version)s (by /u/%(reddit_username)s)" % {"platform" : sys.platform, "app_id" : __CLIENT_ID, "version" : __VERSION, "reddit_username" : __AUTHOR_REDDIT_USERNAME}


class AuthData(object):
	def __init__(self, auth_token_response):
		self.json = auth_token_response.json()
		self.token_type = self.json["token_type"]
		self.access_token = self.json["access_token"]
		self.auth_expiration_time = time.time() + int(self.json["expires_in"])

def create_url_filename(url_str, suffix_normalizer):
	'''
	http://stackoverflow.com/a/7406369/1391325
	'''
	split_url = urlsplit(url_str)
	stripped_url_str = "".join(part for part in split_url[1:3])
	result = stripped_url_str.translate(URL_FILENAME_TRANSLATION_TABLE)
	result = suffix_normalizer(result)
	return result
	
def refresh_auth_token(refresh_token, auth):
	post_data = {"grant_type": "refresh_token", "refresh_token" : refresh_token}
	headers = {
		"User-Agent" : __REDDIT_USER_AGENT_STR
	}
	return requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=post_data, headers=headers)	
	
def retrieve_auth_token(auth):
	post_data = {"grant_type": "client_credentials"}
	headers = {
		"User-Agent" : __REDDIT_USER_AGENT_STR
	}
	return requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=post_data, headers=headers)
		
def save_pages(urls, outpath_prefix, outpath_suffix_normalizer, max_attempts):
	result = set()
	url_attempt_queue = deque((url, 0) for url in urls)
	while url_attempt_queue:
		url, attempts = url_attempt_queue.popleft()
		
		outpath_filename = create_url_filename(url, outpath_suffix_normalizer)
		outpath = os.path.join(outpath_prefix, outpath_filename)
		if os.path.exists(outpath):
			print("File path \"%s\" already exists; Skipping." % outpath, file=sys.stderr)
		else:
			#print("Downloading article \"%s\"." % url, file=sys.stderr)
			crawling_response = requests.get(url, headers=__CRAWLING_REQUEST_HEADERS)
			try:
				crawling_response.raise_for_status()
			
				# After getting the response data, write it to file
				outdir = os.path.dirname(outpath)
				if not os.path.exists(outdir):
					os.makedirs(outdir)
				with open(outpath, 'w') as outf:
					outf.write(crawling_response.text)
	
				print("%s > %s" %(url, outpath), file=sys.stderr)
				
			except requests.HTTPError as e:
				attempts += 1
				code = crawling_response.status_code
				if max_attempts < attempts:
					print("Received HTTP status %d while attempting to retrieve \"%s\" (attempt %d); Giving up." %(code, url, attempts), file=sys.stderr)
					result.add(url)
				else:
					print("Received HTTP status %d while attempting to retrieve \"%s\" (attempt %d); Will try again later." %(code, url, attempts), file=sys.stderr)
					url_attempt_queue.append((url, attempts))
					
	return result
		
def scrape_reddit_thing_urls_from_response(response):
	data = response.json()["data"]
	reddit_thing_urls = scrape_reddit_thing_urls(data)
	last_thing_name = data["after"]
	return reddit_thing_urls, last_thing_name

def scrape_reddit_thing_urls(data):
	children = data["children"]
	#print("Processing %d child(ren)." % len(children))
	for child in children:
		child_data = child["data"]
		child_name = child_data["name"]
		url_attr = "url"
		url = child_data.get(url_attr)
		if url:
			yield (child_name, url)
		else:
			print("Reddit thing named \"%s\" has no \"%s\" attribute." %(child_name, url_attr), file=sys.stderr)
			
def __normalize_filename_suffix(filename):
	result = filename
	if result.endswith(os.path.sep):
		result = result[:len(result) - len(os.path.sep)]
	match =  __OUTPATH_SUFFIX_VARIANT_PATTERN.match(result)
	if not match:
		result = result + __CANONICAL_OUTPATH_SUFFIX
	return result
		
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: %s REDDIT_APP_SECRET OUTDIR" % sys.argv[0], file=sys.stderr)
		sys.exit(64)
	else:
		# Oauth2 authentication <https://github.com/reddit/reddit/wiki/OAuth2#user-content-authorization>
		# https://github.com/reddit/reddit/wiki/OAuth2-Quick-Start-Example#user-content-python-example
		secret = sys.argv[1]
		auth=("_JNFnqor9ZT4mQ", secret)
		auth_token_response = retrieve_auth_token(auth)
		auth_token_response.raise_for_status()
		auth_data = AuthData(auth_token_response)
		
		
		params = {"count" : 0, "limit" : 100}
		attempted_urls = set()
		failed_urls = set()
		while auth_token_response:
			batch_urls = []
			if auth_data.auth_expiration_time <= time.time():
				print("Refreshing authentication token.", file=sys.stderr)
				auth_token_response = refresh_auth_token(auth_data.access_token, auth)
				auth_token_response.raise_for_status()
				auth_data = AuthData(auth_token_response)
				
			headers = {
				"Accept" : "application/json",
				"Accept-Charset" : DEFAULT_REQUEST_CHARSET,
				"Authorization": auth_data.token_type + " " + auth_data.access_token,
				"User-Agent": __REDDIT_USER_AGENT_STR}
			next_page_response = requests.get("https://oauth.reddit.com/r/nottheonion/.json", headers=headers, params=params)
			try:
				next_page_response.raise_for_status()
			except requests.HTTPError as e:
				code = next_page_response.status_code
				if code == 401 or code == 403:
					print("Refreshing authentication token.", file=sys.stderr)
					auth_token_response = refresh_auth_token(auth_data.access_token, auth)
					auth_token_response.raise_for_status()
					auth_data = AuthData(auth_token_response)
					
				
			reddit_thing_urls, last_thing_name = scrape_reddit_thing_urls_from_response(next_page_response)
			for name, url in reddit_thing_urls:
				#print("Adding URL \"%s\" to batch." % url, file=sys.stderr)
				old_attempted_urls_len = len(attempted_urls)
				attempted_urls.add(url)
				if len(attempted_urls) > old_attempted_urls_len:
					batch_urls.append(url)
				else:
					print("URL \"%s\" has already been processed; Skipping." % url, file=sys.stderr)
			
			print("Retrieving %d articles." % len(batch_urls), file=sys.stderr)
			outdir = sys.argv[2]
			failed_urls.update(save_pages(batch_urls, outdir, __normalize_filename_suffix, 3))
			
			params["count"] += len(batch_urls)
			if last_thing_name:
				params["after"] = last_thing_name
			else:
				break
				
		successful_url_count = len(attempted_urls) - len(failed_urls)
		print("Retrieved %d out of %d unique pages returned by reddit." % (successful_url_count, len(attempted_urls)), file=sys.stderr)
		print()
		print("Failed URLS:")
		for failed_url in failed_urls:
			print(failed_url)
		
		
