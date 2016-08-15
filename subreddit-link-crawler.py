#!/usr/bin/env python3

'''
A script for crawling all links listed in a given subreddit and saving them to a given directory.
'''

__author__ = "Todd Shore"
__copyright__ = "Copyright 2016 Todd Shore"
__license__ = "GPL"
__reddit_author_username__ = "errantlinguist"
__reddit_app_name__ = "subreddit-link-crawler"
__reddit_redirect_uri__ = "https://github.com/errantlinguist/subreddit-link-crawler"
__version__ = "0.0.1"
__website__ = "https://github.com/errantlinguist/subreddit-link-crawler"

import argparse
import datetime
import mimetypes
import os
import sys
import requests
import string
import time

from cgi import parse_header
from collections import deque
from urllib.parse import urlsplit

DEFAULT_EXPECTED_CONTENT_TYPE="text/html"
DEFAULT_OUTPATH_SUFFIX = mimetypes.guess_extension(DEFAULT_EXPECTED_CONTENT_TYPE)
DEFAULT_REQUEST_CHARSET = "UTF-8"

URL_FILENAME_TRANSLATION_TABLE = {ord(':') : '-', ord('/') : os.path.sep, ord('\\') : '-', ord('*') : '-', ord('?') : '-', ord('"') : '\'', ord('<') : '-', ord('>') : '-', ord('|') : '-', ord('\0') : '0', ord('.') : os.path.sep}

__CRAWLING_REQUEST_HEADERS = {
	"Accept" : DEFAULT_EXPECTED_CONTENT_TYPE + ";application/xhtml+xml",
	"Accept-Charset" : DEFAULT_REQUEST_CHARSET,
	"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36"
}

'''
See: https://github.com/reddit/reddit/wiki/API#user-content-rules
'''
__REDDIT_USER_AGENT_STR = "%(platform)s:%(app_id)s:%(version)s (by /u/%(reddit_username)s)" % {"platform" : sys.platform, "app_id" : __reddit_app_name__, "version" : __version__, "reddit_username" : __reddit_author_username__}

__AUTH_TOKEN_REQUEST_HEADERS = {
		"User-Agent" : __REDDIT_USER_AGENT_STR
}


class AuthTokenData(object):
	def __init__(self, auth_token_response):
		json = auth_token_response.json()
		self.token_type = json["token_type"]
		self.access_token = json["access_token"]
		self.auth_expiration_time = time.time() + int(json["expires_in"])
		
class CrawlStatistics(object):
	def __init__(self):
		self.attempted = set()
		self.failed = set()
		self.time_range = (float("inf"), float("-inf"))
		
	def notify_attempt(self, key, time):
		# Calculate new range even for possible duplicate attempts
		if time < self.time_range[0]:
			self.time_range = (time, self.time_range[1])
		if time > self.time_range[1]:
			self.time_range = (self.time_range[0], time)
		return self.__add_attempt(key, time)
		
	def __add_attempt(self, key, time):
		old_attempted_len = len(self.attempted)
		self.attempted.add(key)
		return len(self.attempted) > old_attempted_len
		
class SubredditLinkCrawler(object):
	def __init__(self, auth, subreddit, outdir, limit, batch_size, max_retries, user_agent):
		self.auth = auth
		self.get_method = "https://oauth.reddit.com/r/" + subreddit + "/.json"
		if limit:
			self.limit = limit
		else:
			self.limit = float("inf")
		self.max_retries = max_retries
		self.outdir = outdir
		self.user_agent = user_agent
		
		self.parsed_url_count = 0
		self.last_parsed_thing = None
		self.stats = CrawlStatistics()
		
		self.processed_url_count = 0
		
		self.__params = {"count" : self.parsed_url_count, "limit" :  batch_size}
	
	@property	
	def params(self):
		self.__params["after"] = self.last_parsed_thing
		self.__params["count"] = self.parsed_url_count
		if self.remaining_url_count < self.__params["limit"]:
			self.__params["limit"] = self.remaining_url_count
		return self.__params
		
	@property
	def remaining_url_count(self):
		return self.limit - self.processed_url_count
		
	def crawl(self):
		auth_token_data = self.retrieve_auth_token()
	
		while self.remaining_url_count > 0:
			batch_urls = []
			if auth_token_data.auth_expiration_time <= time.time():
				print("Refreshing authentication token.", file=sys.stderr)
				auth_token_data = self.refresh_auth_token(auth_token_data)
			
			try:
				next_page_response = self.__request_next_listing(auth_token_data)
			except requests.HTTPError as e:
				code = next_page_response.status_code
				if code == requests.status_codes.codes.unauthorized or code == requests.status_codes.codes.forbidden:
					print("Refreshing authentication token.", file=sys.stderr)
					auth_token_data = self.refresh_auth_token(auth_token_data)
					# Try again after having refreshed the auth token but don't catch the next exception if one is raised
					next_page_response = self.__request_next_listing(auth_token_data)
				else:
					raise e
			
			reddit_thing_urls, last_thing_name = parse_reddit_thing_urls_from_response(next_page_response)
			for name, creation_timestamp, url in reddit_thing_urls:
				self.parsed_url_count += 1
				#print("Adding URL \"%s\" to batch." % url, file=sys.stderr)
				if self.stats.notify_attempt(url, creation_timestamp):
					batch_urls.append(url)
				else:
					print("URL \"%s\" has already been processed; Skipping." % url, file=sys.stderr)
		
			#print("Processing %d crawled URLs." % len(batch_urls), file=sys.stderr)
			failed_urls = save_pages(batch_urls, self.outdir, self.max_retries)
			self.stats.failed.update(failed_urls)
			successful_url_count = len(batch_urls) - len(failed_urls)
			self.processed_url_count += successful_url_count
		
			if last_thing_name:
				self.last_parsed_thing = last_thing_name
			else:
				if self.limit == float("inf"):
					print("End of subreddit encountered; Aborting after having processed %d URLs." %self.processed_url_count, file=sys.stderr)
				else:
					print("End of subreddit encountered; Aborting after having processed %d of an intended %d URLs." %(self.processed_url_count, self.limit), file=sys.stderr)
				break
			
		print("Retrieved %d out of %d unique pages returned by reddit." % (self.processed_url_count, len(self.stats.attempted)), file=sys.stderr)
		return self.stats
			
	def refresh_auth_token(self, auth_token_data):
		auth_token_response = refresh_auth_token(auth_token_data.access_token, self.auth)
		auth_token_response.raise_for_status()
		return AuthTokenData(auth_token_response)
		
	def retrieve_auth_token(self):
		auth_token_response = retrieve_auth_token(self.auth)
		auth_token_response.raise_for_status()
		return AuthTokenData(auth_token_response)
		
	def __request_next_listing(self, auth_token_data):
		# (Re-)create the header dictionary here in the case that the token had to be refreshed
		headers = {
			"Accept" : "application/json",
			"Accept-Charset" : DEFAULT_REQUEST_CHARSET,
			"Authorization": auth_token_data.token_type + " " + auth_token_data.access_token,
			"User-Agent": self.user_agent}
	
		result = requests.get(self.get_method, headers=headers, params=self.params)
		result.raise_for_status()
		return result

def create_url_filename(url_str, content_type):
	# See also: http://stackoverflow.com/a/7406369/1391325
	split_url = urlsplit(url_str)
	netloc = split_url[1]
	netloc_dirname = os.path.sep.join(reversed(netloc.split('.')))
	path = split_url[2]
	query = split_url[3]
	stripped_url_str = "".join((netloc_dirname, path, query))
	url_without_ext, existing_ext = os.path.splitext(stripped_url_str)
	filename_without_ext = url_without_ext.translate(URL_FILENAME_TRANSLATION_TABLE)
	if filename_without_ext.endswith(os.path.sep):
		filename_without_ext = filename_without_ext[:-len(os.path.sep)]
	if existing_ext:
		acceptable_filename_exts = mimetypes.guess_all_extensions(content_type)
		if existing_ext in acceptable_filename_exts:
			# Re-concatenate the now-normalized filename base with the original extension
			result = filename_without_ext + existing_ext
		else:
			canonical_ext = mimetypes.guess_extension(content_type)
			if canonical_ext:
				# If a canonical extension was found for the given content type, concatenate it to the now-normalized filename base
				result = filename_without_ext + canonical_ext
			else:
				# If no canonical extension was found, re-concatenate the original extension after normalizing it
				normalized_existing_ext = normalize_url_component(existing_ext, ".")
				result = filename_without_ext + normalized_existing_ext
	else:
		# Concatenate the canonical extension for the given content type to the result filename in order to avoid potential clashes with other URLs
		canonical_ext = mimetypes.guess_extension(content_type)
		if canonical_ext:
			result = filename_without_ext + canonical_ext
		else:
			# Just add some extention
			result = filename_without_ext + DEFAULT_OUTPATH_SUFFIX
	
	return result
	
def format_timestamp(posix_time):
	'''
	See: http://stackoverflow.com/a/37188257/1391325
	'''
	return datetime.datetime.utcfromtimestamp(posix_time).strftime('%Y-%m-%dT%H:%M:%SZ')
	
def normalize_url_component(component, ignored_prefix=None):
	if ignored_prefix is not None and component.startswith(ignored_prefix):
		substr_to_normalize = component[len(ignored_prefix):]
		normalized_substr = substr_to_normalize.translate(URL_FILENAME_TRANSLATION_TABLE)
		result = ignored_prefix + normalized_substr
	else:
		result = component.translate(URL_FILENAME_TRANSLATION_TABLE)
	return result
	
def print_stats(stats, outfile):
	print("Oldest listed link date: " + format_timestamp(stats.time_range[0]))
	print("Newest listed link date: " + format_timestamp(stats.time_range[1])) 
	print("Failed URLS:", file=outfile)
	for failed_url in stats.failed:
		print(failed_url, file=outfile)
	
def refresh_auth_token(refresh_token, auth):
	post_data = {"grant_type": "refresh_token", "refresh_token" : refresh_token}
	return requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=post_data, headers=__AUTH_TOKEN_REQUEST_HEADERS)	
	
def retrieve_auth_token(auth):
	'''
	Oauth2 authentication <https://github.com/reddit/reddit/wiki/OAuth2#user-content-authorization>
	See: https://github.com/reddit/reddit/wiki/OAuth2-Quick-Start-Example#user-content-python-example
	'''
	post_data = {"grant_type": "client_credentials"}
	return requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=post_data, headers=__AUTH_TOKEN_REQUEST_HEADERS)
		
def save_pages(urls, outpath_prefix, max_retries):
	result = set()
	url_attempt_queue = deque((url, 0) for url in urls)
	while url_attempt_queue:
		url, attempts = url_attempt_queue.popleft()
		
		# First try to guess the MIME type returned by the server for the given URL in order to guess the result filename
		guessed_content_type = mimetypes.guess_type(url)[0]
		if guessed_content_type:
			#print("Guessed URL content type to be \"%s\"." % guessed_content_type, file=sys.stderr)
			pass
		else:
			guessed_content_type = DEFAULT_EXPECTED_CONTENT_TYPE
			#print("Could not guess URL content type; Defaulting to \"%s\"." % guessed_content_type, file=sys.stderr)
		guessed_outpath_filename = create_url_filename(url, guessed_content_type)
		outpath = os.path.join(outpath_prefix, guessed_outpath_filename)
		if os.path.exists(outpath):
			print("File path \"%s\" already exists; Skipping." % outpath, file=sys.stderr)
		else:
			#print("Downloading article \"%s\"." % url, file=sys.stderr)
			try:
				crawling_response = requests.get(url, headers=__CRAWLING_REQUEST_HEADERS)
				try:
					attempts += 1
					crawling_response.raise_for_status()
					response_content_type = crawling_response.headers["Content-Type"]
					if response_content_type:
						# Strip any possible encoding data
						response_content_type = parse_header(response_content_type)[0]
						if response_content_type != guessed_content_type:
							#print("Re-calculating output path for received content type \"%s\"." % response_content_type, file=sys.stderr)
							outpath_filename = create_url_filename(url, response_content_type)
							outpath = os.path.join(outpath_prefix, outpath_filename)
							if os.path.exists(outpath):
								print("File path \"%s\" already exists; Skipping." % outpath, file=sys.stderr)
							else:
								# After getting the response data, write it to file
								write_to_unknown_dir(outpath, crawling_response.text)
								print("%s > %s" %(url, outpath), file=sys.stderr)
						else:
							# After getting the response data, write it to file
							write_to_unknown_dir(outpath, crawling_response.text)
							print("%s > %s" %(url, outpath), file=sys.stderr)
					else:
						# After getting the response data, write it to file
						write_to_unknown_dir(outpath, crawling_response.text)
						print("%s > %s" %(url, outpath), file=sys.stderr)
				
				except requests.HTTPError as e:
					code = crawling_response.status_code
					if attempts > max_retries:
						print("Received HTTP status %d while requesting URL \"%s\" (attempt %d); Giving up." %(code, url, attempts), file=sys.stderr)
						result.add(url)
					else:
						print("Received HTTP status %d while requesting URL \"%s\" (attempt %d); Will try again later." %(code, url, attempts), file=sys.stderr)
						url_attempt_queue.append((url, attempts))
			except requests.RequestException as e:
				print("An irrecoverable error occurred while requesting URL \"%s\" (attempt %d); Giving up: %s" %(url, attempts, e), file=sys.stderr)
				result.add(url)
					
	return result
		
def parse_reddit_thing_urls_from_response(response):
	data = response.json()["data"]
	reddit_thing_urls = parse_reddit_thing_urls(data)
	last_thing_name = data["after"]
	return reddit_thing_urls, last_thing_name

def parse_reddit_thing_urls(data):
	children = data["children"]
	#print("Processing %d child(ren)." % len(children))
	for child in children:
		child_data = child["data"]
		child_name = child_data["name"]
		child_creation_timestamp = child_data["created_utc"]
		url_attr = "url"
		url = child_data.get(url_attr)
		if url:
			yield (child_name, child_creation_timestamp, url)
		else:
			print("Reddit thing named \"%s\" has no \"%s\" attribute." %(child_name, url_attr), file=sys.stderr)
			
def write_to_unknown_dir(outpath, content):
	outdir = os.path.dirname(outpath)
	if not os.path.exists(outdir):
		os.makedirs(outdir)
	with open(outpath, 'w') as outf:
		outf.write(content)
		
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Scrape all links from the subreddit \"nottheonion\" and then crawl and save each the linked page.")
	parser.add_argument("-s", "--subreddit", default="nottheonion", help="The name of the subreddit to crawl.")
	parser.add_argument("secret", help="The reddit application secret.")
	parser.add_argument("outdir", help="The directory path under which the crawled pages will be saved.")
	parser.add_argument("-l", "--limit", default=None, help="The total number of reddit links to process.", metavar="COUNT", type=int)
	parser.add_argument("-b", "--batch-size", default=100, help="The maximum number of reddit things to request at once.", metavar="COUNT", type=int)
	parser.add_argument("-r", "--max-retries", default=3, help="The number of times to re-try requesting a given URL if a non-successful HTTP code is returned on the first attempt.", metavar="COUNT", type=int)
	
	args = parser.parse_args()
	print("Scraping links from subreddit \"%s\" and saving to \"%s\"." % (args.subreddit, args.outdir), file=sys.stderr)
	auth = ("_JNFnqor9ZT4mQ", args.secret)
	crawler = SubredditLinkCrawler(auth, args.subreddit, args.outdir, args.limit, args.batch_size, args.max_retries, __REDDIT_USER_AGENT_STR)
	stats = crawler.crawl()
	print_stats(stats, sys.stdout)
