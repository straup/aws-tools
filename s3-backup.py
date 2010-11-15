#!/usr/bin/env python

"""There are many S3 backup tools. This is mine."""

import os
import os.path
import httplib
import logging
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

logging.basicConfig(level=logging.INFO)

class s3:

	def __init__(self, cfg):

		self.cfg = cfg
		self.conn = None

	def backup(self, options):

		for p in ('directory', 'bucket', 'public', 'prefix', 'modified', 'force', 'debug'):
			logging.info('%s: %s' % (p, getattr(options, p)))

		# sudo put me in another method...

		bucket = None

		try:

			# sudo put me in another method...

			if not self.conn:
				access_key = self.cfg.get('aws', 'access_key')
				access_secret = self.cfg.get('aws', 'access_secret')
				self.conn = S3Connection(access_key, access_secret)

			for b in self.conn.get_all_buckets():

				if b.name == options.bucket:
					bucket = b
					break

			if not bucket:
				bucket = self.conn.create_bucket(options.bucket)

		except Exception, e:
			logging.error('failed to get on with AWS: %s' % e)

			if debug:
				logging.info('debugging is enabled, so carrying on anyway...')
			else:
				return None

		#

		counter = 0

		for root, dirs, files in os.walk(options.directory):
			for f in files:

				fullpath = os.path.join(root, f)
				shortpath = fullpath.replace(options.directory, '').lstrip('/')

                                if options.prefix:
                                        shortpath = '%s/%s' % (options.prefix, shortpath)
                                        
				aws_path = "%s/%s" % (options.bucket, shortpath)
				aws_url = 'http://s3.amazonaws.com/%s' % aws_path

				if options.debug:
					logging.info('fullpath: %s' % fullpath)                                
					logging.info('shortpath: %s' % shortpath)
					logging.info('aws url: %s' % aws_url)                                        
					continue

                                if self.is_cached(options, bucket, shortpath, fullpath, aws_url):
                                        continue

				try:
					k = Key(bucket)
					k.key = shortpath
					k.set_contents_from_filename(fullpath)

					mtime = os.path.getmtime(fullpath)
					k.set_metadata('x-mtime', mtime)

					if options.public:
						k.set_acl('public-read')

					logging.info("%s stored at %s" % (fullpath, aws_url))
					counter += 1

				except Exception, e:
					logging.error("failed to store %s (%s) :%s" % (fullpath, aws_url, e))

		return counter

        def is_cached(self, options, bucket, shortpath, local_path, aws_url):

		if options.force:
		        logging.info('not cached (force enabled)')
                        return False
                
                try:

			aws_t = None

			if options.public:
				http_conn = httplib.HTTPConnection("s3.amazonaws.com")
				# http_conn.set_debuglevel(3)

				http_conn.request("HEAD", aws_url)
				rsp = http_conn.getresponse()

				if rsp.status != 200:

			        	logging.info('HEAD returned %s (%s)' % (rsp.status, aws_url))
                        		return False
                        
				if not options.modified:
					logging.info("%s has already been stored" % aws_url)
					return True

                		last_modified = rsp.getheader('last-modified')

				# Last-Modified: Sun, 11 Jul 2010 15:42:30 GMT
				format = "%a, %d %b %Y %H:%M:%S GMT"

				aws_t = int(time.mktime(time.strptime(last_modified, format)))

			else:

				logging.info('fetch cache info from S3 %s' % shortpath)

				k = Key(bucket)
				k.key = shortpath

				if not k.exists() :
					logging.info('key %s does not exist' % shortpath)
					return False

				aws_t = k.get_metadata('x-mtime')

				if not aws_t:

					k.set_metadata('x-mtime', os.path.getmtime(local_path))
					logging.info('File exists, but has no mtime. Setting x-mtime and returning true.')
					return True

			local_t = os.path.getmtime(local_path)

			logging.info("last modified local:%s remote:%s" % (local_t, aws_t))

                        if local_t <= aws_t:
				logging.info("%s not modified, skipping" % local_path)
				return True
                        
		except Exception, e:
                	logging.error('failed to determine cache status for %s: %s' % (aws_url, e))

                return False

                
if __name__ == '__main__':

	import ConfigParser
	import optparse
	import sys

	parser = optparse.OptionParser()

	parser.add_option('-c', '--config', dest='config', action='store')
	parser.add_option('-D', '--directory', dest='directory', action='store')
	parser.add_option('-B', '--bucket', dest='bucket', action='store')
	parser.add_option('-P', '--public', dest='public', action='store_true', default=False)

	parser.add_option('-p', '--prefix', dest='prefix', action='store', default=None)
	parser.add_option('-f', '--force', dest='force', action='store_true', default=False)
	parser.add_option('-d', '--debug', dest='debug', action='store_true', default=False)
	parser.add_option('-m', '--modified', dest='modified', action='store_true', default=False)
        
	options, args = parser.parse_args()

	cfg = ConfigParser.ConfigParser()
	cfg.read(options.config)

	s = s3(cfg)
	c = s.backup(options)

	logging.info('backup completed, %s new files stored' % c)
	sys.exit()
