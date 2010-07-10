#!/usr/bin/env python

"""There are many S3 backup tools. This is mine."""

import os
import os.path
import httplib
import logging

from boto.s3.connection import S3Connection
from boto.s3.key import Key

logging.basicConfig(level=logging.INFO)

class s3:

	def __init__(self, cfg):

		self.cfg = cfg
		self.conn = None

	def backup(self, directory, bucket_name, **kwargs):

		public = kwargs.get('public', False)
		force = kwargs.get('force', False)
		debug = kwargs.get('debug', False)
		purge = kwargs.get('purge', False)

		logging.info('backup %s to AWS S3 bucket %s' % (directory, bucket_name))
		logging.info('public:%s force:%s debug: %s' % (public, force, debug))

		# sudo put me in another method...

		bucket = None

		try:

			# sudo put me in another method...

			if not self.conn:
				access_key = self.cfg.get('aws', 'access_key')
				access_secret = self.cfg.get('aws', 'access_secret')
				self.conn = S3Connection(access_key, access_secret)

			for b in self.conn.get_all_buckets():

				if b.name == bucket_name:
					bucket = b
					break

			if not bucket:
				bucket = self.conn.create_bucket(bucket_name)

		except Exception, e:
			logging.error('failed to get on with AWS: %s' % e)

			if debug:
				logging.info('debugging is enabled, so carrying on anyway...')
			else:
				return None

		#

		if not debug and purge:
			logging.info('purge all keys from %s' % bucket_name)

			for key in bucket.list():
				logging.info('purging %s' % key.name)
				key.delete()

		#

		counter = 0

		for root, dirs, files in os.walk(directory):
			for f in files:

				fullpath = os.path.join(root, f)
				shortpath = fullpath.replace(directory, '').lstrip('/')

				uri = "%s/%s" % (bucket_name, shortpath)

				# logging.info('file: %s' % fullpath)                                
				# logging.info('uri: %s' % uri)

				if debug:
					continue

				try:

					# sudo put me in another method...

					aws_path = "/%s/%s" % (bucket_name, shortpath)
					aws_url = 'http://s3.amazonaws.com/%s' % aws_path

					# sudo store/check this stuff in a sqlite database...

					if not force:
						http_conn = httplib.HTTPConnection("s3.amazonaws.com")
						http_conn.request("HEAD", aws_path)
						rsp = http_conn.getresponse()

						if rsp.status == 200 :
							logging.info("%s has already been stored" % aws_url)
							continue

						k = Key(bucket)
						k.key = shortpath
						k.set_contents_from_filename(fullpath)

					if public:
						k.set_acl('public-read')

					logging.info("%s stored at %s" % (fullpath, aws_url))
					counter += 1

				except Exception, e:
					logging.error("failed to fetch/store %s (url) :%s" % (fullpath, aws_url, e))

		return counter

if __name__ == '__main__':

	import ConfigParser
	import optparse
	import sys

	parser = optparse.OptionParser()

	parser.add_option('-c', '--config', dest='config', action='store')
	parser.add_option('-D', '--directory', dest='directory', action='store')
	parser.add_option('-B', '--bucket', dest='bucket', action='store')
	parser.add_option('-P', '--public', dest='public', action='store_true', default=False)
	parser.add_option('-f', '--force', dest='force', action='store_true', default=False)
	parser.add_option('-d', '--debug', dest='debug', action='store_true', default=False)

	options, args = parser.parse_args()

	cfg = ConfigParser.ConfigParser()
	cfg.read(options.config)

	s = s3(cfg)
	c = s.backup(options.directory, options.bucket, public=options.public, force=options.force, debug=options.debug)

	logging.info('backup completed, %s new files stored' % c)
	sys.exit()
