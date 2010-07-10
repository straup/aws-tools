#!/usr/bin/env python

import os
import os.path
import httplib
import logging

from boto.s3.connection import S3Connection
from boto.s3.key import Key

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

	import ConfigParser
	import optparse
	import sys

	parser = optparse.OptionParser()

	parser.add_option('-c', '--config', dest='config', action='store')
	parser.add_option('-B', '--bucket', dest='bucket', action='store')

	options, args = parser.parse_args()

	cfg = ConfigParser.ConfigParser()
	cfg.read(options.config)

	bucket = None
	deleted = 0
        
	access_key = cfg.get('aws', 'access_key')
	access_secret = cfg.get('aws', 'access_secret')
	conn = S3Connection(access_key, access_secret)

	for b in conn.get_all_buckets():

		if b.name == options.bucket:
			bucket = b
			break

        if not bucket:
        	logging.error('failed to locate any buckets named %s' % options.bucket)
		sys.exit()

        
        for key in bucket.list():
        	logging.info('delete %s' % key.name)
        	key.delete()
                deleted += 1

	"""
	print bucket.delete()
        sys.exit()
        """
        
	logging.info('deleted %s keys from %s' % (deleted, options.bucket))
	sys.exit()
