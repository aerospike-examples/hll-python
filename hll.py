#!/usr/bin/python

import argparse
import random

import aerospike
from aerospike_helpers.operations import hll_operations
import generator


parser = argparse.ArgumentParser(description='HLL example')
parser.add_argument('--host', dest='host', default='127.0.0.1')
args = parser.parse_args()

HLL_BIN = 'hll_bin'
HLL_INDEX_BITS = 12
HLL_MH_BITS = 0
MONTHS = 12




def init(host, port):
	config = {
		'hosts': [
			( host, port ),
		],
		'policies': {
			'timeout': 1000 # milliseconds
		}
	}

	client = aerospike.client(config).connect()
	return client


def getkey(name, time):
	return ('test', 'hll', '%s:%d' % (name, time))


# Ingest records with id in the range [start, end).
def ingest_month(client, start, end, month):
	# We don't need to initialise HLL bins; performing a HLL add to an empty bin will initialise it.
	print 'Ingest ids %d-%d month %d' % (start, end, month)

	for i in range(start, end):
		profile = generator.get_profile(i, month)
		#print("Profile %d: %s" % (i, profile))
		for tag in profile:
			ops = [
				hll_operations.hll_add(HLL_BIN, [str(i)], HLL_INDEX_BITS)
			]

			_, _, result = client.operate(getkey(tag, month), ops)


def ingest(client):
	for i in range(MONTHS):
		ingest_month(client, 1, 20000, i)


def count(client, tag, month):
	ops = [
		hll_operations.hll_get_count(HLL_BIN)
	]
	_, _, result = client.operate(getkey(tag, month), ops)
	print 'tag:%s month:%d count:%d' % (tag, month, result[HLL_BIN])
	#print result


def get_union_count(client, tag, t0, months):
	times = range(t0+1, months)
	records = [record[2][HLL_BIN] for record in client.get_many([getkey(tag, time) for time in times])]

	ops = [
		hll_operations.hll_get_union_count(HLL_BIN, records)
	]

	_, _, result = client.operate(getkey(tag, t0), ops)
	print 'tag:%s months:%d-%d count:%d' % (tag, t0, t0+months-1, result[HLL_BIN])


def get_intersect_count(client, tags, month):
	records = [record[2][HLL_BIN] for record in client.get_many([getkey(tag, month) for tag in tags[1:]])]

	ops = [
		hll_operations.hll_get_intersect_count(HLL_BIN, records)
	]

	_, _, result = client.operate(getkey(tags[0], month), ops)
	print 'tags:%s month:%d count:%d' % (tags, month, result[HLL_BIN])


def main():
	client = init(args.host, 3000)
	ingest(client)

	# Get counts
	for i in range(MONTHS):
		count(client, 'aerospike', i)

	# Get union
	get_union_count(client, 'aerospike', 0, MONTHS)


	# Get intersection
	count(client, 'vancouver', 0)
	count(client, 'canada', 0)
	count(client, 'washington', 0)
	get_intersect_count(client, ['vancouver', 'washington'], 0)


main()
