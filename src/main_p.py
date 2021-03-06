#!/usr/bin/python
# -*- coding: utf-8 -*-
import mpi4py
from mpi4py import MPI
import json
import sys
from collections import Counter, defaultdict
import time
import re

MASTER_RANK = 0

# Saving grid information in a global list
grids = list()

# Data Structure for Grids
class Grid:

	def __init__(
			self,
			id,
			xmin,
			xmax,
			ymin,
			ymax,
	):
		self.id = id
		self.xmin = xmin
		self.xmax = xmax
		self.ymin = ymin
		self.ymax = ymax

	def check_grid(self, x, y):
		return x >= self.xmin and x <= self.xmax and y >= self.ymin \
			   and y <= self.ymax


# Get grid of a lat-log point
def getGrid(p):
	if p:
		result = list()
		for grid in grids:
			if grid.check_grid(p[0], p[1]):
				result.append(grid.id)
		grids_no = len(result)
		if grids_no == 1:
			return result[0]
		elif grids_no > 1:
			return sorted(result)[0]
		else:
			return None
	else:
		return None


# Read map file and save it to global list
def readMap():
	filename = '../data/melbGrid.json'
	with open(filename) as f:
		data = json.load(f)
	data = data['features']
	for grid in data:
		grids.append(Grid(grid['properties']['id'], grid['properties'
		]['xmin'], grid['properties']['xmax'],
						  grid['properties']['ymin'], grid['properties'
						  ]['ymax']))


# Marshall Tweets from all processes
def marshall_tweets(comm):
	processes = comm.Get_size()
	counts = []
	# Now ask all processes except oursevles to return counts
	for i in range(processes - 1):
		# Send request
		comm.send('return_data', dest=i + 1, tag=i + 1)
	for i in range(processes - 1):
		# Receive data
		counts.append(comm.recv(source=i + 1, tag=MASTER_RANK))

	return counts


# Process tweets line by line and count post and hashtags if in grid
def process_tweets(rank, input_file, size):
	with open(input_file, encoding='utf8') as f:
		f.readline()
		post_counts = Counter()
		hashtag_counts = defaultdict(Counter)

		# read the file
		for (i, line) in enumerate(f):
			if i % size == rank:
				try:
					if line.strip()[-1] == ',':
						data = json.loads(line.strip()[:-1])
					else:
						data = json.loads(line.strip())
				except:
					continue
				try:
					grid_name = getGrid(data['doc']['coordinates'
										]['coordinates'])
				except:
					continue
				if grid_name:
					post_counts[grid_name] += 1
					hashtags = re.findall(r"(?<=\s)#\S+(?=\s)", data['doc']['text'])
					for hashtag in hashtags:
						hashtag_counts[grid_name][hashtag.lower()] += 1

	return post_counts, hashtag_counts


# Merging results
def master_tweet_processor(comm, input_file):
	# Read our tweets
	rank = comm.Get_rank()
	size = comm.Get_size()
	total_count_posts = Counter()
	total_count_hashtags = defaultdict(Counter)

	(total_count_posts, total_count_hashtags) = process_tweets(rank, input_file, size)
	if size > 1:
		counts = marshall_tweets(comm)
		# Marshall  data
		for worker_count in counts:
			total_count_posts = total_count_posts + worker_count[0]
			for (gridHashKey, gridHashDict) in worker_count[1].items():
				for (hashtag, hashtag_count) in gridHashDict.items():
					total_count_hashtags[gridHashKey][hashtag] = \
						total_count_hashtags[gridHashKey][hashtag] \
						+ hashtag_count

	# Turn everything off
	for i in range(size - 1):
		# Receive data
		comm.send('exit', dest=i + 1, tag=i + 1)
	print_results(total_count_posts,total_count_hashtags)


# Count tweets by slave
def slave_tweet_processor(comm, input_file):
	# We want to process all relevant tweets and send our counts back
	# to master when asked
	# Find my tweets
	rank = comm.Get_rank()
	size = comm.Get_size()
	(post_counts, hastag_counts) = process_tweets(rank, input_file, size)

	# Now that we have our counts then wait to see when we return them.
	while True:
		in_comm = comm.recv(source=MASTER_RANK, tag=rank)
		# Check if command
		if isinstance(in_comm, str):
			if in_comm in 'return_data':
				# Send data back
				comm.send((post_counts, hastag_counts), dest=MASTER_RANK, tag=MASTER_RANK)
			elif in_comm in 'exit':
				exit(0)


# Print results
def print_results(total_count_posts,total_count_hashtags):
	for grid in total_count_posts.most_common():
		print(grid[0],":",grid[1],"posts")
	print("***************************")
	for grid in total_count_posts.most_common():
		print(grid[0], ":", total_count_hashtags[grid[0]].most_common(5))


def main(argv):
	# Get Input file
	input_file = argv[1]

	# Work out our rank, and run either master or slave process
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	size = comm.Get_size()
	print('Runining with size:', size, ' Rank:', rank)
	readMap()

	if rank == 0:
		# We are master
		# print("Sending to master, rank=", rank, "size=", size)
		master_tweet_processor(comm, input_file)
	else:
		# We are slave
		# print("Sending to slave, rank=", rank, "size=", size)
		slave_tweet_processor(comm, input_file)


if __name__ == '__main__':
	start = time.time()
	main(sys.argv)
	end = time.time()
	(hours, rem) = divmod(end - start, 3600)
	(minutes, seconds) = divmod(rem, 60)
	print('Time Elapsed :', '{:0>2}:{:0>2}:{:05.2f}'.format(int(hours), int(minutes), seconds))