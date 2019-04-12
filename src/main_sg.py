import numpy as np
import mpi4py
from mpi4py import MPI
import json
import sys
from collections import Counter, defaultdict
import time
import os
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
    filename = 'melbGrid.json'
    with open(filename) as f:
        data = json.load(f)
    data = data['features']
    for grid in data:
        grids.append(Grid(grid['properties']['id'], grid['properties'
        ]['xmin'], grid['properties']['xmax'],
                          grid['properties']['ymin'], grid['properties'
                          ]['ymax']))


# Split the file in chunks
def get_chunks(input_file,size):
    chunks=list()
    total_lines=int(os.popen('wc -l '+input_file).readline().split()[0])-1
    
    chunk_size=int(total_lines/size)
    
    # initialize chunk size to make sure we have dummy work for all cores
    chunks=[(0,0)]*size
    
    for chunkIndex in range(size):
        if (chunkIndex==size-1):
            chunks[chunkIndex]=(chunkIndex*chunk_size,total_lines)
        else:
            chunks[chunkIndex]=(chunkIndex*chunk_size,chunkIndex*chunk_size+chunk_size-1)


    
    return chunks


# print result
def print_results(total_count_posts,total_count_hashtags):
	for grid in total_count_posts.most_common():
		print(grid[0],":",grid[1],"posts")
	print("***************************")
	for grid in total_count_posts.most_common():
		print(grid[0], ":", total_count_hashtags[grid[0]].most_common(5))


def main(argv):
    # Get
    input_file = '' + argv[1]

    # Work out our rank, and run either master or slave process
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    # print('Runining with size:', size, ' Rank:', rank)
    readMap() 
    data = None
    chunks = [] 
    
    if rank==0:
        chunks=get_chunks(input_file,size)
    else:
        chunks=None
    my_chunk=comm.scatter(chunks,root=0)
    init, end = my_chunk
    post_counts = Counter()
    hashtag_counts = defaultdict(Counter)
    with open(input_file, encoding='utf8') as f:
        f.readline()
        for (i, line) in enumerate(f):
            if i>=init and i<= end:
                try:
                    if line.strip()[-1] == ',':
                        data = json.loads(line.strip()[:-1])
                    else:
                        data = json.loads(line.strip())
                except:
                    continue
                try:
                    grid_name = getGrid(data['doc']['coordinates']['coordinates'])
                except:
                    continue
                if grid_name:
                    post_counts[grid_name] += 1
                    hashtags = re.findall(r"(?<=\s)#\S+(?=\s)", data['doc']['text'])
                    for hashtag in hashtags:
                        hashtag_counts[grid_name][hashtag.lower()] += 1
   
    counts=comm.gather((post_counts, hashtag_counts),root=0)

    total_count_posts = Counter()
    total_count_hashtags = defaultdict(Counter)

    if rank==0:
        # Put everything together
        for worker_count in counts:
            total_count_posts = total_count_posts + worker_count[0]
            for (gridHashKey, gridHashDict) in worker_count[1].items():
                for (hashtag, hashtag_count) in gridHashDict.items():
                    total_count_hashtags[gridHashKey][hashtag] = \
                    total_count_hashtags[gridHashKey][hashtag] \
                    + hashtag_count
    
        # print
        print_results(total_count_posts,total_count_hashtags)
        

if __name__ == '__main__':
    start = time.time()
    argv=sys.argv
    if (len(argv)<2):
        argv.append("smallTwitter.json")
    main(argv)
    end = time.time()
    (hours, rem) = divmod(end - start, 3600)
    (minutes, seconds) = divmod(rem, 60)
    print('Time Elapsed :', '{:0>2}:{:0>2}:{:05.2f}'.format(int(hours), int(minutes), seconds))
    