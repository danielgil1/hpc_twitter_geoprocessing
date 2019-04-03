import mpi4py
from mpi4py import MPI
import json
import sys
from collections import Counter, defaultdict
import time

import os

MASTER_RANK = 0

grids = list()


class Grid:
  def __init__(self, id, xmin, xmax, ymin, ymax):
    self.id = id
    self.xmin = xmin
    self.xmax = xmax
    self.ymin = ymin
    self.ymax = ymax

  def check_grid(self, x, y):
    return (x >= self.xmin and x <= self.xmax) and (y >= self.ymin and y <= self.ymax)


def marshall_tweets(comm):
  processes = comm.Get_size()
  counts = []
  # Now ask all processes except oursevles to return counts
  for i in range(processes - 1):
    # Send request
    comm.send('return_data', dest=(i + 1), tag=(i + 1))
  for i in range(processes - 1):
    # Receive data
    counts.append(comm.recv(source=(i + 1), tag=MASTER_RANK))

  return counts


def getGrid(p):
  if (p):
    result = list()
    for grid in grids:
      if grid.check_grid(p[0], p[1]):
        result.append(grid.id)
    if result:
      return sorted(result)[0]
    else:
      return None

  else:
    # print(p)
    return None


def process_tweets(rank, input_file, size):
  with open(input_file, encoding="utf8") as f:
    f.readline()
    post_counts = Counter()
    hashtag_counts = defaultdict(Counter)

    # Send tweets to slave processes
    for i, line in enumerate(f):
      if i % size == rank:
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
          hashtags = data['doc']['entities']['hashtags']
          for hashtag in hashtags:
            hashtag_counts[grid_name][hashtag['text'].lower()] += 1

  return post_counts, hashtag_counts


def master_tweet_processor(comm, input_file):
  # Read our tweets
  counts=[]
  rank = comm.Get_rank()
  size = comm.Get_size()
  total_count_posts = Counter()
  
  total_count_hashtags=defaultdict(Counter)
  total_count_posts, total_count_hashtags = process_tweets(rank, input_file, size)
  
  #print("Rank:", rank, "Post count:", total_count_posts)
  
  if size > 1:
    counts = marshall_tweets(comm)
  # Marshall  data

  for worker_count in counts:
    total_count_posts = total_count_posts + worker_count[0]
    for gridHashKey,gridHashDict in worker_count[1].items():
      
      for hashtag,hashtag_count in gridHashDict.items():
        total_count_hashtags[gridHashKey][hashtag] = total_count_hashtags[gridHashKey][hashtag] + hashtag_count

  # Turn everything off
  for i in range(size - 1):
    # Receive data
    comm.send('exit', dest=(i + 1), tag=(i + 1))

  # Print output
  # for grid in total_count_posts.most_common():
  #   print(grid[0],":",grid[1],"posts")
  # print("***************************")
  # for grid in total_count_posts.most_common():
  #   print(grid[0], ":", total_count_hashtags[grid[0]].most_common(5))


def slave_tweet_processor(comm, input_file):
  # We want to process all relevant tweets and send our counts back
  # to master when asked
  # Find my tweets
  rank = comm.Get_rank()
  size = comm.Get_size()

  # counts = process_tweets(rank, input_file, size)
  post_counts, hastag_counts = process_tweets(rank, input_file, size)
  # print("Rank:", rank, "Post count:", post_counts)
  # Now that we have our counts then wait to see when we return them.
  while True:
    in_comm = comm.recv(source=MASTER_RANK, tag=rank)
    # Check if command
    if isinstance(in_comm, str):
      if in_comm in ("return_data"):
        # Send data back
        # print("Process: ", rank, " sending back ", len(counts), " items")
        comm.send((post_counts, hastag_counts), dest=MASTER_RANK, tag=MASTER_RANK)
      elif in_comm in ("exit"):
        exit(0)


def readMap():
  filename = "../data/melbGrid.json"
  with open(filename) as f:
    data = json.load(f)
  data = data['features']
  for grid in data:
    grids.append(Grid(grid['properties']['id'], grid['properties']['xmin'], grid['properties']['xmax'],
              grid['properties']['ymin'], grid['properties']['ymax']))


def main(argv):

  # Get
  input_file = "../data/"+argv[1]

  # Work out our rank, and run either master or slave process
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  size = comm.Get_size()
  print("Runining with size:",size," Rank:",rank)
  readMap()

  if rank == 0:
    # We are master
    # print("Sending to master, rank=", rank, "size=", size)
    master_tweet_processor(comm, input_file)
  else:
    # We are slave
    # print("Sending to slave, rank=", rank, "size=", size)
    slave_tweet_processor(comm, input_file)


# Run the actual program
if __name__ == "__main__":
  start = time.time()
  main(sys.argv)
  end = time.time()
  hours, rem = divmod(end - start, 3600)
  minutes, seconds = divmod(rem, 60)
  print("Time Elapsed :","{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
