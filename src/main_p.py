import mpi4py
from mpi4py import MPI
import json
import pandas as pd
# from shapely.geometry import shape, Point
from pandas.io.json import json_normalize
from collections import Counter, defaultdict
from main import getGrid

import os

MASTER_RANK = 0

def lineByLineApproach(line):
    post_counts = Counter()
    hashtag_counts = defaultdict(Counter)
    count=0
   
    if line.strip()[-1] == ',':
        data = json.loads(line.strip()[:-1])
    else:
        data = json.loads(line.strip())
    
    count += 1
    
    hashtags = data['doc']['entities']['hashtags']
  
    
    grid_name = getGrid(data['doc']['coordinates']['coordinates'])
    
    if grid_name:
        post_counts[grid_name] += 1
    for hashtag in hashtags:
        hashtag_counts[grid_name][hashtag['text'].lower()] += 1

    return post_counts, hashtag_counts

def marshall_tweets(comm):
    processes = comm.Get_size()
    counts = []
    #Now ask all processes except oursevles to return counts
    for i in range(processes-1):
        # Send request
        comm.send('return_data', dest=(i+1), tag=(i+1))
    for i in range(processes-1):
        # Receive data
        counts.append(comm.recv(source=(i+1), tag=MASTER_RANK))

    return counts

def process_tweets(rank, input_file, size):
    with open(input_file, encoding="utf8") as f:
        count={}
        # Send tweets to slave processes
        for i, line in enumerate(f):
            if i%size == rank:
                print(i," => data: ",line[0],", size=",size,", rank=",rank)
                count[rank]=i
        
    return count

def master_tweet_processor(comm, input_file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()
    occurences=defaultdict()
    
    counts=process_tweets(rank, input_file, size)
    if size > 1:
      counts = marshall_tweets(comm)
      # Marshall  data
      

      # Turn everything off
      for i in range(size-1):
        # Receive data
        comm.send('exit', dest=(i+1), tag=(i+1))

    # Print output
    #print(occurences)


def slave_tweet_processor(comm,input_file):
  # We want to process all relevant tweets and send our counts back
  # to master when asked
  # Find my tweets
  rank = comm.Get_rank()
  size = comm.Get_size()

  #counts = process_tweets(rank, input_file, size)
  counts = process_tweets(rank, input_file, size)
  # Now that we have our counts then wait to see when we return them.
  while True:
    in_comm = comm.recv(source=MASTER_RANK, tag=rank)
    # Check if command
    if isinstance(in_comm, str):
      if in_comm in ("return_data"):
        # Send data back
        # print("Process: ", rank, " sending back ", len(counts), " items")
        comm.send(counts, dest=MASTER_RANK, tag=MASTER_RANK)
      elif in_comm in ("exit"):
        exit(0)

def main():
  # Get
  input_file ="data/test.csv"
  
  # Work out our rank, and run either master or slave process
  comm=MPI.COMM_WORLD
  rank=comm.Get_rank()
  size= comm.Get_size()
  
  
  if rank == 0 :
    # We are master
    print("Sending to master, rank=",rank,"size=",size)
    master_tweet_processor(comm, input_file)
  else:
    # We are slave
    print("Sending to slave, rank=",rank,"size=",size)
    slave_tweet_processor(comm, input_file)

# Run the actual program
if __name__ == "__main__":
  main()