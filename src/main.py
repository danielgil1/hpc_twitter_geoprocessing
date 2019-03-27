import json
import pandas as pd
from shapely.geometry import shape, Point
from pandas.io.json import json_normalize
from collections import Counter, defaultdict

grids = dict()

def getGrid(p):
	point = Point(p)
	result = list()
	for key, value in grids.items():
		if value.intersects(point):
			result.append(key)
	if result:
		return sorted(result)[0]
	else:
		return None


def readMap():
	filename = "../data/melbGrid.json"
	with open(filename) as f:
		data = json.load(f)
	data = data['features']
	for grid in data:
		grids[grid['properties']['id']] = shape(json.loads(json.dumps(grid['geometry'])))


def load(filename):
	with open(filename, encoding="utf8") as f:
		data = json.load(f)

	return data


def preprocess(json_data):
	df_raw = pd.DataFrame.from_dict(json_normalize(json_data['rows']), orient='columns')
	return df_raw[
		(df_raw['value.properties.location'] == 'melbourne') & (len(df_raw['doc.coordinates.coordinates']) > 0) & (
					len(df_raw['doc.entities.hashtags']) > 0)][['doc.coordinates.coordinates', 'doc.entities.hashtags']]


def lineByLineApproach(filename):
	post_counts = Counter()
	hashtag_counts = defaultdict(Counter)
	with open(filename, encoding="utf8") as f:
		f.readline()
		count = 1
		for line in f:
			try:
				if line.strip()[-1] == ',':
					data = json.loads(line.strip()[:-1])
				else:
					data = json.loads(line.strip())
			except:
				print(count, line)
			count = count + 1
			hashtags = data['doc']['entities']['hashtags']
			grid_name = getGrid(data['value']['geometry']['coordinates'])
			if grid_name:
				post_counts[grid_name] += 1
				for hashtag in hashtags:
					hashtag_counts[grid_name][hashtag['text'].lower()] += 1

	return post_counts, hashtag_counts


def dataFrameApproach(filename):
	df = preprocess(load(filename))
	count_hashtags = defaultdict(Counter)
	count_posts = Counter()

	for index, row in df.iterrows():
		coordinates = row['doc.coordinates.coordinates']
		hashtags = row['doc.entities.hashtags']
		grid_name = getGrid(coordinates)
		if grid_name:
			count_posts[grid_name] += 1
			for hashtag in hashtags:
				count_hashtags[grid_name][hashtag['text'].lower()] += 1

	return count_posts, count_hashtags


def main():
	filename = "../data/tinyTwitter.json"
	readMap()

	# line by line approach
	counts, hashtag_counts = lineByLineApproach(filename)

	## Counts using dataframe
	# counts, hashtag_counts = dataFrameApproach(filename)

	for grid in counts.most_common():
		print(grid[0],":",grid[1],"posts")
	print("***************************")
	for grid in counts.most_common():
		print(grid[0], ":", hashtag_counts[grid[0]].most_common(5))

	## Check grid location
	# print(getGrid([144.850000, -37.600000])) # horizontal overlap
	# print(getGrid([144.92340088, -37.95935781]))
	# print(getGrid((144.850000, -37.650000))) # four box overlap
	# print(getGrid((145.400000, -38.000000))) #inside box
	# print(getGrid((145.300000, -38.100000))) # Vertical overlap


if __name__ == "__main__":
	main()