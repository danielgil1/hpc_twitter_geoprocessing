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
		return "Z1" # filter this tweets


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


def df_count_hashtags(df):
	grid_dict = defaultdict(Counter)
	for index, row in df.iterrows():
		coordinates = row['doc.coordinates.coordinates']
		hashtags = row['doc.entities.hashtags']
		grid_name = getGrid(coordinates)
		for hashtag in hashtags:
			grid_dict[grid_name][hashtag['text']] += 1

	return grid_dict


def count_posts(df):
	grid_dict = Counter()
	for index, row in df.iterrows():
		grid_name = getGrid(row['doc.coordinates.coordinates'])
		grid_dict[grid_name] += 1

	return grid_dict

def main():
	filename = "../data/smallTwitter.json"
	df = preprocess(load(filename))
	readMap()
	counts = count_posts(df)
	hashtag_counts = df_count_hashtags(df)
	for grid in counts.most_common():
		print(grid[0],":",grid[1],"posts")
	print("***************************")
	for grid in counts.most_common():
		print(grid[0], ":", hashtag_counts[grid[0]].most_common(5))
	# print(getGrid([144.850000, -37.600000])) # horizontal overlap
	# print(getGrid([144.92340088, -37.95935781]))
	# print(getGrid((144.850000, -37.650000))) # four box overlap
	# print(getGrid((145.400000, -38.000000))) #inside box
	# print(getGrid((145.300000, -38.100000))) # Vertical overlap


if __name__ == "__main__":
	main()