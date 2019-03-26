import json
from shapely.geometry import shape, Point

grids = dict()

def getGrid(p):
	point = Point(p)
	result = list()
	for key, value in grids.items():
		if value.intersects(point):
			result.append(key)
		# # Doesn't consider the border case
		# if point.within(value):
		# 	result.append(key)
		# if value.contains(point):
		# 	result.append(key)
	print(point, " in ", result)
	return sorted(result)[0]


def readMap():
	filename = "data/melbGrid.json"
	with open(filename) as f:
		data = json.load(f)
	data = data['features']
	for grid in data:
		grids[grid['properties']['id']] = shape(json.loads(json.dumps(grid['geometry'])))


def main():
	readMap()
	print(getGrid([144.850000, -37.600000])) # horizontal overlap
	# print(getGrid((144.850000, -37.650000))) # four box overlap
	# print(getGrid((145.400000, -38.000000))) #inside box
	# print(getGrid((145.300000, -38.100000))) # Vertical overlap

if __name__ == "__main__":
	main()