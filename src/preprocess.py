import pandas as pd
import json

from shapely.geometry import shape, Point
from pandas.io.json import json_normalize
from collections import Counter
from collections import defaultdict


def load(filename):
    with open(filename) as f:
        data = json.load(f)
    
    return data

def preprocess(json_data):

    df_raw=pd.DataFrame.from_dict(json_normalize(json_data['rows']), orient='columns')
    #print(df_raw['doc.coordinates.coordinates'].head())
    # get preview of column  - values - type
    #for column in df_raw.columns:
    #    print(column,"->",df_raw[column].iloc[0],"->",df_raw[column].dtype)
    
    # filter the dataset by location, coordinates and hashtags
    return df_raw[(df_raw['value.properties.location']=='melbourne') & (len(df_raw['doc.coordinates.coordinates'])>0) & (len(df_raw['doc.entities.hashtags'])>0)][['doc.coordinates.coordinates','doc.entities.hashtags']]
   
    


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
	if result:
		return sorted(result)[0]
	else:
		return "Z1" # filter this tweets


def readMap():
	filename = "data/melbGrid.json"
	with open(filename) as f:
		data = json.load(f)
	data = data['features']
	for grid in data:
		grids[grid['properties']['id']] = shape(json.loads(json.dumps(grid['geometry'])))


def df_count_hashtags(df):
    grid_dict=defaultdict(Counter)
    for index,row in df.iterrows():
        coordinates=row['doc.coordinates.coordinates']
        hashtags=row['doc.entities.hashtags']
        
        grid_name=getGrid(coordinates)
        
        for hashtag in hashtags:
            grid_dict[grid_name][hashtag['text']]+=1
            
        
        

    return grid_dict



if __name__ == "__main__":
    dic_polygon=defaultdict()
    filename="data/tinyTwitter.json"
    readMap()
    df=preprocess(load(filename))

    print(df_count_hashtags(df[0:50]))
    
    
    