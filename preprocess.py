import pandas as pd
from pandas.io.json import json_normalize
import json
from pprint import pprint

def preprocess():
    filename="data/tinyTwitter.json"
    #df=pd.read_json(filename)
    #print(df.head())

    with open(filename) as f:
        data = json.load(f)
        #pprint(data)

    
    df3=pd.DataFrame.from_dict(json_normalize(data['rows']), orient='columns')
    print(df3.columns)
    df4=df3[df3['value.properties.location']=='melbourne'][['doc._id','doc.coordinates.coordinates','doc.text']]
    print(df4.head())
    print(df4.shape)

if __name__ == "__main__":
    preprocess()