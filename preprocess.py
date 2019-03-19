import pandas as pd

def preprocess():
    df=pd.read_json("data/tinyTwitter.json")
    print(df.head())

if __name__ == "__main__":
    preprocess()