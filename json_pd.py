import json
import pandas as pd
import requests
from textblob import TextBlob
tesla_senti=pd.read_json('amazon_data.json')
print(type(tesla_senti))
req=tesla_senti['results']
req=pd.json_normalize(req)

df=req[['url','text']]
for i in df.index:
    a= df['text'][i]
    check=TextBlob(a)
    print(check.sentiment)

df.to_excel('amazon_senti.xlsx')
