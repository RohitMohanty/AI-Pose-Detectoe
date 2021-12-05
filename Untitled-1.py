import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import numpy as np
from boto.s3.key import Key
import boto3
import io
import xlrd
from watson_developer_cloud import DiscoveryV1



# authenticator = IAMAuthenticator('Ot1eLhMpkOUmtdmH53JOJNm0A8GaUp-WQ4i7xBXBhzT1')
# discovery = DiscoveryV1(
#     version='2019-04-30',
#     authenticator=authenticator
# )

class Sentiment:
    def __init__(self):
        pass
    
    def get_watson_company_sentiment(self,name,feature):
        #name="IBM"
        #count=200
        discovery = DiscoveryV1(
                                version="2018-03-05",
                                username="apikey",
                                password="vyV-_GflS_g-c5SsKNu7FeDV-0m6EhP75kmvV68yAOcD"
                            )
        my_query = discovery.query("system","news-en", filter='enriched_text.semantic_roles.object.text:"salary"', query='enriched_title.entities.text:"amazon",enriched_title.entities.type::"Company",enriched_text.concepts.text:"salary"',natural_language_query=None,passages=None, aggregation=None, count=None, return_fields=None, offset=None, sort=None, highlight=None, passages_fields=None, passages_count=None, passages_characters=None, deduplicate=True, deduplicate_field=None)
        
        with open('data.txt', 'w') as outfile:
            json.dump(my_query.result, outfile)
        return(my_query.result)

Sentiment().get_watson_company_sentiment('Tesla','Energy efficiency policy')
        

# enriched_text.entities.text:"google",enriched_text.entities.type:"company",enriched_text.keywords.text:"Diversity"
# enriched_text.entities.text:"amazon",enriched_text.entities.type:"company",enriched_text.concepts.text:"salary "
# enriched_text.entities.text:"levis",enriched_text.entities.type:"company",enriched_text.concepts.text:"salary "
# enriched_text.entities.text:"levis",enriched_text.entities.type:"company",enriched_text.concepts.text:"diversity"



