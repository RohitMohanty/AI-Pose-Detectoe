import re
import pandas as pd
import nltk
from nltk.tokenize import sent_tokenize
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pandas_datareader import data as pdr
import json 
import requests
import datetime
from datetime import date
import schedule
import yfinance as yf
from pandas_datareader import data as pdr
import logging
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

def connect_elasticsearch_aws():
    _es = None
    _es = Elasticsearch(
['https://search-equbot-ln-es-v2v7spplqpcsjwxg5hg3qmhajq.us-east-1.es.amazonaws.com'],
    http_auth=('equbot_es', 'Equb0t!23'),
    scheme="https",
    port=443
)
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es

es_aws = connect_elasticsearch_aws()




url= "http://52.2.217.192:8080/masteractivefirms/getmasterbytag?tag=aimax"
response = requests.get(url)
json_data = response.json()
type(json_data['data']['masteractivefirms_aimax'])
master_list= pd.DataFrame(json_data['data']['masteractivefirms_aimax'])
master_list=master_list.drop(columns=["gvkey","country_code","exchange","ind_code","cusip","sedol","tags","ciq","updated_at","flag","bloombergticker","custodyticker","ipodate","fin_src","wind_code"])

sample_list=master_list.sample(n=20)
esg_list=[]

def get_wastage_query(company_name):
    query_temp_wastage='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"carbon footprint","operator":"and"}}},{"match":{"content":{"query":"carbon emission","operator":"and"}}},{"match":{"content":{"query":"greenhouse gases","operator":"and"}}},{"match":{"content":{"query":"CO2 emission","operator":"and"}}},{"match":{"content":{"query":"particulate matter","operator":"and"}}},{"match":{"content":{"query":"flaring gases","operator":"and"}}},{"match":{"content":"e-waste"}},{"match":{"content":{"query":"water pollution","operator":"and"}}},{"match":{"content":{"query":"ISO 14000","operator":"and"}}},{"match":{"content":"EMS"}},{"match":{"content":{"query":"carbon pricing","operator":"and"}}},{"match":{"content":{"query":"environmental fines","operator":"and"}}},{"match":{"content":{"query":"VOC emissions","operator":"and"}}},{"match":{"content":{"query":"NOx emissions","operator":"and"}}},{"match":{"content":"NO2"}},{"match":{"content":"NO3"}},{"match":{"content":{"query":"SOx emissions","operator":"and"}}},{"match":{"content":"SO2"}},{"match":{"content":"SO3"}},{"match":{"content":{"query":"accidental spills","operator":"and"}}}]}}]}}}'
        
    return es_aws.search(index="ln_index", body=query_temp_wastage, size=200)

# gives wastage_based articles 

def get_sustainable_query(company_name):
    query_temp_sustainable='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"Cement Energy","operator":"and"}}},{"match":{"content":{"query":"water efficiency","operator":"and"}}},{"match":{"content":{"query":"Renewable Energy","operator":"and"}}},{"match":{"content":{"query":"indirect Energy","operator":"and"}}},{"match":{"content":{"query":"Green Buildings","operator":"and"}}},{"match":{"content":{"query":"sustainable energy","operator":"and"}}},{"match":{"content":{"query":"waste recycle","operator":"and"}}},{"match":{"content":{"query":"energy efficiency","operator":"and"}}},{"match":{"content":{"query":"nuclear safety","operator":"and"}}},{"match":{"content":{"query":"toxic chemical","operator":"and"}}},{"match":{"content":{"query":"grid loss","operator":"and"}}},{"match":{"content":{"query":"coal produced","operator":"and"}}},{"match":{"content":{"query":"paper consumption","operator":"and"}}},{"match":{"content":{"query":"enviornment management","operator":"and"}}}]}}]}}}'
    
    return es_aws.search(index="ln_index", body=query_temp_sustainable, size=200)

# gives sustainable_based articles 

def get_innovate_query(company_name):
    query_temp_innovate='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query": "Hybrid Vehicles","operator":"and"}}},{"match":{"content":{"query":"Noise Reduction","operator":"and"}}},{"match":{"content":{"query":"nuclear energy","operator":"and"}}},{"match":{"content":"Agrochemical"}},{"match":{"content":{"query":"Animal Testing","operator":"and"}}},{"match":{"content":{"query":"Water technologies","operator":"and"}}},{"match":{"content":{"query":"equator principles","operator":"and"}}},{"match":{"content":{"query":"environmental projects","operator":"and"}}},{"match":{"content":{"query":"labeled wood","operator":"and"}}},{"match":{"content":{"query":"organic products","operator":"and"}}},{"match":{"content":{"query":"fleet fuel","operator":"and"}}}]}}]}}}'
    
    return es_aws.search(index="ln_index", body=query_temp_innovate, size=200)

# gives innovation_based articles 

def get_community_query(company_name):
    query_social_community= '{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"Whistleblower protection","operator":"and"}}},{"match":{"content":"whistleblower"}},{"match":{"content":"politics"}},{"match":{"content":{"query":"political contribution","operator":"and"}}},{"match":{"content":{"query":"tax fraud","operator":"and"}}},{"match":{"content":{"query":"business ethics","operator":"and"}}},{"match":{"content":{"query":"public health","operator":"and"}}},{"match":{"content":{"query":"lobbying contributions","operator":"and"}}},{"match":{"content":{"query":"intellectual property","operator":"and"}}},{"match":{"content":"bribery"}},{"match":{"content":"corruption"}},{"match":{"content":"fraud"}},{"match":{"content":"donations"}},{"match":{"content":{"query":"community involvement","operator":"and"}}},{"match":{"content":{"query":"fair competition","operator":"and"}}},{"match":{"content":{"query":"anti competition","operator":"and"}}},{"match":{"content":{"query":"Extractive Industries Transparency Initiative","operator":"and"}}},{"match":{"content":{"query":"Crisis Management Systems","operator":"and"}}},{"match":{"content":"OECD"}},{"match":{"content":{"query":"critical countries","operator":"and"}}},{"match":{"content":{"query":"corporate responsibility","operator":"and"}}},{"match":{"content":"CSR"}}]}}]}}}'
    
    return es_aws.search(index="ln_index", body=query_social_community, size=200)

# gives community_based articles 

def get_humanright_query(company_name):
    query_social_humanright='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"human rights","operator":"and"}}},{"match":{"content":{"query":"child labor","operator":"and"}}},{"match":{"content":{"query":"forced labor","operator":"and"}}},{"match":{"content":{"query":"freedom of association","operator":"and"}}},{"match":{"content":{"query":"ethical trading","operator":"and"}}},{"match":{"content":{"query":"ethical trading initiative","operator":"and"}}}]}}]}}}'

    return es_aws.search(index="ln_index", body=query_social_humanright, size=200)

# gives humanright_based articles 

def get_workforce_query(company_name):
    query_social_workforce='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"workers union","operator":"and"}}},{"match":{"content":{"query":"employee turnover","operator":"and"}}},{"match":{"content":{"query":"labor contract","operator":"and"}}},{"match":{"content":{"query":"employee diversity","operator":"and"}}},{"match":{"content":{"query":"workforce diversity","operator":"and"}}},{"match":{"content":{"query":"employee satisfaction","operator":"and"}}},{"match":{"content":{"query":"women employee","operator":"and"}}},{"match":{"content":{"query":"day care","operator":"and"}}},{"match":{"content":{"query":"internal promotion","operator":"and"}}},{"match":{"content":{"query":"training hours","operator":"and"}}},{"match":{"content":{"query":"management training","operator":"and"}}},{"match":{"content":{"query":"health and safety training","operator":"and"}}},{"match":{"content":{"query":"health & safety training","operator":"and"}}},{"match":{"content":{"query":"employee health and safety","operator":"and"}}},{"match":{"content":{"query":"occupational diseases","operator":"and"}}},{"match":{"content":{"query":"contractor accidents","operator":"and"}}},{"match":{"content":{"query":"salary gap","operator":"and"}}},{"match":{"content":{"query":"employee accidents","operator":"and"}}},{"match":{"content":{"query":"employee fatalities","operator":"and"}}},{"match":{"content":{"query":"contractor fatalities","operator":"and"}}},{"match":{"content":{"query":"trade union","operator":"and"}}},{"match":{"content":"strikes"}},{"match":{"content":"layoffs"}},{"match":{"content":{"query":"medical insurance coverage","operator":"and"}}},{"match":{"content":{"query":"employee benefits","operator":"and"}}},{"match":{"content":{"query":"gender pay gap","operator":"and"}}},{"match":{"content":{"query":"employee resource group","operator":"and"}}},{"match":{"content":{"query":"supplier ESG training","operator":"and"}}},{"match":{"content":{"query":"ESG training","operator":"and"}}},{"match":{"content":{"query":"management departures","operator":"and"}}},{"match":{"content":{"query":"HIV-AIDS program","operator":"and"}}},{"match":{"content":{"query":"diversity opportunities","operator":"and"}}},{"match":{"content":{"query":"working hours","operator":"and"}}},{"match":{"content":{"query":"employee with disabilities","operator":"and"}}},{"match":{"content":{"query":"career development","operator":"and"}}},{"match":{"content":{"query":"skills training","operator":"and"}}},{"match":{"content":{"query":"employment creation","operator":"and"}}},{"match":{"content":{"query":"training and development","operator":"and"}}},{"match":{"content":{"query":"work injury","operator":"and"}}},{"match":{"content":{"query":"minority employee","operator":"and"}}},{"match":{"content":{"query":"HSMS certified","operator":"and"}}}]}}]}}}'
    
    return es_aws.search(index="ln_index", body=query_social_workforce, size=200)

# gives workforce_based articles 

def get_product_query(company_name):
    query_social_product='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"consumer complaints","operator":"and"}}},{"match":{"content":{"query":"quality management","operator":"and"}}},{"match":{"content":{"query":"responsible marketing","operator":"and"}}},{"match":{"content":{"query":"product quality","operator":"and"}}},{"match":{"content":{"query":"drug delay","operator":"and"}}},{"match":{"content":{"query":"customer satisfaction","operator":"and"}}},{"match":{"content":{"query":"consumer health and safety","operator":"and"}}},{"match":{"content":{"query":"data privacy","operator":"and"}}},{"match":{"content":{"query":"fair trade","operator":"and"}}},{"match":{"content":{"query":"product responsibility","operator":"and"}}},{"match":{"content":{"query":"ISO 9000","operator":"and"}}},{"match":{"content":{"query":"six sigma","operator":"and"}}},{"match":{"content":{"query":"retailing responsibility","operator":"and"}}},{"match":{"content":"alcohol"}},{"match":{"content":"gambling"}},{"match":{"content":"tobacco"}},{"match":{"content":"armaments"}},{"match":{"content":"pornography"}},{"match":{"content":"contraceptives"}},{"match":{"content":{"query":"obesity risk","operator":"and"}}},{"match":{"content":{"query":"product access","operator":"and"}}},{"match":{"content":{"query":"product delays","operator":"and"}}},{"match":{"content":{"query":"product recall","operator":"and"}}},{"match":{"content":"FDA"}},{"match":{"content":"privacy"}},{"match":{"content":{"query":"responsible R&D","operator":"and"}}},{"match":{"content":{"query":"QMS certified","operator":"and"}}},{"match":{"content":"abortifacients"}},{"match":{"content":{"query":"pork products","operator":"and"}}},{"match":{"content":{"query":"cyber security","operator":"and"}}},{"match":{"content":{"query":"animal well-being","operator":"and"}}},{"match":{"content":{"query":"healthy products","operator":"and"}}}]}}]}}}'
    
    return  es_aws.search(index="ln_index", body=query_social_product, size=200)

# gives product_based articles 

def get_management_query(company_name):
    query_governance_management='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"Audit committee","operator":"and"}}},{"match":{"content":{"query":"compensation committee","operator":"and"}}},{"match":{"content":{"query":"board meeting","operator":"and"}}},{"match":{"content":{"query":"succession plan"}}},{"match":{"content":{"query":"external consultant","operator":"and"}}},{"match":{"content":{"query":"nomination committee","operator":"and"}}},{"match":{"content":{"query":"board attendance","operator":"and"}}},{"match":{"content":{"query":"board structure","operator":"and"}}},{"match":{"content":{"query":"board tenure","operator":"and"}}},{"match":{"content":{"query":"board cultural diversity","operator":"and"}}},{"match":{"content":{"query":"board gender diversity","operator":"and"}}},{"match":{"content":{"query":"corporate governance","operator":"and"}}},{"match":{"content":{"query":"board functions","operator":"and"}}},{"match":{"content":{"query":"board size","operator":"and"}}},{"match":{"content":{"query":"board background","operator":"and"}}},{"match":{"content":{"query":"non-executive board member","operator":"and"}}},{"match":{"content":{"query":"board independence","operator":"and"}}},{"match":{"content":{"query":"board size","operator":"and"}}},{"match":{"content":{"query":"board experience","operator":"and"}}},{"match":{"content":{"query":"board diversity","operator":"and"}}},{"match":{"content":{"query":"nomination board committee","operator":"and"}}},{"match":{"content":"chairman"}},{"match":{"content":{"query":"CEO Board Member","operator":"and"}}},{"match":{"content":"CEO"}},{"match":{"content":{"query":"board membership","operator":"and"}}},{"match":{"content":{"query":"independent Board Member","operator":"and"}}},{"match":{"content":{"query":"Board Member Affiliation","operator":"and"}}},{"match":{"content":{"query":"board re-election","operator":"and"}}},{"match":{"content":{"query":"executive compensation","operator":"and"}}},{"match":{"content":{"query":"highest renumeration","operator":"and"}}},{"match":{"content":{"query":"board member compensation","operator":"and"}}},{"match":{"content":{"query":"sustainability compensation","operator":"and"}}},{"match":{"content":{"query":"executive retention","operator":"and"}}},{"match":{"content":{"query":"board committe compensation","operator":"and"}}},{"match":{"content":{"query":"CEO compensation","operator":"and"}}},{"match":{"content":{"query":"senior executives","operator":"and"}}},{"match":{"content":{"query":"senior executive compensation","operator":"and"}}},{"match":{"content":{"query":"management compensation","operator":"and"}}},{"match":{"content":{"query":"executive member gender diversity","operator":"and"}}},{"match":{"content":{"query":"audit board committee","operator":"and"}}},{"match":{"content":{"query":"internal audit","operator":"and"}}},{"match":{"content":{"query":"diversity officer","operator":"and"}}},{"match":{"content":{"query":"executives cultural diversity","operator":"and"}}},{"match":{"content":{"query":"senior executives compensation","operator":"and"}}}]}}]}}}'
    
    return es_aws.search(index="ln_index", body=query_governance_management, size=200)

# gives managment_based articles 

def get_shareholder_query(company_name):
    query_governance_shareholders='{"query":{"bool": {"must":[{"match":{"title":{"query":"'+company_name+'","operator":"and"}}},{"range":{"harvestDate":{"gte":"2020-01-01T00:00:00","lt":"now"}}},{"bool":{"should":[{"match":{"content":{"query":"voting rights","operator":"and"}}},{"match":{"content":{"query":"shareholder engagement","operator":"and"}}},{"match":{"content":{"query":"voting right share","operator":"and"}}},{"match":{"content":{"query":"voting cap","operator":"and"}}},{"match":{"content":{"query":"shares to vote","operator":"and"}}},{"match":{"content":{"query":"director election","operator":"and"}}},{"match":{"content":{"query":"shareholder vote","operator":"and"}}},{"match":{"content":{"query":"veto power","operator":"and"}}},{"match":{"content":{"query":"golden share","operator":"and"}}},{"match":{"content":{"query":"public availability corporate statutes","operator":"and"}}},{"match":{"content":{"query":"state owned enterprise","operator":"and"}}},{"match":{"content":{"query":"poison pill","operator":"and"}}},{"match":{"content":{"query":"blank check","operator":"and"}}},{"match":{"content":{"query":"unlimited authorized capital","operator":"and"}}},{"match":{"content":{"query":"classified board structure","operator":"and"}}},{"match":{"content":{"query":"staggered board structure","operator":"and"}}},{"match":{"content":{"query":"supermajority vote","operator":"and"}}},{"match":{"content":{"query":"golden parachute","operator":"and"}}},{"match":{"content":{"query":"limited shareholder rights","operator":"and"}}},{"match":{"content":{"query":"cumulative voting rights","operator":"and"}}},{"match":{"content":{"query":"pre-emptive rights","operator":"and"}}},{"match":{"content":{"query":"company cross shareholding","operator":"and"}}},{"match":{"content":{"query":"confidential voting","operator":"and"}}},{"match":{"content":{"query":"director liability","operator":"and"}}},{"match":{"content":{"query":"shareholder rights","operator":"and"}}},{"match":{"content":{"query":"fair price provision","operator":"and"}}},{"match":{"content":{"query":"removal of directors","operator":"and"}}},{"match":{"content":{"query":"shareholder approval significant transactions","operator":"and"}}},{"match":{"content":{"query":"shareholder proposals","operator":"and"}}},{"match":{"content":{"query":"notice period","operator":"and"}}},{"match":{"content":{"query":"written consent requirements","operator":"and"}}},{"match":{"content":{"query":"expanded constituency provision","operator":"and"}}},{"match":{"content":{"query":"poison pill adoption","operator":"and"}}},{"match":{"content":{"query":"poison pill expiration","operator":"and"}}},{"match":{"content":{"query":"earnings restatement","operator":"and"}}},{"match":{"content":{"query":"profit warnings","operator":"and"}}},{"match":{"content":{"query":"insider dealings","operator":"and"}}},{"match":{"content":{"query":"auditor tenure","operator":"and"}}},{"match":{"content":{"query":"accounting","operator":"and"}}},{"match":{"content":{"query":"litigation","operator":"and"}}},{"match":{"content":{"query":"anti takeover","operator":"and"}}}]}}]}}}'
    
    return es_aws.search(index="ln_index", body=query_governance_shareholders, size=200)

# gives shareholders_based articles 
    
def process_sentence(sentences,company_name):
    temp_processed_sentences=list()
    for j in sentences:
         if(company_name in j):
            clean=re.sub(r"(\xe9|\362)", "", j)
            temp_processed_sentences.append(clean)
                        
    return temp_processed_sentences

# takes artice as input and cleans it and returns sentences with occurence of company name

def date_cal(lexis_result,i):
    temp_Date="NULL"

    if('estimatedPublishedDate' in lexis_result['hits']['hits'][i]['_source']):
        temp_Date=lexis_result['hits']['hits'][i]['_source']['estimatedPublishedDate']

    elif('harvestDate' in lexis_result['hits']['hits'][i]['_source']):
        temp_Date=lexis_result['hits']['hits'][i]['_source']['harvestDate']

    elif('publishedDate' in lexis_result['hits']['hits'][i]['_source']):
        temp_Date=lexis_result['hits']['hits'][i]['_source']['publishedDate']

    start=temp_Date.find("T")
    temp_Date =temp_Date[0:start: ]
    
    return temp_Date
            
# gets the publish date of article
    
    
def sentiment_cal(processed_sentences):
    calculate_Sentiment=SentimentIntensityAnalyzer()
    temp_score=list()
    for j in processed_sentences:
                    val=calculate_Sentiment.polarity_scores(j)
                    temp_score.append(val)
    
    return temp_score
    
#calculates sentiment score for an article

def esg_cal(company_name):
    
        
    start_date= date(2020,1,1)
    end_date= date.today()
    days= pd.date_range(start_date, end_date , freq='D')
    data_esg= pd.DataFrame({'days':days})
    data_esg=data_esg.set_index('days')

        
    result_temp_wastage = get_wastage_query(company_name) #wastage articles returnes from LN
    if len(result_temp_wastage['hits']['hits']) == 0:
        print("no wastage data available for ",company_name)
        
    else:
        dic_temp_wastage={}
        for i in range(len(result_temp_wastage['hits']['hits'])): 
            original_content=result_temp_wastage['hits']['hits'][i]['_source']['content'] 
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences= process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences

            if(len(processed_sentences)!=0):

                Date=date_cal(result_temp_wastage,i) #getting latest date of article
                  
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 
                   

                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()
                # creating dataframe of te sentiment score


                if Date in dic_temp_wastage.keys():
                    dic_temp_wastage[Date][0]=dic_temp_wastage[Date][0]+1
                    dic_temp_wastage[Date][1]=dic_temp_wastage[Date][1]+sentiment_avg_pos
                    dic_temp_wastage[Date][2]=dic_temp_wastage[Date][2]+sentiment_avg_neg


                else:
                    dic_temp_wastage[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
                    
                # sorting articles relevenat sentiment score on article publish date


        for i in dic_temp_wastage:
            dic_temp_wastage[i][1]=(dic_temp_wastage[i][1]/dic_temp_wastage[i][0])*100
            dic_temp_wastage[i][2]=(dic_temp_wastage[i][2]/dic_temp_wastage[i][0])*100
       

        dic_dictionary_wastage= pd.DataFrame.from_dict(dic_temp_wastage,orient='index')
        if dic_dictionary_wastage.empty == False:
            dic_dictionary_wastage= dic_dictionary_wastage.drop(columns=[0])
            dic_dictionary_wastage=dic_dictionary_wastage.rename(columns={1: "wastage_positive_sentiment", 2: "wastage_negative_sentiment"})
            data_esg= data_esg.join(dic_dictionary_wastage)
            print("scheduled wastage for ",company_name)
                
        else:
            print("no wastage data for ",company_name)
            
        # wastage sentiment score of company created
            
    result_temp_sustainable = get_sustainable_query(company_name) #sustainable articles returnes from LN
    if len(result_temp_sustainable['hits']['hits']) == 0:
        print("no sustainable data for ",company_name)  
    else:
        dic_temp_sustainable={}
        for i in range(len(result_temp_sustainable['hits']['hits'])): 
            original_content=result_temp_sustainable['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences
                
            
            if(len(processed_sentences)!=0):

                Date=date_cal(result_temp_sustainable,i)  #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 

                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()


                if Date in dic_temp_sustainable.keys():
                    dic_temp_sustainable[Date][0]=dic_temp_sustainable[Date][0]+1
                    dic_temp_sustainable[Date][1]=dic_temp_sustainable[Date][1]+sentiment_avg_pos
                    dic_temp_sustainable[Date][2]=dic_temp_sustainable[Date][2]+sentiment_avg_neg


                else:
                    dic_temp_sustainable[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
            
        for i in dic_temp_sustainable:
            dic_temp_sustainable[i][1]=(dic_temp_sustainable[i][1]/dic_temp_sustainable[i][0])*100
            dic_temp_sustainable[i][2]=(dic_temp_sustainable[i][2]/dic_temp_sustainable[i][0])*100
       
        dic_dictionary_sustainable= pd.DataFrame.from_dict(dic_temp_sustainable,orient='index')
        if dic_dictionary_sustainable.empty== False:
            dic_dictionary_sustainable= dic_dictionary_sustainable.drop(columns=[0])
            dic_dictionary_sustainable=dic_dictionary_sustainable.rename(columns={1: "sustainable_positive_sentiment", 2: "sustainable_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_sustainable)
            print("scheduled sustainable for ",company_name)
                
        else:
            print("no sustainable data for ",company_name)
             # sustainable sentiment score of company created
     
    result_temp_innovate = get_innovate_query(company_name) #innovate articles returnes from LN
    if len(result_temp_innovate['hits']['hits']) == 0:
        print("no innovate data for ",company_name)
            
    else:
        dic_temp_innovate={}
        for i in range(len(result_temp_innovate['hits']['hits'])):
            original_content=result_temp_innovate['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences
            
            if(len(processed_sentences)!=0):
                Date=date_cal(result_temp_innovate,i) #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 

        
                    
                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()
                

                if Date in dic_temp_innovate.keys():
                    dic_temp_innovate[Date][0]=dic_temp_innovate[Date][0]+1
                    dic_temp_innovate[Date][1]=dic_temp_innovate[Date][1]+sentiment_avg_pos
                    dic_temp_innovate[Date][2]=dic_temp_innovate[Date][2]+sentiment_avg_neg
             
                else:
                    dic_temp_innovate[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
             # sorting articles relevenat sentiment score on article publish date

        for i in dic_temp_innovate:
            dic_temp_innovate[i][1]=(dic_temp_innovate[i][1]/dic_temp_innovate[i][0])*100
            dic_temp_innovate[i][2]=(dic_temp_innovate[i][2]/dic_temp_innovate[i][0])*100
        

        dic_dictionary_innovate= pd.DataFrame.from_dict(dic_temp_innovate,orient='index')
        if dic_dictionary_innovate.empty == False :
            dic_dictionary_innovate= dic_dictionary_innovate.drop(columns=[0])
            dic_dictionary_innovate=dic_dictionary_innovate.rename(columns={1: "innovate_positive_sentiment", 2: "innovate_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_innovate)
            print("scheduled innovate for ",company_name)
                
        else:
            print("no innovate data for ",company_name)
             # innovate sentiment score of company created
            
    result_social_community = get_community_query(company_name) #community articles returnes from LN
    if len(result_social_community['hits']['hits']) == 0:
        print("no community data for ",company_name)
            
    else:
        dic_social_community={}
        for i in range(len(result_social_community['hits']['hits'])):
            original_content=result_social_community['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences
                
                    
            if(len(processed_sentences)!=0):

                Date=date_cal(result_social_community,i)  #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 
                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()


                if Date in dic_social_community.keys():
                    dic_social_community[Date][0]=dic_social_community[Date][0]+1
                    dic_social_community[Date][1]=dic_social_community[Date][1]+sentiment_avg_pos
                    dic_social_community[Date][2]=dic_social_community[Date][2]+sentiment_avg_neg


                else:
                    dic_social_community[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
 # sorting articles relevenat sentiment score on article publish date

        for i in dic_social_community:
            dic_social_community[i][1]=(dic_social_community[i][1]/dic_social_community[i][0])*100
            dic_social_community[i][2]=(dic_social_community[i][2]/dic_social_community[i][0])*100
        
        
        dic_dictionary_social_community= pd.DataFrame.from_dict(dic_social_community,orient='index')
        if dic_dictionary_social_community.empty == False:
            dic_dictionary_social_community= dic_dictionary_social_community.drop(columns=[0])
            dic_dictionary_social_community=dic_dictionary_social_community.rename(columns={1: "community_positive_sentiment", 2: "community_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_social_community)
            print("scheduled community for ",company_name)
                
        else:
            print("no community data for ",company_name)
             # community sentiment score of company created
        
    result_social_humanright = get_humanright_query(company_name) #humanright articles returnes from LN
    if len(result_social_humanright['hits']['hits']) == 0:
        print("no humanright data for ",company_name)
        
    else:
        dic_social_humanright={}
        for i in range(len(result_social_humanright['hits']['hits'])):
            original_content=result_social_humanright['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences

            if(len(processed_sentences)!=0):

                Date=date_cal(result_social_humanright,i) #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 

                  

                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()


                if Date in dic_social_humanright.keys():
                    dic_social_humanright[Date][0]=dic_social_humanright[Date][0]+1
                    dic_social_humanright[Date][1]=dic_social_humanright[Date][1]+sentiment_avg_pos
                    dic_social_humanright[Date][2]=dic_social_humanright[Date][2]+sentiment_avg_neg


                else:
                    dic_social_humanright[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
 # sorting articles relevenat sentiment score on article publish date

        for i in dic_social_humanright:
            dic_social_humanright[i][1]=(dic_social_humanright[i][1]/dic_social_humanright[i][0])*100
            dic_social_humanright[i][2]=(dic_social_humanright[i][2]/dic_social_humanright[i][0])*100
        
            
        dic_dictionary_social_humanright= pd.DataFrame.from_dict(dic_social_humanright,orient='index')
        if dic_dictionary_social_humanright.empty== False:
            dic_dictionary_social_humanright= dic_dictionary_social_humanright.drop(columns=[0])
            dic_dictionary_social_humanright=dic_dictionary_social_humanright.rename(columns={1: "humanright_positive_sentiment", 2: "humanright_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_social_humanright)
            print("scheduled humanright for ",company_name) 
            
        else:
            print("no humanright data for ",company_name)
             # humanright sentiment score of company created
            
    result_social_workforce =get_workforce_query(company_name) #workforce articles returned from LN
    if len(result_social_workforce['hits']['hits']) == 0:
        print("no workforce data for ",company_name) 
        
    else:
        dic_social_workforce={}
        for i in range(len(result_social_workforce['hits']['hits'])):
            original_content=result_social_workforce['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences
            

            if(len(processed_sentences)!=0):

                Date=date_cal(result_social_workforce,i)  #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 

                    
                        
                   

                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()


                if Date in dic_social_workforce.keys():
                    dic_social_workforce[Date][0]=dic_social_workforce[Date][0]+1
                    dic_social_workforce[Date][1]=dic_social_workforce[Date][1]+sentiment_avg_pos
                    dic_social_workforce[Date][2]=dic_social_workforce[Date][2]+sentiment_avg_neg


                else:
                    dic_social_workforce[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
 # sorting articles relevenat sentiment score on article publish date

        for i in dic_social_workforce:
            dic_social_workforce[i][1]=(dic_social_workforce[i][1]/dic_social_workforce[i][0])*100
            dic_social_workforce[i][2]=(dic_social_workforce[i][2]/dic_social_workforce[i][0])*100
       
        
        dic_dictionary_social_workforce= pd.DataFrame.from_dict(dic_social_workforce,orient='index')
        if dic_dictionary_social_workforce.empty == False:
            dic_dictionary_social_workforce= dic_dictionary_social_workforce.drop(columns=[0])
            dic_dictionary_social_workforce=dic_dictionary_social_workforce.rename(columns={1: "workforce_positive_sentiment", 2: "workforce_negative_sentiment"}) 
            data_esg=data_esg.join(dic_dictionary_social_workforce)
            print("scheduled workforce for ",company_name)
            
        else:
            print("no workforce data for ",company_name)
         # workforce sentiment score of company created
     
    result_social_product = get_product_query(company_name) #product articles returned for LN
    if len(result_social_product['hits']['hits']) == 0:
        print("no product data for ",company_name)
    else:
        dic_social_product={}
        for i in range(len(result_social_product['hits']['hits'])):
            original_content=result_social_product['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences 
                
            if(len(processed_sentences)!=0):

                Date=date_cal(result_social_product,i)  #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 

                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()


                if Date in dic_social_product.keys():
                    dic_social_product[Date][0]=dic_social_product[Date][0]+1
                    dic_social_product[Date][1]=dic_social_product[Date][1]+sentiment_avg_pos
                    dic_social_product[Date][2]=dic_social_product[Date][2]+sentiment_avg_neg


                else:
                    dic_social_product[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
             # sorting articles relevenat sentiment score on article publish date

        for i in dic_social_product:
            dic_social_product[i][1]=(dic_social_product[i][1]/dic_social_product[i][0])*100
            dic_social_product[i][2]=(dic_social_product[i][2]/dic_social_product[i][0])*100
       
        
        dic_dictionary_social_product= pd.DataFrame.from_dict(dic_social_product,orient='index')
        if dic_dictionary_social_product.empty == False:
            dic_dictionary_social_product= dic_dictionary_social_product.drop(columns=[0])
            dic_dictionary_social_product=dic_dictionary_social_product.rename(columns={1: "product_positive_sentiment", 2: "product_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_social_product)
            print("scheduled socialproduct for ",company_name)
            
        else:
            print("no socialproduct data for ",company_name)
            
            # product sentiment score of company created 
    
    result_governance_management = get_management_query(company_name)#management articles returnes from LN
    if len(result_governance_management['hits']['hits']) == 0:
        print("no management data for ",company_name) 
    else:
        dic_governance_management={}
        for i in range(len(result_governance_management['hits']['hits'])):
            original_content=result_governance_management['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences

                
            if(len(processed_sentences)!=0):

                Date=date_cal(result_governance_management,i)  #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 
 # sorting articles relevenat sentiment score on article publish date

                  
                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()


                if Date in dic_governance_management.keys():
                    dic_governance_management[Date][0]=dic_governance_management[Date][0]+1
                    dic_governance_management[Date][1]=dic_governance_management[Date][1]+sentiment_avg_pos
                    dic_governance_management[Date][2]=dic_governance_management[Date][2]+sentiment_avg_neg


                else:
                    dic_governance_management[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]

        
        for i in dic_governance_management:
            dic_governance_management[i][1]=(dic_governance_management[i][1]/dic_governance_management[i][0])*100
            dic_governance_management[i][2]=(dic_governance_management[i][2]/dic_governance_management[i][0])*100
        
            
        dic_dictionary_governance_management= pd.DataFrame.from_dict(dic_governance_management,orient='index')
        if dic_dictionary_governance_management.empty == False :
            dic_dictionary_governance_management= dic_dictionary_governance_management.drop(columns=[0])
            dic_dictionary_governance_management=dic_dictionary_governance_management.rename(columns={1: "management_positive_sentiment", 2: "management_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_governance_management)
            print("scheduled management for ",company_name)
            
        else:
            print("no management data for ",company_name)
         # management sentiment score of company created
    result_governance_shareholders = get_shareholder_query(company_name) ##shareholders articles returnes from LN
    if len(result_governance_shareholders['hits']['hits']) == 0:
        print("no shareholders data for ",company_name)
        
    else:
        dic_governance_shareholders={}
        for i in range(len(result_governance_shareholders['hits']['hits'])):
            original_content=result_governance_shareholders['hits']['hits'][i]['_source']['content']
            sentences=sent_tokenize(original_content) # article tokenized to sentences
            processed_sentences=process_sentence(sentences,company_name) # cleaning sentences and storing relevant sentences
                        
            if(len(processed_sentences)!=0):

                Date=date_cal(result_governance_shareholders,i)  #getting latest date of article
                score=sentiment_cal(processed_sentences) # calculating sentiment score of an article 
 # sorting articles relevenat sentiment score on article publish date

                   
                        
                df_sentiment=pd.DataFrame(score)
                df_sentiment['net_pos']= df_sentiment['pos']/(df_sentiment['neg'] + df_sentiment['neu']+ df_sentiment['pos'])
                df_sentiment['net_neg']=df_sentiment['neg']/(df_sentiment['neg']+df_sentiment['neu']+ df_sentiment['pos'])
                sentiment_avg_pos=df_sentiment['net_pos'].mean()
                sentiment_avg_neg=df_sentiment['net_neg'].mean()
                if Date in dic_governance_shareholders.keys():
                    dic_governance_shareholders[Date][0]=dic_governance_shareholders[Date][0]+1
                    dic_governance_shareholders[Date][1]=dic_governance_shareholders[Date][1]+sentiment_avg_pos
                    dic_governance_shareholders[Date][2]=dic_governance_shareholders[Date][2]+sentiment_avg_neg
            
            
                else:
                    dic_governance_shareholders[Date]=[1,  sentiment_avg_pos,sentiment_avg_neg]
            
        for i in dic_governance_shareholders:
            dic_governance_shareholders[i][1]=(dic_governance_shareholders[i][1]/dic_governance_shareholders[i][0])*100
            dic_governance_shareholders[i][2]=(dic_governance_shareholders[i][2]/dic_governance_shareholders[i][0])*100
                
        dic_dictionary_governance_shareholders= pd.DataFrame.from_dict(dic_governance_shareholders,orient='index')
        if dic_dictionary_governance_shareholders.empty==False:
            dic_dictionary_governance_shareholders= dic_dictionary_governance_shareholders.drop(columns=[0])
            dic_dictionary_governance_shareholders=dic_dictionary_governance_shareholders.rename(columns={1: "shareholders_positive_sentiment", 2: "shareholders_negative_sentiment"})
            data_esg=data_esg.join(dic_dictionary_governance_shareholders)
            print("scheduled shareholder for ",company_name)
        else:
            print("no shareholder data for ",company_name)
             # shareholder sentiment score of company created
        
        
    if len(data_esg.columns) <= 1:
        print("no esg data for ", company_name)
        return "no data"
        
    else:
        data_esg=data_esg.fillna(method='ffill')
        return data_esg




    