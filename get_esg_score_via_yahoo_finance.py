import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool


def get_esg_risk_score(list_elements):
    esg=0
    try:
        yahoo_link="https://finance.yahoo.com/quote/"+list_elements[0]+"/sustainability?p="+list_elements[0]+""
        link_test=requests.get(yahoo_link).text
        soup=BeautifulSoup(link_test,'lxml')
        esg=soup.find("div", class_="Fz(36px) Fw(600) D(ib) Mend(5px)").string
        
    except Exception as e:
        print("esg failed for: ",list_elements[1])
        print(e)
        esg=None
    finally:
        
        return (esg,list_elemnts[1])