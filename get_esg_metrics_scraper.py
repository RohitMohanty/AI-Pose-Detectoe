import requests
import json
from bs4 import BeautifulSoup
board_gender_ratio_data=pd.DataFrame()
for i in range(len(company_list)):
    try:
        esg_censible_link="https://censible-search.herokuapp.com/companies/search?q=" + company_list["company_name"].values[i]
        link_test=requests.get(esg_censible_link).text
        link_json=json.loads(link_test)
        print(link_json)
        if len(link_json)!=0 :
                get_esg_cesnible_info= "https://esg.censible.co/companies/"+link_json[0]["slug_name"]
                esg_censible_info=requests.get(get_esg_cesnible_info).text
                soup=BeautifulSoup(esg_censible_info,'lxml')
                if soup.find_all("td",class_="gender svelte-1miyno8"):
                    table=soup.find_all("td",class_="gender svelte-1miyno8")
                    male=0
                    female=0
                    for stuff in table:
                        print(stuff.text)
                        if(stuff.text == "Male"):
                                male+=1
                        elif(stuff.text == "Female"):
                                female+=1
                    temp_pd=pd.DataFrame()
                    temp_pd["company_name"]=[company_list["company_name"].values[i]]
                    temp_pd["male_count"]=[male]
                    temp_pd['female_count']=[female]
                    board_gender_ratio_data=board_gender_ratio_data.append(temp_pd)
    except Exception as e:
        print(e)
        pass

    
                