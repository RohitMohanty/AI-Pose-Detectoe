from selenium import webdriver
from time import sleep      
browser = webdriver.Chrome()
url="https://esg.censible.co/companies/Amazon-com"
browser.get(url)
sleep(8)
corporate_list=browser.find_element_by_class_name('data')
# //*[@id="sapper"]/main/div/div/div/div/main/div/section/hgroup/a/div/div/ul[1]
for i in corporate_list.find_elements_by_tag_name('li'):
    print(i.get_attribute("title"))
# /html/body/div[1]/main/div/div/div/div/main/div/section/hgroup/a/div/div/ul[1]
