# libraries
import requests as r
from bs4 import BeautifulSoup
import pandas as pd
import os
import re


# relocate the project
os.chdir(r'C:\Users\eldii\Documents\GitHub\proj_election\parser_preliminary_results')
#main link and authorisation
url="http://www.vybory.izbirkom.ru/region/izbirkom"

headers="""Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Accept-Encoding: gzip, deflate
Accept-Language: en-US,en;q=0.9,ru;q=0.8,fr;q=0.7
Cache-Control: no-cache
Connection: keep-alive
Content-Length: 152
Content-Type: application/x-www-form-urlencoded
Cookie: __utmc=252441553; __utmz=252441553.1607361652.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); izbSession=fa90d86f-6f89-4877-b62d-aadc79fe48f9; __utma=252441553.205305655.1607361652.1607361652.1607636441.2; __utmt=1; JSESSIONID=e9d818ee7ceb67a6f1733013b352; __utmb=252441553.2.10.1607636441
DNT: 1
Host: www.vybory.izbirkom.ru
Origin: http://www.vybory.izbirkom.ru
Pragma: no-cache
Referer: http://www.vybory.izbirkom.ru/region/izbirkom
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36 OPR/71.0.3770.441"""


headers=dict(i.split(': ') for i in headers.split("\n"))

# date can be modified. change the start_date and end_date
data="""start_date=01.02.2010&urovproved=all&vidvibref=all&vibtype=all&end_date=30.04.2021&sxemavib=all&action=search_by_calendar&region=0&ok=%C8%F1%EA%E0%F2%FC"""

# get the data
resp=r.post(url, headers=headers, data=data)
soup=BeautifulSoup(resp.content)

#validation of number of entries
total_number_of_entries=int(soup.select("form+table nobr")[0].text.split(": ")[1])
print("Total number of entries is:",total_number_of_entries)

#create a data dictionary
table=soup.select("form+table+table")[0]
dates=[i for i,j in enumerate(table.select('tr')) if j.has_attr("bgcolor")]

dct={}
for i,j in enumerate(table.select('tr')):
    #if current line is a date, create a new entry in dct
    if i in dates: 
        current=j.text.strip()
        dct[current]=[]
    else: #add the data into current entry
        dct[current].append(j)
        
# flatten a dictionary
def boo(x):
    ddct=[]
    for i in x:
        if i.select('nobr'): 
            location=i.select('nobr b')[0].text.strip()
        
        loc2,topic=i.select('td')
        loc2=loc2.text.split(location)[-1].strip()
        
        text=topic.text
        link=topic.select('a')[0].get('href')
        
        ddct.append({
            'loc1':location, 
            'loc2':loc2,
            'text':text,
            'link':link,
            'vrn':re.findall('vrn=(.*?)&',link)[0],
            'region':re.findall('region=(.*?)&',link)[0],
            'prver':re.findall('prver=(.*?)&',link)[0],
            'pronetvd':re.findall('pronetvd=(.*)',link)[0]
        })

    return ddct

# convert flattened dict into dataframe
def get_dataframe_from_soup1(x):
    return pd.DataFrame(boo(x[1])).assign(date=x[0])

# create dataframe with all the data from start page
pp=list(map(get_dataframe_from_soup1, dct.items()))
df=pd.concat(pp, ignore_index=True)

#small cleaning
df[['vrn','region','prver']]=df[['vrn','region','prver']].astype(float)

months={
    'февраля':2, 
    'марта':3, 
    'апреля':4,
    'мая':5,
    'июня':6,
    'июля':7,
    'августа':8,
    'сентября':9,
    'октября':10,
    'ноября':11, 
    'декабря':12, 
    'января':1}

df[['day','month','year']]=df.date.str.split(expand=True).loc[:,:2]
df.month=df.month.map(months)
#save the data
df.to_csv('start_page_2010.csv', index=False)