#!/usr/bin/env python
# coding: utf-8

# In[1]:
import os
os.chdir(r'D:\Documents\GitHub\proj_election\parser_preliminary_results')
import pytesseract
# path setup
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

import pickle
import pandas as pd
from selenium import webdriver
import time
from helper import *

# In[2]:
# load the dataframe with links
df = pd.read_csv('start_page_2010.csv')

driver_path = "D:\Downloads_new\chromedriver_win32/chromedriver.exe"
driver = webdriver.Chrome(driver_path)
# get links for  UIKs
data = dict()
links_failed = []

# test example
number_of_rows_to_test=10
flag_pickle=0
for j in df.loc[:number_of_rows_to_test-1, 'link'].to_list():
    try:
        data.update({j: get_links_UIK(j, dct={}, driver=driver)})
    except:
        links_failed.append(j)
    if (len(data) % 50 == 0) and len(data):
        time.sleep(5)
        with open(f'./dicts/data_{flag_pickle}.pickle', 'wb') as foo:
            pickle.dump(data, foo, protocol=pickle.HIGHEST_PROTOCOL)
        data = dict()
        flag_pickle=+1

trying_failed_links=0
while len(links_failed)>0:
    j=links_failed.pop()
    try:
        data.update({j: get_links_UIK(j, dct={}, driver=driver)})
    except:
        links_failed.append(j)
    if (len(data) % 50 == 0) and len(data):
        time.sleep(5)
        with open(f'./dicts/data_{flag_pickle}.pickle', 'wb') as foo:
            pickle.dump(data, foo, protocol=pickle.HIGHEST_PROTOCOL)
        data = dict()
        flag_pickle=+1
    trying_failed_links+=1
    
    if trying_failed_links>5: break
with open(f'./dicts/data_{flag_pickle}.pickle', 'wb') as foo:
    pickle.dump(data, foo, protocol=pickle.HIGHEST_PROTOCOL)
# In[5]:
# save failed data
print(len(links_failed))
if not 'failed_links.pickle' in os.listdir('./dicts'):
    with open('./dicts/failed_links.pickle', 'wb') as foo:
        pickle.dump(links_failed, foo, protocol=pickle.HIGHEST_PROTOCOL)
else:
    saved=list(filter(lambda x: 'failed_links' in x, os.listdir('./dicts')))
    cleaned=list(map(lambda x: x.lstrip('failed_nks').rstrip('pickle'), saved))
    max_saved=max(map(lambda x: int(x) if x else 0, cleaned))
    with open(f'./dicts/failed_links{max_saved+1}.pickle', 'wb') as foo:
        pickle.dump(links_failed, foo, protocol=pickle.HIGHEST_PROTOCOL)