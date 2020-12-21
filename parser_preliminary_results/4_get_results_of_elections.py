#!/usr/bin/env python
# coding: utf-8

# In[1]:
# libraries and path setup
import os
os.chdir(r'D:\Documents\GitHub\proj_election\parser_preliminary_results')

import pandas as pd
from helper import runner

# to give it a try with multiprocessing. Splits dict into a list of dicts to run the runner per chunk
from itertools import islice

def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

# In[2]:
# load the dataframe with links
df = pd.read_csv('preliminary_results.csv')


# get the data
output=runner(df.link_to_UIK.to_dict())
# In[3]:

# save the xls
# id from preliminary results file will be passed into filename
def store_data(x:dict) ->None:
    
    for i,j in x.items():
        j.to_excel(f'./reports/report {i}.xls', index=True)
    
    return None

store_data(output)