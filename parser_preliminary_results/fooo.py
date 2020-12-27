#!/usr/bin/env python
# coding: utf-8

# In[1]:
# libraries and path setup
import os
os.chdir(r'C:\Users\eldii\Documents\GitHub\proj_election\parser_preliminary_results')
import pytesseract
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe'
driver_path = "./chromedriver.exe"

import pandas as pd
from helper import runner

from itertools import islice

def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}
        
        
def kickstarter():
    from multiprocessing import Pool
    pool = Pool(5)
    res=pool.map(runner, chunks(df.link_to_UIK[:20].to_dict(), 10))
    pool.close()
    pool.join()
    return res
# In[2]:

# load the dataframe with UIK links
df = pd.read_csv('preliminary_results.csv')




res=kickstarter()