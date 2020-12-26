#!/usr/bin/env python
# coding: utf-8

# In[1]:
# libraries and path setup
import os
os.chdir(r'C:\Users\eldii\Documents\GitHub\proj_election\parser_preliminary_results')

import pandas as pd
from helper import runner

from itertools import islice

def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

# In[2]:
# load the dataframe with links
df = pd.read_csv('preliminary_results.csv')



output=runner(df.link_to_UIK.sample(10).to_dict())