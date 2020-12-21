#load libraries
import pandas as pd
import pickle
import os

#functions
def load_pickle(file_name:str) -> dict:
    with(open('./dicts/'+file_name, 'rb')) as f:
        return pickle.load(f)

#list of dicts
ld=[load_pickle(i) for i in os.listdir('./dicts') if os.stat('./dicts/'+i).st_size>100]

#remove failed_links
ld=list(filter(lambda x: isinstance(x,dict),ld))

#load start page data
df=pd.read_csv('start_page_2010.csv')

#mapping of row_number to link
ids=df['link'].to_dict()

#retrieve a full path to UIKs and list of UIK links
def split_pickles(x:dict) -> (list,list):
    paths=[]
    values=[] 
    for i,j in x.items():
        if isinstance(j, list):
            path,value=i,j
        else:
            path, value=split_pickles(j)
            path=[i]+path
        paths.append(path)
        values.append(value)
    return paths, values

# convert a dict from pickle into a nicely structured DataFrame
def format_pickles(dct:dict) -> pd.DataFrame:
    x,y=split_pickles(dct)
    x,y=pd.DataFrame(x),pd.DataFrame(y)

    y.columns=range(1,y.columns.shape[0]+1)

    d2=y.lookup(y.index.tolist()*y.shape[1], [j for i in y.columns for j in [i]*y.shape[0]])
    d1=x.lookup(x.index.tolist()*y.shape[1], [j for i in y.columns for j in [i]*x.shape[0]])
    d0=x.lookup(x.index.tolist()*y.shape[1], [j for i in y.columns for j in [0]*x.shape[0]])

    data=pd.DataFrame([d0,d1,d2]).T.set_index(0).dropna().explode(2).explode(2).reset_index()
    
    return data

#merge and format them all
output=pd.concat(list(map(format_pickles, ld)))
output.columns=['link','location','link_to_UIK']   
merged_df=df.merge(output, on='link')

#save the dict with all the data
merged_df.to_csv('preliminary_results.csv', index=False) 