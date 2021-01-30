"""
Analysis of how histogenex inventory columns are renamed

This report shows the column renamings for Histogenex Inventory files after the datasets are transformed in OpenRefine.
If the old column name is blank, then there was a new column created with custom grel or jython in OpenRefine.
This report was created by crawling all json transformation rules files for the 80 Histogenex Inventory datasets in _bodi_.
The OpenRefine json transformation rules files are a list of dictionaries and the script searched for keys equal 
    to 'oldColumnName' and 'newColumnName', which is the formatting convention for OpenRefine.

Brandon Boylan
"""

import pandas as pd
from glob import glob
import json


def get_jsons(mydataframe):
    """
    get the paths of all json files in the subdirectories in mydataframe
    pass in your dataframe of subdirectories
    assumes subdirs are in a column called 'subdirs'
    assumes no further subdirectories beneath those in dataframe ie no recursive scrape
    returns a list
    """
    json_list = []
    for i in range(0, len(mydataframe)):
        all_json = glob(mydataframe['subdirs'][i] + '*.json')
        for j in range(0, len(all_json)):
            json_list.append(all_json[j])
    return json_list


def get_new_or_renamed_columns(mydataframe):
    """
    returns a dataframe of all the renamings in the json files
    input dataframe of paths to json files
    assumes column name is 'path_json'
    output  column names are 'oldColumnName' and 'newColumnName' which are the same as the keys
    
    if the first column is 'None' and the second column isn't, then there was a new column created with custom grel or gython in OpenRefine
    """
    renaming_list = []
    for ii in range(0, len(mydataframe)):
        f = open(mydataframe['path_json'][ii])
        myjson = json.load(f) #returns as a list
        for jj in range(0, len(myjson)): #loop through each dictionary in the json file; the json file is a list of dictionaries
            old = myjson[jj].get('oldColumnName') #note that not all dictionaries in the json have renamings
            new = myjson[jj].get('newColumnName')
            renaming_list.append([old,new])
    return pd.DataFrame(renaming_list, columns = ['oldColumnName','newColumnName'])


bodi_subdirs = glob('Z:/_bodi_/*/')

df = pd.DataFrame(bodi_subdirs)
df.columns = ['subdirs']

# Restrict to Histogenex Inventory datasets
df_hgx = df[df['subdirs'].str.contains('_H', case = False)]
df_hgx_inv = df_hgx[df_hgx['subdirs'].str.contains('_Inv', case = False)]

df_hgx_inv = df_hgx_inv.reset_index()
df_hgx_inv = df_hgx_inv.drop(columns = ['index'])

#get_jsons
jsons_hgx_inv = get_jsons(df_hgx_inv)
df_jsons_hgx_inv = pd.DataFrame(jsons_hgx_inv, columns = ['path_json'])

#get_new_or_renamed_columns
df_hinorc = get_new_or_renamed_columns(df_jsons_hgx_inv)

#remove records from dictionaries with no new columns
df_hinorc = df_hinorc[df_hinorc['newColumnName'].astype(str) != 'None']
df_hinorc = df_hinorc.reset_index()
df_hinorc = df_hinorc.drop(columns = ['index'])

#remove table schemas
df_hinorc['newColumnName'] = df_hinorc['newColumnName'].astype(str).replace('Samples.', '', regex=True)
df_hinorc['newColumnName'] = df_hinorc['newColumnName'].astype(str).replace('Specimens.', '', regex=True)
df_hinorc['newColumnName'] = df_hinorc['newColumnName'].astype(str).replace('Sample.', '', regex=True)
df_hinorc['newColumnName'] = df_hinorc['newColumnName'].astype(str).replace('Specimen.', '', regex=True)
df_hinorc['newColumnName'] = df_hinorc['newColumnName'].astype(str).replace('Assays.', '', regex=True)
df_hinorc['newColumnName'] = df_hinorc['newColumnName'].astype(str).replace('Study.', '', regex=True)

#remove dups
df_hinorc.drop_duplicates(keep = 'first', inplace=True)	