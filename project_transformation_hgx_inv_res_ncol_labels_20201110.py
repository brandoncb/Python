"""
Analysis of the variance in column counts and column labels for  Histogenex Inventory and Results files in _bodi_

Brandon Boylan
"""


import pandas as pd
import os
import csv
from glob import glob

bodi_subdirs = glob('Z:/_bodi_/*/')
df = pd.DataFrame(bodi_subdirs)

df.columns = ['subdirs']

df_hgx = df[df['subdirs'].str.contains('_H', case = False)]

#HGX Inventory folder paths
df_hgx_inv = df_hgx[df_hgx['subdirs'].str.contains('_Inv', case = False)]
df_hgx_inv = df_hgx_inv.reset_index()
df_hgx_inv = df_hgx_inv.drop(columns = ['index'])

#HGX Results folder paths
df_hgx_res = df_hgx[df_hgx['subdirs'].str.contains('_Res', case = False)]
df_hgx_res = df_hgx_res.reset_index()
df_hgx_res = df_hgx_res.drop(columns = ['index'])

#uncomment for results file analysis: #df_hgx_inv = df_hgx_res


def get_newest_CSV(mydataframe):
    """
    get the file paths of the newest csv files for a dataframe of subdirectories on _bodi_
    pass in your dataframe of subdirectories
    subdirs are in a column called 'subdirs'
    assumes no further subdirectories beneath those in dataframe ie no recursive scrape
    """
    file_list = []
    for i in range(0, len(mydataframe)):
        my_subdir_glob_path = mydataframe['subdirs'][i] + '*.csv'
        all_csv = glob(my_subdir_glob_path)
        newest_csv = max(all_csv, key=os.path.getctime)
        file_list.append(newest_csv)
    return file_list


inv_files = get_newest_CSV(df_hgx_inv)
df_inv_files = pd.DataFrame(inv_files, columns = ['newest_csv'])
df_hgx_inv['newest_csv'] = df_inv_files['newest_csv']


def include_col_count(mydataframe):
    """
    counts the number of columns in a csv file given the paths to the csv files in a dataframe
    column name to those paths is 'newest_csv'
    returns a list
    """
    mylist = []
    for i in range(0, len(mydataframe)):
        datafilename = mydataframe['newest_csv'][i]
        f = open(datafilename,'r')
        reader = csv.reader(f)
        ncol = len(next(reader)) # Read first line and count columns
        mylist.append(ncol)
    return mylist
    
    
inv_ncol = include_col_count(df_hgx_inv)
df_inv_ncol = pd.DataFrame(inv_ncol, columns = ['ncol'])

df_hgx_inv['ncol'] = df_inv_ncol['ncol']


def get_col_labels(mydataframe):
    """
    returns a list of all the column labels from all the csv files to then create a histogram
    column name to those csv file paths is 'newest_csv'
    returns a list
    """
    mylist = []
    for i in range(0, len(mydataframe)):
        datafilename = mydataframe['newest_csv'][i]
        f = open(datafilename,'r')
        reader = csv.reader(f)
        labels = next(reader)
        for j in range(0, len(labels)):
            mylist.append(labels[j])
    return mylist


labels_list = get_col_labels(df_hgx_inv)    

from collections import Counter
labels_counts = Counter(labels_list)
df_labels_counts = pd.DataFrame(list(labels_counts.items()),columns = ['label','count']) 
#df_labels_counts.plot(kind='bar')