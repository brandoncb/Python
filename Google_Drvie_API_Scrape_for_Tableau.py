import pandas as pd
import os
import time
import csv
from glob import glob
import numpy as np


#DEFINE METHODS

def get_newest_CSV(mydataframe):
    """
    get the file paths of the newest csv files for a dataframe of subdirectories on _bodi_
    pass in your dataframe of subdirectories
    subdirs must be in a column called 'subdirs'
    assumes no further subdirectories beneath those in dataframe ie no recursive scrape
    """
    file_list = []
    for i in range(0, len(mydataframe)):
        my_subdir_glob_path = mydataframe['subdirs'][i] + '*.csv'
        all_csv = glob(my_subdir_glob_path)
        newest_csv = max(all_csv, key=os.path.getctime)
        file_list.append(newest_csv)
    return file_list

def dropcols(panda_df, col_list):
    """
    drop columns from a dataframe and reindex
    provide dataframe and columns to drop (as a list)
    """
    panda_df = panda_df.drop(col_list, axis = 1)
    panda_df = panda_df.reset_index()
    panda_df = panda_df.drop(['index'], axis = 1)
    return panda_df



# IDENTIFY SUB-DIRECTORIES ON _bodi_

nak = glob('/___*/')
icon = glob('/___*/')


# COMBINE ALL SUBDIRS INTO ONE DATAFRAME

for x in icon:
  nak.append(x)
dirs = nak
# dirs

df_dirs = pd.DataFrame(dirs)
df_dirs.columns = ['subdirs']


# GET NEWEST CSV FILE FROM EACH SUBDIR

mydf = get_newest_CSV(df_dirs)


# EXTRACT THE FILENAME FROM THE PATH AND CREATE A NEW DATAFRAME WITH COLUMNS FILEPATH AND FILE NAME

mylist = []
for i in range(0,len(mydf)):
    new_record = mydf[i].split('/')[-1]
    mylist.append(new_record)

mydf = pd.DataFrame(mydf)
mydf.columns = ['Filepath']
mydf['Filename'] = mylist


# CREATE DATAFRAME OF THE ENTRIES FROM THE GSHEET CONFIG FILE
# 'Original_Filename' IS THE FILENAME IN THE OBDFS STUDY FOLDER AND 'BODI_Filename' IS WHAT IT GETS RENAMED TO IN _bodi_

mydict = {'____<DATE>': '____<DATE>',
          , '____<DATE>': '____<DATE>'
          , '____<DATE>': '____<DATE>'
          , '____<DATE>': '____<DATE>'
         }

config_file_df = pd.DataFrame(list(mydict.items()),columns = ['Original_Filename','BODI_Filename'])


# CREATE COLUMN FOR MERGING
# THIS CONFIG FILE DATAFRAME WILL BE MERGED WITH THE SCRAPE OF _bodi_ FOR THE NEWEST CSV FILES

config_file_df[['for_merge', 'for_merge_drop']] = config_file_df.BODI_Filename.str.split("<", expand=True)

config_file_df = dropcols(config_file_df,['for_merge_drop'])

mydf[['for_merge', 'for_merge_drop']] = mydf.Filename.str.split("202", expand=True)

mydf = dropcols(mydf, ['for_merge_drop'])


# MERGE CONFIG FILE DATAFRAME WITH THE THE NEWEST CSV FILES FROM _bodi_

df = pd.merge(mydf, config_file_df, how='left', left_on='for_merge', right_on='for_merge')
df = df.dropna()
df = dropcols(df, ['for_merge', 'BODI_Filename']) # can drop 'BODI_Filename' because it is duplicative of the 'Filename' column


# CREATE SEPERATE LISTS FOR CLEAN-IL6-DATA AND CLEAN-FLOW-DATA

il6 = 'il6_<DATE> or il-6_<DATE>'
ptid = 'pt-id_<DATE>'
irr = '___<DATE>'
toci = '___<DATE>'

for i in range(0, len(df)):
    if il6 in df['Original_Filename'][i]:
        df1 = pd.DataFrame(np.array(list(df.iloc[i, 0:3])).reshape(1,3))
    if ptid in df['Original_Filename'][i]:
        df2 = pd.DataFrame(np.array(list(df.iloc[i, 0:3])).reshape(1,3))
    if irr in df['Original_Filename'][i]:
        df3 = pd.DataFrame(np.array(list(df.iloc[i, 0:3])).reshape(1,3))
    if toci in df['Original_Filename'][i]:
        df4 = pd.DataFrame(np.array(list(df.iloc[i, 0:3])).reshape(1,3))
frames = [df1, df2, df3, df4]
df_il6 = pd.concat(frames)
df_il6.columns = ['Filepath', 'Filename', 'Original_Filename']


# FOR CLEAN-FLOW-DATA, JUST DROP THE IL6 FILE

for i in range(0, len(df)):
    if il6 in df['Original_Filename'][i]:
        il6_index = i

df_cfd = df.drop([il6_index])
df_cfd = df_cfd.reset_index()
df_cfd = df_cfd.drop(['index'], axis = 1)

# df_il6.to_csv('/___.csv', index=False)
# df_cfd.to_csv('/___.csv', index=False)


# IF THE R SCRIPT RECENTLY RAN SUCCESSFULLY THEN USE THE UPDATED LIST OF FILES BY OVERWRITING THE UPLOAD CSV
# ELSE DO NOT OVERWRITE THE CSV LIST OF FILES SO IT STILL REFLECTS THE LAST TIME THE SCRIPT RAN SUCCESSFULLY
# THIS IS IN PLACE UNTIL CONFORMANCE CHECKS ARE LIVE
# LAST MODIFICATION DATES CORRESPOND TO INTERMEDIATE FILES THAT GET UPDATED IF THE R SCRIPTS RUN SUCCESSFULLY (ie IF FILE'S LAST MODIFICATION DATE = TODAY'S DATE)

testpath_cfd = '/___.csv'
last_mod_date_cfd = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime(testpath_cfd)))

testpath_il6 = '/___.csv'
last_mod_date_il6 = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime(testpath_il6)))

from datetime import datetime
today_date = datetime.today().strftime('%Y%m%d')

if last_mod_date_cfd == today_date:
    df_cfd.to_csv('/___.csv', index=False)
    print("clean-flow-data.R *HAS* run successfully today since the last modification date of an intermediatary file from the R script *IS* equal to today's date")
else:
    print("clean-flow-data.R has *NOT* run successfully today since the last modification date of an intermediatary file from the R script is *NOT* equal to today's date")
   
if last_mod_date_il6 == today_date:
    df_il6.to_csv('/___.csv', index=False)
    print("clean-il6-data.R *HAS* run successfully today since the last modification date of an intermediatary file from the R script *IS* equal to today's date")
else:
    print("clean-il6-data.R has *NOT* run successfully today since the last modification date of an intermediatary file from the R script is not equal to today's date")


# UPLOAD TO SERVICE ACCOUNT VIA GoogleDrive API    

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile('/___.json')
if gauth.credentials is None:
    # Authenticate if they're not there
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    # Refresh them if expired
    gauth.Refresh()
else:
    # Initialize the saved creds
    gauth.Authorize()
# Save the current credentials to a file
# gauth.SaveCredentialsFile("mycreds.txt")

drive = GoogleDrive(gauth)



# # ---------------------------------------------------------
# # This is only needed to start the process 
# # ---------------------------------------------------------

# # this specifies the gDrive folder for dsisys44
# fileID = '___'

# # create csv file in the above folder
# file1 = drive.CreateFile({"mimeType": "txt/csv", "parents": [{"kind": "drive#fileLink", "id": fileID}]})

# # upload this csv file from Rosalind and print confirmation
# # file1.SetContentFile('/___.csv')
# file1.SetContentFile('/___.csv')

# file1.Upload()
# print('Created file %s with mimeType %s' % (file1['title'], file1['mimeType']))

# # Initialize GoogleDriveFile instance with file id
# myfile = drive.CreateFile({'id': file1['id']})

# # for the first time for the study and after the above csv file has been written to the gDrive, call the myfile object 
# # this will produce it's metadata in dictionary format
# file1

# # save this metadata to myfile_refresh so that you can write to that file
# # writing to the same csv file in gDrive is essential because Tableau is connected to that file by its unique hash

# # ---------------------------------------------------------
# # until here
# # ---------------------------------------------------------

# this metadata in dictionary format corresponds to the csv file in gDrive and was obtained by calling 'file1' following the first upload of the csv file to gDrive
myfile_refresh1 = drive.CreateFile({'mimeType': 'txt/csv', 'parents': ___})

# upload the refreshed csv (stored on Rosalind) to the csv in gDrive for dsisys44
myfile_refresh1.SetContentFile('/___.csv')
myfile_refresh1.Upload()


# this metadata in dictionary format corresponds to the csv file in gDrive and was obtained by calling 'file1' following the first upload of the csv file to gDrive
myfile_refresh2 = drive.CreateFile({'mimeType': 'txt/csv', 'parents': ___})

# upload the refreshed csv (stored on Rosalind) to the csv in gDrive for dsisys44
myfile_refresh2.SetContentFile('/___.csv')
myfile_refresh2.Upload()

