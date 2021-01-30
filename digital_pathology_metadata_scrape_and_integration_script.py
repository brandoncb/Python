# DIGITAL PATHOLOGY - BRANDON BOYLAN 

# ORACLE DATABASE CONNECTION

import pandas as pd
import cx_Oracle

gslide_conn = cx_Oracle.connect("___","___","___")

cur = gslide_conn.cursor()

query = cur.execute("""
select
  SLIDE_ID as 	Slide_id
  , SLIDE_PATH as 	File_path
  , BARCODE_LABEL_BARCODE_TEXT as 	Barcode
  , BD_ADD_TD_TIS_COM as 	Add_tis_com
  , BD_ADDITIONAL_TD_TISSUE_ABBREV as 	Additional_tissue_abbrev
  , BD_ADDITIONAL_TD_TISSUE_NAME as 	Additional_tissue_name
  , SLD_SLIDE_SLIDE_TYPE as 	Slide_type
  , SLD_SLIDE_EXPERIMENT as 	Slide_experiment
  , SLD_SLIDE_SLIDE_NUMBER as 	Slide_number
  , SLD_SLIDE_ANTIBODY as 	Slide_antibody
  , SLD_SLIDE_AB_ROW as 	Slide_ab_row
  , SLD_SLIDE_CLONE as 	Slide_clone
  , SLD_SLIDE_DILUTION as 	Slide_dilution
  , SLD_SLIDE_PCR_PRODUCT as 	Slide_pcr_product
  , SLD_SLIDE_CDNA_DATA_CDNA_ID as 	Slide_cdna_data_cdna_id
  , SLD_SLIDE_CDNA_DATA_CDNA_NAME as 	Slide_cdna_data_cdna_name
  , SLD_SLIDE_CDNA_DATA_DNAID as 	Slide_cdna_data_dnaid
  , SLD_SLIDE_STAIN as 	Slide_stain
  , BD_BLOCK as 	Block_
  , BD_REQUEST as 	Request
  , BD_TMA_TITLE as 	Tma_title
  , BD_SBD_IS_CONTROL_GROUP as 	Is_control_group
  , BD_SBD_GROUP as 	Group_
  , BD_SBD_ANIMAL as 	Animal
  , BD_SBD_SPECIES as 	Species
  , BD_SBD_STRAIN as 	Strain
  , BD_SBD_TREATMENT as 	Treatment
  , BD_SBD_DOSAGE as 	Dosage
  , BD_SBD_ROUTE as 	Route
  , BD_SBD_CELL_LINE as 	Cell_line
  , BD_TD_TISSUE_TYPE_ABBREV as 	Tissue_type_abbrev
  , BD_TD_TISSUE_TYPE_NAME as 	Tissue_type_name
  , BD_PATIENT_NUMBER as 	Patient_number
  , BD_PATIENT_SOURCE as 	Patient_source
  , BD_DIA_DAT_PAT_DIA as 	Dia_dat_pat_dia
  , BD_TD_TISSUE_DIAGNOSIS as 	Tissue_diagnosis
  , BD_VERIFICATION_STATUS as 	Verification_status
  , BD_TD_PCT_TUMOR as 	Pct_tumor
  , BD_TD_MET_SIT_OF_ORI_TIS_TYP_A as 	Met_sit_of_ori_tis_typ_a
  , BD_TD_MET_SIT_OF_ORI_TIS_TYP_N as 	Met_sit_of_ori_tis_typ_n
  , RD_REQUEST as 	Request
  , RD_REQUEST_TITLE as 	Request_title
  , RD_ASSOCIATED_PROJECT_CODE as 	Associated_project_code
  , RD_PATHOLOGIST as 	Pathologist
  , RD_PRIMARY_INVESTIGATOR as 	Primary_investigator
  , RD_SLIDE_REQUESTOR as 	Slide_requestor
  , PATHLIMS_SYNC as 	Pathlims_sync
  , SCAN_DATE as 	Scan_date
  
  , concat('[', concat(substr(bd_request, 1, (instr(bd_request, '-(') -1)), ']')) as Study
  , case when substr(slide_path, -4, 4) = 'ndpi' then '.ndpi' else substr(slide_path, -4, 4) end as File_Extension
  , concat(
      '___', --can only concatenate two strings at a time...
      concat(
        substr(SLIDE_PATH,
          instr(SLIDE_PATH, '/', 1, 2) + 1, --in slide_path, start at the first character and return the index of the second '/' --then add one to the index to start the substring command
          instr(SLIDE_PATH, '/', 1, 3) - (instr(SLIDE_PATH, '/', 1, 2) + 1)), --length of the substring command is equal to the difference between the index location of the third '/' minus the index location of the second '/' minus one
      concat(
        '%2F',  
        substr(SLIDE_PATH,
          instr(SLIDE_PATH, '/', 1, 3) + 1,
          (length(SLIDE_PATH) + 1) - (instr(SLIDE_PATH, '/', 1, 3) + 1)) --length of the substring command is equal to the difference between the index location of the final character minus the index location of the third '/' minus one
          ))) as gSV_URL
   
from
  scanned_slides
  
where
    BARCODE_LABEL_BARCODE_TEXT is not null and
--    SCAN_DATE >= (trunc(sysdate) - 2) and --look back two days 
  
  slide_id in (
  
    select
    slide_id
    
    from  
      (
      select
        slide_id
        , barcode_label_barcode_text
        , row_number() over (partition by barcode_label_barcode_text order by slide_id desc) as RowNumber --most recent scan
            
      from
        scanned_slides
            
      where
        substr(slide_path,0,12) = 'NDP_tox_path'
      ) a
      
    where
      RowNumber = 1 --most recent scan
    )
    
order by
  scan_date desc, 
  barcode_label_barcode_text
  """)

sqlquery = query.fetchall()
gSV_data = pd.DataFrame(sqlquery)
gSV_data.columns = ['SLIDE_ID',	'FILE_PATH',	'BARCODE',	'ADD_TIS_COM',	'ADDITIONAL_TISSUE_ABBREV',	'ADDITIONAL_TISSUE_NAME',	'SLIDE_TYPE',	'SLIDE_EXPERIMENT',	'SLIDE_NUMBER',	'SLIDE_ANTIBODY',	'SLIDE_AB_ROW',	'SLIDE_CLONE',	'SLIDE_DILUTION',	'SLIDE_PCR_PRODUCT',	'SLIDE_CDNA_DATA_CDNA_ID',	'SLIDE_CDNA_DATA_CDNA_NAME',	'SLIDE_CDNA_DATA_DNAID',	'SLIDE_STAIN',	'BLOCK_',	'REQUEST',	'TMA_TITLE',	'IS_CONTROL_GROUP',	'GROUP_',	'ANIMAL',	'SPECIES',	'STRAIN',	'TREATMENT',	'DOSAGE',	'ROUTE',	'CELL_LINE',	'TISSUE_TYPE_ABBREV',	'TISSUE_TYPE_NAME',	'PATIENT_NUMBER',	'PATIENT_SOURCE',	'DIA_DAT_PAT_DIA',	'TISSUE_DIAGNOSIS',	'VERIFICATION_STATUS',	'PCT_TUMOR',	'MET_SIT_OF_ORI_TIS_TYP_A',	'MET_SIT_OF_ORI_TIS_TYP_N',	'REQUEST_1',	'REQUEST_TITLE',	'ASSOCIATED_PROJECT_CODE',	'PATHOLOGIST',	'PRIMARY_INVESTIGATOR',	'SLIDE_REQUESTOR',	'PATHLIMS_SYNC',	'SCAN_DATE',	'STUDY',	'FILE_EXTENSION',	'GSV_URL',]


# LOAD IN PREVIOUS DATA FROM GOOGLE SHEETS 
import gspread
from oauth2client.service_account import ServiceAccountCredentials

credendials_json = '___/cf73a8eb69d0.json'

scope = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(credendials_json, scope)

client = gspread.authorize(credentials)

wks = client.open('___').sheet1


# CONVERT TO PANDAS DATAFRAME
data = wks.get_all_values()
headers = data.pop(0)
gsheet = pd.DataFrame(data, columns=headers)


# CREATE NEW COLUMN BY CONCATENATING BARCODE WITH SCAN DATE
# THIS ALLOWS US TO KEEP NEW SCANS OF A SLIDE IN THE NEXT CELL
# NOTE THAT ONLY NEW SCANS WILL BE KEPT (IE ALL OLD SCANS WILL BE DROPPED) AT THE END OF THE SCRIPT

gSV_data['BARCODE_SCANDATE'] = gSV_data['BARCODE'].map(str) + '_' + gSV_data['SCAN_DATE'].map(str)


# THIS FOR LOOP CONVERTS DATES FROM EXCEL INTEGER DATE FORMAT TO YYYY-MM-DD

import datetime as dt
for i in range(len(gsheet['SCAN_DATE'])):
    if len(gsheet['SCAN_DATE'][i]) < 10: # ex: len(43846) = 5 vs len(2020-01-16) = 10
        mytempdf = gsheet[i : (i + 1)]
        placeholder = pd.TimedeltaIndex(mytempdf['SCAN_DATE'].astype(int), unit = 'd') + dt.datetime(1899, 12, 30) # https://stackoverflow.com/questions/38454403/convert-excel-style-date-with-pandas
        placeholder_df = pd.DataFrame(placeholder)
        gsheet['SCAN_DATE'][i] = pd.to_datetime(placeholder_df[0][0])
        
           
# CONVERT SCAN_DATE COLUMN TO DATETIME TYPE IN PYTHON
gsheet['SCAN_DATE'] = pd.to_datetime(gsheet['SCAN_DATE'])


# CREATE NEW COLUMN BY CONCATENATING BARCODE WITH SCAN DATE AS ABOVE
gsheet['BARCODE_SCANDATE'] = gsheet['BARCODE'].map(str) + '_' + gsheet['SCAN_DATE'].map(str)

# gsheet[(gsheet['BARCODE'] == '9KY-PL20') | (gsheet['BARCODE'] == '3700-PL14')].SCAN_DATE


# ONLY WANT BARCODES NOT CURRENTLY IN INVENTORY TOOL

gSV_data_keep = gSV_data[~gSV_data.BARCODE_SCANDATE.isin(gsheet.BARCODE_SCANDATE)]


gSV_data_keep.shape


# DROP EXTRA COLUMNS THAT WERE JUST CREATED

del gSV_data['BARCODE_SCANDATE']
del gsheet['BARCODE_SCANDATE']
del gSV_data_keep['BARCODE_SCANDATE']


# DEFINE FOLDER ON ROSALIND (DNA FILESHARE) TO SCRAPE METADATA

path_walk = '___'


# CREATE PROGRAM TO SCRAPE ROSALIND FOR METADATA

import os
import time
import csv

def main(path_walk):
    all_paths = []
    for root, dirs, files in os.walk(path_walk):
        for f in files:
            p = os.path.join(root,f)
            extension = os.path.splitext(p)[1]
            size = os.path.getsize(p)
            unix_time = os.path.getmtime(p)
            t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(unix_time))
#             print(p, extension,size, t)
            all_paths.append([p, extension,size,t])
    return all_paths


# EXECUTE PROGRAM

rosalind_data = main(path_walk)


# CONVERT TO PANDAS DATAFRAME

df = pd.DataFrame(rosalind_data)


# RENAME COLUMN HEADERS

df.columns = ['File_path_OS','Extension','Size','Last_Modified_Date']
df.shape


# FILTER FILE TYPES DOWN TO ONLY THOSE ACCEPTED BY OPENSLIDE

openslideformats = ['.svs', '.tif', '.ndpi', '.vms', '.vmu', '.scn', '.mrxs', '.tiff', '.svslide', '.bif']
df = df[df.Extension.isin(openslideformats)]


# CREATE COLUMN FOR INNER JOIN (ie MERGE) TO gSLIDE VIEWER DATA

df['Path_inner_join'] = df.apply(lambda row: row.File_path_OS[31:], axis=1)


# INNER JOIN BETWEEN ROSALIND DNA FILESHARE WITH gSV

output_for_tableau = pd.merge(gSV_data_keep, df, how='inner', left_on='FILE_PATH', right_on='Path_inner_join')
output_for_tableau.shape


# EXTRACT MAGNIFICATION FROM EACH SLIDE AND STORE IT IN A LIST

import openslide as osl

max_magnification = []

for example_file in output_for_tableau['File_path_OS']:
    # create an OpenSlide object
    ex_openslide = osl.OpenSlide(example_file)

    # slide propoerties from which we will later extract the objective power which equals the magnification
    slide_properties = ex_openslide.properties

    # 'Barcode','Max_Magnification'   
    # split on '/' delimeter -> grab last piece -> split that at the decimal -> return the piece before the decimal
    try: 
        max_magnification.append([os.path.basename(example_file).split('.')[0], str(slide_properties['openslide.objective-power'] + 'x')])  
    except KeyError:
        max_magnification.append([os.path.basename(example_file).split('.')[0], 'Null'])


max_mag = pd.DataFrame(max_magnification)
max_mag.shape


# CONVERT MAGNIFICATION DATA TO A DATAFRAME AND LABEL COLUMNS

max_mag = pd.DataFrame(max_magnification)
max_mag.columns = ['Barcode','Max_Magnification']


# INCORPORATE MAGNIFICATION DATA TO THE REST OF THE DATA

output_for_tableau_with_mag = pd.merge(output_for_tableau, max_mag, how='left', left_on='BARCODE', right_on='Barcode')


# REMOVE COLUMNS NOT CURRENTLY IN USE

output_for_tableau_with_mag = output_for_tableau_with_mag.drop(['ADD_TIS_COM', 'ADDITIONAL_TISSUE_ABBREV', 'ADDITIONAL_TISSUE_NAME', 'SLIDE_EXPERIMENT', 'SLIDE_NUMBER', 'SLIDE_ANTIBODY', 'SLIDE_AB_ROW', 'SLIDE_CLONE', 'SLIDE_DILUTION', 'SLIDE_PCR_PRODUCT', 'SLIDE_CDNA_DATA_CDNA_ID', 'SLIDE_CDNA_DATA_CDNA_NAME', 'SLIDE_CDNA_DATA_DNAID', 'PATIENT_NUMBER', 'PATIENT_SOURCE', 'DIA_DAT_PAT_DIA', 'TISSUE_DIAGNOSIS', 'VERIFICATION_STATUS', 'PCT_TUMOR', 'MET_SIT_OF_ORI_TIS_TYP_A', 'MET_SIT_OF_ORI_TIS_TYP_N', 'ASSOCIATED_PROJECT_CODE', 
                      'TMA_TITLE', 'IS_CONTROL_GROUP', 'STRAIN', 'ROUTE', 'CELL_LINE', 'TISSUE_TYPE_ABBREV', 'PRIMARY_INVESTIGATOR', 'SLIDE_REQUESTOR', 'PATHLIMS_SYNC', 'Extension','Path_inner_join', 'File_path_OS', 'Barcode', 'SLIDE_ID', 'SLIDE_STAIN', 'REQUEST_1', 'REQUEST_TITLE', 'Last_Modified_Date'], axis=1)


# LOAD IN PREVIOUS DATA FROM GOOGLE SHEETS 

import gspread
from oauth2client.service_account import ServiceAccountCredentials

credendials_json = '___/cf73a8eb69d0.json'

scope = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(credendials_json, scope)

client = gspread.authorize(credentials)

wks = client.open('___').sheet1


# CONVERT TO PANDAS DATAFRAME

data = wks.get_all_values()
headers = data.pop(0)
gsheet = pd.DataFrame(data, columns=headers)


# APPEND NEW DATA TO GOOGLE SHEET
# BUT FIRST CONVERT SCAN_DATE BACK TO STR MATCH WITH GSHEET SO THAT FOR LOOP BELOW WILL NOT ERROR

output_for_tableau_with_mag['SCAN_DATE'] = output_for_tableau_with_mag['SCAN_DATE'].astype(str)
gsheet = gsheet.append(output_for_tableau_with_mag)


# RESET DATAFRAME INDEX AFTER APPEND AND DROP OLD INDEX COLUMN

gsheet = gsheet.reset_index()
gsheet.drop(['index'], axis=1, inplace=True)


# CONVERT SCAN_DATE COLUMN TO DATETIME TYPE IN PYTHON
# THIS FOR LOOP CONVERTS DATES FROM EXCEL INTEGER DATE FORMAT TO YYYY-MM-DD
import datetime as dt
for i in range(len(gsheet['SCAN_DATE'])):
    if len(gsheet['SCAN_DATE'][i]) < 10: # ex: len(43846) = 5 vs len(2020-01-16) = 10
        mytempdf = gsheet[i : (i + 1)]
        placeholder = pd.TimedeltaIndex(mytempdf['SCAN_DATE'].astype(int), unit = 'd') + dt.datetime(1899, 12, 30) # https://stackoverflow.com/questions/38454403/convert-excel-style-date-with-pandas
        placeholder_df = pd.DataFrame(placeholder)
        gsheet['SCAN_DATE'][i] = pd.to_datetime(placeholder_df[0][0])
 
           
# CONVERT SCAN_DATE COLUMN TO DATETIME TYPE IN PYTHON
gsheet['SCAN_DATE'] = pd.to_datetime(gsheet['SCAN_DATE'])


# SORT BY BARCODE ascending THEN SCAN_DATE descending

gsheet = gsheet.sort_values(['BARCODE', 'SCAN_DATE'], ascending=[True, False])


# CREATE WINDON FUNCTION TO LATER DROP OLD SCANS

gsheet['Row_Number_Barcode_Window_Function'] = gsheet.groupby('BARCODE')['SCAN_DATE'].rank(ascending=0, method='first')
gsheet['My_Row_Num'] = gsheet.reset_index().index


# STORE INDECES OF SLIDES TO DROP

indeces_to_drop = gsheet['My_Row_Num'][gsheet['Row_Number_Barcode_Window_Function'] > 1].tolist()


# DROP OLDER SCANS AND REMOVE Row_Number_Barcode_Window_Function AND My_Row_Num COLUMNS

gsheet = gsheet[~gsheet['My_Row_Num'].isin(indeces_to_drop)]

gsheet = gsheet.drop(['Row_Number_Barcode_Window_Function'], axis=1)
gsheet = gsheet.drop(['My_Row_Num'], axis=1)


# EXPORT TO GOOGLE SHEETS

import pygsheets
gc = pygsheets.authorize(service_file = credendials_json)

#open the google spreadsheet 
sh = gc.open('___')

#select the first sheet 
wks = sh[0]

#update the first sheet with df, starting at cell A1 
wks.set_dataframe(gsheet, 'A1', fit=True)
