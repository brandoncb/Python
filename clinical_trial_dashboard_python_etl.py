import sys
import bodi_library
import os
import pandas as pd
import numpy as np
import glob
from PythonTeradata import PythonTeradata

# login credentials for MDH connection
uid = '____'
pwd = '____'

# ---------------------------------------------------------
# BEGIN USER INPUT
# ---------------------------------------------------------

# study for this dashbaord
dashboard_study = 'GO41003'

# set the OBDFS mapping
BODIroot = '____' #Rosalind mapping of OBDFS

# foldernames in _bodi_ of datasets
bodi_data_folders = ['GA41003_CCLS_LABLINK'
                    , 'GA41003_BRM_SAMPLELIST'
                    # , 'GA41003_UNKNOWN_SUBJECTSUMMARYREPORT'
                     ]

# specify the list of columns to keep for each dataset
# columns to keep for *FIRST* dataset
columns_to_keep0 = [
'Patient ID',
'Screening Number',
'Accession Number',
'Container Number',
'Container Barcode',
'Protocol Visit Code',
'Visit Description',
'Visit Date/Time',
'Specimen Collection Date',
'Specimen Collection Time',
'Container Receipt Date',
'SMART Specimen Class', #using expression grel:substring(value,0,4)
'SMART Specimen Class Description',
'SMART Specimen Status',
'Shipped To Location Organization Name',
'Shipped On Date/Time'
]

# columns to keep for *SECOND* dataset
columns_to_keep1 = [
'Visit Name',
'Enrollment Number',
'Screening Number',
'SAMi Sample ID',
'External Barcode',
'Parent External Barcode',
'Sample Type',
'Sample Status',
'Received By',
'Received Date',
'Location'  
]


# ---------------------------------------------------------
# END USER INPUT
# ---------------------------------------------------------



# bring in patient clinical data
# most of this is to get the correct subject status which comes from two places
# RAVE file in Patients table is the most accurate but only has statuses for randomized subjects
# for non-randomized subjects, this is obtained by selecting each subject's most recent visit from gCORE_DI tables
td = PythonTeradata()
td.connect(uid=uid, pwd=pwd, dsn="US")
clin_pa = td.query("""
select
	pat.*
	, status.subject_status
	, case when status.file_name is null then 'gCORE_DI_MDH' else status.file_name end as file_name
	, case when status.file_date is null then (current_date() - 1) else status.file_date end as file_date
												
from 	
	(select
		s.study_id
		, s.therapeutic_area
		, s.full_title
		, s.molecule_name
		, pa.screening_number
		, pa.patient_number
		, pa.arm_name
		, pa.cohort_name
		, pa.indication
		, pa.gender
		, pa.DOB
		, pa.site_number
		, pa.site_name
		, pa.site_country
		, pa.principal_investigator
	
	from
		BODI_TRANSFORM_PROD.GCOR_DI_Studies s
		
	left join
		BODI_TRANSFORM_PROD.GCOR_DI_Patients pa
		on s.study_rec_id = pa.study_rec_id
		
	where	
		s.study_id = 'GA41003') pat

left join 
 	
	(select
		mrv_stat.screening_number
		, coalesce(sub_stat.subject_status, mrv_stat.mrv_subject_status) SUBJECT_STATUS
		, sub_stat.source_file_name file_name
		, cast(
			concat(
				substring(
					substring(source_file_name, 25, 8),
						1, 4)
							, '-', substring(
								substring(source_file_name, 25, 8),
									5, 2)
										, '-', substring(
											substring(source_file_name, 25, 8),
												7, 2)) as date) file_date
	 	
	from
		(select
			c.screening_number
			, c.patient_number
			, c.site_number
			, c.site_name
			, b."VALUE" SUBJECT_STATUS
			, a.source_file_name
			
		from bodi_transform_prod.source_files a
			
		inner join
			bodi_transform_prod.tdi_flex_data b
			on a.source_file_rec_id = b.source_file_rec_id
		 	
		inner join
			bodi_transform_prod.patients c
			on a.source_file_rec_id = c.source_file_rec_id
			and b.origin_rec_id = c.origin_rec_id
		 		
		where
			a.source_file_name like '%GA41003%'
			and b.attribute_name = 'SUBJECT_STATUS') sub_stat
	 	
	full join
	
		(select
			mrv.screening_number
			, case when mrv.visit_name = 'Screen Failure' then 'Screen Failure'
				when mrv.visit_name = 'Screening' then 'Screening'
				when mrv.visit_name = 'Re-Screened' then 'Re-Screened'
				when mrv.visit_name = 'Randomization Visit' then 'Randomized'
				else 'OTHER' end as mrv_subject_status
			
		from	
		(select
			pa.screening_number
			, pa.patient_number
			, pv.visit_name
			, pv.visit_date
			, row_number() over (partition by pa.screening_number order by pv.visit_date desc) most_recent_visit
		
		from
			BODI_TRANSFORM_PROD.GCOR_DI_Studies s
			
		left join
			BODI_TRANSFORM_PROD.GCOR_DI_Patients pa
			on s.study_rec_id = pa.study_rec_id
			
		left join
			BODI_TRANSFORM_PROD.GCOR_DI_Patient_Visits pv
			on pa.patient_rec_id = pv.patient_rec_id
			
		where	
			s.study_id = 'GA41003') mrv
			
		where
			mrv.most_recent_visit = 1) mrv_stat
	
	on sub_stat.screening_number = mrv_stat.screening_number
	) status

on pat.screening_number = status.screening_number

order by pat.screening_number
;
""")


# clin_pa.head()


# clinical visit data from gCORE_DI tables
td = PythonTeradata()
td.connect(uid=uid, pwd=pwd, dsn="US")
clin_vis = td.query("""
select
 	pa.screening_number
 	, pv.visit_name
 	, pv.visit_date
 
 from
 	BODI_TRANSFORM_PROD.GCOR_DI_Studies s
 	
 left join
 	BODI_TRANSFORM_PROD.GCOR_DI_Patients pa
 	on s.study_rec_id = pa.study_rec_id
 	
 left join
 	BODI_TRANSFORM_PROD.GCOR_DI_Patient_Visits pv
 	on pa.patient_rec_id = pv.patient_rec_id
 	
 where	
 	s.study_id = 'GA41003'
 
 order by
 	pa.screening_number
 	, pv.visit_date;
""")

#clin_vis.head()

# list of filepaths to each dataset's most recent csv
path_list = bodi_library.get_newest_CSV(bodi_data_folders, BODIroot)
print(path_list)
#read source files assisgn to datasets
dataset0 = pd.read_csv(path_list[0])
dataset1 = pd.read_csv(path_list[1])

# only keep certain columns from each dataset; then reset the index
dataset0 = dataset0[columns_to_keep0]
dataset0 = dataset0.reset_index()
dataset0.drop(['index'], axis=1, inplace=True)

dataset1 = dataset1.rename(columns = {
	'VISIT_NAME' : 'Visit Name',
	'ENROLLMENTNUM' : 'Enrollment Number',
	'SCREENINGNUM' : 'Screening Number',
	'SAMPLEID' : 'SAMi Sample ID',
	'EXTERNAL_BARCODE' : 'External Barcode',
	'PARENT_EXTERNAL_BARCODE' : 'Parent External Barcode',
	'SAMPLETYPE' : 'Sample Type',
	'SAMPLESTATUS' : 'Sample Status',
	'FIRSTCUSTODIALDEPARTMENT' : 'Received By',
	'FIRST_RECEIVED_DATE' : 'Received Date',
	'LOCATION' : 'Location'		
})
dataset1 = dataset1[columns_to_keep1]
dataset1 = dataset1.reset_index()
dataset1.drop(['index'], axis=1, inplace=True)

# some visits had asterisks which is an artifact
dataset0['Protocol Visit Code'] = dataset0['Protocol Visit Code'].str.replace('*', '')
dataset0['Visit Description'] = dataset0['Visit Description'].str.replace('*', '')

# some columns read in as float with a period and trailing zero - this removes those and stores as a string for merging later
dataset0['Patient ID'] = dataset0['Patient ID'].astype(str).replace('\.0', '', regex=True)
dataset0['Accession Number'] = dataset0['Accession Number'].astype(str).replace('\.0', '', regex=True)
dataset0['Container Barcode'] = dataset0['Container Barcode'].astype(str).replace('\.0', '', regex=True)
dataset0['Screening Number'] = dataset0['Screening Number'].astype(str).replace('\.0', '', regex=True)
dataset1['Enrollment Number'] = dataset1['Enrollment Number'].astype(str).replace('\.0', '', regex=True)
dataset1['Screening Number'] = dataset1['Screening Number'].astype(str).replace('\.0', '', regex=True)

# add file name and file date columns to datasets
filename_filedate = bodi_library.create_filename_filedate_list_of_lists(path_list)
bodi_library.add_filename_filedate_columns(filename_filedate, 0, dataset0)
bodi_library.add_filename_filedate_columns(filename_filedate, 1, dataset1)

# create the the Roche_Specimen_ID for CCLS data
# first create a column that has a leading zero if the value of 'Container Number' is a single digit
# then concatenate to Accession Number
dataset0['Container Number'] = dataset0['Container Number'].astype(str)       

result = []      
for i in range(len(dataset0['Container Number'])):
    if len(dataset0['Container Number'][i]) == 1: 
        result.append('0' + dataset0['Container Number'][i])
    else:
        result.append(dataset0['Container Number'][i])        


dataset0['Container_Number_two_digits'] = result       
dataset0['Roche_Specimen_ID'] = dataset0['Accession Number'].map(str) + dataset0['Container_Number_two_digits'].map(str)


# split up sample types in different dataframes, group by number of brm levels
'''
ex Nasal_Swab has 3 levels because:
1) the CCLS Accession Number gets a SAMi Sample ID
2) the L & R nostril samples get combined into an elution tube which gets a SAMi Sample ID
3) this then gets split into the final tubes which are sent to CROs; each tube gets a SAMi Sample ID
'''
# 3 brm levels
Nasal_Swab = dataset1[dataset1['Sample Type'] == 'Nasal Swab']

# 2 brm levels
Serum = dataset1[dataset1['Sample Type'] == 'Serum']
Bronchosorption = dataset1[dataset1['Sample Type'] == 'Bronchosorption']

# 1 brm level
Blood = dataset1[dataset1['Sample Type'] == 'Blood']
Epithelial_Brushing = dataset1[dataset1['Sample Type'] == 'Epithelial Brushing']
Urine = dataset1[dataset1['Sample Type'] == 'Urine']

# 0 brm levels
Endobronchial_Biopsy = dataset1[dataset1['Sample Type'] == 'Endobronchial Biopsy']

# Nasal_Swab
# Serum
# Bronchosorption
# Blood
# Epithelial_Brushing
# Urine
# Endobronchial_Biopsy


# create empty dataframe for sample types that do not qet aliquoted as much as nasal swabs and therefore do not need as many extra columns aka 'levels'
empty_df = pd.DataFrame(np.nan, index=[0], columns=['SAMi Sample ID','External Barcode','Parent External Barcode','Sample Type','Sample Status','Received By','Received Date','Location'])
# empty_df

# Nasal_Swab
# separate dataframes by level
Nasal_Swab_L1 = Nasal_Swab[Nasal_Swab['Parent External Barcode'].isnull() == True]

# only certain columns will be needed for level 2 and 3 datasets
Nasal_Swab_L2 = Nasal_Swab[['SAMi Sample ID','External Barcode','Parent External Barcode','Sample Type','Sample Status','Received By','Received Date','Location']]
Nasal_Swab_L3 = Nasal_Swab[['SAMi Sample ID','External Barcode','Parent External Barcode','Sample Type','Sample Status','Received By','Received Date','Location']]

# add prefix of brm_level
Nasal_Swab_L1 = Nasal_Swab_L1.add_prefix('brm_level1_')
Nasal_Swab_L2 = Nasal_Swab_L2.add_prefix('brm_level2_')
Nasal_Swab_L3 = Nasal_Swab_L3.add_prefix('brm_level3_')

# merge
Nasal_Swab_all_levels = pd.merge(Nasal_Swab_L1, Nasal_Swab_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Parent External Barcode')
Nasal_Swab_all_levels = pd.merge(Nasal_Swab_all_levels, Nasal_Swab_L3, how='left', left_on='brm_level2_External Barcode', right_on='brm_level3_Parent External Barcode')


# Serum
# separate dataframes by level
Serum_L1 = Serum[Serum['Parent External Barcode'].isnull() == True]
Serum_L2 = Serum[(Serum['Parent External Barcode'].isnull() == False) & (Serum['External Barcode'].isnull() == False)]

# only certain columns will be needed for level 2 and 3 datasets
Serum_L2 = Serum_L2[['SAMi Sample ID','External Barcode','Parent External Barcode','Sample Type','Sample Status','Received By','Received Date','Location']]

# add prefix of brm_level
Serum_L1 = Serum_L1.add_prefix('brm_level1_')
Serum_L2 = Serum_L2.add_prefix('brm_level2_')
Serum_L3 = empty_df.add_prefix('brm_level3_')

# merge; use empty_df for unused levels (see comments where empty_df is created for decription of levels)
Serum_all_levels = pd.merge(Serum_L1, Serum_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Parent External Barcode')
Serum_all_levels = pd.merge(Serum_all_levels, Serum_L3, how='left', left_on='brm_level1_External Barcode', right_on='brm_level3_Location')


# Bronchosorption
# separate dataframes by level
Bronchosorption_L1 = Bronchosorption[Bronchosorption['Parent External Barcode'].isnull() == True]
Bronchosorption_L2 = Bronchosorption[(Bronchosorption['Parent External Barcode'].isnull() == False) & (Bronchosorption['External Barcode'].isnull() == False)]

# only certain columns will be needed for level 2 and 3 datasets
Bronchosorption_L2 = Bronchosorption_L2[['SAMi Sample ID','External Barcode','Parent External Barcode','Sample Type','Sample Status','Received By','Received Date','Location']]

# add prefix of brm_level
Bronchosorption_L1 = Bronchosorption_L1.add_prefix('brm_level1_')
Bronchosorption_L2 = Bronchosorption_L2.add_prefix('brm_level2_')
Bronchosorption_L3 = empty_df.add_prefix('brm_level3_')

# merge; use empty_df for unused levels (see comments where empty_df is created for decription of levels)
Bronchosorption_all_levels = pd.merge(Bronchosorption_L1, Bronchosorption_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Parent External Barcode')
Bronchosorption_all_levels = pd.merge(Bronchosorption_all_levels, Bronchosorption_L3, how='left', left_on='brm_level1_External Barcode', right_on='brm_level3_Location')

# Blood
# separate dataframes by level
Blood_L1 = Blood[Blood['Parent External Barcode'].isnull() == True]

# add prefix of brm_level
Blood_L1 = Blood_L1.add_prefix('brm_level1_')
Blood_L2 = empty_df.add_prefix('brm_level2_')
Blood_L3 = empty_df.add_prefix('brm_level3_')

# merge; use empty_df for unused levels (see comments where empty_df is created for decription of levels)
Blood_all_levels = pd.merge(Blood_L1, Blood_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Location')
Blood_all_levels = pd.merge(Blood_all_levels, Blood_L3, how='left', left_on='brm_level1_External Barcode', right_on='brm_level3_Location')


# Epithelial_Brushing
# separate dataframes by level
Epithelial_Brushing_L1 = Epithelial_Brushing[Epithelial_Brushing['Parent External Barcode'].isnull() == True]

# add prefix of brm_level
Epithelial_Brushing_L1 = Epithelial_Brushing_L1.add_prefix('brm_level1_')
Epithelial_Brushing_L2 = empty_df.add_prefix('brm_level2_')
Epithelial_Brushing_L3 = empty_df.add_prefix('brm_level3_')

# merge; use empty_df for unused levels (see comments where empty_df is created for decription of levels)
Epithelial_Brushing_all_levels = pd.merge(Epithelial_Brushing_L1, Epithelial_Brushing_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Location')
Epithelial_Brushing_all_levels = pd.merge(Epithelial_Brushing_all_levels, Epithelial_Brushing_L3, how='left', left_on='brm_level1_External Barcode', right_on='brm_level3_Location')


# Urine
# separate dataframes by level
Urine_L1 = Urine[Urine['Parent External Barcode'].isnull() == True]

# add prefix of brm_level
Urine_L1 = Urine_L1.add_prefix('brm_level1_')
Urine_L2 = empty_df.add_prefix('brm_level2_')
Urine_L3 = empty_df.add_prefix('brm_level3_')

# merge; use empty_df for unused levels (see comments where empty_df is created for decription of levels)
Urine_all_levels = pd.merge(Urine_L1, Urine_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Location')
Urine_all_levels = pd.merge(Urine_all_levels, Urine_L3, how='left', left_on='brm_level1_External Barcode', right_on='brm_level3_Location')


# Endobronchial_Biopsy
# separate dataframes by level
Endobronchial_Biopsy_L1 = Endobronchial_Biopsy[Endobronchial_Biopsy['Parent External Barcode'].isnull() == True]

# add prefix of brm_level
Endobronchial_Biopsy_L1 = Endobronchial_Biopsy_L1.add_prefix('brm_level1_')
Endobronchial_Biopsy_L2 = empty_df.add_prefix('brm_level2_')
Endobronchial_Biopsy_L3 = empty_df.add_prefix('brm_level3_')

# merge; use empty_df for unused levels (see comments where empty_df is created for decription of levels)
Endobronchial_Biopsy_all_levels = pd.merge(Endobronchial_Biopsy_L1, Endobronchial_Biopsy_L2, how='left', left_on='brm_level1_External Barcode', right_on='brm_level2_Location')
Endobronchial_Biopsy_all_levels = pd.merge(Endobronchial_Biopsy_all_levels, Endobronchial_Biopsy_L3, how='left', left_on='brm_level1_External Barcode', right_on='brm_level3_Location')


BRM_data = Nasal_Swab_all_levels.append(Serum_all_levels).append(Bronchosorption_all_levels).append(Blood_all_levels).append(Epithelial_Brushing_all_levels).append(Urine_all_levels).append(Endobronchial_Biopsy_all_levels)

# add column prefixes based on vendor/source
dataset0 = dataset0.add_prefix('CCLS_')
clin_vis = clin_vis.add_prefix('CLIN_')
clin_pa = clin_pa.add_prefix('CLIN_')

CCLS_BRM = pd.merge(dataset0, BRM_data, how='outer', left_on='CCLS_Roche_Specimen_ID', right_on='brm_level1_External Barcode')

VIS_CCLS_BRM = pd.merge(clin_vis, CCLS_BRM, how='outer', left_on='CLIN_VISIT_NAME', right_on='brm_level1_External Barcode')

# coalesce screening numbers between CCLS and BRM for merge to clinical data
VIS_CCLS_BRM['coalesce_screening_number'] = np.nan
VIS_CCLS_BRM['coalesce_screening_number'] = VIS_CCLS_BRM['coalesce_screening_number'].fillna(VIS_CCLS_BRM['CLIN_SCREENING_NUMBER']).fillna(VIS_CCLS_BRM['CCLS_Screening Number']).fillna(VIS_CCLS_BRM['brm_level1_Screening Number'])
VIS_CCLS_BRM['coalesce_screening_number'] = np.where(VIS_CCLS_BRM['coalesce_screening_number'].isnull() == True, 'screening_number_not_found', VIS_CCLS_BRM['coalesce_screening_number'])

# print(VIS_CCLS_BRM['coalesce_screening_number'].unique())

VIS_CCLS_BRM['coalesce_screening_number'] = VIS_CCLS_BRM['coalesce_screening_number'].astype(str).replace('\.00', '', regex=True)
VIS_CCLS_BRM['coalesce_screening_number'] = VIS_CCLS_BRM['coalesce_screening_number'].astype(str).replace('\.0', '', regex=True)

VIS_CCLS_BRM.drop(['CLIN_SCREENING_NUMBER'], axis = 1, inplace = True)

# format columns for merging
clin_pa['CLIN_SCREENING_NUMBER'] = clin_pa['CLIN_SCREENING_NUMBER'].astype(str)
VIS_CCLS_BRM['coalesce_screening_number'] = VIS_CCLS_BRM['coalesce_screening_number'].astype(str)

CLIN_VIS_CCLS_BRM = pd.merge(clin_pa, VIS_CCLS_BRM, how='outer', left_on='CLIN_SCREENING_NUMBER', right_on='coalesce_screening_number')

# remove extra column(s)
output_for_tableau_trim = CLIN_VIS_CCLS_BRM
output_for_tableau_trim.drop(['coalesce_screening_number'], axis = 1, inplace = True)

# sort for output
output_for_tableau_trim = output_for_tableau_trim.sort_values(by=['CLIN_SCREENING_NUMBER', 'CLIN_VISIT_DATE', 'CCLS_Screening Number', 'CCLS_Visit Date/Time', 'brm_level1_Screening Number', 'brm_level1_Received Date'], na_position ='first')
#output_for_tableau_trim = output_for_tableau_trim.sort_values(by=['brm_level1_Received Date','brm_level1_Screening Number','CCLS_Visit Date/Time','CCLS_Screening Number','CLIN_VISIT_DATE','CLIN_SCREENING_NUMBER'], na_position ='first')
#output_for_tableau_trim = output_for_tableau_trim.sort_values(by=['CCLS_Screening Number','CCLS_Visit Date/Time','brm_level1_Screening Number','brm_level1_Received Date','CLIN_VISIT_DATE','CLIN_SCREENING_NUMBER'], na_position ='first')


# export to _bodi_
output_for_tableau_trim.to_excel(BODIroot + '/tableau/' + dashboard_study.lower() + '_tableau_updated.xlsx', index=False)



# export to dsisys44 workspace as csv
output_for_tableau_trim.to_csv('____' + 'ga41003_tableau_updated_excel_python.csv', index=False)

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile('____.json')
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

# ---------------------------------------------------------
# This is only needed to start the process for a new study
# ---------------------------------------------------------

# this specifies the gDrive folder for dsisys44
## fileID = '____'

# create csv file in the above folder
## file1 = drive.CreateFile({"mimeType": "txt/csv", "parents": [{"kind": "drive#fileLink", "id": fileID}]})

# upload the following csv file from Rosalind and print confirmation
## file1.SetContentFile('____.csv')
## file1.Upload()
## print('Created file %s with mimeType %s' % (file1['title'], file1['mimeType']))

# for the first time for the study and after the above csv file has been written to the gDrive, call the myfile object 
# this will produce it's metadata in dictionary format

##file1 #myfile

# save this metadata to myfile_refresh so that you can write to that file
# writing to the same csv file in gDrive is essential because Tableau is connected to that file by its unique hash

# ---------------------------------------------------------
# until here
# ---------------------------------------------------------




# this metadata in dictionary format corresponds to the csv file in gDrive and was obtained by calling 'file1' following the first upload of the csv file to gDrive
myfile_refresh = drive.CreateFile({'mimeType': 'txt/csv', 'parents': [{'kind': 'drive#parentReference', 'id': '____', })

# upload the refreshed csv (stored on Rosalind) to the csv in gDrive for dsisys44
myfile_refresh.SetContentFile('____.csv')
myfile_refresh.Upload()
