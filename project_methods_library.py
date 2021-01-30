import pandas as pd
import numpy as np
import os
import glob

###############################################################################################################
def get_newest_CSV(bodi_data_folders, BODIroot):
    """
    grabs newest csv from each bodi data folder specified above and returns names in a list
    returns path_list
    
    ex
    #BODIroot = '____' #Rosalind mapping of OBDFS
    BODIroot = 'Z:/_bodi_/' #local mapping of OBDFS

    bodi_data_folders = ['GO40554_CCLS_LABLINK_Prelim' 
                     ,'GO40554_HGX_InventoryTrackerSampleLevel_Prelim'
                     ,'GO40554_HGX_Results_Prelim'
                     #,''
                     ]    
    """
    path_list = []
    for i in range(0,len(bodi_data_folders)):
        mypath = os.path.join(BODIroot, bodi_data_folders[i])
        all_csv = glob.glob(mypath + '/*.csv')
        newest_csv = max(all_csv, key=os.path.getctime)
        path_list.append(newest_csv)
    return path_list
###############################################################################################################


###############################################################################################################
def import_clean_newest_RAVETDQ_file(dashboard_study, BODIroot):
    '''
    input your study for the dashboard and it will return the most recent RAVE TDQ file with cleaned headers (based on the JSON rules)
    
    ex
    dashboard_study = 'GO40554'
    '''
    BODIroot = BODIroot
    RAVETDQ_path = get_newest_CSV(['TDQ_RAVE'], BODIroot)
    RAVETDQ_frame = pd.read_csv(RAVETDQ_path[0])
    
    # only keep certain columns (based on JSON file); then reset the index
    #print(RAVETDQ_frame.shape)
    RAVETDQ_columns_to_keep = [
    'study_id',
    'subject_name',
    'study_event_data_oid',
    'tisbiop_rrefid',
    'tisbiop_tisbid',
    'tisbiop_tumtyp',
    'tisbiop_loc',
    'tisbiop_locbio',
    'tisbiop_sampp',
    'tisbiop_srosmp',
    'tisbiop_sampd'
    ]
    RAVETDQ_frame = RAVETDQ_frame[RAVETDQ_columns_to_keep]
    #print(RAVETDQ_frame.shape)
    RAVETDQ_frame = RAVETDQ_frame.reset_index()
    
    #filter on study of interest
    dashboard_study_list = [dashboard_study]
    RAVETDQ_frame = RAVETDQ_frame[RAVETDQ_frame.study_id.isin(dashboard_study_list)]
    # print(RAVETDQ_frame.shape)
    
    #rename columns
    RAVETDQ_frame.rename(columns={# 'study_id':'____',
                                  'subject_name':'ENROLLMENT_ID',
                                  'study_event_data_oid':'VISIT_KIT_TYPE',
                                  'tisbiop_rrefid':'ROCHE_SPECIMEN_ID',
                                  'tisbiop_tisbid':'SURGICAL_PATH_ID',
                                  'tisbiop_tumtyp':'TUMOR_TYPE',
                                  'tisbiop_loc':'LESION_SITE',
                                  'tisbiop_locbio':'HARVEST_LOCATION',
                                  'tisbiop_sampp':'PREPARATION_TYPE',
                                  'tisbiop_srosmp':'COLLECTION_METHOD',
                                  'tisbiop_sampd':'COLLECT_DATETIME'
                                  }, inplace=True)
    
    # format date and create specimen type column = 'Tissue'
    RAVETDQ_frame['COLLECT_DATETIME'] = pd.to_datetime(RAVETDQ_frame['COLLECT_DATETIME'])
    RAVETDQ_frame['SPECIMEN_TYPE'] = 'Tissue'
    
    # ROCHE_SPECIMEN_ID was read in with a period and trailing zero - this removes those
    RAVETDQ_frame['ROCHE_SPECIMEN_ID'] = RAVETDQ_frame['ROCHE_SPECIMEN_ID'].astype(str).replace('\.0', '', regex=True)
    
    return RAVETDQ_frame
###############################################################################################################


###############################################################################################################
def CCLS_SM_class_specimen_type_dictionary(mydict):
    '''
    input a blank dictionary and it will fill it CCLS SM Class definitions for *specimen type*
    '''
    mydict = {"SM01": "Serum", 	"SM02": "Serum", 	"SM03": "Serum", 	"SM04": "Serum", 	"SM05": "Serum", 	"SM06": "Serum", 	"SM07": "Serum", 	"SM09": "Serum", 	"SM10": "Plasma", 	"SM11": "Plasma", 	"SM12": "Plasma", 	"SM13": "Plasma", 	"SM14": "Serum", 	"SM15": "Serum"}
    return mydict
###############################################################################################################


###############################################################################################################
def CCLS_SM_class_timepoint_dictionary(mydict):
    '''
    input a blank dictionary and it will fill it CCLS SM Class definitions for *timepoint*
    '''
    mydict = {"SM01": "", 	"SM02": "PREDOSE", 	"SM03": "EOI", 	"SM04": "2HR", 	"SM05": "24HR", 	"SM06": "", 	"SM07": "PREDOSE", 	"SM09": "PREDOSE", 	"SM10": "", 	"SM11": "PREDOSE", 	"SM12": "EOI", 	"SM13": "24HR", 	"SM14": "", 	"SM15": "PREDOSE"}
    return mydict
###############################################################################################################


###############################################################################################################
def pivot_assay_columns(mydataframe, column_for_merging, assay_file_column_nopivot_list, index_assay_col_start):
    """
    TLDR: returns your dataframe with the assay columns pivoted around your nopivot (ie anchor) columns with blank results removed
    
    use this if the source file has many many columns (one for each assay) where the cell values are the assay results
    mydataframe is the assay results file, so we transpose to two columns (assay and assay_result) and blow up the grain
    the other columns you want to keep (such as screening number, etc) are called the nopivot (ie anchor) columns
    these will serve as the "anchor" for which the assay data will be transposed around
    
    the below function iteratively grabs each row of data and creates a separate dictionary, which then gets converted into a dataframe
    this essentially transposes the horizontal data into a vertical pair
    these dataframes then get a third column so that you can merge back to the nopivot columns (screening number can be a good option)
    output is the assays_transposed file
    
    if not specified, it is assumed the assay columns start after the nopivot columns
    
    reference:
    assay_file_column_list_all = list(mydataframe.columns.values)
    assay_file_column_nopivot_list = ['HGX Identifier', 'Study ID', 'Sample ID', 'Patient ID', 'Screening ID', 'Protocol Site', 'Visit Code']
    assays_to_pivot = [x for x in assay_file_column_list_all if x not in assay_file_column_nopivot_list]
    
    examples:
    index_assay_col_start = len(assay_file_column_nopivot_list) #assay columns start at this index
    column_for_merging = 'Screening ID'
    assay_file_column_nopivot_list = ['HGX Identifier', 'Study ID', 'Sample ID', 'Patient ID', 'Screening ID', 'Protocol Site', 'Visit Code']
    """
    
    df_union_all = pd.DataFrame(columns = ['assays','assay_results'])
     
    for i in range(0, mydataframe.shape[0]):
        d = {'assays': list(mydataframe.columns.values), 'assay_results': mydataframe.values[i]} # grab columns headers and ith row of results
        assay_pivot = pd.DataFrame(d)
        assay_pivot = assay_pivot[index_assay_col_start:] # first few columns before the assay columns are usually nopivot columns
        assay_pivot['column_for_merging'] = mydataframe[column_for_merging][i].astype(str) # create a third column for which to merge back to the nopivot columns
        assay_pivot = assay_pivot.dropna(how='any',axis=0) # delete rows with blank results

        frames = [df_union_all, assay_pivot]
        df_union_all = pd.concat(frames) # union all

    mydataframe[column_for_merging] = mydataframe[column_for_merging].astype(str) # string so no issue merging
    df_union_all['column_for_merging'] = df_union_all['column_for_merging'].astype(str)

    assays_transposed = pd.merge(mydataframe[assay_file_column_nopivot_list], df_union_all, how='outer', left_on=column_for_merging, right_on='column_for_merging')

    assays_transposed = assays_transposed.drop('column_for_merging', axis=1)    
    
    return assays_transposed
###############################################################################################################


###############################################################################################################
def create_filename_filedate_list_of_lists(path_list):
    '''
    create list of lists of source file name and file timestamp to later append to your datasets
    input is a filepath list for your datasets (as a list)
    
    ex
    pathlist = ['____.csv', ...]
    '''
    file_metadata = []
    for i in range(0,len(path_list)):
        file_name = os.path.basename(path_list[i])
        file_name_fields = file_name.split('_')
        file_date_dotformat = file_name_fields[-1]
        file_date_split = file_date_dotformat.split('.')
        file_date = file_date_split[0]
        
        file_metadata.append([file_name, file_date])
    # return file_metadata
    filename_filedate = pd.DataFrame(file_metadata, columns = ['file_name', 'file_date'])
    return filename_filedate
###############################################################################################################


###############################################################################################################
def add_filename_filedate_columns(filename_filedate_list_of_lists, row_from_list, mydataframe):
    '''
    assign filename and filedate columns to your dataframe
    row_from_list refers to the filename filedate pair extracted during the create_filename_filedate_list_of_lists method call 
    '''
    mydataframe['file_name'] = filename_filedate_list_of_lists['file_name'][row_from_list]
    mydataframe['file_date'] = filename_filedate_list_of_lists['file_date'][row_from_list]
    mydataframe['file_date'] = pd.to_datetime(mydataframe['file_date'])
    return mydataframe
###############################################################################################################

