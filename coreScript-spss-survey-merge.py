from boxsdk import DevelopmentClient
from boxsdk import JWTAuth, Client
import certifi
from cmath import nanj
import numpy as np
import os
import pandas as pd
import pymongo
import pyreadstat
import shutil

#If 'Temp' folder already exists, it should be deleted to avoid a subsequent error when the mkdir function is called.  This usually only occurs if an error was thrown before the script completed.  

try: 

    shutil.rmtree('temp')

except:

    pass

### -- DECLARE GLOBAL VARIABLES -- ###

parent_file = 'PARENT_FILE'
retain_specific_files = []
always_retain = [] #Add variables manually if need be
always_remove = [] #Add variables manually if need be



#Print important information
print("Current working directory: " + str(os.getcwd()))


### -- ESTABLISH CONNECTION TO BOX -- ###

def establish_box_connection():

    auth = JWTAuth.from_settings_file(r'box_json.json') #file is hidden from public repository
    box_client = Client(auth)

    service_account = box_client.user().get()

    print(f"Connected to: {service_account}")

    return box_client

#Download SPSS files from Box to Local Directory

def download_spss_files(box_client):

    folder_id = 'DIRECTORY_ID'

    directory_items = box_client.folder()

    os.mkdir('temp')

    for item in directory_items:

        file_name = 'temp/' + str(file_name)

        file_id = str(item.id)

        with open(file_name, 'wb') as open_file:

            box_client.file(file_id).download_to(open_file)

            open_file.close()

#Establish & properly order list of files in 'temp' directory
            
def determine_import_list():

    directory_files = os.listdir('temp')

    all_original_spss_files = [item for item in directory_files if item.endswith('.sav')]

    all_original_spss_files.insert(0, all_original_spss_files.pop(all_original_spss_files.index(parent_file))) #Moves the parent file to the top of the sequence, so it will serve as the reference file in any overridden metadata conflicts

    return all_original_spss_files

### -- IMPORT BOX SHEET WITH EXPLICIT VARIABLE INCLUSION/EXCLUSION MANUAL OVERRIDES -- ##

def download_explicit_overrides(box_client):
        
    file_id = 'FILE_ID'

    file = box_client.file(file_id)

    with open('explicit_overrides.xlsx', 'wb') as open_file:

        box_client.file('FILE_ID').download_to(open_file)

        open_file.close()

    explicit_overrides = pd.read_excel('explicit_overrides.xlsx')

    return explicit_overrides

### -- IMPORT DATA FROM MONGODB FOR RECLASSIFICATION SYSTEM -- ##
    
def connect_to_mongo():

    ca = certifi.where()

    client = pymongo.MongoClient('MONGO_CLIENT', tlsCAFile=ca) #Client credentials are hidden from public repository

    db = client['SampleSPSS-MappingAndCorrections']

    # ** add additional database connections here as they become apparent ***

## -- EXTRACT AND CATALOG METADATA FROM EACH SPSS FILE -- ##    

def extract_metadata(all_original_spss_files):

    #Generate empty dictionaries to house extracted metadata
    all_column_names_dict = {}
    all_column_labels_dict = {}
    all_column_names_to_labels_dict = {}
    variable_value_labels_dict = {}
    variable_measure_dict = {}
    variable_display_width_dict = {}
    value_labels_dict = {}
    missing_ranges_dict = {}
    variable_types_dict = {}

    #Generate lists of all metadata types/storage vehicles to enable efficient looping later in function
    all_metadata_types = ['column_names', 'column_labels', 'column_names_to_labels', 'variable_value_labels', 'variable_measure', 'variable_display_width', 'value_labels', 'missing_ranges', 'variable_types']

    all_metadata_dicts = [all_column_names_dict, all_column_labels_dict, all_column_names_to_labels_dict, variable_value_labels_dict, variable_measure_dict, variable_display_width_dict, value_labels_dict, missing_ranges_dict, variable_types_dict]

    #Determine which files should be merged - leaving "retain_particular_files" blank will merge all files in the directory

    if len(retain_specific_files) > 1:

        active_files = [x for x in all_original_spss_files if x in retain_specific_files]

    elif len(retain_specific_files) == 0:

        active_files = all_original_spss_files

    print("Merging " + str(len(active_files)) + " files.")

    #Extract metadata using the pyreadstat package
    for file in all_original_spss_files:

        df, meta = pyreadstat.read_sav('temp/' + str(file))

        #Extract each piece of metadata from the SAV file

        column_names = meta.column_names
        column_labels = meta.column_labels
        column_names_to_labels = meta.column_names_to_labels

        variable_value_labels = meta.variable_value_labels
        variable_measure = meta.variable_measure
        variable_display_width = meta.variable_display_width

        value_labels = meta.value_labels
        missing_ranges = meta.missing_ranges
        variable_types = meta.original_variable_types

        #Store each set of metadata as a dictionary entry, KEYED by file

        all_column_names_dict[file] = column_names
        all_column_labels_dict[file] = column_labels
        all_column_names_to_labels_dict[file] = column_names_to_labels

        variable_value_labels_dict[file] = variable_value_labels
        variable_measure_dict[file] = variable_measure
        variable_display_width_dict[file] = variable_display_width

        value_labels_dict[file] = value_labels
        missing_ranges_dict[file] = missing_ranges
        variable_types_dict[file] = variable_types

    #Create dictionary housing all metadata, KEYED by type, SUB-KEYED by file

    all_original_metadata = {all_metadata_types[i]: all_metadata_dicts[i] for i in range(len(all_metadata_dicts))}

    return all_original_metadata

## -- BASED ON EXPLICIT OVERRIDES & BASELINE CRITERIA, DETERMINE VARIABLE INCLUSION -- ##

def determine_variable_inclusion(all_original_metadata, explicit_overrides):

    all_unique_variables = []
    all_column_names_dict = all_original_metadata[0]

    for survey in all_original_metadata['column_names']:

        for colname in all_column_names_dict[survey]:

            if colname not in all_unique_variables:

                all_unique_variables.append(colname)

    active_variables = all_unique_variables

    #Count how frequently each variable appears

    all_variable_instances = {}

    for survey in all_column_names_dict:

        for colname in all_column_names_dict[survey]:

            if colname not in all_variable_instances:

                all_variable_instances[colname] = 1

            elif colname in all_variable_instances:

                all_variable_instances[colname] += 1

    # Append variables to always_retain / always_remove lists from explicit overrides Google Sheet

    #Force-include

    filtered_comments_include = explicit_overrides.loc[explicit_overrides['Force-Include / Force-Exclude'] == 'FORCE-INCLUDE', 'Variable']

    always_retain = [var for var in filtered_comments_include if var not in always_retain]

    #Force-exclude

    filtered_comments_exclude = explicit_overrides.loc[explicit_overrides['Force-Include / Force-Exclude'] == 'FORCE EXCLUDE', 'Variable']

    always_remove = [var for var in filtered_comments_exclude if var not in always_remove]


