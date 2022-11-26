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

    return all_original_metadata, active_files

## -- BASED ON EXPLICIT OVERRIDES & BASELINE CRITERIA, DETERMINE VARIABLE INCLUSION -- ##

def determine_variable_inclusion(extracted_metadata, explicit_overrides):

    all_unique_variables = []
    all_column_names_dict = extracted_metadata[0]

    for survey in extracted_metadata['column_names']:

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

    return all_unique_variables, always_retain, always_remove, all_variable_instances

## -- ORGANIZE METADATA OF VARIOUS TYPES INTO A DICTIONARY KEYED BY VARIABLE -- ##

def organize_metadata_by_var(extracted_metadata, variable_inclusion):

    all_unique_variables = variable_inclusion[0]
    all_original_metadata = extracted_metadata[0]

    #GROUP METADATA INTO DICTIONARIES KEYED BY COLUMN NAME

    column_names_to_labels_cleaned = {}
    variable_value_labels_cleaned = {}
    missing_ranges_cleaned = {}
    variable_display_width_cleaned = {}
    variable_measure_cleaned = {}

    inconsistent_column_names_to_labels = {}
    inconsistent_variable_value_labels = {}
    inconsistent_missing_ranges = {}
    inconsistent_variable_display_width = {}
    inconsistent_variable_measures = {}

    key_metadata_types = {'column_names_to_labels': [column_names_to_labels_cleaned, inconsistent_column_names_to_labels],
    'variable_value_labels': [variable_value_labels_cleaned, inconsistent_variable_value_labels],
    'missing_ranges': [missing_ranges_cleaned, inconsistent_missing_ranges],'variable_display_width': [variable_display_width_cleaned, inconsistent_variable_display_width], 'variable_measure': [variable_measure_cleaned, inconsistent_variable_measures]}

    for type in key_metadata_types: #Cycles through metadata type

        full_file_set = all_original_metadata[type]

        for file in full_file_set: #Cycles through files

            instance = full_file_set[file]

            for colname in instance: #Cycles through individual variable (column) names within each file

                if colname in all_unique_variables: #If the variable (column) name appears in active unique variables, the corresponding metadata is appended to a cleaned dictionary

                    cleaned_dict = key_metadata_types[type][0]

                    if colname not in cleaned_dict: 

                        cleaned_dict[colname] = [instance[colname]]
                    
                    elif colname in cleaned_dict:

                        cleaned_dict[colname].append(instance[colname])    

    return key_metadata_types

## -- DETECT INCONSISTENCIES BETWEEN FILES IN SPECIFIC VARIABLES; STAGE FOR EXCLUSION -- ##

def find_inconsistent_variables(key_metadata_types):

    #FIND ACTIVE VARIABLES WITH INCONSISTENT METADATA

    inconsistent_variables = []

    #Code in function below (doesn't run by default) detects variables with a change in any type of metadata (col labels, var-val labels, var measures, var widths)

    def detect_all_inconsistencies(key_metadata_types):

        for type in key_metadata_types:

            cleaned_metadata_dict = key_metadata_types[type][0]

            for variable in cleaned_metadata_dict:

                variable_instances = cleaned_metadata_dict[variable]

                if len(variable_instances) > 0:

                    comparison_result = all(ele == variable_instances[0] for ele in variable_instances)

                    if comparison_result == False:

                        inconsistent_variables.append(variable)

        return inconsistent_variables

    #Detects column label inconsistencies only; runs by default for awareness, but doesn't actually exclude variables by default
    
    def detect_col_label_inconsistencies(key_metadata_types):

        inconsistent_column_labels = []

        for type in key_metadata_types:

            cleaned_metadata_dict = key_metadata_types['column_names_to_labels'][0]

            for variable in cleaned_metadata_dict:

                variable_instances = cleaned_metadata_dict[variable]

                variable_instances = set(variable_instances)

                if len(variable_instances) > 1:

                    inconsistent_column_labels.append(variable)

        return inconsistent_column_labels

    inconsistent_column_labels = detect_col_label_inconsistencies(key_metadata_types)

    #Code in function below (runs by default) detects inconsistencies in var-val-labels ONLY, which is the most consequential type of metadata inconsistency

    def detect_critical_inconsistencies(key_metadata_types):

        var_val_dict = key_metadata_types['variable_value_labels'][0]

        for variable in var_val_dict:

            val_instances = var_val_dict[variable]

            if len(val_instances) > 0:

                comparison_result = all(ele == val_instances[0] for ele in val_instances)

                if comparison_result == False:

                    inconsistent_variables.append(variable)

        return inconsistent_variables

    inconsistent_variables = detect_critical_inconsistencies(key_metadata_types)

    #Find inconsistent variables that will indeed be included (those that are force-included)

    inconsistent_variables = set(inconsistent_variables)

    print("Variables with inconsistent metadata: " + str(len(inconsistent_variables)))

    for i in inconsistent_variables:

        print(i)

    included_inconsistent_variables = []

    for var in always_retain:

        if var in inconsistent_variables:

            included_inconsistent_variables.append(var)

    print("WARNING - the following variables with inconsistent metadata are included in the dataframe: " + str(included_inconsistent_variables) + ".  Be sure to over-write metadata entries for each of these variables at the end of the script if needed." )

    return inconsistent_variables, inconsistent_column_labels

## -- CONSTRUCT CSV FOR ACTIVE, CONSISTENT VARIABLES -- ##

def construct_csv(extracted_metadata, inconsistencies, variable_inclusion):

    active_files = extracted_metadata[1]
    all_variable_instances = variable_inclusion[3]
    inconsistent_variables = inconsistencies[0]

    #Exctract CSV Data from SAV files

    all_extracted_csv_files = []

    for file in active_files:

        df, meta = pyreadstat.read_sav('temp/' + str(file))

        all_extracted_csv_files.append(df)

    #Drop columns from data frame based on a variety of criteria

    for file in all_extracted_csv_files:

        for col in file.columns:

            if col in inconsistent_variables and col not in always_retain: #inconsistent metadata

                file.drop(col, axis=1, inplace=True)

            if all_variable_instances[col] == 1: #column only appears in one file (lower threshold to 0 if needed)

                try:

                    file.drop(col, axis=1, inplace=True)

                except:

                    pass

            if col in always_remove:
                
                try:

                    file.drop(col, axis=1, inplace=True)

                except:

                    pass

    #Append 'wave' column to enable filtering/display over time

    wave_counter = 0

    for file in all_extracted_csv_files:

        file['wave'] = active_files[wave_counter]

        wave_counter += 1
        
    #Concatenate trimmed dataframes into one

    full_dataframe = pd.concat(all_extracted_csv_files, keys = active_files)

    #Move supplementary/calculated columns and columns with inconsistent metadata that were force-included to the end of the dataframe

    wave_column = full_dataframe.pop('wave')
    full_dataframe.insert(len(full_dataframe.columns), 'wave', wave_column)

    return full_dataframe

## -- CREATE MERGED SPSS FILE -- ##

def create_spss_file(full_dataframe, key_metadata_types, inconsistencies):

    inconsistent_column_labels = inconsistencies[1]
    column_names_to_labels_cleaned = key_metadata_types[0][1]

    #Create placeholders for final metadata

    final_col_labels = []
    final_col_labels_key = []
    final_var_val_labels = {}
    final_var_widths = {}
    final_var_measures = {}

    #Fill variables with metadata entries based on columns in final dataframe

    for var in full_dataframe.columns: 

        try:
            
            final_col_labels.append(key_metadata_types['column_names_to_labels'][0][var][0]) #Looks in the cleaned metadata dictionary by type, then by variable, then by position within the corresponding values for that variable (first instance by default).  Appends this value to the final_column_label list for each active variable.

            final_col_labels_key.append(var)

            #Not every variable name has a corresponding dictionary entry for the metadata types below.  For this reason, these steps are nested in a "try"/"except" sequence.

            try:
                
                final_var_val_labels[var] = key_metadata_types['variable_value_labels'][0][var][0]

                final_var_measures[var] = key_metadata_types['variable_measure'][0][var][0]

                final_var_widths[var] = key_metadata_types['variable_display_width'][0][var][0]

            except:

                pass
        
        except:

            pass

    # -- TEST FOR KEY/LABEL MATCHING -- #

    zipped_labels = zip(final_col_labels_key, final_col_labels)

    #Tests if proper label is associated with the proper variable

    for (var, label) in zipped_labels:

        try:

            if label != column_names_to_labels_cleaned[var][0] and var not in inconsistent_column_labels:

                print("ADVISORY: There is a potential mismatch between the column label and associated key.  This can also be caused by including variables with inconsistent metadata.  Double check before using this dataset.  This advisory was flagged at the following variable: " + var)

                print(column_names_to_labels_cleaned[var][0])

        except: 

            print(var + " was assigned '" + label + "' as a column label manually.  Not present in original dataset.")

    counter = 0

    #Tests if labels are ordered properly (must be identical or order of the variables in the dataframe)

    for (var, label) in zipped_labels:

        if var != full_dataframe.columns[counter]:

            print("WARNING: There is a mismatch between the dataframe's column order and the order of columns in the final zipped column key-label pairs.  Double check for extraneous or misplaced columns. This error was flagged at the following variable: " + var)

        counter += 1

    full_dataframe = full_dataframe.reindex(columns=final_col_labels_key)

    print("FULL DATAFRAME: " + str(full_dataframe.shape))

    #WRITE FINAL CSV

    full_dataframe.to_csv('FinalDataFrame.csv') #CSV must be written locally and read back in (final_csv variable) to be written into SPSS file; not 100% sure why this is the case but it's been necessary for the pyreadstat command to work properly and yield a DisplayR compatible file

    #Produce SPSS file

    path = 'FinalSPSSFile.sav'

    final_csv = pd.read_csv('FinalCSVDataFrameCopy.csv')

    try:

        final_csv.drop('Unnamed: 0', axis=1, inplace=True)
        final_csv.drop('Unnamed: 1', axis=1, inplace=True)

    except: 

        pass

    print("Final dataframe shape: " + str(final_csv.shape))
    print("Number of column labels: " + str(len(final_col_labels)))

    final_spss_file = pyreadstat.write_sav(final_csv, path, column_labels=final_col_labels, variable_value_labels=final_var_val_labels, variable_measure=final_var_measures, variable_display_width=final_var_widths)

    return final_spss_file

## -- POST MERGED SPSS FILE TO BOX -- ##

def post_to_box(box_client):

    folder_id = 'FOLDER_ID_HERE'
    sav_file_id = 'SPSS_FILE_ID_HERE'
    csv_file_id = 'CSV_FILE_ID_HERE'

    existing_files = box_client.folder(folder_id = folder_id).get_items()

    file_names = []

    updated_sav_file = box_client.file(sav_file_id).update_contents('FinalSPSSFile.sav')
    print(f'{updated_sav_file.name} has been updated with a new version.')

    updated_csv_file = box_client.file(csv_file_id).update_contents('FinalCSVDataFrameCopy.csv')
    print(f'{updated_csv_file.name} has been updated with a new version.')

    shutil.rmtree('temp')
    os.remove('team_comments.xlsx')
    os.remove('comments.xlsx')

## -- FUNCTION CALLS -- ##

box_client = establish_box_connection()
download_spss_files(box_client)
all_original_spss_files = determine_import_list()
explicit_overrides = download_explicit_overrides(box_client)
mongo_client = connect_to_mongo()

extracted_metadata = extract_metadata(all_original_spss_files)
variable_inclusion = determine_variable_inclusion(extracted_metadata, explicit_overrides)
key_metadata_types = organize_metadata_by_var(extracted_metadata, variable_inclusion)
inconsistencies = find_inconsistent_variables(key_metadata_types)

full_dataframe = construct_csv(extracted_metadata, inconsistencies, variable_inclusion)
final_spss_file = create_spss_file(full_dataframe, key_metadata_types, inconsistencies)
post_to_box(box_client)
