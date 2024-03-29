"""
Build the final cleaned and concatenated TRACE dataset. Loads all necessary functions specified in 
the data management files to merge the reading-in and data cleeaning steps so as to produce the 
final dataset. Define all final dataset specifications in the dataset_specs dictionary.
""" 

import gc
import time
import pandas as pd
pd.options.mode.chained_assignment = None
# Initialize the timer
t0 = time.time()
import numpy as np
import os
from pathlib import Path

# Import function to read in daily raw data and store on a yearly level
from read_TRACE import read_TRACE_all
# Import function to concatenate yearly data and merge the rating, issue and bond info data to the 
# transaction data
from concatenate_merge_TRACE_MERGENT import conct_merge_data
# Import function to read in the bond background information
from read_bond_background_TRACE import get_unique_bond_info
# Import the inter-dealer transaction and agency trade filter according to 
# Dick-Nielsen & Poulsen (2019)
from clean_TRACE import del_interd_transact
# Import the cleaning function as specified in Bessembinder et al. (2018)
from clean_TRACE import clean_trade_level
# Import further specific cleaning steps
from clean_TRACE import clean_df_general
# Import the cleaning steps for individual trading dates
from clean_TRACE import add_clean_trading_dates
# Import function to read in all available reported dates iN TRACE
from read_TRACE import get_all_rpt_dates
# Import function to read generate the later on required variables
from prepare_variables import create_necessary_vars
# Import the function to generate all necessary event time variables
from prepare_variables import define_event_time_week
# Import the function to merge the rating data to the TRACE transaction data
from concatenate_merge_TRACE_MERGENT import merge_transact_rating
# Import the function to merge the issue data
from prepare_variables import merge_issue_info
# Import the function to assign the regulatory period indicator and map the ratings to the Basl 
# risk weights
from prepare_variables import map_risk_weight_reg_period
# Import the function to construct the necessary input and output folders
from general_functions import construct_nec_folders

######
# 0) Set up project specifications
## 0.1) Define the project root path
## 0.2) Define the necessary file paths and input/output folders
## 0.2) Define the model specifications
## 0.3) Set up file paths to check it read-in steps can be skipped or not
######
# Project root path
project_path = str(Path(os.getcwd()).parent)

# Construct the necessary input/output folders
construct_nec_folders(project_path)

# Model specifications
dataset_specs = {
    # Specify the sample time span of the dataset
    'sample_time_span': [2007, 2018],
    # Specify the variables to keep from the ratings data
    'ratings': {
        'varlist': ['complete_cusip', 'rating_date', 'rating']
    },
    # Specify the variables to keep from the transaction (raw TRACE) data.
    'transactions': {
        'varlist': ['CUSIP_ID', 'RPTG_PARTY_ID','RPT_SIDE_CD','BUY_CPCTY_CD','SELL_CPCTY_CD',
                   'CNTRA_PARTY_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 'TRD_RPT_DT', 
                   'TRD_EXCTN_DT', 'TRD_EXCTN_TM',  'CMSN_TRD', 'TRDG_MKT_CD']
    },
    # Specify the variables to keep from the issue data
    'issue_data': {
        'varlist': ['CUSIP_ID', 'maturity', 'offering_amt', 'offering_price', 'principal_amt', 
                    'bond_type', 'amount_outstanding', 'effective_date', 'putable', 'offering_date', 
                    'active_issue']
    },
    # Specify the variables to keep from the bond background information data
    'bond_info': {
        'varlist': ['CUSIP_ID', 'RULE_144A_FL']
    },
    # Specify the variables to be kept in the final DataFrame of the cleaning step 
    # (All variables here need to be in the union of the variables specified above)
    'dataset_clean': {
        'varlist': ['CUSIP_ID', 'RPTG_PARTY_ID', 'CNTRA_PARTY_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 
                    'RPT_SIDE_CD', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM', 'CMSN_TRD', 'TRDG_MKT_CD', 
                    'BUY_CPCTY_CD', 'SELL_CPCTY_CD', 'maturity', 'rating_date', 'rating', 
                    'rating_numeric', 'principal_amt', 'offering_date', 'offering_amt', 
                    'offering_price', 'amount_outstanding', 'effective_date', 'active_issue']
    }
}

# 0.3) Set up file paths to automatically check if specific steps have already been executed and 
# hence can be skiped
TRACE_first_df = f"TRACE_clean_{dataset_specs['sample_time_span'][0]}"
TRACE_last_df = f"TRACE_clean_{dataset_specs['sample_time_span'][1]}"
path_TRACE_raw_clean_first = (
    f"{os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))}/bld/data/TRACE/TRACE_raw_clean/{TRACE_first_df}"
)
path_TRACE_raw_clean_last = (
    f"{os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))}/bld/data/TRACE/TRACE_raw_clean/{TRACE_last_df}"
)
path_TRACE_bond_info = (
    f"{os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))}/bld/data/TRACE/TRACE_raw_clean/bond_info.pkl"
)
df_all_rpt_dates = (
    f"{os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))}/bld/data/TRACE/TRACE_raw_clean/TRACE_info/TRACE_rpt_dates.pkl"
)

print("START TO READ AND CLEAN THE ACADEMIC TRACE DATA FOR THE TIME RANGE: {} to {}".
    format(dataset_specs['sample_time_span'][0], dataset_specs['sample_time_span'][1]))

# 0.4) Load the required versions of the papers
os.system('pip install -r  requirements.txt')

######
# 1) Read in the daily transaction data as well as the data bond background information
    # 1.1) Read in daily raw data and save on yearly basis (leave commented out if already done)
    # 1.2) Read in the daily bond background characteristics
    # 1.3) Read in the list of all reported dates in TRACE
######

# 1.1) Read in the daily raw transaction data
if ((os.path.isfile(path_TRACE_raw_clean_first)) & (os.path.isfile(path_TRACE_raw_clean_last))):
    print("")
    print("STEP 1.1: Raw Trace data is already read, cleaned and saved. Proceed with next step")
elif not os.path.isfile(path_TRACE_raw_clean_first):
    print("")
    print("STEP 1.1: Start reading and concatenating the raw TRACE data")
    read_TRACE_all(project_path, dataset_specs)
    print("STEP 1.1: Finished reading and concatenating the raw TRACE data")
    print("")
    print("STEP 1.2: Start reading in the bond background characteristics")
    get_unique_bond_info(project_path, dataset_specs)
    print("STEP 1.2: Finished reading in the bond background characteristics")
    print("")
    print("STEP 1.3: Start reading in the list of all reported dates in TRACE")
    get_all_rpt_dates(project_path)
    print("STEP 1.3: Finished reading in the list of all reported dates in TRACE")

    ######
    # 2) Implement the remaining cleaning steps:
    # 2.0) Read in the concatenated dataset
    # 2.1) Clean the agency trades and delete one side of the inter-dealer trades
    # 2.2) Implement the cleaning steps as in Bessembinder et al. (2018)
    # 2.3) Implement some further general data cleaning steps
    ######

    # 2.0) Read in the concatenated data from step 1)
    df_merged = conct_merge_data(project_path, dataset_specs)
    # Define a selection variable based on which the inter-dealer transactions are deleted.
    df_merged['I_drop'] = np.arange(0, len(df_merged))
    # Select only the necessary variables
    df_merged_red = df_merged[['CUSIP_ID', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM', 'ENTRD_VOL_QT', 'RPTD_PR', 'RPTG_PARTY_ID',
                               'CNTRA_PARTY_ID', 'RPT_SIDE_CD', 'I_drop']].copy()


    # 2.1) Clean the agency trades and delete one side of the inter-dealer trades
    # NOTE: This applies the cleaning step proposed in Dick-Nielsen & Poulsen (2019) for the agency trades and the
    # inter-dealer trades. However, this step is not necessary and has to be explicitly motivated.
    # Motivation: I decide to exclude:
    # a) Double-counted inter-dealer trades. If we leave them in, every D-D trade has two entries, one from the buying and
    # one from the selling dealer. However, there is only one trade. By cancelling one of these two reports no information
    # is lost and there is no risk of double-counting
    # b) Agency trades without commission: If we leave them in we would
    # essentially see some  agency trades that seem to be very cheap. However, Dick-Nielsen (2014) points out that there
    # are some unreported costs (e.g. fees) in the background. -> Currently agency trades are NOT excluded
    list_keep = del_interd_transact(df_merged_red)
    df_merged_cleaned_1 = df_merged.loc[df_merged['I_drop'].isin(list_keep)]
    del [df_merged, df_merged_red]
    gc.collect()
    df_merged_cleaned_1 = df_merged_cleaned_1[df_merged_cleaned_1.columns.drop(['I_drop'])]


    # 2.2) Implement the cleaning steps as in Bessembinder et al. (2018) and Anand et al. (2021)
    df_merged_cleaned_2 = clean_trade_level(df_merged_cleaned_1)
    del [df_merged_cleaned_1]
    gc.collect()

    # 2.3) Implement the general data cleaning steps
    df_merged_cleaned_3 = clean_df_general(df_merged_cleaned_2, dataset_specs)
    del [df_merged_cleaned_2]
    gc.collect()

    # 2.4) Implement the cleaning steps for TRACE holidays and non-week days
    df_merged_cleaned_4 = add_clean_trading_dates(df_merged_cleaned_3, project_path)
    del [df_merged_cleaned_3]
    gc.collect()

    # 3.1) Add additional necessary variables
    df_merged_cleaned_5_1 = create_necessary_vars(df_merged_cleaned_4)
    del [df_merged_cleaned_4]

    # 3.2) Add the necessary event time variables
    df_merged_cleaned_5_2 = define_event_time_week(df_merged_cleaned_5_1)
    del [df_merged_cleaned_5_1]
    gc.collect()

    ######
    # 4) Save the final concatenated and cleaned dataset in pickle format
    ######
    print('Saving the DataFrame has started')
    df_merged_cleaned_5_2.to_pickle(project_path + '/bld/data/TRACE/TRACE_final_clean/TRACE_final.pkl')


    # Stop the time
    t1 = time.time()
    print("ALL FINISHED! The total data generation part took {}".format(t1-t0))


