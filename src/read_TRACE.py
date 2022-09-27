"""Read in the daily raw Academic TRACE data and apply the corrections as described in Dick-Nielsen & Poulsen (2019).
The steps are as follows:
    Step 1:     Read in the raw datasets. Due to the TRACE reporting change on 06.02.2012 this step needs to be separate for
                the pre- and post 06.02.2012 period.
    Step 2:     Adjust the date formats. In particular, the date variables need to be in the correct date format.
                The trading time needs to be in the correct date-time format.
    Step 3:     Select the bonds that are to be kept in the dataset. I follow Bessembinder et al. (2018) in keeping only
                bonds specified as U.S. Corporate Debentures and U.S. Corporate Bank Notes. Bond specifications come
                from the MERGENT FISD database.
    Step 4:     Automatically source the directories and folder structures of the raw input data. Store the data on an
                annual basis. The latter is necessary given the large size of the dataset. This is done separately
                prior and post the reporting change. Also implement the cleaning steps by Dick-Nielsen & Poulsen (2019)
                directly at this stage
    Step 5:     Loop over all years and concatenate the daily dataset and store data on a yearly level
    
"""

# Read in the necessary packages
import pandas as pd
pd.options.mode.chained_assignment = None
import gc
import os
import pickle as pickle
from joblib import Parallel, delayed
import tqdm

# Import the cleaning steps according to Dick-Nielsen & Poulsen (2019) from the sheet clean_TRACE.py
# (pre 2012)
from clean_TRACE import prior_2012_clean
# (post 2012)
from clean_TRACE import post_2012_clean


########
# Step 1
########
def read_in_adj_dtyp_pre_2012(in_path):
    """Read in the daily raw transaction data and adjust the data types for the period PRIOR to 06.02.2012.
    On 06.02.2012, FINRA changed the reporting standards which requires a different data formatting.

    Args:
    --------
    in_path (str): Path specification of the daily raw dataset

    Returns:
    --------
    df (pd.DataFrame): Daily raw transactions with adjusted data types
    """

    # Read in the metadata of the folder containing the data. Important to read in as str variables
    # to preserve the leading 0 in the date structures.
    df = pd.read_csv(in_path, sep="|", engine='python', dtype={'TRD_EXCTN_DT': str, 'EXCTN_TM': str,
                                                               'TRD_RPT_DT': str, 'TRD_RPT_TM': str,
                                                               'TRD_STLMT_DT': str})
    # Drop the last two rows as they only contain FINRA identifier information
    df = df.iloc[:-2]
    # Define the required data types
    dtypes = {
        'REC_CT_NB': float,
        'TRC_ST': str,
        'BOND_SYM_ID': str,
        'CUSIP_ID': str,
        'SCRTY_TYPE_CD': str,
        'WIS_CD': str,
        'CMSN_TRD_FL': str,
        'ENTRD_VOL_QT': float,
        'RPTD_PR': float,
        'YLD_SIGN_CD': str,
        'YLD_PT': float,
        'ASOF_CD': str,
        'TRD_EXCTN_DT': str,
        'EXCTN_TM': str,
        'TRD_RPT_DT': str,
        'TRD_RPT_TM': str,
        'TRD_STLMT_DT': str,
        'SALE_CNDTN_CD': str,
        'SALE_CNDTN2_CD': str,
        'RPT_SIDE_CD': str,
        'BUY_CMSN_RT': float,
        'BUY_CPCTY_CD': str,
        'SELL_CMSN_RT': float,
        'SELL_CPCTY_CD': str,
        'AGU_TRD_ID': str,
        'SPCL_PR_FL': str,
        'TRDG_MKT_CD': str,
        'DISSEM_FL': str,
        'PREV_REC_CT_NB': float
    }
    # Convert the variable types
    df = df.astype(dtypes)

    return df


def read_in_adj_dtyp_post_2012(in_path):
    """Read in the daily raw transaction data and adjust the data types for the period AFTER to 06.02.2012.
    On 06.02.2012, FINRA changed the reporting which requires a different data formatting.

    Args:
    --------
    in_path (str): Path specification of the daily raw dataset

    Returns:
    --------
    df (pd.DataFrame): Daily raw transactions with adjusted data types

    """

    # Read in the metadata of the folder. Important to read in as str variables
    # to preserve the leading 0 in the date structures.
    df = pd.read_csv(in_path, sep="|", engine='python', dtype={'TRD_EXCTN_DT': str, 'TRD_EXCTN_TM': str,
                                                               'TRD_RPT_DT': str, 'TRD_RPT_TM': str,
                                                               'TRD_STLMT_DT': str})
    # Drop the last two rows as they only contain FINRA identifier information
    df = df.iloc[:-2]
    # Define the required data types
    dtypes = {
        'REC_CT_NB': float,
        'TRD_ST_CD': str,
        'ISSUE_SYM_ID': str,
        'CUSIP_ID': str,
        'PRDCT_SBTP_CD': str,
        'WIS_DSTRD_CD': str,
        'NO_RMNRN_CD': str,
        'ENTRD_VOL_QT': float,
        'RPTD_PR': float,
        'YLD_DRCTN_CD': str,
        'CALCD_YLD_PT': float,
        'ASOF_CD': str,
        'TRD_EXCTN_DT': str,
        'TRD_EXCTN_TM': str,
        'TRD_RPT_DT': str,
        'TRD_RPT_TM': str,
        'TRD_STLMT_DT': str,
        'TRD_MDFR_LATE_CD': str,
        'RPT_SIDE_CD': str,
        'BUYER_CMSN_AMT': float,
        'BUYER_CPCTY_CD': str,
        'SLLR_CMSN_AMT': float,
        'SLLR_CPCTY_CD': str,
        'LCKD_IN_FL': str,
        'TRDG_MKT_CD': str,
        'PBLSH_FL': str,
        'SYSTM_CNTRL_DT': str,
        'SYSTM_CNTRL_NB': str,
        'PREV_TRD_CNTRL_DT': str,
        'PREV_TRD_CNTRL_NB': str,
        'FIRST_TRD_CNTRL_DT': str,
        'FIRST_TRD_CNTRL_NB': float
    }
    # Convert the variable types
    df = df.astype(dtypes)

    return df


########
# Step 2
########
def format_ex_tm_dt(df, date_var):
    """Construct the reporting and execution dates and time in the correct dateformat.
    For the reporting/execution date the variables are in the format YY-MM-DD. For the
    exact transaction time, the variables are in the format YY-MM-DD-HH-MM-SS. This function
    is the same for both prior and post 2012 data.

    Args:
    --------
    df (pd.DataFrame): DataFrame with the raw transaction data
    date_var (np.array): Array containing both the date and the time

    Returns:
    --------
    df (pd.DataFrame): DataFrame where execution time and date is in the correct date format

    """

    # Indicator for whether  EXCTN_TM = 0. Necessary as sometimes execution time is 0 instead of a time.
    df['tmp_{}'.format(date_var[0])] = (df['{}'.format(date_var[0])] == 0) * 1

    # Generate trade execution time for trades that do NOT report '0' for trade execution time
    # Append the date variable (YYYY-MM-DD) and the trade time (HH-MM-SS)
    df.loc[df['tmp_{}'.format(date_var[0])] == 0, '{}'.format(date_var[0])] = (
            df.loc[df['tmp_{}'.format(date_var[0])] == 0, '{}'.format(date_var[1])].astype(str) +
            df.loc[df['tmp_{}'.format(date_var[0])] == 0, '{}'.format(date_var[0])].astype(str)
    )
    # Reconvert the string to float format
    df['{}'.format(date_var[0])] = df['{}'.format(format(date_var[0]))].astype(float)
    # Bring the float variable to datetime format (YYYY-MM-DD-HH-MM-SS)
    df.loc[df['tmp_{}'.format(date_var[0])] == 0, '{}'.format(date_var[0])] = (
        pd.to_datetime(df.loc[df['tmp_{}'.format(date_var[0])] == 0, '{}'.format(date_var[0])], format='%Y%m%d%H%M%S')
    )
    # Drop the temp variable
    df = df.drop(columns=['tmp_{}'.format(date_var[0])])
    # Convert the trading day variables in date format
    df['{}'.format(date_var[1])] = pd.to_datetime(df['{}'.format(date_var[1])], format='%Y%m%d').dt.date

    return df


def adj_dt_format_pre_2012(in_path):
    """Read in the daily raw data prior to 06.02.2012 using the function read_in_adj_dtyp(),
    adjust the date format of the date variables and return the daily cleaned TRACE DataFrame.

    Args:
    --------
    in_path (str): Specify the input path to the raw data file

    Returns:
    --------
    df (pd.DataFrame): Adjusted DataFrame (prior 06.02.2012)

    """

    # Read in the dataset using read_in_adj_dtyp()
    df = read_in_adj_dtyp_pre_2012(in_path)

    # Sometimes there are typos which make the date too large (e.g. 30140101 instead of 20140101). This
    # is excluded by the code below. 20810401 is the maximal number possible
    date_vars = ['TRD_EXCTN_DT', 'TRD_RPT_DT', 'TRD_STLMT_DT']
    for dv in date_vars:
        df = df[(df[dv].astype(float) > 20200101) == False]

    # Define the dictionary with the variables that are to be converted in the correct date format.
    date_dict = {
        'execution_date': ['EXCTN_TM', 'TRD_EXCTN_DT'],
        'reporting_date': ['TRD_RPT_TM', 'TRD_RPT_DT']
    }
    # Loop over both the reporting and execution date using format_ex_tm_dt()
    for v in ['execution_date', 'reporting_date']:
        df = format_ex_tm_dt(df, date_dict['{}'.format(v)])
    # Convert the settlement date (only date no time available)
    df['TRD_STLMT_DT'] = pd.to_datetime(df['TRD_STLMT_DT'], format='%Y%m%d').dt.date

    return df


def adj_dt_format_post_2012(in_path):
    """Read in the daily raw data prior to 06.02.2012, adjust the date format of the date variables and
    return the daily cleaned TRACE DataFrame.

    Args:
    --------
    in_path (str): Specify the input path to the raw data file

    Returns:
    --------
    df (pd.DataFrame): Adjusted DataFrame (post 06.02.2012)

    """

    # Read in the dataset using read_in_adj_dtyp()
    df = read_in_adj_dtyp_post_2012(in_path)

    # It was noted that sometimes there are typos in the raw data which make the date too large
    # (e.g. 30140101 instead of 20140101). This is excluded by the code below:
    date_vars = ['TRD_EXCTN_DT', 'TRD_RPT_DT', 'TRD_STLMT_DT', 'SYSTM_CNTRL_DT', 'PREV_TRD_CNTRL_DT',
                 'FIRST_TRD_CNTRL_DT']
    for dv in date_vars:
        df = df[(df[dv].astype(float) > 20200101) == False]

    # Define the dictionary with the variables that are to be converted in the correct date format.
    date_dict = {
        'execution_date': ['TRD_EXCTN_TM', 'TRD_EXCTN_DT'],
        'reporting_date': ['TRD_RPT_TM', 'TRD_RPT_DT']
    }
    # Loop over both the reporting and execution date using format_ex_tm_dt()
    for v in ['execution_date', 'reporting_date']:
        df = format_ex_tm_dt(df, date_dict['{}'.format(v)])
    # Convert the settlement date (only date no time available)
    df['TRD_STLMT_DT'] = pd.to_datetime(df['TRD_STLMT_DT'], format='%Y%m%d').dt.date
    df['SYSTM_CNTRL_DT'] = pd.to_datetime(df['SYSTM_CNTRL_DT'], format='%Y%m%d').dt.date
    df['PREV_TRD_CNTRL_DT'] = pd.to_datetime(df['PREV_TRD_CNTRL_DT'], format='%Y%m%d').dt.date
    df['FIRST_TRD_CNTRL_DT'] = pd.to_datetime(df['FIRST_TRD_CNTRL_DT'], format='%Y%m%d').dt.date

    return df


########
# Step 3
########
def select_bonds(path):
    """"Select bonds based on characteristics according to Bessembinder et al. (2018). Only keep the bond transactions
    that fulfill the following criteria:
        i)   bond type is either U.S. Corporate Debenture or U.S. Corporate Bank Note
        ii)  bond is non-puttable
        iii) bond has a reported maturity

    Args:
    --------
    path (str): Specify the input path to the raw data file

    Returns:
    --------
    issue_data['CUSIP_ID'] (np.array): CUSIP IDs that are to be retained in the dataset

    """

    # Read in the Mergent issue data
    issue_data = pd.read_pickle(path + 'src/original_data/Mergent_FISD/' + 'issue_data.pkl')
    # Follow Bessembinder et al. (2018) in keeping only non-puttable U.S. Corporate Debentures and U.S.
    # Corporate Bank Notes (bond type = CDEB or USBN) with a reported maturity
    issue_data = issue_data.loc[issue_data.bond_type.isin(['CDEB', 'USBN'])]
    issue_data = (issue_data.loc[issue_data.putable == 'N']).dropna(subset=['maturity'])
    # Define the full CUSIP ID
    issue_data['CUSIP_ID'] = issue_data['issuer_cusip'] + issue_data['issue_cusip']

    # Only return the CUSIP-IDs that are to be maintained
    return issue_data['CUSIP_ID']


########
# Step 4
########
def read_post_2012(year_ind, annual_fld, path):
    """Read in TRACE data in the years post 2012 (i.e. > 2012). In a first step, source automatically
    the directories where the files are stored. In a second step, loop through all days in a yearly folder
    and clean and concatenate the data to generate a yearly file. Only keep the bonds as specified in select_bonds().

    Args:
    --------
    year_ind (int):  Year indicator for which the TRACE dataset is to be generated (0 = 2002, 1 = 2003, etc.)
    annual_fld (str): List of annual folder names
    path (str): Project root path

    Returns:
    --------
    df (pd.DataFrame): Output dataset where all daily files are concatenated to one yearly file.
    unmatched (pd.DataFrame): Necessary to return the unmatched trades for the cleaning step of the pre 2012 data

    """

    # Get list of CUSIP IDs that are to be maintained in the sample using select_bonds()
    cusip_list_keep = select_bonds(path)
    # Define the path to the annual TRACE dataset that is to be cleaned
    ann_fld_path = path + 'src/original_data/academic_TRACE/TRACE_raw/' + annual_fld[year_ind - 1]

    # Get a list of the daily files within the annual folder. Note that the actual transaction data filename does
    # NOT start with '0033-corp-bond' whereas the supplementary files do. Thus only transaction data is selected.
    daily_files = (
        [f for f in sorted(os.listdir(ann_fld_path))
         if not (f.startswith('0033-corp-bond') | f.startswith('.'))]
    )
    # Initialize trsct count
    # Loop over all days in one yearly folder and concatenate the dataset
    for day in range(0, len(daily_files)):
        print('Currently reading Year: 20{}, Trading Day: {}'.format(year_ind + 1, day))
        if day == 0:
            # Read in the new daily dataset.
            df = adj_dt_format_post_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df = df.loc[df['CUSIP_ID'].isin(cusip_list_keep)]
        else:
            # Read in the new daily dataset.
            df_tmp = adj_dt_format_post_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df_tmp = df_tmp.loc[df_tmp['CUSIP_ID'].isin(cusip_list_keep)]
            # Concatenate the datasets
            df = pd.concat([df, df_tmp])

    # Implement the Dick-Nielsen (2019) corrections using post_2012_clean()
    df_post, unmatched = post_2012_clean(df)
    # Store the yearly TRACE data to disc:
    df_post.to_pickle(path + 'bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}'.format(year_ind + 1 + 2000))
    # Drop DataFrame from memory to save memory space
    del [df]
    gc.collect()

    return unmatched



def read_2012(year_ind, annual_fld, path, unmatched_in):
    """Read in TRACE data in the year 2012. FINRA changed the reporting on 06.02.2012 which requires a different
    reading-in procedure before and after this date. In a first step, source automatically
    the directories where the files are stored. In a second step, loop through all days in a yearly folder
    and clean and concatenate the data to generate a yearly file. Only keep the bonds as specified in select_bonds().
    Note: 06.02.2012 is the 23rd trading day of this year.

    Args:
    --------
    year_ind (int): Year indicator variable. Has to be 10 (=2012) in this case.
    annual_fld (list): List of annual folder names
    path (str): Project root path
    unmatched_in (pd.DataFrame): Read in the unmatched transactions from the post_2012 cleaning step

    Returns:
    --------
    df (pd.DataFrame): Output dataset where all daily files are concatenated to one yearly file.

    """

    # Get list of CUSIP IDs that are to be maintained in the sample using select_bonds()
    cusip_list_keep = select_bonds(path)
    # Define the path to the annual TRACE dataset that is to be cleaned
    ann_fld_path = path + 'src/original_data/academic_TRACE/TRACE_raw/' + annual_fld[year_ind - 1]
    # Get a list of the daily files within the annual folder. Note that the actual transaction data filename does
    # NOT start with '0033-corp-bond' whereas the supplementary files do. Thus only transaction data is selected.
    daily_files = (
        [f for f in sorted(os.listdir(ann_fld_path))
         if not (f.startswith('0033-corp-bond') | f.startswith('.'))]
    )
    # Loop over all days in one yearly folder and concatenate the dataset
    for day in range(0, len(daily_files)):
        print('Currently reading Year: 20{}, Trading Day: {}'.format(year_ind + 1, day))
        if (day == 0):
            # Read in the new daily dataset for the first day of 2012
            df_2012_prior = adj_dt_format_pre_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df_2012_prior = df_2012_prior.loc[df_2012_prior['CUSIP_ID'].isin(cusip_list_keep)]
        elif (day > 0) & (day <= 22):
            # Read in the new daily dataset.
            df_2012_prior_tmp = adj_dt_format_pre_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df_2012_prior_tmp = df_2012_prior_tmp.loc[df_2012_prior_tmp['CUSIP_ID'].isin(cusip_list_keep)]
            # Concatenate the datasets
            df_2012_prior = pd.concat([df_2012_prior, df_2012_prior_tmp])
        elif (day == 23):
            # Read in the new daily dataset for the first day after the reporting standards changed on 06.02.2012
            df_2012_post = adj_dt_format_post_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df_2012_post = df_2012_post.loc[df_2012_post['CUSIP_ID'].isin(cusip_list_keep)]
        else:
            # Read in the new daily dataset.
            df_2012_post_tmp = adj_dt_format_post_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df_2012_post_tmp = df_2012_post_tmp.loc[df_2012_post_tmp['CUSIP_ID'].isin(cusip_list_keep)]
            # Concatenate the datasets
            df_2012_post = pd.concat([df_2012_post, df_2012_post_tmp])

    # Apply the cleaning steps by Dick-Nielsen & Poulsen (2019) for the post 2012 data:
    df_2012_post_cl_DN, unmatched_tmp = post_2012_clean(df_2012_post)
    unmatched = pd.concat([unmatched_in, unmatched_tmp])
    # Apply the cleaning steps by Dick-Nielsen & Poulsen (2019) for the pre 2012 data:
    df_2012_pre_cl_DN = prior_2012_clean(df_2012_prior, unmatched)
    # Store the yearly TRACE data
    df_2012_post_cl_DN.to_pickle(
        path + 'bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}_post'.format(year_ind + 1 + 2000))
    df_2012_pre_cl_DN.to_pickle(
        path + 'bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}_prior'.format(year_ind + 1 + 2000))

    # Drop DataFrame from memory to save memory space
    del [[df_2012_post_cl_DN, df_2012_pre_cl_DN]]
    gc.collect()

    return unmatched


def read_pre_2012(year_ind, annual_fld, path, unmatched_in):
    """Read in TRACE data in the years prior to 2012 (i.e. <= 2011). In a first step source automatically
    the directories where the files are stored. In a second step, loop through all days in a yearly folder
    and clean and concatenate the data to generate a yearly file. Only keep the bonds as specified in select_bonds().

    Args:
    --------
    year_ind (int): Year for which the TRACE dataset is to be generated
    annual_fld (list): List of annual folder names
    path (str): Project root path

    Returns:
    --------
    df (pd.DataFrame): Output dataset where all daily files are concatenated to one yearly file

    """

    # Get list of CUSIP IDs that are to be maintained in the sample using select_bonds()
    cusip_list_keep = select_bonds(path)
    # Define the path to the annual TRACE folder that is to be cleared
    ann_fld_path = path + 'src/original_data/academic_TRACE/TRACE_raw/' + annual_fld[year_ind - 1]
    # Get a list of the daily files within the annual folder. Note that the actual transaction data filename does
    # NOT start with '0033-corp-bond' whereas the supplementary files do. Thus only transaction data is selected.
    daily_files = (
        [f for f in sorted(os.listdir(ann_fld_path))
         if not (f.startswith('0033-corp-bond') | f.startswith('.'))]
    )
    # Loop over all days in one yearly folder and concatenate the dataset
    for day in range(0, len(daily_files)):
        if year_ind >= 10:
            print('Currently reading Year: 20{}, Trading Day: {}'.format(year_ind, day))
        else:
            print('Currently reading Year: 200{}, Trading Day: {}'.format(year_ind, day))
        if (day == 0):
            # Read in the new daily dataset.
            df = adj_dt_format_pre_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df = df.loc[df['CUSIP_ID'].isin(cusip_list_keep)]
        else:
            # Read in the new daily dataset.
            df_tmp = adj_dt_format_pre_2012(ann_fld_path + '/' + daily_files[day])
            # Keep only the bonds according to the specifications in select_bonds()
            df_tmp = df_tmp.loc[df_tmp['CUSIP_ID'].isin(cusip_list_keep)]
            # Concatenate the datasets
            df = pd.concat([df, df_tmp])

    # Implement the Dick-Nielsen (2019) corrections using post_2012_clean()
    df_prior = prior_2012_clean(df, unmatched_in)
    # Store the yearly TRACE data
    df_prior.to_pickle(path + 'bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}'.format(year_ind + 1 + 2000))
    # Drop DataFrame from memory to save memory space
    del [df_prior]
    gc.collect()


########
# Step 5
########
def read_TRACE_all(path):
    """Read in the entire TRACE dataset by executing the above steps. I.e., read in the daily text files, apply the
    correction steps and concatenate all files on a yearly level. Then save the dataset for each year in pickle format.

    Args:
    --------
    path: Project root path

    Note:
    --------
    Executes all previously specified reading-in steps.

    """
    # Get the name of the annual TRACE data folders. Exclude the listing of system files
    annual_fld_names = (
        [f for f in sorted(os.listdir(path + 'src/original_data/academic_TRACE/TRACE_raw/'))
         if not f.startswith('.')]
    )
    # Apply the reading-in procedure in the respective years. Loop backwards to assure that the unmatched data
    # of the post period are available for the pre-period.
    for year_ind in range(len(annual_fld_names), 0, -1):
        if year_ind == len(annual_fld_names):
            unmatched = read_post_2012(year_ind, annual_fld_names, path)
        # Note: 11 corresponds to 2012 which is the cutoff year due to the change in the TRACE dataset format
        elif (year_ind < len(annual_fld_names)) & (year_ind > 11):
            unmatched_tmp = read_post_2012(year_ind, annual_fld_names, path)
            unmatched = pd.concat([unmatched, unmatched_tmp])
        elif year_ind == 11:  # corresponds to 2012 (!!mind the reverse counting!!)
            unmatched_tmp = read_2012(year_ind, annual_fld_names, path, unmatched)
            unmatched_fin = pd.concat([unmatched, unmatched_tmp])
        else:
            read_pre_2012(year_ind, annual_fld_names, path, unmatched_fin)


def read_TRACE_all_PARALLEL_post_2012(path, annual_fld_names, year_ind):
    """Read in the entire TRACE dataset by executing the above steps. I.e., read in the daily text files, apply the
    correction steps and concatenate all files on a yearly level. Then save the dataset for each year in pickle format.

    Args:
    -----------
    path (str): Project root path

    Notes:
    --------
    Executes all previously specified reading-in steps.

    """

    # Apply the reading-in procedure in the respective years. Loop backwards to assure that the unmatched data
    # of the post period are available for the pre-period.
    if year_ind == len(annual_fld_names):
        unmatched = read_post_2012(year_ind, annual_fld_names, path)
    # Note: 11 corresponds to 2012 which is the cutoff year due to the change in the TRACE dataset format
    elif (year_ind < len(annual_fld_names)) & (year_ind > 11):
        unmatched = read_post_2012(year_ind, annual_fld_names, path)
    else:
        print('Reading-in step misspecified')

    return unmatched


def read_TRACE_all_PARALLEL_prior_2012(path, annual_fld_names, unmatched, year_ind):
    """Read in the entire TRACE dataset by executing the above steps. I.e., read in the daily text files, apply the
    correction steps and concatenate all files on a yearly level. Then save the dataset for each year in pickle format.

    Args:
    --------
    path (str): Project root path

    Returns:
    --------
    Executes all previously specified reading-in steps.

    """

    # Apply the reading-in procedure in the respective years. Loop backwards to assure that the unmatched data
    # of the post period are available for the pre-period.
    if year_ind < 11:
        read_pre_2012(year_ind, annual_fld_names, path, unmatched)
    else:
        print('Reading-in step misspecified')




def read_TRACE_all_PARALLEL_2(path, N_workers):
    """Parallelize the reading-in steps to increase performance. Read in the annual folder names in a first step.

    Args:
    --------
    path (str): Project root path
    N_workers (int): Define how many cores should be allocated to the reading-in step (-1 -> use all available cores)

    Returns:
    --------
    Executes all previously specified reading-in steps.

    """

    print("The reading-in step is started")

    # Get the name of the annual TRACE data folders. Exclude the listing of system files
    annual_fld_names = (
        [f for f in sorted(os.listdir(path + 'src/original_data/academic_TRACE/TRACE_raw/'))
         if not f.startswith('.')]
    )

    # Perform the parallelization from the last sample year until 2013
    unmatched_out_parallel = (
        Parallel(n_jobs = N_workers)(delayed(read_TRACE_all_PARALLEL_post_2012)(path, annual_fld_names, year) for year in range(len(annual_fld_names), 11, -1))
    )
    unmatched = pd.concat(unmatched_out_parallel)
    print("The reading-in for the most recent sample year until 2013 is finalized")

    # Perform the reading-in step for 2012:
    unmatched = read_2012(11, annual_fld_names, path, unmatched)
    print("The reading-in for the year 2012 is finalised")


    # Perform the reading-in for the years 2002 - 2011:
    Parallel(n_jobs=N_workers)(delayed(read_TRACE_all_PARALLEL_prior_2012)(path, annual_fld_names, unmatched, year) for year in range(10, 0, -1))
    print("The reading-in for the years 2011 - 2002 is finalized")


    print("SUCCESS: The reading-in and concatenation to individual year files (including the Dick-Nielsen and Poulsen (2019) correction is finalised")




def get_all_rpt_dates(path):
    """Get all reporting dates in the raw TRACE data. That is, extract the date from every single raw.txt file in
    the TRACE data. This is important as e.g. on some weekdays (where there is no holiday) there is no TRACE report
    available. On such days the number of transactions is very low and should be filtered out. Save the final list
    with all available dates in pkl format.

    Args:
    --------
    path: Project root path

    Returns:
    --------
    TRACE_rpt_days_yearly (pd.DataFrame): DataFrame with all TRACE reporting dates

    """
    annual_fld_names = (
        [f for f in sorted(os.listdir(path + 'src/original_data/academic_TRACE/TRACE_raw/'))
         if not (f.startswith('.') | f.startswith('zip'))]
    )
    # Initialize list
    TRACE_rpt_days_all = []
    for year in range(0, len(annual_fld_names)):
        ann_fld_path = path + 'src/original_data/academic_TRACE/TRACE_raw/' + annual_fld_names[year]
        # Get a list of the daily files within the annual folder. Note that the actual transaction data filename does NOT
        # start with '0033-corp-bond' whereas the supplementary files do. Thus only transaction data is selected.
        daily_files = (
            [f for f in sorted(os.listdir(ann_fld_path))
             if not (f.startswith('0033-corp-bond') | f.startswith('.'))]
        )
        TRACE_rpt_days_yearly = [c[30:40] for c in daily_files]
        TRACE_rpt_days_all = TRACE_rpt_days_all + TRACE_rpt_days_yearly

    with open(path + 'bld/data/TRACE/TRACE_info/TRACE_rpt_dates.pkl', 'wb') as f:
        pickle.dump(TRACE_rpt_days_all, f)


def get_full_sample_info(path):
    """Get the total number of transactions available in the raw TRACE data. This is to get information on the total
    size of the raw data which is unknown as I directly perform filtering in the reading-in step to save on memory.

    Args:
    --------
    path (str): Project root path

    Returns:
    --------
    total_trsct_COUNT (int): Total number of transactions in the raw data

    """
    # Initialiye the transaction count variable
    total_trsct_COUNT = 0
    # Get the name of the annual TRACE data folders. Exclude the listing of system files
    annual_fld_names = (
        [f for f in sorted(os.listdir(path + 'src/original_data/academic_TRACE/TRACE_raw/'))
         if not f.startswith('.')]
    )
    for year in range(len(annual_fld_names), 0, -1):
        ann_fld_path = path + 'src/original_data/academic_TRACE/TRACE_raw/' + annual_fld_names[year - 1]
        daily_files = (
            [f for f in sorted(os.listdir(ann_fld_path))
             if not (f.startswith('0033-corp-bond') | f.startswith('.'))]
        )
        if year == len(annual_fld_names):
            for day in range(0, len(daily_files)):
                total_trsct_COUNT = total_trsct_COUNT + len(adj_dt_format_post_2012(ann_fld_path / daily_files[day]))
        elif (year < len(annual_fld_names)) & (year > 11):
            for day in range(0, len(daily_files)):
                total_trsct_COUNT = total_trsct_COUNT + len(adj_dt_format_post_2012(ann_fld_path / daily_files[day]))
        elif year == 11:  # corresponds to 2012 (!!mind the reverse counting!!)
            for day in range(23, len(daily_files)):
                total_trsct_COUNT = total_trsct_COUNT + len(adj_dt_format_post_2012(ann_fld_path / daily_files[day]))
            for day in range(0, 23):
                total_trsct_COUNT = total_trsct_COUNT + len(adj_dt_format_pre_2012(ann_fld_path / daily_files[day]))
        else:
            for day in range(0, len(daily_files)):
                total_trsct_COUNT = total_trsct_COUNT + len(adj_dt_format_pre_2012(ann_fld_path / daily_files[day]))

    return total_trsct_COUNT




