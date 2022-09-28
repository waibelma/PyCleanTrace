""""Read in the bond background information. Data comes in daily textfiles and contains
background information for the bonds that are traded on a given date. Note, so far I only ammend a given bond
to this list if the bond CUSIP is not on the list yet. The advantage is that I get a list of all unique bond
identifiers that is ever traded. The disadvantage is that I lose the time-varying information such as the
coupon rate.

"""

import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime


def read_bond_info(in_path):
    """Read in the daily bond information files and specify the datatype

    Args:
    --------
    in_path (str): Location of the daily raw dataset

    Returns:
    --------
    df (DataFrame):  Daily raw transactions with adjusted data types
    """

    # Read in the metadata of the folder. Important to read in as str variables
    # to preserve the leading 0 in the date structures.
    df = pd.read_csv(in_path, sep="|", engine='python', dtype={'TRD_RPT_EFCTV_DT' : str, 'MTRTY_DT':str})
    # Drop the last two rows as they only contain FINRA identifier information
    df = df.iloc[:-2]
    # Delete all rows with missing CUSIP IDs
    df = df.dropna(subset=['CUSIP_ID'])

    # Define the required data types
    dtypes = {
        'FINRA_SCRTY_ID' : float,
        'CUSIP_ID' : str,
        'SYM_CD' : str,
        'CMPNY_NM' : str,
        'SUB_PRDCT_TYPE_CD' : str,
        'SCRTY_TYPE_CD' : str,
        'SCRTY_SBTP_CD' : str,
        'CPN_RT' : float,
        'CPN_TYPE_CD' : str,
        'TRD_RPT_EFCTV_DT' : str,
        'MTRTY_DT' : str,
        'TRACE_GRADE_CD' : str,
        'RULE_144A_FL' : str,
        'DSMTN_FL' : str,
        'ACCRD_INTRS_AM' : str,
        'CNVRB_FL' : str
    }
    # Convert the variable types
    df = df.astype(dtypes)

    return df

def def_unique_bond_info(path):
    """
    Construct one dataset containing all unique CUSIPs that are ever registered over the entire sample period.
    Note: Every day information on the traded bonds is stored in the file "0033-corp-bond-YYYY-MM-DD.txt"
    (before 2012-02-06) and the files "0033-corp-bond-YYYY-MM-DD.txt" and "0033-corp-bond-supplemental-YYYY-MM-DD.txt"
    (after 2012-02-06). Thus, many observations are repetitions which requires to loop through all files and only
    extract the new bond information to produce one single file with all bond transactions over the sample period (2002-
    2018). It needs to be noted that after the reporting change on 2012-02-06 the bond information is stored in both
    the "0033-corp-bond-YYYY-MM-DD.txt" and the supplementary file ("0033-corp-bond-supplemental-YYYY-MM-DD.txt").
    Therefore, both have to be read in.

    Parameters:
    ----------
    path (str):  Project path

    Returns:
    --------
    df (DataFrame):  Daily raw transactions with adjusted data types

    """
    # Define the folder path to the raw TRACE data
    annual_fld = (
        [f for f in sorted(os.listdir(path + 'src/original_data/academic_TRACE/TRACE_raw/'))
         if not f.startswith('.')]
    )
    # Loop over all years in the sample
    for year in range(0, len(annual_fld)):
        ann_fld_path = path + 'src/original_data/academic_TRACE/TRACE_raw/' + annual_fld[year]
        # Get a list of the daily files within the annual folder (both basic and supplemental)
        daily_files_basic = (
            [f for f in sorted(os.listdir(ann_fld_path))
             if not (f.startswith('0033-corp-academic') | f.startswith('0033-corp-bond-supplemental') |
                     f.startswith('.'))]
        )
        daily_files_supp = (
            [f for f in sorted(os.listdir(ann_fld_path))
             if not (f.startswith('0033-corp-academic') | f.startswith('0033-corp-bond-20') |
                     f.startswith('.'))]
        )
        # Loop over all sample days in the respective year
        for day in range(0, len(daily_files_basic)):
            if year >= 10:
                print('Currently reading Year: 20{}, Trading Day: {}'.format(year+2, day))
            else:
                print('Currently reading Year: 200{}, Trading Day: {}'.format(year+2, day))
            # Note that there were no supplementary files prior to the reform in 2012-02-06. Thus, this needs
            # to be treated separately
            if (datetime.strptime('{}'.format(daily_files_basic[day][-14:-4]), '%Y-%m-%d') <=
                datetime.strptime('2012-02-06', '%Y-%m-%d')):
                # Define the inpath for the basic files
                in_path_basic = ann_fld_path + '/' + daily_files_basic[day]
                if (year == 0) & (day == 0):
                    # Set up the base dataset by reading in bond data using read_bond_info()
                    bond_info_df = read_bond_info(in_path_basic)
                else:
                    bond_info_df_tmp_basic = read_bond_info(in_path_basic)
                    # Get the unique CUSIPs in both the base dataset and the added daily dataset
                    unique_CUSIP_base = set(bond_info_df.CUSIP_ID.unique())
                    unique_CUSIP_add_basic = set(bond_info_df_tmp_basic.CUSIP_ID.unique())
                    # Get the unique CUSIPs that are added (i.e. the CUSIPs that are in the added daily dataset
                    # but so far not in the base dataset.)
                    add_CUSIPs_basic = list(unique_CUSIP_base.union(unique_CUSIP_add_basic) - unique_CUSIP_base)
                    # Concatenate the base dataset with the newly added bond information.
                    bond_info_df = pd.concat(
                        [bond_info_df,
                         bond_info_df_tmp_basic.loc[bond_info_df_tmp_basic.CUSIP_ID.isin(add_CUSIPs_basic)]]
                    )
            # After 2012-02-06 there are also supplementary information for bonds
            else:
                # Specify the input path for the normal bond info datasets
                in_path_basic  = ann_fld_path + '/' +  daily_files_basic[day]
                if year == 10: #Note year number needs to correspond to the year 2012
                    # Specify separately for year 2012 as the first 22 trading days are subject to the
                    # pre-2012 reporting standards
                    in_path_supp  = ann_fld_path + '/' + daily_files_supp[day-23]
                else:
                    # Specify the input path for the supplemental bond info datasets
                    in_path_supp  = ann_fld_path + '/' + daily_files_supp[day]
                bond_info_df_tmp_basic = read_bond_info(in_path_basic)
                bond_info_df_tmp_supp  = read_bond_info(in_path_supp)
                # Get the unique CUSIPs in both the existing base dataset and the newly added bonds from both
                # the respectivel normal daily bond info dataset and the respective supplemental dataset
                unique_CUSIP_base = set(bond_info_df.CUSIP_ID.unique())
                unique_CUSIP_add_basic = set(bond_info_df_tmp_basic.CUSIP_ID.unique())
                unique_CUSIP_add_supp = set(bond_info_df_tmp_supp.CUSIP_ID.unique())
                # Get the CUSIPs that are to be added
                add_CUSIPs_basic = list(unique_CUSIP_base.union(unique_CUSIP_add_basic) - unique_CUSIP_base)
                add_CUSIPs_supp = (
                    list(set(unique_CUSIP_add_supp).union(add_CUSIPs_basic).union(unique_CUSIP_base) -
                         set(add_CUSIPs_basic).union(unique_CUSIP_base))
                )
                # Concatenate the datasets
                bond_info_df = pd.concat(
                    [bond_info_df,
                     bond_info_df_tmp_basic.loc[bond_info_df_tmp_basic.CUSIP_ID.isin(add_CUSIPs_basic)],
                     bond_info_df_tmp_supp.loc[bond_info_df_tmp_supp.CUSIP_ID.isin(add_CUSIPs_supp)]]
                )
    # Store the dataset
    bond_info_df.to_pickle(path + 'bld/data/TRACE/TRACE_raw_clean/bond_info.pkl')

    return bond_info_df


