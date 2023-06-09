"""
Perform the cleaning steps as outlined in Dick-Nielsen & Poulsen (2019). In a next step
perform cleaning steps as described in Bessembinder et al. (2018) and Anand et al. (2021).

Step 1:     Perform the cleaning steps as in Bessembinder et al. (2018) and Anand et al. (2021). 
            The naming of the
            variables is kept as analoguous as possible to  Dick-Nielsen & Poulsen (2019) to assure comparability.
            The code is tested on a smaller sample with the SAS code by Dick-Nielsen & Poulsen (2019) and is found
            to produce identical results.
Step 2:     Harmonise the dataset names between the prior and post datasets to be able to concatenate the data
Step 3:     Perform individual cleaning steps as in Bessembinder et al. (2018) and Anand et al. (2021)

"""

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
from datetime import datetime
# Import the dates of US holidays
from data_specs.US_holidays.US_holiday_list import get_US_holiday_dates

def post_2012_clean(df_post):
    """
    Rreplicate the code by Dick-Nielsen and Poulsen (2019) for the post-2012 data. In the code,
    I document the pages in the paper where the respective passage original SAS code is displayed.

    Parameters:
    -----------
    df_post (DataFrame): Post-2012 raw dataset

    Returns:
    --------
    temp_raw3_NEW:  Data cleaned by reports that are matched by the reversals
    unmatched: Reversals referring to trades before Feb 6th, 2012
    """

    # Step 1.1: (Dick Nielsen and Thomas Poulsen (2019), p.13)
    # Report the party executing the trade in the RPTG_PARTY_ID field
    df_post.loc[df_post.RPTG_PARTY_GVP_ID.isna()==False, 'RPTG_PARTY_ID'] = (
        df_post.loc[df_post.RPTG_PARTY_GVP_ID.isna()==False, 'RPTG_PARTY_GVP_ID']
    )
    # Report the contra party executing the trade in the RPTG_PARTY_ID field
    df_post.loc[df_post.CNTRA_PARTY_GVP_ID.isna()==False, 'CNTRA_PARTY_ID'] = (
        df_post.loc[df_post.CNTRA_PARTY_GVP_ID.isna()==False, 'CNTRA_PARTY_GVP_ID']
    )
    # Set up temp delete files consistent with Dick Nielsen and Poulsen (2019)
    temp_deleteI_NEW_cols = (['CUSIP_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM',
                              'RPT_SIDE_CD', 'CNTRA_PARTY_ID', 'CNTRA_PARTY_GVP_ID', 'SYSTM_CNTRL_NB']
    )
    temp_deleteII_NEW_cols =(['CUSIP_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM','RPT_SIDE_CD',
                              'CNTRA_PARTY_ID', 'CNTRA_PARTY_GVP_ID', 'PREV_TRD_CNTRL_NB', 'TRD_RPT_DT', 'TRD_RPT_TM']
    )
    # Delete the observations without a CUSIP ID
    temp_raw = df_post[df_post.CUSIP_ID.isna() == False]
    # Takes out all cancellations and corrections. These transactions should be deleted together
    # with the original report;
    temp_deleteI_NEW = temp_raw.loc[temp_raw.TRD_ST_CD.isin(['X','C']), temp_deleteI_NEW_cols]
    # Reversals: These have to be deleted as well together with
    # the original report;
    temp_deleteII_NEW = temp_raw.loc[temp_raw.TRD_ST_CD.isin(['Y']), temp_deleteII_NEW_cols]
    # The rest of the data;
    temp_raw = temp_raw.loc[(temp_raw.TRD_ST_CD != 'X') & (temp_raw.TRD_ST_CD != 'C') & (temp_raw.TRD_ST_CD != 'Y')]


    # Step 1.2: (Dick Nielsen and Thomas Poulsen (2019), p.13-14)
    # Deletes the cancellations and corrections as identified by the reports in temp_deleteI_NEW.
    # These transactions can be matched by message sequence number and date. We furthermore match on
    # cusip, volume, price, date, time, buy-sell side, contra party. This is as suggested by the variable description;

    merge_vars_tmp2 = ['CUSIP_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM',
                       'RPT_SIDE_CD', 'CNTRA_PARTY_ID', 'CNTRA_PARTY_GVP_ID', 'SYSTM_CNTRL_NB']
    # Indicator which rows are to be dropped
    temp_deleteI_NEW['drop'] = 1
    # Generate the merged temp file. This is the equivalent to the SQL command on page 14 (upper part)
    temp_raw2 = temp_raw.merge(temp_deleteI_NEW, left_on = merge_vars_tmp2,
                               right_on = merge_vars_tmp2, how = 'left')
    # Drop rows based on the drop indicator
    temp_raw2 = temp_raw2.loc[temp_raw2["drop"] != 1]
    # Drop unnecessary variable
    temp_raw2 = temp_raw2.drop(columns=['drop'])

    # Step 1.3: (Dick Nielsen and Thomas Poulsen (2019), p.14)
    # Deletes the reports that are matched by the reversals;
    merge_vars_raw2 = ['CUSIP_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM',
                       'RPT_SIDE_CD', 'CNTRA_PARTY_ID', 'CNTRA_PARTY_GVP_ID', 'SYSTM_CNTRL_NB']
    merge_vars_raw_delII = ['CUSIP_ID', 'ENTRD_VOL_QT', 'RPTD_PR', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM',
                           'RPT_SIDE_CD', 'CNTRA_PARTY_ID', 'CNTRA_PARTY_GVP_ID', 'PREV_TRD_CNTRL_NB']
    temp_deleteII_NEW['drop'] = 1
    # Generate the merged temp file. This is the equivalent of the SQL command on page 14 (lower part)
    temp_raw3_NEW = temp_raw2.merge(temp_deleteII_NEW[merge_vars_raw_delII + ['drop']], left_on = merge_vars_raw2,
                                        right_on = merge_vars_raw_delII, how = 'left', suffixes=('', '_temp_delII'))
    # Drop rows based on indicator
    temp_raw3_NEW = temp_raw3_NEW.loc[temp_raw3_NEW["drop"] != 1]
    temp_raw3_NEW = temp_raw3_NEW.drop(columns = ['drop', 'PREV_TRD_CNTRL_NB_temp_delII'])

    # Save reversals referring to trades before Feb 6th, 2012
    temp_deleteII_NEW['TRD_EXCTN_DT'] = pd.to_datetime(temp_deleteII_NEW['TRD_EXCTN_DT'])
    unmatched = temp_deleteII_NEW.loc[temp_deleteII_NEW.TRD_EXCTN_DT < datetime(2012,2,6)]

    return temp_raw3_NEW, unmatched


def prior_2012_clean(df_pre, unmatched):
    """
    Replicate the code by Dick Nielsen and Poulsen (2019) for the pre-2012 data. In the code
    I document the pages in the paper where the respective passage original SAS code is displayed.

    Parameters:
    -----------
    df_pre: Pre-2012 raw dataset
    unmatched: Reversals referring to trades before Feb 6th, 2012 [Output from post_2012_clean()]

    Returns:
    --------
    temp_raw4: Data cleaned by reports that are matched by the reversals
    """

    # Step 2.1: (Dick Nielsen and Thomas Poulsen (2019), p.15)
    # Report the party executing the trade in the RPTG_PARTY_ID field
    df_pre.loc[df_pre.RPTG_SIDE_GVP_MP_ID.isna()==False, 'RPTG_MKT_MP_ID'] = (
        df_pre.loc[df_pre.RPTG_SIDE_GVP_MP_ID.isna()==False, 'RPTG_SIDE_GVP_MP_ID']
    )
    df_pre.loc[df_pre.CNTRA_GVP_ID.isna()==False, 'CNTRA_MP_ID'] = (
        df_pre.loc[df_pre.CNTRA_GVP_ID.isna()==False, 'CNTRA_GVP_ID']
    )
    # Delete observations with missing CUSIP codes
    df_pre = df_pre.loc[df_pre.CUSIP_ID.isna() == False]


    # Step 2.2: (Dick Nielsen and Thomas Poulsen (2019), p.15)
    # Takes same-day corrections and splits them into two data sets;
    # One for all the correct trades, and one for the corrections;
    # Take out all trade cancellations ('C'). Include them in the temp_delete dataset.
    # Also include trade corrections (WAS).
    temp_delete = df_pre.loc[(df_pre.TRC_ST == 'C')|(df_pre.TRC_ST == 'W'), ['TRD_RPT_DT', 'PREV_REC_CT_NB']]
    # Put both corrections ('W' and 'N') and the actual trade reports ('T') in the temp_raw file.
    temp_raw = df_pre.loc[(df_pre.TRC_ST == 'T')|(df_pre.TRC_ST == 'W')|(df_pre.TRC_ST == 'N')]
    # Delete the error trades as identified by the message sequence numbers. Same day
    # corrections and cancellations.
    temp_raw_tmp  = temp_raw[['TRD_RPT_DT', 'REC_CT_NB']]
    # Add an indicator for which rows to drop
    temp_delete['drop'] = 1
    # Generate the merged temp file. This is the equivalent to the SQL command on page 15-16
    temp_raw_red = temp_raw_tmp.merge(temp_delete, left_on = ['REC_CT_NB', 'TRD_RPT_DT'],
                                      right_on = ['PREV_REC_CT_NB', 'TRD_RPT_DT'], how = 'left')
    # Drop rows based on indicator
    temp_raw_red = temp_raw_red.loc[temp_raw_red["drop"] != 1]
    # Sort variables
    temp_raw_red = temp_raw_red.sort_values(['REC_CT_NB', 'TRD_RPT_DT'])[['REC_CT_NB', 'TRD_RPT_DT']]
    temp_raw2 = temp_raw.merge(temp_raw_red, on = ['REC_CT_NB', 'TRD_RPT_DT'], how='inner')


    # Step 2.3: (Dick Nielsen and Thomas Poulsen (2019), p.16)
    # Take out reversals into a dataset;
    temp_raw2['N'] = np.arange(1,len(temp_raw2)+1)
    reversal  = temp_raw2.loc[temp_raw2.ASOF_CD=='R']
    temp_raw3 = temp_raw2.loc[temp_raw2.ASOF_CD!='R']
    # Include reversals referring to transactions before February 6th, 2012 that
    # are reported after this date;
    unmatched = unmatched[['TRD_EXCTN_DT', 'CUSIP_ID', 'TRD_EXCTN_TM', 'RPTD_PR', 'ENTRD_VOL_QT', 'RPT_SIDE_CD',
                           'CNTRA_PARTY_ID', 'TRD_RPT_DT', 'TRD_RPT_TM']].copy()
    # Rename variables
    unmatched.rename(columns={"TRD_EXCTN_TM": "EXCTN_TM", "CNTRA_PARTY_ID": "CNTRA_MP_ID"}, inplace=True)
    # Get all variables that are in reversal but not in unmatched
    list_var_diff = list(set(reversal.columns) - set(unmatched.columns))
    # Add the missing variables to unmatched and fill with np.nan
    unmatched[list_var_diff] = pd.DataFrame([[np.nan]*len(list_var_diff)], index = unmatched.index)
    #Adjust the ordering according to the reversal ordering
    unmatched = unmatched[reversal.columns]
    # Concatenate the reversals and unmatched trades:
    reversal = pd.concat([reversal, unmatched])
    # Check for duplicates
    reversal = reversal.drop_duplicates(subset = ['TRD_EXCTN_DT', 'CUSIP_ID', 'EXCTN_TM', 'RPTD_PR', 'ENTRD_VOL_QT',
                                                  'RPT_SIDE_CD', 'CNTRA_MP_ID', 'TRD_RPT_DT', 'TRD_RPT_TM', 'REC_CT_NB'])

    # Step 2.4: (Dick Nielsen and Thomas Poulsen (2019), p.16-17)
    # Assign a unique reversal ID
    reversal['REV_ID'] = np.arange(1, len(reversal)+1)
    # Identify all transactions that matches the reversals. This code is analogous to the SQL command on page. 17
    reversal_tmp = reversal[['CUSIP_ID', 'EXCTN_TM', 'RPTD_PR', 'ENTRD_VOL_QT', 'RPT_SIDE_CD','CNTRA_MP_ID', 'REV_ID',
                             'TRD_RPT_TM']]
    reversal2 = temp_raw3.merge(reversal_tmp, on=['CUSIP_ID', 'EXCTN_TM', 'RPTD_PR', 'ENTRD_VOL_QT', 'RPT_SIDE_CD',
                                                  'CNTRA_MP_ID'], how = 'inner', suffixes=('', '_reversal'))
    # Reversals must be reported after the matching transaction
    reversal2 = reversal2.loc[reversal2['TRD_RPT_TM']<reversal2['TRD_RPT_TM_reversal']]
    reversal2['datetime_dist'] = reversal2['TRD_RPT_TM_reversal'] - reversal2['TRD_RPT_TM']
    reversal2 = reversal2.sort_values('N')
    # Keep the earliest transaction (in a chronological sense) that matches each
    # reversal;
    reversal2 = reversal2.sort_values(['REV_ID', 'datetime_dist'])
    reversal2 = reversal2.drop_duplicates(subset=['REV_ID'])
    # Sort the data
    reversal2 = reversal2.sort_values(['N'])
    temp_raw3 = temp_raw3.sort_values(['N'])
    # Delete the matching reversals
    temp_raw3['drop_ind_rev_2'] = (temp_raw3['N'].isin(reversal2['N']))*1
    temp_raw4 = temp_raw3.loc[temp_raw3.drop_ind_rev_2 == 0]
    temp_raw4 = temp_raw4.drop(columns=['drop_ind_rev_2'])

    return temp_raw4


#######
#Step 2
#######
def harmon_pre_post_data(df, pre_post_id):
    """
    Replicate the code by Dick Nielsen and Poulsen (2019) for the post-2012 data. In the code
    I document the pages in the paper where the respective passage original SAS code is displayed.
    In particular, this code renames the variables in the DataFrame prior and post the reporting change in a
    harmonized way. This is important for concatenating datasets prior and post the change.

    Parameters:
    -----------
    df (DataFrame): Either pre or post 2012 DataFrame

    Returns:
    --------
    df (DataFrame):  Cleaned combined pre- and post data
    """

    # If the DataFrame is from the pre period:
    if pre_post_id == 'PRE':
        df.rename(columns={"RPTG_MKT_MP_ID":"RPTG_PARTY_ID", "RPTG_SIDE_GVP_MP_ID":"RPTG_PARTY_GVP_ID",
                              "CNTRA_MP_ID":"CNTRA_PARTY_ID", "CNTRA_GVP_ID":"CNTRA_PARTY_GVP_ID",
                               "EXCTN_TM":"TRD_EXCTN_TM", "WIS_CD":"WIS_DSTRD_CD", "CMSN_TRD_FL":"CMSN_TRD"},
                     inplace=True)
    # If the DataFrame is from the post period:
    elif pre_post_id == 'POST':
        # Rename variables in temp_raw3_new
        df.rename(columns={
            "ISSUE_SYM_ID": "BOND_SYM_ID", "CALCD_YLD_PT": "YLD_PT",
            "BUYER_CMSN_AMT": "BUY_CMSN_RT", "SLLR_CMSN_AMT": "SELL_CMSN_RT",
            "NO_RMNRN_CD": "CMSN_TRD", "YLD_DRCTN_CD": "YLD_SIGN_CD",
            "SLLR_CPCTY_CD": "SELL_CPCTY_CD", "BUYER_CPCTY_CD": "BUY_CPCTY_CD",
            "PBLSH_FL": "DISSEM_FL", "PRDCT_SBTP_CD": "SCRTY_TYPE_CD",
            "TRD_ST_CD":"TRC_ST"}, inplace=True)
    else:
        print("ERROR! The pre-post-ID has not been defined")

    return df


#######
#Step 3
#######
def filter_agency_trd(df):
    """
    Delete the agency transactions. Note that this code is not part of the error detection filter. This step can be
    deleted if you want to keep all agency transactions. In particular, it deletes agency customer transactions without
    commission. These transactions will have the same price as the interdealer transaction (if reported correctly).

    Parameters:
    -----------
    df (DataFrame): Input dataset

    Returns:
    --------
    df_out (DataFrame):  Output dataset cleaned for agency transactions
    """

    # Define an indicator for the buying capacity of the reporting entitiy. This is the only side that can be trusted.
    df['agency'] = np.nan
    df.loc[df.RPT_SIDE_CD =='B',  'agency'] =(
        df.loc[df.RPT_SIDE_CD =='B',  'BUY_CPCTY_CD']
    )
    df.loc[df.RPT_SIDE_CD =='S',  'agency'] =(
        df.loc[df.RPT_SIDE_CD =='S',  'SELL_CPCTY_CD']
    )
    # Delete agency transactions which are dealer-customer transactions without commission
    temp_raw6 =(
        df[((df.agency == 'A')& # Indicated on an agency-basis
            (df.CNTRA_PARTY_ID == 'C')& # Dealer-customer trades
            (df.CMSN_TRD == 'N'))==False] # Only keep transactions for which a comission is charged
    )

    return temp_raw6


def del_interd_transact(df_in_concat):
    """
    Delete the inter-dealer transactions (one of the sides). This is not necessary and entirely depends on the
    researcher's discretion. See Dick-Nielsen (2014, 2019) for a discussion.

    Parameters:
    -----------
    df_in_concat (DataFrame): Concatenated and merged yearly TRACE data

    Returns:
    --------
    list_trnsct_keep (list): List with all transaction-specific identifiers that are to be kept after the cancellation
                             of double inter-dealer trades
    """
    print("")
    print('STEP 3.1: The interdealer trade cancellation is started')

    # Filter out the agency trades
    #temp_raw6 = filter_agency_trd(df_in_concat)
    temp_raw6 = df_in_concat.copy()

    # Sort the data and assign an unique ID to each observation
    temp_raw6 = temp_raw6.sort_values(['CUSIP_ID', 'TRD_EXCTN_DT', 'TRD_EXCTN_TM'])
    temp_raw6['id'] = np.arange(0,len(temp_raw6))

    # Identify all inter-dealer trades
    inter_dealer = temp_raw6.loc[temp_raw6.CNTRA_PARTY_ID != 'C']

    # Keep all inter-dealer buys
    dealer_buys = inter_dealer.loc[inter_dealer.RPT_SIDE_CD=='B']

    # Identify matching inter-dealer transactions:
    merge_int_deal_vars = ['CUSIP_ID', 'TRD_EXCTN_DT', 'ENTRD_VOL_QT', 'RPTD_PR', 'RPTG_PARTY_ID',
                           'CNTRA_PARTY_ID']
    merge_deal_buy_vars = ['CUSIP_ID', 'TRD_EXCTN_DT', 'ENTRD_VOL_QT', 'RPTD_PR', 'CNTRA_PARTY_ID',
                           'RPTG_PARTY_ID']

    # Identify matching inter-dealer transactions
    matches = inter_dealer.merge(dealer_buys[merge_deal_buy_vars+ ['id']], left_on = merge_int_deal_vars,
                                 right_on=merge_deal_buy_vars, how='inner', suffixes=('', '_match'))
    matches = matches.loc[matches.RPT_SIDE_CD=='S'].sort_values(['id'])

    # Delete one side of each inter-dealer transaction (double counting)
    # Sort the temp_raw6 by the ID variable
    temp_raw6 = temp_raw6.sort_values(['id'])
    temp_raw6['select_d'] = (temp_raw6['id'].isin(matches['id']))*1
    temp_raw7 = temp_raw6.loc[temp_raw6.select_d == 0]

    list_trnsct_keep = list(temp_raw7.I_drop)
    # Drop id and match
    #temp_raw7 = temp_raw7.drop(columns=['id', 'select_d'])

    # Print final statement
    print('STEP 3.1: The interdealer trade cancellation is completed')

    return list_trnsct_keep


#######
#Step 4
#######
def clean_trade_level(df_in):
    """
    Perform additional cleaning steps following Bessembinder et al. (2018) p. 1623 and Anand et al (2021)
    p. 12. They exclude bonds based on the following criteria:

    a) Keep a bond only in the sample if it has more than 5 trades over the entire sample period:
    b) Exclude trades associated to new issuances (i.e. exclude all primary market transactions = keep only secondary
       market transactions)
    c) Exclude transactions that are reported after the bond's amount outstanding is reported by FISD as zero
    d) Exclude bonds with a reported trade size that exceeds the bond's offer size.

    Parameters:
    -----------
    df_in (DataFrame): Input DataFrame that is to be cleared

    Returns:
    --------
    df_clean (DataFrame): Output DataFrame that is cleaned according to the above categories

    """
    print("")
    print('STEP 3.2: The trade-level cleaning is started')

    df_clean = df_in.copy()
    # Get the initial (pre-cleaning) number of transactions
    N_trnsct = len(df_clean)

    # a) Keep a bond only in the sample if it has more than 5 trades over the entire sample period:
    df_clean = df_clean[df_in.groupby('CUSIP_ID')['CUSIP_ID'].transform(lambda x: len(x)) > 5]
    print('STEP 3.2.1: Keeping only bonds with more than 5 trades over the sample deletes {} transactions'.format(N_trnsct - len(df_clean)))
    N_trnsct = len(df_clean)

    # b) Exclude trades associated to new issuances (i.e. exclude all primary market transactions = keep only secondary
    # market transactions) # NOTE: Also here one has to potentially include changes suggested by Bessembinder et al. (2018)
    df_clean = df_clean[df_clean.TRDG_MKT_CD == "S1"]
    print('STEP 3.2.2: Keeping only secondary market transaction deletes {} transactions'.format(N_trnsct - len(df_clean)))
    N_trnsct = len(df_clean)

    ## c) Exclude trades that are reported after the bond's amount outstanding is reported by FISD as zero
    # Exclude all trades where the outstanding amount is reported to be zero but there is a reporting date for
    # the trade after the effective date when the outstanding amount is already reported to be zero
    df_clean['TRD_RPT_DT'] = pd.to_datetime(df_clean['TRD_RPT_DT'])
    df_clean['effective_date'] = pd.to_datetime(df_clean['effective_date'])
    df_clean = df_clean[((df_clean['TRD_RPT_DT'] > df_clean['effective_date']) & (df_clean.amount_outstanding == 0)) == False]
    print('Excluding trades  that are reported after the bonds amount outstanding is reported by FISD as zero deletes {} transactions'.
          format(N_trnsct - len(df_clean)))
    N_trnsct = len(df_clean)

    # d) Bessembinder et al. (2018) and Anand et al. (2021) exclude bonds with a reported trade size that exceeds the bond's
    # offer size.
    # Compute the traded quantity of bonds -> This can never be larger than what has been issued in the first place.
    df_clean['trd_quantity'] = df_clean.ENTRD_VOL_QT / df_clean.principal_amt
    # Select only those bonds for which the trade size is smaller than the issue size
    #df_clean['D_trd_size_offer_size'] = (df_clean.trd_quantity > df_clean.offering_amt) * 1
    df_clean['D_trd_size_offer_size'] = (df_clean.trd_quantity > df_clean.offering_amt) * 1
    df_clean['D_trd_size_offer_size_max'] = df_clean.groupby('CUSIP_ID')['D_trd_size_offer_size'].transform('max')
    # Only keep bonds where the trading amount is indeed lower than the offering amount, i.e. exclude those bonds
    # where the maximum is 1 (which implies that the trade size is indeed larger than the offer size).
    #df_clean = df_clean.loc[df_clean.D_trd_size_offer_size_max == 0]
    df_clean = df_clean.loc[df_clean.D_trd_size_offer_size == 0]

    print('STEP 3.2.3: Excluding trades with a reported trade size that exceeds the bonds offer size deletes {} transactions'.
        format(N_trnsct - len(df_clean)))

    # Print finalization statement
    print('STEP 3.2:The trade-level cleaning is done')

    return df_clean


def clean_df_general(df_in, dict_spec):
    """
    Perform general cleaning steps to make the final dataset easily handable

    a) Only keep the relevant variables in the dataset
    b) Transform all column names from all capital to small letters
    c) Only keep the trade if the execution date is after the offering date
    d) Delete all bond transactions w/o rating information
    e) Exclude trades that are reported priorm to the official start of TRACE
    f) Exclude specific dealers if they report implausibly large transactions, etc.

    Parameters:
    -----------
    df_in (DataFrame): Input DataFrame that is to be cleaned

    Returns:
    --------
    df_out       Output DataFrame that is cleaned according to the above bullet points

    """

    df_clean_df = df_in.copy()
    print("")
    print('STEP 4.1: The general cleaning steps are started')

    # Get the initial (pre-cleaning) number of transactions
    N_trnsct = len(df_clean_df)

    # a) Only keep the specified variables in the final dataset
    df_clean_df = df_clean_df[dict_spec['dataset_clean']['varlist']]

    # b) Change all variable names from capital letters to small letters
    df_clean_df.columns = map(str.lower, df_clean_df.columns)

    # c) Keep only trades if the trade execution date is after the offering date (error exists for few bonds)
    df_clean_df = df_clean_df.loc[df_clean_df.trd_exctn_tm >= df_clean_df.offering_date]
    print('STEP 4.1.1: Keeping only trades if the trade execution date is after the offering date deletes {} transactions'.format(
        N_trnsct - len(df_clean_df)))
    N_trnsct = len(df_clean_df)

    # d) Keep only trades if the trade execution date is not after the bond's maturity date
    df_clean_df = df_clean_df.loc[df_clean_df.trd_exctn_tm <= df_clean_df.maturity]
    print('STEP 4.1.2: Keeping only trades if the trade execution date is not after the bonds maturity date deletes {} transactions'.format(
        N_trnsct - len(df_clean_df)))
    N_trnsct = len(df_clean_df)

    # e) Delete all bond transactions without rating information
    df_clean_df = df_clean_df.dropna(subset=['rating'])
    print('STEP 4.1.3: Keeping only trades with existing rating information deletes {} transactions'.format(
            N_trnsct - len(df_clean_df)))
    N_trnsct = len(df_clean_df)

    # f) Only keep trades that are executed within the TRACE sample period (i.e. after 01.07.2002)
    df_clean_df = df_clean_df.loc[df_clean_df.trd_exctn_tm >= datetime(2002, 7, 1)]
    print('STEP 4.1.4: Keeping only trades that are executed within the TRACE sample period (i.e. after 01.07.2002) deletes {} transactions'.format(
        N_trnsct - len(df_clean_df)))
    N_trnsct = len(df_clean_df)

    # g) Remove a dealer with an implausibly large transaction on 2014-01-17
    #df_clean_df = df_clean_df.loc[(df_clean_df.rptg_party_id != '1663f11b4f68a7006d7e1eb4ff7348414351ecfe') &
    #                    (df_clean_df.cntra_party_id != '1663f11b4f68a7006d7e1eb4ff7348414351ecfe')]
    # h) Delete transactions with missing, zero or negative prices and prices that are larger than 220 (see Asquith et al. (2016)
    #    Yesol Huh uses 250
    # Exclude transactions with missing prices
    df_clean_df = df_clean_df.dropna(subset = ['rptd_pr'])
    # Exclude transactions with negative or zero prices
    df_clean_df = df_clean_df.loc[df_clean_df.rptd_pr > 0]
    # Exclude transactions with prices larger than 220
    df_clean_df = df_clean_df.loc[df_clean_df.rptd_pr < 220]
    print('STEP 4.1.5: Keeping only trades that with nonmissing and positive prices deletes {} transactions'.format(
            N_trnsct - len(df_clean_df)))
    N_trnsct = len(df_clean_df)
    # i) Delete transactions with missing, zero or negative traded volume
    df_clean_df = df_clean_df.dropna(subset=['entrd_vol_qt'])
    df_clean_df = df_clean_df.loc[df_clean_df.entrd_vol_qt > 0]
    print('STEP 4.1.6: Keeping only trades that with nonmissing and positive traded volume deletes {} transactions'.format(
        N_trnsct - len(df_clean_df)))

    # Print finalization statement
    print('The general cleaning steps are finalized')

    return df_clean_df


def clean_trd_days(df_in, project_path):
    """
    Implement additional cleaning steps that cleans the dataset from transactions according to the date when they are
    executed.
        1) Only keep those transactions executed on a weekday
        2) Only keep transactions for which there exists a TRACE reporting file on this specific date
        3) Only keep those transactions that are not executed on a federal holiday.

    Parameters:
    -----------
    df_in (DataFrame): Input dataframe containing the fully concatenated and cleaned dataset
    project_path: Add the input path to the project folder

    Returns:
    --------
    df_clean_dates (DataFrame): DataFrame additionally cleaned by the respective transaction dates
    """
    print("")
    print('STEP 5.1: Cleaning of the transaction dates: started')

    df_clean_dates = df_in.copy()

    # 1) Drop bonds traded on weekends (this is always a very low number)
    df_clean_dates = df_clean_dates.loc[df_clean_dates.week_day.isin([0, 1, 2, 3, 4])]

    # 2) Keep only those transactions that are executed on a date where there exists a  TRACE reporting file
    # (as dates without a reported file sometimes have an exceptionally small number of trades).
    # The TRACE_rpt_dates.pkl is constructed in the read_TRACE.py script.
    TRACE_rpt_days = pd.read_pickle(project_path + '/' + 'bld/data/TRACE/TRACE_info/TRACE_rpt_dates.pkl')
    TRACE_rpt_days = [datetime.strptime(date, '%Y-%m-%d').date() for date in TRACE_rpt_days]
    df_clean_dates = df_clean_dates.loc[df_clean_dates.trd_exctn_dt.isin(TRACE_rpt_days)]

    # 3) Keep only those transactions that are not executed on a federal holiday. The federal holidays are hand-collected
    # and are stored in the file US_holiday_list.py in the data specification folder. Note, sometimes also
    # exceptional dates such as the early closure of the corp. bond market due to Hurricane Cathrina are excluded.
    holiday_date_list = get_US_holiday_dates()
    df_clean_dates = df_clean_dates.loc[df_clean_dates.trd_exctn_dt.isin(holiday_date_list) == False]

    # Exclude the christmas days
    df_clean_dates = df_clean_dates[((df_clean_dates.month == 12) & (df_clean_dates.day.isin([24, 25]))) == False]

    print('STEP 5.1: Cleaning of the transaction dates: finalized')

    return df_clean_dates


def add_clean_trading_dates(df_in, project_path):
    """
    Add the necessary date variables and drop all transactions executed on a weekend.

    Parameters:
    -----------
    df_in (DataFrame): Input DataFrame

    Returns:
    --------
    df_add_dates (DataFrame): DataFrame containing the necessary date variables
    """

    df_add_dates = df_in.copy()

    # Define year, month and week identifiers
    df_add_dates.loc[:, 'year'] = df_add_dates.trd_exctn_tm.dt.year
    df_add_dates.loc[:, 'month'] = df_add_dates.trd_exctn_tm.dt.month
    df_add_dates.loc[:, 'day'] = df_add_dates.trd_exctn_tm.dt.day
    df_add_dates.loc[:, 'quarter'] = df_add_dates.trd_exctn_tm.dt.quarter
    df_add_dates.loc[:, 'week'] = df_add_dates.trd_exctn_tm.dt.isocalendar()['week']
    # Get the respective day of the week [0=Monday, 1=Tuesday, etc.]
    df_add_dates.loc[:, 'week_day'] = df_add_dates.trd_exctn_tm.dt.dayofweek

    # Clean the dataset for non-trading days (e.g. the public holidays). These dates are specified in the script
    # prepare_variables.py
    df_add_dates = clean_trd_days(df_add_dates, project_path)

    return df_add_dates








