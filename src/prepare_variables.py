"""
Use the fully concatenated and cleaned TRACE dataset as inputs and add necessary variables for the subsequent
computations.
"""

from data_specs.mapping_risk_weights_rating.mapping_risk_weights_rating import remap_ratings_dict, remap_ECRA_risk_weights, remap_ECRA_risk_weights_names
from datetime import date, datetime
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None


def create_necessary_vars(df_in):
    """Clean specific trading dates and add necessary default variables

    Parameters:
    -----------
    df_in (DataFrame): Input DataFrame that is to be cleaned

    Returns:
    --------
    df_add_vars (DataFrame): Output DataFrame that is cleaned according to the above bullet points
    
    """
    print("")
    print('STEP 7.1: Additional variable creation: finalized')
    df_add_vars = df_in.copy()

    # Build the trade size in USD
    df_add_vars['trd_size'] = (df_add_vars.entrd_vol_qt / df_add_vars.principal_amt) * ((df_add_vars.rptd_pr / 100) * df_add_vars.principal_amt)
    # Express the trade size in USD Million
    df_add_vars['trd_size'] = df_add_vars['trd_size'] / 1000000

    # Build the indicator for whether it is an agency transaction
    # Define an indicator for the buying capacity of the reporting entitiy. This is the only side that can be trusted.
    df_add_vars['agency'] = np.nan
    df_add_vars.loc[df_add_vars.rpt_side_cd == 'B', 'agency'] = (
        df_add_vars.loc[df_add_vars.rpt_side_cd == 'B', 'buy_cpcty_cd']
    )
    df_add_vars.loc[df_add_vars.rpt_side_cd == 'S', 'agency'] = (
        df_add_vars.loc[df_add_vars.rpt_side_cd == 'S', 'sell_cpcty_cd']
    )
    # Delete the raw variables necessary to the agency indicator classification
    df_add_vars = df_add_vars[df_add_vars.columns.drop(['buy_cpcty_cd', 'sell_cpcty_cd'])]

    print('Additional variable creation: finalized')

    return df_add_vars


def define_event_time_week(df_in):
    """
    Attach to every date in the sample the respective event day and event week. This is important as in the
    subsequent quarter-end analyses there are frequently analyses requiring the usage of event time. In a last step,
    add the quarter event dummies. This is necessary as we often have to compare different quarter-ends within a
    quarter. In this regard, it is very important that we do not group by quarter as this would essentially imply,
    for instance, that we compare the end of quarter 4 with the beginning of quarter 4 whereas we would in fact like
    to compare the end of quarter 4 with the beginning of quarter 1 in the subsequent year.

    Parameters:
    -----------
    df_in (DataFrame): Input DataFrame that is partially cleaned

    Returns:
    --------
    df_add_event_time (DataFrame): Output DataFrame that is ammended by the respective event time variables
    """

    print("")
    print('STEP 7.2: Create necessary event-time variables')
    df_add_event_time = df_in.copy()

    # Define the event time. In particular, split the quarter in two parts in the middle (month 1.5, 4.5,...)
    # Define days from quarter beginning to the middle with increasing event time numbers and afterwards
    # decreasing towards the sample end.
    event_day_tmp = df_add_event_time.drop_duplicates(subset=['year', 'month', 'day']).sort_values('trd_exctn_dt')
    # Define an indicator variable for whether the respective day is in the first or second half of the given
    # quarter (1 = first, 0=second)
    event_day_tmp['first_half_quarter'] = ((event_day_tmp.month.isin([1, 4, 7, 10])) | (
            (event_day_tmp.month.isin([2, 5, 8, 11])) & (event_day_tmp.day <= 15))) * 1
    event_day_tmp['event_day_1'] = event_day_tmp.groupby(['year', 'quarter', 'first_half_quarter']).cumcount(
        ascending=True) + 1
    event_day_tmp['event_day_2'] = event_day_tmp.groupby(['year', 'quarter', 'first_half_quarter']).cumcount(
        ascending=False)
    event_day_tmp['event_day'] = (
            event_day_tmp['event_day_1'] * event_day_tmp['first_half_quarter'] +
            (event_day_tmp['event_day_2'] * (1 - event_day_tmp['first_half_quarter'])) * (-1)
    )
    event_day_tmp = event_day_tmp[['year', 'month', 'day', 'event_day']]
    # Merge the event time to the main dataset
    df_add_event_time = df_add_event_time.merge(event_day_tmp, on=['year', 'month', 'day'], how='left')

    ## Define the event week
    event_week_tmp = df_add_event_time.drop_duplicates(subset=['quarter', 'week']).sort_values(['week'])[['quarter', 'week']]
    event_week_tmp['first_half_quarter'] = (
            (event_week_tmp.week.isin(
                [1, 2, 3, 4, 5, 6, 7, 15, 16, 17, 18, 19, 20, 28, 29, 30, 31, 32, 33, 41, 42, 43, 44, 45, 46])
            ) * 1
    )
    event_week_tmp['event_week_1'] = event_week_tmp.groupby(['quarter', 'first_half_quarter']).cumcount(
        ascending=True) + 1
    event_week_tmp['event_week_2'] = event_week_tmp.groupby(['quarter', 'first_half_quarter']).cumcount(
        ascending=False)
    event_week_tmp['event_week'] = (
            event_week_tmp['event_week_1'] * event_week_tmp['first_half_quarter'] +
            (event_week_tmp['event_week_2'] * (1 - event_week_tmp['first_half_quarter'])) * (-1)
    )
    # Merge the event week to the main dataset
    df_add_event_time = df_add_event_time.merge(event_week_tmp[['quarter', 'week', 'event_week']], on=['quarter', 'week'],
                        how='left')
    # Implement necessary corrections due to differences in day/week structure
    df_add_event_time.loc[(df_add_event_time.quarter == 2) & (df_add_event_time.week == 13), 'event_week'] = 1
    df_add_event_time.loc[(df_add_event_time.quarter == 2) & (df_add_event_time.week == 14), 'event_week'] = 1
    df_add_event_time.loc[(df_add_event_time.quarter == 3) & (df_add_event_time.week == 26), 'event_week'] = 1
    df_add_event_time.loc[(df_add_event_time.quarter == 3) & (df_add_event_time.week == 27), 'event_week'] = 1
    df_add_event_time.loc[(df_add_event_time.quarter == 4) & (df_add_event_time.week == 39), 'event_week'] = 1
    df_add_event_time.loc[(df_add_event_time.quarter == 4) & (df_add_event_time.week == 40), 'event_week'] = 1
    # Sometimes the first week in the new year starts already in the old year -> Correct event week to 0
    df_add_event_time.loc[(df_add_event_time.month == 12) & (df_add_event_time.event_week > 0), 'event_week'] = 0

    # Build the quarter_event_dummy: This is important to guarantee that one uses the second half of the prior quarter
    # and the first half of the next quarter as quarter-end comparisons. Otherwise, one would compare e.g. the end
    # of quarter 1 with the beginning of quarter 1 instead of the end of quarter 1 with the beginning of quarter 2
    df_add_event_time['quarter_event_dummy'] = (
        # Quarter 4
        (((df_add_event_time.quarter == 1) & (df_add_event_time.event_day > 0)) | (
                (df_add_event_time.quarter == 4) & (df_add_event_time.event_day <= 0))) * 1 * 4 +
        # Quarter 3
        (((df_add_event_time.quarter == 4) & (df_add_event_time.event_day > 0)) | (
                (df_add_event_time.quarter == 3) & (df_add_event_time.event_day <= 0))) * 1 * 3 +
        (((df_add_event_time.quarter == 3) & (df_add_event_time.event_day > 0)) | (
                (df_add_event_time.quarter == 2) & (df_add_event_time.event_day <= 0))) * 1 * 2 +
        (((df_add_event_time.quarter == 2) & (df_add_event_time.event_day > 0)) | (
                (df_add_event_time.quarter == 1) & (df_add_event_time.event_day <= 0))) * 1 * 1
    )

    return df_add_event_time


def map_risk_weight_reg_period(df_in):
    """
    Map the rating categories to Basel III risk weights and define the regulatory periods following
    Bessembinder et al. (2018).

    The regulatory periods are as follows:

    1) Pre-crisis period: 07/2002 - 06/2007
    2) Crisis period: 07/2007 - 04/2009
    2) Post-crisis period: 05/2009 - 12/2014
    3) Regulation period: 01/2015 - end

    Note: Due to memory issues I only select individual columns for both the regulatory period and the Basel risk weights

    Parameters:
    -----------
    df_in (DataFrame): Input DataFrame that is partially cleaned

    Returns:
    --------
    df_map_rw (DataFrame): Output DataFrame containing both the regulatory periods and the risk weights mapped to the
                           respective Basel risk weights

    """

    # Extract the input data
    tmp_reg_period = df_in[['trd_exctn_tm']].copy()

    # Assign the regulatory period
    tmp_reg_period['reg_period'] = (
            (tmp_reg_period.trd_exctn_tm < datetime(2007, 7, 1)) * 1 * 1 +
            ((tmp_reg_period.trd_exctn_tm >= datetime(2007, 7, 1)) & (tmp_reg_period.trd_exctn_tm < datetime(2009, 5, 1))) * 1 * 2 +
            ((tmp_reg_period.trd_exctn_tm >= datetime(2009, 5, 1)) & (tmp_reg_period.trd_exctn_tm < datetime(2015, 1, 1))) * 1 * 3 +
            (tmp_reg_period.trd_exctn_tm >= datetime(2015, 1, 1)) * 1 * 4
    )

    tmp_remap_rw = df_in[['rating']].copy()

    # Remap the ratings (first to common categories, then to the ECRA risk weights)
    # remap ratings
    tmp_remap_rw['rating_remap'] = tmp_remap_rw.replace({'rating': remap_ratings_dict})
    # Map ratings to risk weights
    tmp_remap_rw['ECRA'] = tmp_remap_rw.rating_remap
    tmp_remap_rw = tmp_remap_rw.replace({'ECRA': remap_ECRA_risk_weights})

    return tmp_reg_period[['reg_period']], tmp_remap_rw[['ECRA']]


def merge_issue_info(df_in, path, dict_spec):
    """
    Merge the issue level information to the TRACE dataset.

    Parameters:
    -----------
    None

    Returns:
    --------
    df_SAS_raw_merge_issue (DataFrame): TRACE dataset containing the merged issue information

    """

    # Read in the Mergent issue data
    issue_data = pd.read_pickle(path + 'src/original_data/Mergent_FISD/' + 'issue_data.pkl')
    # Follow Bessembinder et al. (2018) in keeping only non-puttable U.S. Corporate Debentures and U.S.
    # Corporate Bank Notes (bond type = CDEB or USBN) with a reported maturity
    issue_data = issue_data.loc[issue_data.bond_type.isin(['CDEB', 'USBN'])]
    issue_data = (issue_data.loc[issue_data.putable == 'N']).dropna(subset=['maturity'])
    # Define the full CUSIP ID
    issue_data['CUSIP_ID'] = issue_data['issuer_cusip'] + issue_data['issue_cusip']

    # Specify the variables to keep from the issue data
    issue_data_var_list = dict_spec['issue_data']['varlist']
    issue_data_red = issue_data[issue_data_var_list]

    df_merge_issue = df_in.merge(issue_data_red, on = ['CUSIP_ID'], how = 'left')

    return df_merge_issue