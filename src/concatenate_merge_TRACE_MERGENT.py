"""Concatenate the yearly dataset and merge relevant variables from:
 i)   rating data:      Rating data from MERGENT FISD
 ii)  issue data:       Information on bond type, amount outstanding, etc.
 iii) bond info data:   Information on 144-A status, etc.

Step 1: Prepare and merge the ratings data to the transaction data. Note that sometimes the rating date is on a non-
        trading date. In this cases, I merge the rating to the next available date. Note that the select_unmatchable
        function is not necessary but useful to check if all non directly mergeable ratings are properly merged.

Step 2: Concatenate the yearly dataset to one large dataset. For computational reasons, merge the issue and the bond
        info data already during the concatenate-step. Finally, save the concatenated dataset as pickle format.
"""

import pandas as pd
pd.options.mode.chained_assignment = None

# Import the concatenating function from Dick-Nielsen & Poulsen (2019)
from clean_TRACE import harmon_pre_post_data

#########
# Step 1: Prepare and merge ratings data
########
def rd_cl_ratings(path, rating_varlist):
    """
    Prepare the rating information. In particular, map the ratings to a numeric index. Follow Becker et al. (2021) in
    cleaning the ratings.

    1) Only keep ratings from Fitch, Moody's and S&P
    2) If we have two ratings for a bond on the same rating date -> keep the lower one

    Parameters
    ----------
    None\

    Returns:
    --------
    df_ratings (DataFrame): DataFrame containing the cleaned rating information

    """

    # Load in the Mergent FISD rating data
    df_ratings = pd.read_pickle(path + '/src/original_data/Mergent_FISD/ratings.pkl')

    # Define the mapping of ratings to integers\n",
    remap_ratings_integer_dict = {
        # triple A
        'AAA': 1, 'Aaa': 1,
        # double A
        'Aa1': 2, 'Aa2': 3, 'Aa3': 4, 'AA+': 2, 'AA-': 3, 'AA': 4, 'Aa': 3,
        # A
        'A': 6, 'A+': 5, 'A-': 7, 'A1': 5, 'A2': 6, 'A3': 7,
        # tripe B
        'BBB+': 8, 'BBB': 9, 'BBB-': 10, 'Baa1': 8, 'Baa2': 9, 'Baa3': 10, 'Baa': 9,
        # double B
        'BB+': 11, 'BB': 12, 'BB-': 13, 'Ba1': 11, 'Ba2': 12, 'Ba3': 13, 'Ba': 12,
        #  B
        'B+': 14, 'B': 15, 'B-': 16, 'B1': 14, 'B2': 15, 'B3': 16,
        #  triple C
        'CCC+': 17, 'CCC': 18, 'CCC-': 19, 'Caa1': 17, 'Caa2': 18, 'Caa3': 19, 'Caa': 17,
        #  remaining C
        'CC': 20, 'Ca': 20, 'C': 21,
        # D
        'DDD': 22, 'DD': 23, 'D': 24, 'NR': 25
    }
    df_ratings.loc[:,'rating_numeric'] = df_ratings['rating'].copy()

    df_ratings = df_ratings.replace({'rating_numeric': remap_ratings_integer_dict})

    # Keep only the non-string ratings
    df_ratings = df_ratings[df_ratings['rating_numeric'].map(type) != str]

    # Only keep the three main rating agencies (Moddy's, Fitch, S&P) in the sample -> Excludes the Duff and Phelps Rating
    df_ratings = df_ratings.loc[df_ratings.rating_type.isin(['FR', 'MR', 'SPR'])]

    # Make sure that if there are more than one ratings on a given date, only keep the lowest one
    # For each CUSIP and each rating date, compute the maximum rating -> Exclude all ratings that are lower than the maximum rating
    # This will exclude all the ratings
    df_ratings['min_rating_cusip_date'] = df_ratings.groupby(['rating_date', 'complete_cusip'])['rating_numeric'].transform('min')
    # Generate an indicator if there are more than one rating available at a
    df_ratings['D_more_one_rating_tmp'] = (
        df_ratings.groupby(['rating_date', 'complete_cusip'])['rating_numeric'].transform('count')
    )
    df_ratings['D_more_one_rating'] = (df_ratings.D_more_one_rating_tmp > 1) * 1
    df_ratings = df_ratings.drop(columns=['D_more_one_rating_tmp'])
    # df_ratings['D_more_one_rating'] = (
    #     df_ratings.groupby(['rating_date', 'complete_cusip'])['rating_numeric'].transform(
    #     lambda x: len(x) > 1) * 1
    # )
     
    # Bond-dates where there is no conflicting rating
    df_ratings_single_rating = df_ratings.loc[df_ratings.D_more_one_rating == 0]
     
    # Bond-dates where there is more than one rating
    df_ratings_more_one_rating = df_ratings.loc[df_ratings.D_more_one_rating == 1]
    # Set the rating equal to the minimum rating
    df_ratings_more_one_rating.loc[:, 'rating_numeric'] = df_ratings_more_one_rating.loc[:, 'min_rating_cusip_date'].copy()
    # Drop the duplicates per bond-date, i.e. keep only the minimum rating
    df_ratings_more_one_rating = df_ratings_more_one_rating.drop_duplicates(subset=['rating_date', 'complete_cusip'])
     
    # Concatenate the files to one rating dataset
    df_ratings = pd.concat([df_ratings_single_rating, df_ratings_more_one_rating])
     
    # Only maintain the relevant variables
    df_ratings = df_ratings[rating_varlist + ['rating_numeric']]
     
    # Rename the variables to common routine to have common merge names
    df_ratings = df_ratings.rename(columns={'complete_cusip': 'CUSIP_ID'})

    # Adjust the time format of the rating year and date variable to allow for as_of merging
    df_ratings['rating_year'] = df_ratings['rating_date'].dt.year
    df_ratings['rating_date'] = df_ratings['rating_date'].dt.date

    return df_ratings


#def rd_cl_ratings(path, ratings_varlist):
#    """
#    Read and clean the raw ratings data such that they can be merged to the transaction data.

#    Parameters:
#    -----------
#    path (str): Project path
#    ratings_varlist (list): List with all variables that should be kept in the transaction dataset
#
#    Returns:
#    --------
#    df_ratings (DataFrame): Cleaned rating data#
#
#    """
#
#    df_ratings = pd.read_pickle(path + '/' + 'src/original_data/Mergent_FISD/ratings.pkl')
#    # Keep only specified variables
#    df_ratings = df_ratings[ratings_varlist]
#    # Rename the variables to common routine to have common merge names
#    df_ratings = df_ratings.rename(columns={'complete_cusip':'CUSIP_ID', 'reason':'rating_reason'})
#    # Adjust the time format of the rating year and date variable to allow for as_of merging
#    df_ratings['rating_year'] = df_ratings['rating_date'].dt.year
#    df_ratings['rating_date'] = df_ratings['rating_date'].dt.date
#
#    return df_ratings


def merge_transact_rating(path, df_transact, dict_spec, df_rating):
    """
    Merge the transaction data (TRACE) and the ratings data (MERGENT FISD). Importantly, some ratings are issued on a
    date when the bond is not traded. In case a rating date can not directly be merged to a transaction date, I assign
    the rating to the closest transaction in the future. This assures that the rating information is only assigned to
    trades where it was already known to the market. This merging can be achieved using merge_asof()

    Parameters:
    -----------
    df_transact (DataFrame): Transaction dataset
    df_rating (DataFrame): Rating dataset
    dict_spec (dictionary): Dictionary containing the dataset specifications

    Returns:
    --------
    merge_transact_rating (DataFrame):  Output DataFrame consisting of both the transaction and the rating data

    """

    # Read-in the ratings data
    #df_rating = rd_cl_ratings(path, dict_spec['ratings']['varlist'])

    # Add a common date identifier and sort values (transaction data)
    # Note: merge_asof requires no missing values in the merge variable
    df_transact = df_transact.dropna(subset=['TRD_EXCTN_DT'])
    df_transact['date'] = pd.to_datetime(df_transact['TRD_EXCTN_DT'])
    # merge_asof requires variables to be sorted first along the merge variable
    df_transact = df_transact.sort_values(['date', 'CUSIP_ID'])

    # Add a common date identifier and sort values (rating data)
    # Note: merge_asof requires no missing values in the merge variable
    df_rating['date'] = pd.to_datetime(df_rating['rating_date'])
    # Restrict the rating data to one year prior to the earliest transaction. This avoids that the
    # matching algorithm assigns only ratings that are not older than a year. If there was no
    # such rating, the rating observation is missing.
    df_rating = df_rating.loc[df_rating.rating_year >= dict_spec['sample_time_span'][0]-1]
    # merge_asof requires variables to be sorted first along the merge variable
    df_rating = df_rating.sort_values(['date', 'CUSIP_ID'])
    # Apply the merging. All ratings that can be directly matched to a trading date on the CUSIP level are
    # directly merged. All ratings that cannot be directly merged are merged to the next closest transaction (looking
    # forward in time).
    merge_transact_rating = (pd.merge_asof(df_transact, df_rating, on='date', left_by='CUSIP_ID', right_by='CUSIP_ID',
                                           direction='backward').sort_values(
        ['CUSIP_ID', 'date'])
    )

    return merge_transact_rating



#########
# Step 2:
## 2.1) Concatenate the yearly data
## 2.2) Merge the issue and bond info data to the transaction data
########

def conct_merge_data(path, dict_spec):
    """
    Concatenate the yearly cleaned TRACE transaction data over the entire sample period available. Due to the large
    sample size issue and rating data have to be merged directly after reading in the transaction data as o.w.
    the final dataset gets too large (the reason is that Python pre-allocates a lot of memory during the merging step).

    Parameters:
    -----------
    path (string): Project path
    dict_spec (dict): Final dataset specifications

    Returns:
    --------
    df_concat (DataFrame): Return one whole dataset where all transaction data are concatenated

    """
    print("")
    print('STEP 2: The concatenation and cleaning step has started. Finished years will be displayed')

    # Read in the ratings dataset
    df_ratings = rd_cl_ratings(path, dict_spec['ratings']['varlist'])
    
    # Read in the issue data
    df_issue = pd.read_pickle(path + '/src/original_data/Mergent_FISD/issue_data.pkl')

    # Read in the bond info data
    df_bond_info = pd.read_pickle(path + '/bld/data/TRACE/TRACE_raw_clean/bond_info.pkl')

    # Subtract 1 year from the beginning year to account for Python 0 counting (i.e. actually include that year)
    for year in range(dict_spec['sample_time_span'][1], dict_spec['sample_time_span'][0]-1, -1):
        print('Dataset concatenated until:{}'.format(year))
        # last year of the sample
        if (year == dict_spec['sample_time_span'][1])&(year>2012):
            df_concat = (harmon_pre_post_data(
                pd.read_pickle(path + '/bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}.pkl'.format(year)), pre_post_id='POST')
            [dict_spec['transactions']['varlist']]
            )
            # Merge the rating information
            df_concat = merge_transact_rating(path, df_concat, dict_spec, df_ratings)
            # Merge the issue information
            df_concat = df_concat.merge(df_issue[dict_spec['issue_data']['varlist']], on='CUSIP_ID', how='left')
            # Merge with the bond info data
            df_concat = df_concat.merge(df_bond_info[dict_spec['bond_info']['varlist']], on='CUSIP_ID', how='left')
        # 2013 - last year
        elif (year < dict_spec['sample_time_span'][1]) & (year > 2012):
            df_tmp = (
                # Merge the new year data with the ratings data
                merge_transact_rating(path,
                # Read-in the transaction data of the new year
                harmon_pre_post_data(
                    pd.read_pickle(path + '/bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}.pkl'.format(year)),
                    pre_post_id='POST'
                )[dict_spec['transactions']['varlist']],
                # Merge the new data with the ratinf data
                dict_spec, df_ratings
                )
            )
            # Merge the issue information
            df_tmp = df_tmp.merge(df_issue[dict_spec['issue_data']['varlist']], on='CUSIP_ID', how='left')
            # Merge with the bond info data
            df_tmp = df_tmp.merge(df_bond_info[dict_spec['bond_info']['varlist']], on='CUSIP_ID', how='left')
            # Concatenate the previous year data and the newly read data
            df_concat = pd.concat([df_concat, df_tmp])
        # 2012
        elif year == 2012:
            # Concatenate the pre- and post dataset in 2012
            ## Post 06.02.2012
            df_tmp_post = (
                # Merge the new year data with the ratings data
                merge_transact_rating(path,
                    # Read-in the transaction data of the new year
                    harmon_pre_post_data(
                        pd.read_pickle(path + '/bld/data/TRACE/TRACE_raw_clean/TRACE_clean_2012_post.pkl'),
                        pre_post_id='POST'
                    )[dict_spec['transactions']['varlist']],
                    # Merge the new data with the ratinf data
                    dict_spec, df_ratings
                )
            )
            # Merge the issue information
            df_tmp_post = df_tmp_post.merge(df_issue[dict_spec['issue_data']['varlist']], on='CUSIP_ID', how='left')
            # Merge with the bond info data
            df_tmp_post = df_tmp_post.merge(df_bond_info[dict_spec['bond_info']['varlist']], on='CUSIP_ID', how='left')
            # Concatenate the previous year data and the newly read data
            df_concat = pd.concat([df_concat, df_tmp_post])
            ## Pre 06.02.2012
            df_tmp_prior = (
                # Merge the new year data with the ratings data
                merge_transact_rating(path,
                    # Read-in the transaction data of the new year
                    harmon_pre_post_data(
                        pd.read_pickle(path + '/bld/data/TRACE/TRACE_raw_clean/TRACE_clean_2012_prior.pkl'),
                        pre_post_id='PRE'
                    )[dict_spec['transactions']['varlist']],
                    # Merge the new data with the ratinf data
                    dict_spec, df_ratings
                )
            )
            # Merge the issue information
            df_tmp_prior = df_tmp_prior.merge(df_issue[dict_spec['issue_data']['varlist']], on='CUSIP_ID', how='left')
            # Merge with the bond info data
            df_tmp_prior = df_tmp_prior.merge(df_bond_info[dict_spec['bond_info']['varlist']], on='CUSIP_ID', how='left')
            # Concatenate the previous year data and the newly read data
            df_concat = pd.concat([df_concat, df_tmp_prior])
        # 2002-2011
        else:
            df_tmp = (
                # Merge the new year data with the ratings data
                merge_transact_rating(path,
                    # Read-in the transaction data of the new year
                    harmon_pre_post_data(
                        pd.read_pickle(path + '/bld/data/TRACE/TRACE_raw_clean/TRACE_clean_{}.pkl'.format(year)),
                        pre_post_id='PRE'
                    )[dict_spec['transactions']['varlist']],
                    # Merge the new data with the ratinf data
                    dict_spec, df_ratings
                )
            )
            # Merge the issue information
            df_tmp = df_tmp.merge(df_issue[dict_spec['issue_data']['varlist']], on='CUSIP_ID', how='left')
            # Merge with the bond info data
            df_tmp = df_tmp.merge(df_bond_info[dict_spec['bond_info']['varlist']], on='CUSIP_ID', how='left')
            # Concatenate the previous year data and the newly read data
            df_concat = pd.concat([df_concat, df_tmp])

    return df_concat

