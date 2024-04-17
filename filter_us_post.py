# -------------------------------------------------------------------------------
# Name:        filter_us_post
# Purpose:     pre-process US Post office dataset to make suitable for viz.
#
# Author:      Kevin Whelan
#
# Created:     01/04/2024
# Copyright:   (c) Kevin Whelan 2024
# Licence:     MIT
# -------------------------------------------------------------------------------

import pandas as pd

# read original data file
df = pd.read_csv('post_offices.csv')

# create year range
years = range(1772, 2001)

# remove states with MI/HO and VAy
df.drop(df[df['state'] == 'MI/OH'].index, inplace=True)
df.drop(df[df['state'] == 'VAy'].index, inplace=True)
df.fillna(0, inplace=True)
df['established'] = df['established'].astype(int)
df['discontinued'] = df['discontinued'].astype(int)

# rename DC (old name for Washington) to WA
df.loc[df['state'] == 'DC', 'state'] = 'WA'

# group data by state
df_g = df.groupby('state')
groups = dict(list(df_g))

data_dict = {}

for state in groups.keys():
    print(f'Processing state {state}')
    data_dict[state] = {}
    # extract row for this state
    state_data = groups[state]
    running_total = 0
    for idx, year in enumerate(years):

        data_dict[state][year] = {
            "established": 0,
            "discontinued": 0,
            "operating": 0
        }

        if year in state_data['established'].values:
            # one or more PO opened in this year
            year_data = state_data[state_data['established'] == year]
            data_dict[state][year]['established'] += len(year_data)
            running_total += len(year_data)
        if year in state_data['discontinued'].values:
            # one or more PO closed in this year
            year_data = state_data[state_data['discontinued'] == year]
            data_dict[state][year]['discontinued'] += len(year_data)
            running_total -= len(year_data)
        # recording number of operating post offices in that year
        data_dict[state][year]['operating'] = running_total

# Convert the nested dictionary into a DataFrame
out_df = pd.DataFrame.from_dict({(state, year): values
                                 for state, years in data_dict.items()
                                 for year, values in years.items()}, orient='index')

# Reset the index to separate the keys into columns
out_df.reset_index(inplace=True)

# Rename the columns
out_df.columns = ['code', 'Year', 'Established', 'Discontinued', 'Operating']

# add in additional columns for state name and state ID for use in choropleth
state_code_df = pd.read_csv('codes.csv')
state_code_df.drop(['category'], axis=1, inplace=True)
result_df = pd.merge(out_df, state_code_df, on='code', how='left')

# add in additional column for ID to link with GeoJSON choropleth map
geo_code_df = pd.read_csv('population_engineers_hurricanes.csv')
geo_code_df.drop(['population', 'engineers', 'hurricanes'], axis=1, inplace=True)
result_df = pd.merge(result_df, geo_code_df, on='state', how='left')
result_df['id'] = result_df['id'].astype(int)

# Write DataFrame to CSV file
result_df.to_csv('us_post_office_processed.csv', index=False)
