import pandas as pd
from time import time

def insert_space(string, integer):
    adjust_retval = string[0:integer] + ' ' + string[integer:]
    return adjust_retval

gs_init_data = pd.read_excel('arbiter_etl/data/import/v3.master-schedule.2021-08-28T170118.130-0400.xlsx', sheet_name='Matches')
gs_init_data_df = pd.DataFrame(gs_init_data, columns = ['ID', 'Date', 'Start Time', 'Age', 'Home Team', 'Away Team', 'Venue', 'Pitch'])

gs_delta_data = pd.read_excel('arbiter_etl/data/import/delta/v3.master-schedule.2021-09-05T132925.202-0400.xlsx', sheet_name='Matches')
gs_delta_data_df = pd.DataFrame(gs_delta_data, columns = ['ID', 'Date', 'Start Time', 'Age', 'Home Team', 'Away Team', 'Venue', 'Pitch'])


delta = gs_init_data_df.eq(gs_delta_data_df).all(axis=1) 
delta_true = gs_delta_data_df[gs_init_data_df.eq(gs_delta_data_df).all(axis=1)]
delta_false = gs_delta_data_df[gs_init_data_df.eq(gs_delta_data_df).all(axis=1)==False]

delta_false['Start Time'] = delta_false['Start Time'].apply(lambda x: insert_space(x, 5) )

print (delta)
print (delta_true)
print(delta_false)

delta_false.to_csv('arbiter_etl/data/import/delta/delta.' + str(time()*1000) + '.csv', index = False, header=True)
