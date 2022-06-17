# Arbiter Operations
# 1. import schedule
import pandas as pd
from time import time

def insert_space(string, integer):
    adjust_retval = string[0:integer] + ' ' + string[integer:]
    return adjust_retval

root = 'arbiter_etl/data/spring2022/'
export_file = root + 'export/'

# GotSport Schedule
gs_data = pd.read_excel('arbiter_etl/data/spring2022/import/a-275-v1.master-schedule.2022-02-06T225309.746-0500.xlsx', sheet_name='Matches')
gs_data_df = pd.DataFrame(gs_data, columns = ['Date', 'Start Time', 'ID', 'Age', 'Home Team', 'Away Team', 'Venue', 'Pitch'])
# gs_data_df = pd.DataFrame(gs_data, columns = ['Home Team', 'Away Team', 'Age'])

# Arbiter Mapping
arbiter_teamsMapping = pd.read_excel(root + 'lookup/ArbiterMappings.xlsx', sheet_name='TeamsMap', usecols=['GSMAPCONCAT', 'ARBITERMAP'])
arbiter_teamsMapping_dict = arbiter_teamsMapping.set_index('GSMAPCONCAT')['ARBITERMAP'].to_dict()
gs_data_df = gs_data_df.replace(arbiter_teamsMapping_dict)

arbiter_levelsMapping = pd.read_excel(root + 'lookup/ArbiterMappings.xlsx', sheet_name='LevelsMap', usecols=['AgeMap', 'LevelMap'])
arbiter_levelsMapping_dict = arbiter_levelsMapping.set_index('AgeMap')['LevelMap'].to_dict()
gs_data_df = gs_data_df.replace(arbiter_levelsMapping_dict)

arbiter_sitesMapping = pd.read_excel(root + 'lookup/ArbiterMappings.xlsx', sheet_name='SitesMap', usecols=['GSVENUEMAP', 'ARBITERSITEMAP'])

gs_data_df = pd.merge(gs_data_df, arbiter_sitesMapping, how='left', left_on=['Venue'], right_on = ['GSVENUEMAP'])

# arbiter_sitesMapping_dict = arbiter_sitesMapping.set_index('GSVENUEMAP')['ARBITERSITEMAP'].to_dict()
# gs_data_df = gs_data_df.replace(arbiter_sitesMapping_dict)

# new_df = pd.merge(A_df, B_df,  how='left', left_on=['A_c1','c2'], right_on = ['B_c1','c2'])

arbiter_subsitesMapping = pd.read_excel(root + 'lookup/ArbiterMappings.xlsx', sheet_name='SubsitesMap', usecols=['ARBITERSITEMAP', 'GSSUBSITEMAP', 'ARBITERSUBSITEMAP'])
arbiter_subsitesMapping = arbiter_subsitesMapping.set_index(['ARBITERSITEMAP', 'GSSUBSITEMAP'])
arbiter_subsitesMapping = arbiter_subsitesMapping.loc[arbiter_subsitesMapping.index.dropna()]

gs_data_df = pd.merge(gs_data_df, arbiter_subsitesMapping, how='left', left_on=['ARBITERSITEMAP', 'Pitch'], right_on = ['ARBITERSITEMAP', 'GSSUBSITEMAP'])
print(gs_data_df)

# arbiter_subsitesMapping_dict = arbiter_subsitesMapping['ARBITERSUBSITEMAP'].to_dict()

# gs_data_df = gs_data_df.replace(arbiter_subsitesMapping_dict)

gs_data_df['Partner'] = 'GotSport'
gs_data_df['Sport'] = 'Soccer'
gs_data_df['Game ID'] = ''
gs_data_df['BillTo'] = ''

gs_data_df['Start Time'] = gs_data_df['Start Time'].apply(lambda x: insert_space(x, 5) )

# gotsport export columns
#     ID	Match Number	Round	Date	Start Time	End Time	Venue	Venue State	Pitch	Home Club	Home Team	Home Score	Away Club	Away Team	Away Score	Status	Age	Gender	Division	Broadcaster	Time Slot ID
# arbiter import columns
#     Date	Time	Game ID	Custom Game ID	Partner	Sport	Level	Home Team	Away Team	Site	Subsite	BillTo
# 
# mapping
#     Date -> Date
#     Time -> Start Time
#     Game ID -> BLANK
#     Custom Game ID -> ID
#     Partner -> GotSport
#     Sport -> Soccer
#     Level -> Age - needs matcher
#     Home Team -> Home Team - needs matcher
#     Away Team -> Away Team - needs matcher
#     ---- -> Venue - needs matcher
#     Site - > ARBITERSITEMAP
#     ---- -> Pitch - needs matcher
#     Subsite -> ARBITERSUBSITEMAP - needs matcher
#     BillTo - BLANK


gs_data_df = gs_data_df.rename(columns={
    'ID': 'Custom Game ID', 
    'Start Time': 'Time', 
    'Age': 'Level', 
    'ARBITERSITEMAP' : 'Site', 
    'ARBITERSUBSITEMAP' : 'Subsite'}, 
    errors='raise')

gs_data_df = gs_data_df.drop(['Venue', 'Pitch', 'GSVENUEMAP'], axis=1)

gs_data_df = gs_data_df[[ 'Date', 'Time', 'Game ID', 'Custom Game ID', 'Partner', 'Sport', 'Level', 'Home Team', 'Away Team', 'Site', 'Subsite', 'BillTo']]

print (gs_data_df)

# now = datetime.now() # current date and time

gs_data_df.to_csv(export_file + 'ArbiterImport.' + str(time()*1000) + '.csv', index = False, header=True)

