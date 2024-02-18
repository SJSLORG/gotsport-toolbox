# Arbiter Operations
# 1. import schedule
import pandas as pd
from time import time

def custom_replace(row, prefix):
    club_key = f'{prefix} Club'
    team_key = f'{prefix} Team'
    first_club = row[club_key][:5]
    starts_with_club = row[team_key].strip().startswith(first_club)

    if starts_with_club:
        result = row[team_key].strip()
    else:
        result = row[club_key].strip() + ' ' + row[team_key].strip()  # No change if the first 10 letters don't match

    return result


    
def insert_space(string, integer):
    adjust_retval = string[0:integer] + ' ' + string[integer:]
    return adjust_retval

root = 'arbiter_etl/data/spring2024/'
export_file = root + 'export/'
import_file_folder = root + 'import/'
import_file_name = '2024-03-final-2-7-24.master-schedule.2024-02-18T133800.215-0500.xlsx'
# import_file_path = 'arbiter_etl/data/fall2023/import/a-v1.master-schedule.2023-10-22T202147.703-0400.xlsx'
# GotSport Schedule
# import_file_full = 'data/fall2023/import/a-v1.master-schedule.2023-08-15T203222.530-0400.xlsx'
gs_data = pd.read_excel(import_file_folder  +  import_file_name, sheet_name='Matches')
# gs_data = pd.read_excel(import_file_full, sheet_name='Matches')
gs_data_df = pd.DataFrame(gs_data, columns = ['Date', 'Start Time', 'ID', 'Age', 'Home Club', 'Home Team', 'Away Club', 'Away Team', 'Venue', 'Pitch'])
# gs_data_df = pd.DataFrame(gs_data, columns = ['Home Team', 'Away Team', 'Age'])

gs_data_df['Away Team'] = gs_data_df['Away Team'].replace(gs_data_df['Away Club'].tolist(), '', regex=True, limit=1)
gs_data_df['Home Team'] = gs_data_df['Home Team'].replace(gs_data_df['Home Club'].tolist(), '', regex=True, limit=1)

# gs_data_df['Away Team'] = gs_data_df['Away Club'] + ' ' + gs_data_df['Away Team'].str[1:]
# gs_data_df['Home Team'] = gs_data_df['Home Club'] + ' ' + gs_data_df['Home Team'].str[1:]

gs_data_df['Home Team'] = gs_data_df.apply(custom_replace, prefix='Home', axis=1)
gs_data_df['Away Team'] = gs_data_df.apply(custom_replace, prefix='Away', axis=1)

# gs_data_df['Home Team'] = gs_data_df['Home Team'].str.replace(gs_data_df['Home Club'].str, '')
gs_data_df['Home Team'] = gs_data_df['Home Team'].str.upper()
gs_data_df['Away Team'] = gs_data_df['Away Team'].str.upper()


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
gs_data_df['Custom Game ID'] = ''
gs_data_df['BillTo'] = ''

# gs_data_df['Start Time'] = gs_data_df['Start Time'].apply(lambda x: insert_space(x, 5) )

# gotsport export columns
#     ID	Match Number	Round	Date	Start Time	End Time	Venue	Venue State	Pitch	Home Club	Home Team	Home Score	Away Club	Away Team	Away Score	Status	Age	Gender	Division	Broadcaster	Time Slot ID
# arbiter import columns
#     Date	Time	Game ID	Custom Game ID	Partner	Sport	Level	Home Team	Away Team	Site	Subsite	BillTo
# 
# mapping
#     Date -> Date
#     Time -> Start Time
#     Game ID -> ID
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
    'ID': 'Game ID',
    # 'ID': 'Custom Game ID', 
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
# gs_data_df.to_csv( 'export/ArbiterImport.' + str(time()*1000) + '.csv', index = False, header=True)

