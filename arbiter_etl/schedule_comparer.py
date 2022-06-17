# Dataframe for weekly arbiter
# GameID	FromDate	FromTime	ToDate	Status	SiteName	BillToName	SportName	LevelName	HomeTeams	AwayTeams
#
# Dataframe for weekly Gotsport
# ID	Match Number	Round	Date	Start Time	End Time	Venue	Venue State	Pitch	Home Club	Home Team	Home Score	Away Club	Away Team	Away Score	Status	Age	Gender	Division	Broadcaster	Time Slot ID	Host
# 
#
# Create comparable export from Gotsport to Arbiter
#
# Questions
#
# What games in GotSport are NOT in Arbiter?
# What games in Arbiter are NOT in GotSport
#
#
#
import pandas as pd
from time import time

gs_data = pd.read_excel('arbiter_etl/data/weekly/09262021/ArbiterImport.1632245966492.377.xlsx', sheet_name='Matches')
gs_data_df = pd.DataFrame(gs_data,columns=['HomeTeam', 'AwayTeam'])
#gs_data_df = gs_data_df.set_index(['HomeTeam', 'AwayTeam'])

ar_data = pd.read_excel('arbiter_etl/data/weekly/09262021/GamesViewFile.xlsx', sheet_name='Matches')
ar_data_df = pd.DataFrame(ar_data,columns=['HomeTeam', 'AwayTeam'])
#ar_data_df = ar_data_df.set_index(['HomeTeam', 'AwayTeam'])

#print(gs_data_df)
#print(ar_data_df)

# gs_data_df.compare(ar_data_df, align_axis=0)



delta = gs_data_df.eq(ar_data_df).all(axis=1)

delta_false = ar_data_df[gs_data_df.eq(ar_data_df).all(axis=1)==True]

#print(delta_false)

df_diff = pd.concat([gs_data_df,ar_data_df]).drop_duplicates(keep=False)

print(df_diff)

#delta_df = pd.DataFrame.isin(gs_data_df, ar_data_df)

#print (delta_df)

df_diff.to_csv('arbiter_etl/data/weekly/09262021/DIFF.' + str(time()*1000) + '.csv', index = False, header=True)
