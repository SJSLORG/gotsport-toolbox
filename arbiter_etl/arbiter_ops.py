import pandas as pd
from time import time

class ArbiterOperations:
    def __init__(self, root, import_file_folder, import_file_name, export_file):
        self.root = root
        self.import_file_folder = import_file_folder
        self.import_file_name = import_file_name
        self.export_file = export_file

    def custom_replace(self, row, prefix):
        club_key = f'{prefix} Club'
        team_key = f'{prefix} Team'

        value = row.get(club_key)

        if isinstance(value, str) and value.strip():
            first_club = value[:5]
        else:
            return

        starts_with_club = row[team_key].strip().startswith(first_club)

        if starts_with_club:
            result = row[team_key].strip()
        else:
            result = row[club_key].strip() + ' ' + row[team_key].strip()  # No change if the first 10 letters don't match

        return result

    def insert_space(self, string, integer):
        adjust_retval = string[0:integer] + ' ' + string[integer:]
        return adjust_retval

    def process_data(self):
        gs_data = pd.read_excel(self.import_file_folder + self.import_file_name, sheet_name='Matches')
        gs_data_df = pd.DataFrame(gs_data, columns=['Date', 'Start Time', 'ID', 'Age', 'Home Club', 'Home Team', 'Away Club', 'Away Team', 'Venue', 'Pitch'])

        gs_data_df['Away Team'] = gs_data_df['Away Team'].replace(gs_data_df['Away Club'].tolist(), '', regex=True)
        gs_data_df['Home Team'] = gs_data_df['Home Team'].replace(gs_data_df['Home Club'].tolist(), '', regex=True)

        gs_data_df['Home Team'] = gs_data_df.apply(self.custom_replace, prefix='Home', axis=1)
        gs_data_df['Away Team'] = gs_data_df.apply(self.custom_replace, prefix='Away', axis=1)

        gs_data_df['Home Team'] = gs_data_df['Home Team'].str.upper()
        gs_data_df['Away Team'] = gs_data_df['Away Team'].str.upper()

        arbiter_levelsMapping = pd.read_excel(self.root + '/lookup/ArbiterMappings.xlsx', sheet_name='LevelsMap', usecols=['AgeMap', 'LevelMap'])
        arbiter_levelsMapping_dict = arbiter_levelsMapping.set_index('AgeMap')['LevelMap'].to_dict()
        gs_data_df = gs_data_df.replace(arbiter_levelsMapping_dict)

        arbiter_sitesMapping = pd.read_excel(self.root + '/lookup/ArbiterMappings.xlsx', sheet_name='SitesMap', usecols=['GSVENUEMAP', 'ARBITERSITEMAP'])
        gs_data_df = pd.merge(gs_data_df, arbiter_sitesMapping, how='left', left_on=['Venue'], right_on=['GSVENUEMAP'])

        arbiter_subsitesMapping = pd.read_excel(self.root + '/lookup/ArbiterMappings.xlsx', sheet_name='SubsitesMap', usecols=['ARBITERSITEMAP', 'GSSUBSITEMAP', 'ARBITERSUBSITEMAP'])
        arbiter_subsitesMapping = arbiter_subsitesMapping.set_index(['ARBITERSITEMAP', 'GSSUBSITEMAP'])
        arbiter_subsitesMapping = arbiter_subsitesMapping.loc[arbiter_subsitesMapping.index.dropna()]

        gs_data_df = pd.merge(gs_data_df, arbiter_subsitesMapping, how='left', left_on=['ARBITERSITEMAP', 'Pitch'], right_on=['ARBITERSITEMAP', 'GSSUBSITEMAP'])

        gs_data_df['Partner'] = 'GotSport'
        gs_data_df['Sport'] = 'Soccer'
        gs_data_df['Custom Game ID'] = ''
        gs_data_df['BillTo'] = ''

        gs_data_df = gs_data_df.rename(columns={
            'ID': 'Game ID',
            'Start Time': 'Time',
            'Age': 'Level',
            'ARBITERSITEMAP': 'Site',
            'ARBITERSUBSITEMAP': 'Subsite'},
            errors='raise')

        gs_data_df = gs_data_df.drop(['Venue', 'Pitch', 'GSVENUEMAP'], axis=1)

        gs_data_df = gs_data_df[['Date', 'Time', 'Game ID', 'Custom Game ID', 'Partner', 'Sport', 'Level', 'Home Team', 'Away Team', 'Site', 'Subsite', 'BillTo']]

        gs_data_df.to_csv(self.export_file + 'ArbiterImport.' + str(time() * 1000) + '.csv', index=False, header=True)


if __name__ == '__main__':
    season = 'fall2025'
    root = 'arbiter_etl/data/' + season
    export_file_folder = root + '/export/'
    import_file_folder = root + '/import/'

    import_file_name = 'fall-2025-final-v1.master-schedule.2025-08-16T191233.729-0400.xlsx'

# 'arbiter_etl/data/fall2025/import/a-v1.master-schedule.2025-08-16T191233.729-0400.xlsx'

    arbiter_ops = ArbiterOperations(root, import_file_folder, import_file_name, export_file_folder)
    arbiter_ops.process_data()
