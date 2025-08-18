import pandas as pd
from pathlib import Path

from ..arbiter_ops import ArbiterOperations


def test_process_data_merges_and_exports(tmp_path: Path):
    # Create a small Matches sheet
    matches = pd.DataFrame([
        {'Date': '2025-08-16', 'Start Time': '09:00', 'ID': 1, 'Age': 'U12', 'Home Club': 'ALPHA FC', 'Home Team': 'ALPHA U12', 'Away Club': 'BRAVO', 'Away Team': 'BRAVO U12', 'Venue': 'V1', 'Pitch': 'P1'},
        {'Date': '2025-08-16', 'Start Time': '11:00', 'ID': 2, 'Age': 'U14', 'Home Club': 'CHARLIE', 'Home Team': 'UNITED', 'Away Club': None, 'Away Team': 'DRAGONS', 'Venue': 'V2', 'Pitch': 'P2'},
    ])

    # Create mappings Excel with three sheets
    lookup_file = tmp_path / 'lookup' / 'ArbiterMappings.xlsx'
    lookup_file.parent.mkdir(parents=True)

    levels = pd.DataFrame({'AgeMap': ['U12', 'U14'], 'LevelMap': ['12', '14']})
    sites = pd.DataFrame({'GSVENUEMAP': ['V1', 'V2'], 'ARBITERSITEMAP': ['SiteA', 'SiteB']})
    subsites = pd.DataFrame({'ARBITERSITEMAP': ['SiteA', 'SiteB'], 'GSSUBSITEMAP': ['P1', 'P2'], 'ARBITERSUBSITEMAP': ['SubA', 'SubB']})

    with pd.ExcelWriter(lookup_file) as xw:
        levels.to_excel(xw, sheet_name='LevelsMap', index=False)
        sites.to_excel(xw, sheet_name='SitesMap', index=False)
        subsites.to_excel(xw, sheet_name='SubsitesMap', index=False)

    # Create input Matches workbook
    data_dir = tmp_path / 'data' / 'season'
    import_dir = data_dir / 'import'
    export_dir = data_dir / 'export'
    import_dir.mkdir(parents=True)
    export_dir.mkdir(parents=True)

    matches_file = import_dir / 'test_matches.xlsx'
    with pd.ExcelWriter(matches_file) as xw:
        matches.to_excel(xw, sheet_name='Matches', index=False)

    # Prepare ArbiterMappings under root/lookup as expected by ArbiterOperations
    root = data_dir
    (root / 'lookup').mkdir(exist_ok=True)
    lookup_file_dest = root / 'lookup' / 'ArbiterMappings.xlsx'
    lookup_file.replace(lookup_file_dest)

    ops = ArbiterOperations(root, 'test_matches.xlsx', export_dir)
    ops.process_data()

    # Expect output CSV in export_dir
    files = list(export_dir.glob('ArbiterImport.*.csv'))
    assert len(files) == 1

    df_out = pd.read_csv(files[0])
    # Check columns exist and mapping applied
    assert 'Site' in df_out.columns
    assert 'Subsite' in df_out.columns
    assert 'Level' in df_out.columns
    assert df_out.loc[df_out['Game ID'] == 1, 'Site'].iloc[0] == 'SiteA'
    assert df_out.loc[df_out['Game ID'] == 1, 'Subsite'].iloc[0] == 'SubA'
    assert df_out.loc[df_out['Game ID'] == 1, 'Level'].iloc[0] == 12 or df_out.loc[df_out['Game ID'] == 1, 'Level'].iloc[0] == '12'
