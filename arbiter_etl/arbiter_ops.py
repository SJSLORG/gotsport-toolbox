from pathlib import Path
from datetime import datetime, timezone
import logging
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)
if not logger.handlers:
    # Basic logging configuration for script usage; library users can configure separately
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class ArbiterOperations:
    """Utility to transform GotSport master schedule into Arbiter import CSV.

    Notes:
    - Paths provided can be strings or Path-like; they're normalized with pathlib.Path.
    - The class focuses on correctness and defensive checks; it avoids unsafe .strip() on NaNs.
    """

    def __init__(
        self,
        root: str | Path,
        import_file_folder: Optional[str | Path] = None,
        import_file_name: Optional[str | Path] = None,
        export_file: Optional[str | Path] = None,
    ) -> None:
        """Initialize with either:
        - ArbiterOperations(root, import_folder, import_file_name, export_folder)
        - ArbiterOperations(root, import_file_path)

        Behavior:
        - If the import path (folder or file) is relative, it is taken relative to root / "import".
        - If export_file is not provided, defaults to root / "export" and is created.
        """
        self.root = Path(root)

        # Backwards-compatibility helper: user may call ArbiterOperations(root, 'file.xlsx', export_dir)
        # In that case import_file_folder will be the filename and import_file_name will be the export dir.
        if import_file_name is not None and Path(import_file_name).is_dir():
            # swap: treat the third positional as export_file
            export_file = import_file_name
            import_file_name = None

        # Determine import path
        import_path: Optional[Path] = None
        if import_file_name is not None and import_file_folder is not None:
            import_path = Path(import_file_folder) / Path(import_file_name)
        elif import_file_folder is not None:
            # Single argument: treat as either a folder or a full file path (could be filename)
            import_path = Path(import_file_folder)
        else:
            raise ValueError("import file or folder must be provided")

        if not import_path.is_absolute():
            import_path = (self.root / "import" / import_path)
        self.import_file_path = import_path.resolve()

        # Determine export folder
        if export_file is not None:
            self.export_file = Path(export_file)
        else:
            self.export_file = (self.root / "export")
        self.export_file.mkdir(parents=True, exist_ok=True)

    def _vectorized_normalize(self, df: pd.DataFrame, prefix: str) -> pd.Series:
        club_col = f"{prefix} Club"
        team_col = f"{prefix} Team"

        clubs = df[club_col].astype(pd.StringDtype())
        teams = df[team_col].astype(pd.StringDtype())

        club_stripped = clubs.str.strip()
        team_stripped = teams.str.strip()

        first_club = club_stripped.str.slice(0, 5)
        both_missing = club_stripped.isna() & team_stripped.isna()
        club_missing = club_stripped.isna() & team_stripped.notna()
        team_missing = team_stripped.isna() & club_stripped.notna()
        starts_with = team_stripped.str.slice(0, 5) == first_club
        combined = club_stripped + " " + team_stripped

        result = pd.Series(pd.NA, index=df.index, dtype=pd.StringDtype())
        result = result.mask(both_missing, "")
        result = result.mask(club_missing, team_stripped.fillna(""))
        result = result.mask(team_missing, club_stripped.fillna(""))
        result = result.mask(~(both_missing | club_missing | team_missing) & starts_with, team_stripped)
        result = result.mask(~(both_missing | club_missing | team_missing) & ~starts_with, combined)
        return result.fillna("").str.upper()

    def custom_replace(self, row: pd.Series, prefix: str) -> str:
        """Backward-compatible row-wise replacement used by older callers/tests.

        Safe with missing values (None / NaN). Returns an uppercase string.
        Logic mirrors the vectorized implementation.
        """
        club = row.get(f"{prefix} Club")
        team = row.get(f"{prefix} Team")

        if pd.isna(club) and pd.isna(team):
            return ""

        club_s = None if pd.isna(club) else str(club).strip()
        team_s = None if pd.isna(team) else str(team).strip()

        if club_s is None and team_s is None:
            return ""
        if club_s is None:
            return team_s.upper()
        if team_s is None:
            return club_s.upper()

        first_club = club_s[:5]
        if team_s[:5] == first_club:
            return team_s.upper()
        return f"{club_s} {team_s}".upper()

    def process_data(self) -> None:
        """Read input spreadsheet, normalize, map levels/sites/subsites, and write Arbiter CSV."""
        arbiter_lookup = self.root / "lookup" / "ArbiterMappings.xlsx"

        logger.info("Reading matches from %s", self.import_file_path)
        gs_data = pd.read_excel(self.import_file_path, sheet_name="Matches")

        expected_cols = [
            "Date",
            "Start Time",
            "ID",
            "Age",
            "Home Club",
            "Home Team",
            "Away Club",
            "Away Team",
            "Venue",
            "Pitch",
        ]
        gs_data_df = pd.DataFrame(gs_data, columns=expected_cols)

        gs_data_df["Home Team"] = self._vectorized_normalize(gs_data_df, "Home")
        gs_data_df["Away Team"] = self._vectorized_normalize(gs_data_df, "Away")

        # Map levels (only Age column) to avoid global replace/downcast
        arbiter_levels_mapping = pd.read_excel(
            arbiter_lookup, sheet_name="LevelsMap", usecols=["AgeMap", "LevelMap"]
        )
        mapping_dict: Dict[Any, Any] = arbiter_levels_mapping.set_index("AgeMap")["LevelMap"].to_dict()
        if "Age" in gs_data_df.columns:
            gs_data_df["Age"] = gs_data_df["Age"].replace(mapping_dict)

        # Map sites
        arbiter_sites_mapping = pd.read_excel(
            arbiter_lookup, sheet_name="SitesMap", usecols=["GSVENUEMAP", "ARBITERSITEMAP"]
        )
        gs_data_df = pd.merge(
            gs_data_df, arbiter_sites_mapping, how="left", left_on=["Venue"], right_on=["GSVENUEMAP"]
        )

        # Map subsites
        arbiter_subsites_mapping = pd.read_excel(
            arbiter_lookup,
            sheet_name="SubsitesMap",
            usecols=["ARBITERSITEMAP", "GSSUBSITEMAP", "ARBITERSUBSITEMAP"],
        )
        arbiter_subsites_mapping = arbiter_subsites_mapping.dropna(subset=["ARBITERSITEMAP", "GSSUBSITEMAP"])
        gs_data_df = pd.merge(
            gs_data_df,
            arbiter_subsites_mapping,
            how="left",
            left_on=["ARBITERSITEMAP", "Pitch"],
            right_on=["ARBITERSITEMAP", "GSSUBSITEMAP"],
        )

        gs_data_df["Partner"] = "GotSport"
        gs_data_df["Sport"] = "Soccer"
        gs_data_df["Custom Game ID"] = ""
        gs_data_df["BillTo"] = ""

        gs_data_df = gs_data_df.rename(
            columns={
                "ID": "Game ID",
                "Start Time": "Time",
                "Age": "Level",
                "ARBITERSITEMAP": "Site",
                "ARBITERSUBSITEMAP": "Subsite",
            },
            errors="raise",
        )

        gs_data_df = gs_data_df.drop(["Venue", "Pitch", "GSVENUEMAP"], axis=1, errors="ignore")

        out_cols = [
            "Date",
            "Time",
            "Game ID",
            "Custom Game ID",
            "Partner",
            "Sport",
            "Level",
            "Home Team",
            "Away Team",
            "Site",
            "Subsite",
            "BillTo",
        ]
        gs_data_df = gs_data_df.reindex(columns=out_cols)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_name = f"ArbiterImport.{timestamp}.csv"
        out_path = self.export_file / out_name
        logger.info("Writing arbiter CSV to %s", out_path)
        gs_data_df.to_csv(out_path, index=False, header=True)


def _run_from_args() -> int:
    """Minimal runner: expects positional root and import_path, optional --export."""
    import argparse
    parser = argparse.ArgumentParser(description="Run ArbiterOperations to convert GotSport master schedule to Arbiter CSV")
    parser.add_argument("root", help="root data folder (e.g. arbiter_etl/data/fall2025)")
    parser.add_argument("import_path", help="import file path or name (relative to root/import when not absolute)")
    parser.add_argument("--export", help="optional export folder (defaults to root/export)")
    args = parser.parse_args()

    try:
        op = ArbiterOperations(args.root, import_file_folder=args.import_path, export_file=args.export)
        op.process_data()
        return 0
    except Exception:
        logger.exception("Failed to process arbiter data")
        return 2


if __name__ == "__main__":
    raise SystemExit(_run_from_args())
