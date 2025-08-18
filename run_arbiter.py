#!/usr/bin/env python3
"""Simple runner for ArbiterOperations.

Usage:
    python run_arbiter.py <root> <import_path> [--export <export_dir>]

Examples:
    python run_arbiter.py arbiter_etl/data/fall2025 a-v1.master-schedule.xlsx
    python run_arbiter.py arbiter_etl/data/fall2025 import/myfile.xlsx --export arbiter_etl/data/fall2025/export
"""
import sys
import argparse
from pathlib import Path

from arbiter_etl.arbiter_ops import ArbiterOperations, logger


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run ArbiterOperations to convert GotSport master schedule to Arbiter CSV")
    parser.add_argument("root", help="root data folder (e.g. arbiter_etl/data/fall2025)")
    parser.add_argument("import_path", help="import file path or name (relative to root/import when not absolute)")
    parser.add_argument("--export", help="optional export folder (defaults to root/export)")
    args = parser.parse_args(argv)

    try:
        op = ArbiterOperations(args.root, import_file_folder=args.import_path, export_file=args.export)
        op.process_data()
        return 0
    except Exception:
        logger.exception("Runner failed")
        return 2


if __name__ == "__main__":
    # DEBUG_RUN: set to True to run with the hardcoded variables below instead of parsing CLI args.
    # Use this when you want to set variables in the code for debugging.
    DEBUG_RUN = True
    DEBUG_ROOT = "arbiter_etl/data/fall2025"
    DEBUG_IMPORT = "a-v1.master-schedule.2025-08-16T191233.729-0400.xlsx"
    DEBUG_EXPORT = None  # e.g. "arbiter_etl/data/fall2025/export" or None to use default

    if DEBUG_RUN:
        args_list: list[str] = [DEBUG_ROOT, DEBUG_IMPORT]
        if DEBUG_EXPORT:
            args_list += ["--export", DEBUG_EXPORT]
        raise SystemExit(main(args_list))
    else:
        raise SystemExit(main())
