import argparse
import os
import sys
import tempfile
from pathlib import Path

from src.jina_scraper import fetch_data, parse_data
from loguru import logger
from src.save_db import init_db, save_ipo_data, validate_db


LOCAL_DB_PATH = Path("db") / "ipo_data.db"
LOCAL_DEBUG_OUTPUT_PATH = Path("db") / "jina_hkipo_output.md"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="HK IPO scraper")
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Run in CI mode and write the database into the runner temporary directory.",
    )
    return parser.parse_args(argv)


def resolve_paths(daily_mode):
    if daily_mode:
        runner_temp = os.getenv("RUNNER_TEMP") or tempfile.gettempdir()
        return Path(runner_temp) / "ipo_data.db", None

    return LOCAL_DB_PATH, LOCAL_DEBUG_OUTPUT_PATH


def main(argv=None):
    args = parse_args(argv)
    mode = "daily" if args.daily else "local"
    db_path, debug_output_path = resolve_paths(args.daily)

    logger.info(f"Starting HK IPO data scraping task in {mode} mode...")

    try:
        # 1. Initialize SQLite database
        init_db(str(db_path))

        # 2. Fetch data from website via Jina AI
        raw_text = fetch_data(
            debug_output_path=str(debug_output_path) if debug_output_path else None
        )

        # 3. Parse JSON from the markdown output
        parsed_list = parse_data(raw_text)

        if not parsed_list:
            raise ValueError("No data parsed from upstream response.")

        # 4. Save parsed entries to the database
        saved_count = save_ipo_data(parsed_list, str(db_path))
        row_count = validate_db(str(db_path))

        logger.info(
            f"Task finished successfully. Updated {saved_count} incoming records and validated {row_count} total rows."
        )
        logger.info(f"Database available at: {db_path}")
        return 0
    except Exception as e:
        logger.exception(f"HK IPO scraping task failed in {mode} mode: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
