"""CLI: ingest SEC bulk data into DuckDB.

Examples
--------
    # Just refresh the cross-reference tables from SEC's small index files
    python -m openbb_sec.ingest --skip-facts --skip-submissions
"""

from __future__ import annotations

import argparse
import sys

from openbb_sec.db.ingest import ingest_cross_reference, ingest_multi_cik_overrides, run_update
from openbb_sec.utils.definitions import SEC_DB_PATH


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m openbb_sec.ingest",
        description="Load SEC bulk data into a local DuckDB store.",
    )
    parser.add_argument(
        "--db",
        default=SEC_DB_PATH,
        help=f"DuckDB file path (default: {SEC_DB_PATH}).",
    )
    parser.add_argument(
        "--skip-facts",
        action="store_true",
        help="Skip the companyfacts ingest.",
    )
    parser.add_argument(
        "--skip-submissions",
        action="store_true",
        help="Skip the submissions ingest.",
    )
    parser.add_argument(
        "--skip-xref",
        action="store_true",
        help="Skip refreshing tickers/funds/entities tables from SEC.",
    )
    parser.add_argument(
        "--skip-overrides",
        action="store_true",
        help="Skip rewriting the multi_cik_tickers table.",
    )
    args = parser.parse_args(argv)

    if not args.skip_facts or not args.skip_submissions:
        run_update(
            db_path=args.db,
            download_from_sec=True,
            skip_submissions=args.skip_submissions,
        )

    if not args.skip_xref:
        ingest_cross_reference(db_path=args.db)

    if not args.skip_overrides:
        ingest_multi_cik_overrides(db_path=args.db)

    return 0


if __name__ == "__main__":
    sys.exit(main())
