"""CLI: ingest SEC bulk data into Dolt.

Examples
--------
    # Just refresh the cross-reference tables from SEC's small index files
    python -m sec_app.cli --skip-facts --skip-submissions
"""

from __future__ import annotations

import argparse
import sys

from sec_app.db.ingest import ingest_cross_reference, ingest_multi_cik_overrides, run_update


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sec_app.cli",
        description="Load SEC bulk data into the Dolt store (configured via DOLT_SQL_* env vars).",
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
            download_from_sec=True,
            skip_submissions=args.skip_submissions,
        )

    if not args.skip_xref:
        ingest_cross_reference()

    if not args.skip_overrides:
        ingest_multi_cik_overrides()

    return 0


if __name__ == "__main__":
    sys.exit(main())
