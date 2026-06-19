from __future__ import annotations

import argparse
import os
import sys
import threading
import time

import psutil

from sec_app.db.ingest import (
    ingest_companyfacts_zip,
    ingest_cross_reference,
    ingest_exchange_rates,
    ingest_multi_cik_overrides,
    ingest_submissions_zip,
    materialize_standardized_statements,
    run_update,
)


def _derived_counts(db_path: str | None = None) -> tuple[int, int]:
    from sec_app.db.ingest import _connect, _finalize, _table  # pylint: disable=import-outside-toplevel

    conn = _connect(db_path)
    try:
        processed_row = conn.execute(
            f"SELECT COUNT(*) FROM {_table('processed_ciks')} WHERE has_balance AND has_income AND has_cash_flow"
        ).fetchone()
        standardized_row = conn.execute(f"SELECT COUNT(*) FROM {_table('standardized_statements_enc')}").fetchone()
    finally:
        _finalize(conn)
    processed_any = int(processed_row[0]) if processed_row else 0
    standardized_rows = int(standardized_row[0]) if standardized_row else 0
    return processed_any, standardized_rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m sec_app.run_pipeline")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int, default=None, help="Limit CIKs for smoke tests")
    parser.add_argument(
        "--update",
        action="store_true",
        help="Cron-friendly delta mode: diff source mtimes vs DB and only "
        "re-ingest CIKs that changed since the last run. Skips xref/overrides "
        "(use --skip-xref / --skip-overrides on a normal run for those). "
        "Sources are always downloaded from SEC.",
    )
    parser.add_argument(
        "--download-cache",
        help="Directory for SEC bulk-data downloads. Defaults to $SEC_DOWNLOAD_CACHE or /var/cache/openbb_sec.",
    )
    parser.add_argument("--skip-facts", action="store_true")
    parser.add_argument("--skip-xref", action="store_true")
    parser.add_argument("--skip-overrides", action="store_true")
    parser.add_argument("--skip-submissions", action="store_true")
    parser.add_argument("--skip-standardized", action="store_true")
    parser.add_argument("--skip-rates", action="store_true")
    args = parser.parse_args(argv)

    if args.update:
        t_total = time.time()
        print("\n=== UPDATE STAGE 0: ticker/fund/entity lists ===", flush=True)
        result = run_update(
            workers=args.workers,
            download_from_sec=True,
            download_dest=args.download_cache,
            skip_submissions=args.skip_submissions,
        )
        processed_any, standardized_rows = _derived_counts()
        if processed_any > 0 and standardized_rows == 0:
            print("[update] detected empty standardized_statements; running full rematerialization", flush=True)
            recov = materialize_standardized_statements(workers=args.workers)
            print(
                f"[update] rematerialization inserted rows={recov.get('rows_inserted', 0):,}",
                flush=True,
            )
        print(
            f"\n=== UPDATE TOTAL {time.time() - t_total:.1f}s  "
            f"changed_facts={result['changed_facts']:,}  "
            f"changed_subs={result['changed_subs']:,} ===",
            flush=True,
        )
        return 0
    proc = psutil.Process(os.getpid())
    peak_rss = [0]
    peak_uss = [0]
    stop = [False]

    def watch() -> None:
        while not stop[0]:
            try:
                info = proc.memory_full_info()
                if info.rss > peak_rss[0]:
                    peak_rss[0] = info.rss
                if info.uss > peak_uss[0]:
                    peak_uss[0] = info.uss
            except Exception:
                pass
            time.sleep(0.5)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()

    times: dict[str, float] = {}
    t_total = time.time()

    if not args.skip_facts:
        print("\n=== STAGE 1: companyfacts ===", flush=True)
        t0 = time.time()
        from sec_app.db.ingest import download_companyfacts_zip

        companyfacts_zip = download_companyfacts_zip(
            None if not args.download_cache else os.path.join(args.download_cache, "companyfacts.zip")
        )
        ingest_companyfacts_zip(companyfacts_zip, limit=args.limit)
        times["facts"] = time.time() - t0

    if not args.skip_xref:
        print("\n=== STAGE 2: cross-reference ===", flush=True)
        t0 = time.time()
        ingest_cross_reference()
        times["xref"] = time.time() - t0

    if not args.skip_overrides:
        print("\n=== STAGE 3: multi-CIK overrides ===", flush=True)
        t0 = time.time()
        ingest_multi_cik_overrides()
        times["overrides"] = time.time() - t0

    if not args.skip_submissions:
        print("\n=== STAGE 4: submissions.zip ===", flush=True)
        t0 = time.time()
        from sec_app.db.ingest import download_submissions_zip

        submissions_zip = download_submissions_zip(
            None if not args.download_cache else os.path.join(args.download_cache, "submissions.zip")
        )
        ingest_submissions_zip(submissions_zip, limit=args.limit)
        times["submissions"] = time.time() - t0

    if not args.skip_standardized:
        print("\n=== STAGE 5: standardized_statements ===", flush=True)
        t0 = time.time()
        pre_processed, pre_std = _derived_counts()
        print(
            f"[stage5] pre-run: processed_ciks_with_statements={pre_processed:,}  standardized_rows={pre_std:,}",
            flush=True,
        )
        if pre_std > 0:
            print(
                f"[stage5] already materialized inline during Stage 1 (rows={pre_std:,}); skipping second pass",
                flush=True,
            )
        else:
            std_result = materialize_standardized_statements(workers=args.workers)
            post_processed, post_std = _derived_counts()
            print(
                f"[stage5] post-run: processed_ciks_with_statements={post_processed:,}  standardized_rows={post_std:,}  returned={std_result}",
                flush=True,
            )
            if pre_processed > 0 and post_std == 0:
                print(
                    "[pipeline] ERROR: standardized_statements is still 0 after materialize — check logs above for FAILED lines",
                    flush=True,
                )
        times["standardized"] = time.time() - t0

    if not args.skip_rates:
        print("\n=== STAGE 6: exchange rates ===", flush=True)
        t0 = time.time()
        rate_result = ingest_exchange_rates()
        print(f"[stage6] exchange_rates rows={rate_result.get('rows', 0):,}", flush=True)
        times["rates"] = time.time() - t0

    stop[0] = True
    watcher.join(timeout=2)

    print("\n=== TOTAL ===")
    for k, v in times.items():
        print(f"  {k:>14s}: {v:>7.1f}s ({v / 60:.1f} min)")
    print(f"  {'total':>14s}: {time.time() - t_total:.1f}s")
    print(f"Peak RSS: {peak_rss[0] / (1024 * 1024):.0f} MB  Peak USS: {peak_uss[0] / (1024 * 1024):.0f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
