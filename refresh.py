"""Headless incremental-refresh runner for Lead Scraper.

Re-runs saved searches without Streamlit, reusing the existing scraper +
dedup + website-verification pipeline, then records what is new or changed so a
"fresh monthly list" can be exported as a delta.

Intended to be scheduled (Windows Task Scheduler / cron) — see the README
"Scheduling the refresh runner" section.

Import-safe: importing this module has no side effects; all CLI parsing and
execution happens under ``if __name__ == "__main__":``.

Typical usage::

    python refresh.py                       # refresh every saved search
    python refresh.py --search "PJ cafes"   # refresh one saved search
    python refresh.py --list                # list saved searches
    python refresh.py --no-verify           # skip website phone/email checks
    python refresh.py --export-new-since 2026-07-01 --output new_leads.xlsx
"""

import sys
from datetime import datetime

# Windows event loop fix for Playwright subprocess support (mirrors app.py).
if sys.platform == "win32":
    import asyncio

    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from database import (
    init_db,
    create_session,
    get_saved_searches,
    get_saved_search,
    mark_saved_search_run,
    upsert_companies_incremental,
    update_website_phones,
    get_companies_with_website,
    get_companies_added_since_rows,
)
from scrapers import (
    scrape_google_maps,
    scrape_jobstreet,
    scrape_hiredly,
    scrape_website_phones,
    merge_results,
    DEFAULT_CONCURRENCY,
)

# Default parameters filled in when a saved search omits a key.
_PARAM_DEFAULTS = {
    "queries": [],
    "gm_max": 500,
    "use_gmaps": False,
    "use_jobstreet": False,
    "js_locations": [],
    "js_pages": 5,
    "use_hiredly": False,
    "hi_max": 50,
    "skip_large": False,
    "skip_blocklist": None,
}


def _log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def _scrape_from_params(params: dict, concurrency: int) -> list[dict]:
    """Run the configured scrapers for one saved search and merge the results."""
    p = {**_PARAM_DEFAULTS, **(params or {})}
    all_results: list[dict] = []

    if p["use_gmaps"] and p["queries"]:
        all_results.extend(
            scrape_google_maps(
                p["queries"], p["gm_max"], None, concurrency,
                skip_large=p["skip_large"], skip_blocklist=p["skip_blocklist"],
            )
        )
    if p["use_jobstreet"] and p["js_locations"]:
        all_results.extend(
            scrape_jobstreet(p["js_locations"], p["js_pages"], None, concurrency)
        )
    if p["use_hiredly"]:
        all_results.extend(scrape_hiredly(p["hi_max"], None, concurrency))

    return merge_results(all_results) if all_results else []


def run_saved_search(
    search: dict, concurrency: int = DEFAULT_CONCURRENCY, verify: bool = True
) -> dict:
    """Re-run one saved search and apply incremental change detection.

    Returns the upsert summary dict (new / updated / unchanged counts plus ids),
    augmented with 'name' and 'verified'.
    """
    name = search.get("name", "")
    params = search.get("params", {})
    _log(f"Refreshing saved search: {name!r}")

    merged = _scrape_from_params(params, concurrency)
    if not merged:
        _log(f"  no results for {name!r}")
        mark_saved_search_run(search["id"], 0)
        return {"name": name, "new": 0, "updated": 0, "unchanged": 0,
                "new_ids": [], "updated_ids": [], "verified": 0}

    session_id = create_session(
        search.get("source", ""),
        f"refresh: {name}",
        len(merged),
    )
    summary = upsert_companies_incremental(merged, session_id)
    summary["name"] = name
    _log(
        f"  {summary['new']} new, {summary['updated']} updated, "
        f"{summary['unchanged']} unchanged"
    )

    verified = 0
    if verify:
        changed_ids = set(summary["new_ids"]) | set(summary["updated_ids"])
        to_verify = [
            c for c in get_companies_with_website() if c["id"] in changed_ids
        ]
        if to_verify:
            _log(f"  verifying {len(to_verify)} website(s)...")
            results = scrape_website_phones(to_verify, None, concurrency)
            verified = update_website_phones(results)
            _log(f"  {verified} website contact record(s) updated")
    summary["verified"] = verified

    mark_saved_search_run(search["id"], summary["new"] + summary["updated"])
    return summary


def run_all(concurrency: int = DEFAULT_CONCURRENCY, verify: bool = True) -> list[dict]:
    """Refresh every saved search. Returns one summary dict per search."""
    searches = get_saved_searches()
    if not searches:
        _log("No saved searches to refresh. Save one from the app first.")
        return []
    summaries = []
    for search in searches:
        try:
            summaries.append(run_saved_search(search, concurrency, verify))
        except Exception as exc:  # keep going even if one search fails
            _log(f"  ERROR refreshing {search.get('name')!r}: {exc}")
    return summaries


def export_new_since(since: str, output: str | None = None) -> str:
    """Export companies first seen on/after `since` to an Excel file.

    `since` is an ISO date (YYYY-MM-DD) or datetime string. Returns the path
    written.
    """
    import pandas as pd  # imported lazily so import-time stays dependency-light

    rows = get_companies_added_since_rows(since)
    export_cols = [
        "name", "phone", "phone_type", "website_phone", "website_phone2",
        "website_email", "website_email2", "website", "address", "category",
        "company_size", "tags", "sources", "first_seen", "last_updated",
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[[c for c in export_cols if c in df.columns]]

    if output is None:
        output = f"new_leads_since_{since.replace(':', '').replace(' ', '_')}.xlsx"
    df.to_excel(output, index=False, engine="openpyxl")
    _log(f"Exported {len(df)} companies (new since {since}) to {output}")
    return output


def _print_saved_searches() -> None:
    searches = get_saved_searches()
    if not searches:
        print("No saved searches yet.")
        return
    print(f"{len(searches)} saved search(es):")
    for s in searches:
        last = s.get("last_run_at") or "never"
        print(f"  - {s['name']}  [{s.get('source', '')}]  last run: {last}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Headless incremental refresh for Lead Scraper saved searches."
    )
    parser.add_argument(
        "--search", metavar="NAME",
        help="Refresh only the saved search with this name (default: all).",
    )
    parser.add_argument(
        "--list", action="store_true", dest="list_searches",
        help="List saved searches and exit.",
    )
    parser.add_argument(
        "--no-verify", action="store_false", dest="verify",
        help="Skip website phone/email verification of new/changed companies.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
        help=f"Concurrent browser tabs (default: {DEFAULT_CONCURRENCY}).",
    )
    parser.add_argument(
        "--export-new-since", metavar="DATE",
        help="Export companies first seen on/after DATE (YYYY-MM-DD) to Excel "
             "instead of refreshing.",
    )
    parser.add_argument(
        "--output", metavar="FILE",
        help="Output path for --export-new-since (default: auto-named .xlsx).",
    )
    args = parser.parse_args()

    init_db()

    if args.list_searches:
        _print_saved_searches()
        sys.exit(0)

    if args.export_new_since:
        export_new_since(args.export_new_since, args.output)
        sys.exit(0)

    if args.search:
        search = get_saved_search(args.search)
        if not search:
            _log(f"No saved search named {args.search!r}.")
            sys.exit(1)
        run_saved_search(search, args.concurrency, args.verify)
    else:
        run_all(args.concurrency, args.verify)
