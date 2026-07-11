"""Offline tests for saved searches, migration idempotency, and the
incremental change-detection logic.

Playwright and pandas are not installed in the dev/test environment, so they are
stubbed in ``sys.modules`` before importing the app modules (the same trick used
for the scraper modules). All DB tests run against a real temporary SQLite file.

Run with:  python3 test_refresh.py
"""

import sys
import types
import tempfile
from pathlib import Path


# --- Stub heavy/native deps so database.py and refresh.py import offline ---
def _install_stubs():
    if "pandas" not in sys.modules:
        pandas_stub = types.ModuleType("pandas")

        class _DataFrame:  # minimal placeholder; DB tests avoid pandas paths
            def __init__(self, *a, **k):
                self._rows = a[0] if a else []

            @property
            def empty(self):
                return not self._rows

        pandas_stub.DataFrame = _DataFrame
        pandas_stub.read_sql_query = lambda *a, **k: _DataFrame()
        pandas_stub.isna = lambda v: v is None
        pandas_stub.Series = object
        sys.modules["pandas"] = pandas_stub

    if "playwright" not in sys.modules:
        pw = types.ModuleType("playwright")
        pw_async = types.ModuleType("playwright.async_api")
        pw_async.async_playwright = lambda: None

        class _TimeoutError(Exception):
            pass

        pw_async.TimeoutError = _TimeoutError
        sys.modules["playwright"] = pw
        sys.modules["playwright.async_api"] = pw_async


_install_stubs()

import database  # noqa: E402


def _use_temp_db():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    database.DB_PATH = Path(tmp.name)
    return Path(tmp.name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_migration_idempotent():
    _use_temp_db()
    database.init_db()
    database.init_db()  # second run must not raise
    database.init_db()  # third for good measure

    with database._conn() as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(companies)")}
        assert {"first_seen", "last_seen", "last_updated"} <= cols, cols
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "saved_searches" in tables, tables
        # default tags seeded exactly once (no duplicates from re-run)
        n = conn.execute(
            "SELECT COUNT(*) FROM tags WHERE name='Called'"
        ).fetchone()[0]
        assert n == 1, n
    print("PASS test_migration_idempotent")


def test_migration_backfill_existing_rows():
    """A pre-existing companies table (without the new columns) gets them added
    and backfilled from created_at/updated_at."""
    path = _use_temp_db()
    import sqlite3

    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT DEFAULT '',
            website TEXT DEFAULT '',
            session_id INTEGER,
            created_at TEXT DEFAULT '2026-01-01T00:00:00',
            updated_at TEXT DEFAULT '2026-01-02T00:00:00'
        );
        INSERT INTO companies (name) VALUES ('Old Co');
        """
    )
    conn.commit()
    conn.close()

    database.init_db()  # runs migration

    with database._conn() as conn:
        row = conn.execute(
            "SELECT first_seen, last_seen, last_updated FROM companies WHERE name='Old Co'"
        ).fetchone()
    assert row["first_seen"] == "2026-01-01T00:00:00", row["first_seen"]
    assert row["last_updated"] == "2026-01-02T00:00:00", row["last_updated"]
    print("PASS test_migration_backfill_existing_rows")


def test_saved_search_crud():
    _use_temp_db()
    database.init_db()

    params = {"use_gmaps": True, "queries": ["cafe in PJ"], "gm_max": 200}
    sid = database.create_saved_search("PJ cafes", "google_maps", params)
    assert isinstance(sid, int)

    all_s = database.get_saved_searches()
    assert len(all_s) == 1
    assert all_s[0]["name"] == "PJ cafes"
    assert all_s[0]["params"] == params  # decoded back to a dict

    one = database.get_saved_search("PJ cafes")
    assert one is not None and one["params"]["gm_max"] == 200

    # Upsert on same name overwrites params, does not duplicate
    database.create_saved_search("PJ cafes", "google_maps", {"gm_max": 500})
    all_s = database.get_saved_searches()
    assert len(all_s) == 1, "upsert should not create a duplicate"
    assert database.get_saved_search("PJ cafes")["params"]["gm_max"] == 500

    database.mark_saved_search_run(sid, 7)
    refreshed = database.get_saved_search("PJ cafes")
    assert refreshed["last_run_at"], "last_run_at should be set"
    assert refreshed["last_result_count"] == 7

    database.delete_saved_search(sid)
    assert database.get_saved_searches() == []
    print("PASS test_saved_search_crud")


def test_incremental_new_updated_unchanged():
    _use_temp_db()
    database.init_db()
    session = database.create_session("google_maps", "test", 0)

    # First run: two brand-new companies
    batch1 = [
        {"name": "Alpha Sdn Bhd", "phone": "0312345678", "website": "http://alpha.my", "source": "google_maps"},
        {"name": "Beta Enterprise", "phone": "", "website": "", "source": "google_maps"},
    ]
    r1 = database.upsert_companies_incremental(batch1, session)
    assert r1["new"] == 2 and r1["updated"] == 0 and r1["unchanged"] == 0, r1

    # Second run:
    #  - Alpha unchanged (same phone/website) -> unchanged
    #  - Beta gains a phone -> updated
    #  - Gamma is new -> new
    batch2 = [
        {"name": "Alpha Sdn Bhd", "phone": "0312345678", "website": "http://alpha.my", "source": "google_maps"},
        {"name": "Beta Enterprise", "phone": "0123456789", "website": "", "source": "google_maps"},
        {"name": "Gamma Trading", "phone": "0398765432", "website": "", "source": "google_maps"},
    ]
    r2 = database.upsert_companies_incremental(batch2, session)
    assert r2["new"] == 1, r2
    assert r2["updated"] == 1, r2
    assert r2["unchanged"] == 1, r2

    # Beta's phone was written and phone_type reclassified to mobile
    with database._conn() as conn:
        beta = conn.execute(
            "SELECT phone, phone_type, first_seen, last_updated FROM companies WHERE name='Beta Enterprise'"
        ).fetchone()
    assert beta["phone"] == "0123456789", beta["phone"]
    assert beta["phone_type"] == "mobile", beta["phone_type"]
    # Beta was updated, so last_updated should now differ from first_seen
    assert beta["last_updated"] >= beta["first_seen"]
    print("PASS test_incremental_new_updated_unchanged")


def test_incremental_dedup_by_normalized_name():
    """Different legal-suffix spellings of the same name are treated as one."""
    _use_temp_db()
    database.init_db()
    session = database.create_session("google_maps", "test", 0)

    database.upsert_companies_incremental(
        [{"name": "Acme Sdn Bhd", "phone": "0311112222", "source": "google_maps"}],
        session,
    )
    # "Acme Sdn. Bhd." normalizes to the same key -> should match, not insert
    r = database.upsert_companies_incremental(
        [{"name": "Acme Sdn. Bhd.", "website": "http://acme.my", "source": "google_maps"}],
        session,
    )
    assert r["new"] == 0, r
    assert r["updated"] == 1, r  # gained a website
    with database._conn() as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    assert cnt == 1, cnt
    print("PASS test_incremental_dedup_by_normalized_name")


def test_delta_added_since():
    _use_temp_db()
    database.init_db()
    session = database.create_session("google_maps", "test", 0)

    database.upsert_companies_incremental(
        [{"name": "Early Co", "source": "google_maps"}], session
    )
    # Backdate Early Co's first_seen so it falls before the cutoff
    with database._conn() as conn:
        conn.execute(
            "UPDATE companies SET first_seen='2026-06-01T00:00:00' WHERE name='Early Co'"
        )
        conn.commit()

    database.upsert_companies_incremental(
        [{"name": "Late Co", "source": "google_maps"}], session
    )

    rows = database.get_companies_added_since_rows("2026-07-01")
    names = {r["name"] for r in rows}
    assert names == {"Late Co"}, names
    print("PASS test_delta_added_since")


def test_refresh_runner_integration(monkeypatch_free=True):
    """run_saved_search should scrape (stubbed), upsert, and mark the run."""
    _use_temp_db()
    database.init_db()

    import refresh

    # Stub the scraper + verification calls so no browser is needed.
    calls = {"gmaps": 0, "verify": 0}

    def fake_gmaps(queries, gm_max, cb, conc, **kw):
        calls["gmaps"] += 1
        return [
            {"name": "Runner Co", "phone": "0312223333",
             "website": "http://runner.my", "source": "google_maps"},
        ]

    refresh.scrape_google_maps = fake_gmaps
    refresh.merge_results = lambda results: results  # identity for this test
    refresh.scrape_website_phones = lambda comps, cb, conc: [
        {"id": comps[0]["id"], "website_phone": "0388889999",
         "website_phone_type": "landline", "website_phone2": "",
         "website_phone2_type": "", "website_email": "hi@runner.my",
         "website_email2": ""}
    ] if comps else []

    sid = database.create_saved_search(
        "Runner", "google_maps",
        {"use_gmaps": True, "queries": ["co in KL"], "gm_max": 100},
    )
    search = database.get_saved_search("Runner")

    summary = refresh.run_saved_search(search, concurrency=1, verify=True)
    assert calls["gmaps"] == 1
    assert summary["new"] == 1, summary
    assert summary["verified"] == 1, summary

    with database._conn() as conn:
        row = conn.execute(
            "SELECT website_phone, website_email, last_updated FROM companies WHERE name='Runner Co'"
        ).fetchone()
    assert row["website_phone"] == "0388889999", row["website_phone"]
    assert row["website_email"] == "hi@runner.my", row["website_email"]

    # saved search marked as run
    assert database.get_saved_search("Runner")["last_run_at"]
    print("PASS test_refresh_runner_integration")


if __name__ == "__main__":
    tests = [
        test_migration_idempotent,
        test_migration_backfill_existing_rows,
        test_saved_search_crud,
        test_incremental_new_updated_unchanged,
        test_incremental_dedup_by_normalized_name,
        test_delta_added_since,
        test_refresh_runner_integration,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
