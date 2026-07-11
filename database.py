"""SQLite database layer for Lead Scraper."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

from utils import classify_phone, normalize_name, is_safe_column_name

DB_PATH = Path(__file__).parent / "leads.db"


@contextmanager
def _conn():
    """Context manager that yields a connection and always closes it."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Create all tables and seed default tags."""
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                color TEXT DEFAULT '#6B7280',
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT DEFAULT '',
                phone_type TEXT DEFAULT '',
                website TEXT DEFAULT '',
                website_phone TEXT DEFAULT '',
                website_phone_type TEXT DEFAULT '',
                website_phone2 TEXT DEFAULT '',
                website_phone2_type TEXT DEFAULT '',
                website_email TEXT DEFAULT '',
                website_email2 TEXT DEFAULT '',
                address TEXT DEFAULT '',
                category TEXT DEFAULT '',
                company_size TEXT DEFAULT '',
                rating TEXT DEFAULT '',
                sources TEXT DEFAULT '',
                google_maps_url TEXT DEFAULT '',
                jobstreet_url TEXT DEFAULT '',
                hiredly_url TEXT DEFAULT '',
                tags TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                session_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS scrape_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sources TEXT NOT NULL,
                query_info TEXT DEFAULT '',
                result_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS saved_searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                params TEXT NOT NULL DEFAULT '{}',
                last_run_at TEXT DEFAULT '',
                last_result_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_companies_session
                ON companies(session_id);
            CREATE INDEX IF NOT EXISTS idx_companies_website
                ON companies(website) WHERE website != '';
        """)

        # Migrate: add website_phone columns if missing (existing databases)
        existing_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(companies)").fetchall()
        }
        if "website_phone" not in existing_cols:
            conn.execute("ALTER TABLE companies ADD COLUMN website_phone TEXT DEFAULT ''")
            conn.execute("ALTER TABLE companies ADD COLUMN website_phone_type TEXT DEFAULT ''")
        if "website_phone2" not in existing_cols:
            conn.execute("ALTER TABLE companies ADD COLUMN website_phone2 TEXT DEFAULT ''")
            conn.execute("ALTER TABLE companies ADD COLUMN website_phone2_type TEXT DEFAULT ''")
        # Migrate: add website_email columns if missing (existing databases)
        if "website_email" not in existing_cols:
            conn.execute("ALTER TABLE companies ADD COLUMN website_email TEXT DEFAULT ''")
            conn.execute("ALTER TABLE companies ADD COLUMN website_email2 TEXT DEFAULT ''")
        # Migrate: add incremental-refresh tracking columns if missing (existing
        # databases). first_seen / last_seen / last_updated power the scheduled
        # "fresh monthly list" refresh and delta ("new since date X") exports.
        # Existing rows are backfilled once from created_at / updated_at so that
        # delta filters have a sensible baseline instead of empty strings.
        if "first_seen" not in existing_cols:
            conn.execute("ALTER TABLE companies ADD COLUMN first_seen TEXT DEFAULT ''")
            conn.execute("ALTER TABLE companies ADD COLUMN last_seen TEXT DEFAULT ''")
            conn.execute("ALTER TABLE companies ADD COLUMN last_updated TEXT DEFAULT ''")
            conn.execute(
                "UPDATE companies SET first_seen = COALESCE(NULLIF(first_seen, ''), created_at) "
                "WHERE first_seen IS NULL OR first_seen = ''"
            )
            conn.execute(
                "UPDATE companies SET last_seen = COALESCE(NULLIF(last_seen, ''), created_at) "
                "WHERE last_seen IS NULL OR last_seen = ''"
            )
            conn.execute(
                "UPDATE companies SET last_updated = COALESCE(NULLIF(last_updated, ''), updated_at) "
                "WHERE last_updated IS NULL OR last_updated = ''"
            )

        # Migrate: rename old Chinese default tags to English (existing databases).
        # Runs before the default-tag insert so the rename applies cleanly. Idempotent:
        # only acts when the Chinese-named tag still exists. The tag row is updated in
        # place (id + color preserved) and the comma-separated tag strings stored on
        # companies are rewritten so existing assignments carry over.
        tag_renames = {
            "已打電話": "Called",
            "有興趣": "Interested",
            "不要再打": "Do Not Call",
            "待跟進": "Follow Up",
            "重要客戶": "Key Account",
        }
        for old_name, new_name in tag_renames.items():
            if not conn.execute(
                "SELECT 1 FROM tags WHERE name = ?", (old_name,)
            ).fetchone():
                continue
            if conn.execute(
                "SELECT 1 FROM tags WHERE name = ?", (new_name,)
            ).fetchone():
                # English tag already present — drop the stale Chinese duplicate.
                conn.execute("DELETE FROM tags WHERE name = ?", (old_name,))
            else:
                conn.execute(
                    "UPDATE tags SET name = ? WHERE name = ?", (new_name, old_name)
                )
            # Rewrite assignments stored as text in companies.tags either way.
            conn.execute(
                "UPDATE companies SET tags = REPLACE(tags, ?, ?) WHERE tags LIKE ?",
                (old_name, new_name, f"%{old_name}%"),
            )

        default_tags = [
            ("Called", "#3B82F6"),
            ("Interested", "#10B981"),
            ("Do Not Call", "#EF4444"),
            ("Follow Up", "#F59E0B"),
            ("Key Account", "#8B5CF6"),
        ]
        for name, color in default_tags:
            conn.execute(
                "INSERT OR IGNORE INTO tags (name, color) VALUES (?, ?)",
                (name, color),
            )
        conn.commit()


# ---- Sessions ----


def create_session(sources: str, query_info: str, result_count: int) -> int:
    with _conn() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_sessions (sources, query_info, result_count) VALUES (?, ?, ?)",
            (sources, query_info, result_count),
        )
        conn.commit()
        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("INSERT into scrape_sessions did not return a row ID")
        return row_id


def get_sessions() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM scrape_sessions ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def delete_session(session_id: int) -> int:
    with _conn() as conn:
        deleted = conn.execute(
            "DELETE FROM companies WHERE session_id = ?", (session_id,)
        ).rowcount
        conn.execute("DELETE FROM scrape_sessions WHERE id = ?", (session_id,))
        conn.commit()
        return deleted


def get_session_companies(session_id: int) -> pd.DataFrame:
    with _conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM companies WHERE session_id = ? ORDER BY id",
            conn,
            params=(session_id,),
        )


# ---- Saved Searches ----


def create_saved_search(name: str, source: str, params: dict) -> int:
    """Persist a named saved search. Overwrites params if the name already exists."""
    with _conn() as conn:
        cursor = conn.execute(
            "INSERT INTO saved_searches (name, source, params) VALUES (?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET source = excluded.source, "
            "params = excluded.params",
            (name, source, json.dumps(params)),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM saved_searches WHERE name = ?", (name,)
        ).fetchone()
        return row["id"] if row else cursor.lastrowid


def get_saved_searches() -> list[dict]:
    """Return all saved searches with params decoded into a dict."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM saved_searches ORDER BY name"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["params"] = json.loads(d.get("params") or "{}")
            except (ValueError, TypeError):
                d["params"] = {}
            result.append(d)
        return result


def get_saved_search(name: str) -> dict | None:
    """Return a single saved search by name (params decoded), or None."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM saved_searches WHERE name = ?", (name,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["params"] = json.loads(d.get("params") or "{}")
        except (ValueError, TypeError):
            d["params"] = {}
        return d


def delete_saved_search(search_id: int) -> None:
    with _conn() as conn:
        conn.execute("DELETE FROM saved_searches WHERE id = ?", (search_id,))
        conn.commit()


def mark_saved_search_run(search_id: int, result_count: int) -> None:
    """Record that a saved search was just executed by the refresh runner."""
    with _conn() as conn:
        conn.execute(
            "UPDATE saved_searches SET last_run_at = ?, last_result_count = ? "
            "WHERE id = ?",
            (datetime.now().isoformat(), result_count, search_id),
        )
        conn.commit()


# ---- Companies ----


def save_companies(companies: list[dict], session_id: int) -> int:
    with _conn() as conn:
        now = datetime.now().isoformat()
        rows = [
            (
                c.get("name", ""),
                c.get("phone", ""),
                c.get("phone_type", ""),
                c.get("website", ""),
                c.get("address", ""),
                c.get("category", ""),
                c.get("company_size", ""),
                c.get("rating", ""),
                c.get("sources", c.get("source", "")),
                c.get("google_maps_url", ""),
                c.get("jobstreet_url", ""),
                c.get("hiredly_url", ""),
                session_id,
                now, now, now,
            )
            for c in companies
        ]
        conn.executemany(
            """INSERT INTO companies
               (name, phone, phone_type, website, address, category, company_size,
                rating, sources, google_maps_url, jobstreet_url, hiredly_url, session_id,
                first_seen, last_seen, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        return len(rows)


# Fields whose change (a non-empty new value that differs from the stored value)
# marks an existing company as "updated" during an incremental refresh.
CHANGE_FIELDS = ("phone", "website", "website_phone", "website_email")


def upsert_companies_incremental(
    companies: list[dict], session_id: int
) -> dict:
    """Insert new companies and update changed ones, tracking timestamps.

    Matches existing companies by normalized name (same key used for dedup).
    - New company -> INSERT with first_seen = last_seen = last_updated = now.
    - Existing company with a differing non-empty value in any CHANGE_FIELD ->
      UPDATE those fields, bump last_seen and last_updated.
    - Existing company with no change -> bump last_seen only.

    Returns a summary dict: new, updated, unchanged (counts) plus new_ids and
    updated_ids (lists of company IDs) so the caller can verify/export the delta.
    """
    now = datetime.now().isoformat()
    with _conn() as conn:
        by_key: dict[str, dict] = {}
        for row in conn.execute("SELECT * FROM companies").fetchall():
            key = normalize_name(row["name"])
            if key:
                by_key.setdefault(key, dict(row))

        new_ids: list[int] = []
        updated_ids: list[int] = []
        unchanged = 0

        for c in companies:
            name = c.get("name", "")
            key = normalize_name(name)
            if not key:
                continue

            existing = by_key.get(key)
            if existing is None:
                cursor = conn.execute(
                    """INSERT INTO companies
                       (name, phone, phone_type, website, address, category,
                        company_size, rating, sources, google_maps_url,
                        jobstreet_url, hiredly_url, session_id,
                        first_seen, last_seen, last_updated)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        name,
                        c.get("phone", ""),
                        c.get("phone_type", ""),
                        c.get("website", ""),
                        c.get("address", ""),
                        c.get("category", ""),
                        c.get("company_size", ""),
                        c.get("rating", ""),
                        c.get("sources", c.get("source", "")),
                        c.get("google_maps_url", ""),
                        c.get("jobstreet_url", ""),
                        c.get("hiredly_url", ""),
                        session_id,
                        now, now, now,
                    ),
                )
                new_id = cursor.lastrowid
                new_ids.append(new_id)
                # Register so a later row in the same batch matches this insert.
                by_key[key] = {"id": new_id, "name": name, **c}
                continue

            changes: dict[str, str] = {}
            for field in CHANGE_FIELDS:
                new_val = str(c.get(field, "") or "").strip()
                old_val = str(existing.get(field, "") or "").strip()
                if new_val and new_val != old_val:
                    changes[field] = new_val

            if changes:
                set_clause = ", ".join(f"{f} = ?" for f in changes)
                values = list(changes.values())
                # Keep phone_type consistent when phone changed.
                if "phone" in changes:
                    set_clause += ", phone_type = ?"
                    values.append(classify_phone(changes["phone"]))
                values.extend([now, now, existing["id"]])
                conn.execute(
                    f"UPDATE companies SET {set_clause}, "
                    "last_seen = ?, last_updated = ? WHERE id = ?",
                    values,
                )
                updated_ids.append(existing["id"])
                existing.update(changes)
            else:
                conn.execute(
                    "UPDATE companies SET last_seen = ? WHERE id = ?",
                    (now, existing["id"]),
                )
                unchanged += 1

        conn.commit()

    return {
        "new": len(new_ids),
        "updated": len(updated_ids),
        "unchanged": unchanged,
        "new_ids": new_ids,
        "updated_ids": updated_ids,
    }


def get_companies_added_since_rows(since: str) -> list[dict]:
    """Return company rows with first_seen >= since (ISO date/datetime string).

    Pandas-free variant used by the CLI refresh runner and by tests.
    """
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM companies WHERE first_seen != '' AND first_seen >= ? "
            "ORDER BY first_seen",
            (since,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_companies_updated_since_rows(since: str) -> list[dict]:
    """Return company rows changed since `since` (last_updated >= since) that
    were not first seen in the same window (i.e. genuinely updated, not new)."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM companies WHERE last_updated != '' AND last_updated >= ? "
            "AND (first_seen = '' OR first_seen < ?) ORDER BY last_updated",
            (since, since),
        ).fetchall()
        return [dict(r) for r in rows]


def get_companies_added_since(since: str) -> pd.DataFrame:
    """DataFrame of companies with first_seen >= since (for the UI delta view)."""
    with _conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM companies WHERE first_seen != '' AND first_seen >= ? "
            "ORDER BY first_seen DESC",
            conn,
            params=(since,),
        )


def get_all_companies() -> pd.DataFrame:
    with _conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM companies ORDER BY id DESC", conn
        )


def get_company_count() -> int:
    with _conn() as conn:
        return conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]


def update_companies_from_df(df: pd.DataFrame) -> int:
    """Batch update companies from an edited DataFrame. Returns rows updated."""
    editable = {"name", "phone", "phone_type", "website", "website_phone", "website_phone_type", "website_email", "website_email2", "address", "category", "tags", "notes"}
    cols_in_df = [c for c in df.columns if c in editable and is_safe_column_name(c)]
    if not cols_in_df:
        return 0

    sets = ", ".join(f"{c} = ?" for c in cols_in_df)
    now = datetime.now().isoformat()

    rows_to_update: list[tuple[str | int, ...]] = []
    for _, row in df.iterrows():
        cid = row.get("id")
        if cid is None or pd.isna(cid):
            continue
        val_list = [str(row[c]) if not pd.isna(row[c]) else "" for c in cols_in_df]
        val_list.append(now)
        val_list.append(int(cid))
        rows_to_update.append(tuple(val_list))

    with _conn() as conn:
        conn.executemany(
            f"UPDATE companies SET {sets}, updated_at = ? WHERE id = ?",
            rows_to_update,
        )
        conn.commit()
        return len(rows_to_update)


def update_website_phones(results: list[dict]) -> int:
    """Bulk update website phone/email from verification results. Returns count updated."""
    if not results:
        return 0
    now = datetime.now().isoformat()
    rows = [
        (
            r.get("website_phone", ""), r.get("website_phone_type", ""),
            r.get("website_phone2", ""), r.get("website_phone2_type", ""),
            r.get("website_email", ""), r.get("website_email2", ""),
            now, r["id"],
        )
        for r in results if r.get("website_phone") or r.get("website_email")
    ]
    if not rows:
        return 0
    with _conn() as conn:
        conn.executemany(
            "UPDATE companies SET website_phone = ?, website_phone_type = ?, "
            "website_phone2 = ?, website_phone2_type = ?, "
            "website_email = ?, website_email2 = ?, "
            "updated_at = ?, last_updated = ?, last_seen = ? WHERE id = ?",
            [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[6], r[6], r[7]) for r in rows],
        )
        conn.commit()
    return len(rows)


def get_companies_with_website() -> list[dict]:
    """Get companies that have a website URL (for phone verification)."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT id, name, phone, website, website_phone "
            "FROM companies WHERE website LIKE 'http%' ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]


def _dedup_score(r: dict) -> int:
    """Score a company record by data completeness (higher = more data)."""
    s = 0
    if r.get("phone"):
        s += 2
    if r.get("website"):
        s += 1
    if r.get("address"):
        s += 1
    if r.get("category"):
        s += 1
    if r.get("tags"):
        s += 1
    return s


def deduplicate_companies() -> int:
    """Remove duplicates by normalized name, keeping the record with the most data."""
    with _conn() as conn:
        rows = conn.execute("SELECT * FROM companies ORDER BY id").fetchall()

        groups: dict[str, list[dict]] = {}
        for row in rows:
            key = normalize_name(row["name"])
            if not key:
                continue
            groups.setdefault(key, []).append(dict(row))

        to_delete: list[int] = []
        for group in groups.values():
            if len(group) < 2:
                continue
            group.sort(key=_dedup_score, reverse=True)
            for r in group[1:]:
                to_delete.append(r["id"])

        if to_delete:
            placeholders = ",".join("?" * len(to_delete))
            conn.execute(
                f"DELETE FROM companies WHERE id IN ({placeholders})", to_delete
            )
            conn.commit()

        return len(to_delete)


# ---- Tags ----


def get_tags() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute("SELECT * FROM tags ORDER BY name").fetchall()
        return [dict(r) for r in rows]


def add_tag(name: str, color: str = "#6B7280") -> bool:
    with _conn() as conn:
        try:
            conn.execute(
                "INSERT INTO tags (name, color) VALUES (?, ?)", (name, color)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def delete_tag(tag_name: str) -> None:
    with _conn() as conn:
        conn.execute("DELETE FROM tags WHERE name = ?", (tag_name,))
        conn.commit()



def bulk_append_tag(company_ids: list[int], tag: str) -> int:
    """Append a tag to multiple companies in a single transaction. Returns count updated."""
    if not company_ids or not tag:
        return 0
    with _conn() as conn:
        now = datetime.now().isoformat()
        rows = conn.execute(
            f"SELECT id, tags FROM companies WHERE id IN ({','.join('?' * len(company_ids))})",
            company_ids,
        ).fetchall()
        updated = 0
        for row in rows:
            current = row["tags"] or ""
            if tag not in current:
                new_tags = f"{current}, {tag}".strip(", ")
                conn.execute(
                    "UPDATE companies SET tags = ?, updated_at = ? WHERE id = ?",
                    (new_tags, now, row["id"]),
                )
                updated += 1
        conn.commit()
        return updated


# ---- Settings ----


def get_setting(key: str, default: str = "") -> str:
    with _conn() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default


def save_setting(key: str, value: str) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()


# ---- Import ----

MAX_IMPORT_ROWS = 50_000


def import_companies_from_df(df: pd.DataFrame) -> tuple[int, int]:
    """Import companies from a DataFrame. Returns (imported, skipped)."""
    # Enforce row limit
    if len(df) > MAX_IMPORT_ROWS:
        df = df.head(MAX_IMPORT_ROWS)

    # Map common column name variants
    col_map = {
        "company": "name", "company_name": "name", "company name": "name",
        "Company": "name", "Company Name": "name",
        "Phone": "phone", "Website": "website", "Address": "address",
        "Industry": "category", "Category": "category",
    }
    rename = {old: new for old, new in col_map.items() if old in df.columns}
    mapped = df.rename(columns=rename)

    with _conn() as conn:
        existing = conn.execute("SELECT name FROM companies").fetchall()
        existing_keys = {normalize_name(r["name"]) for r in existing}

        imported = 0
        skipped = 0

        rows_to_insert: list[tuple] = []
        for _, row in mapped.iterrows():
            name = str(row.get("name", "")).strip()
            if not name or name == "nan":
                skipped += 1
                continue

            key = normalize_name(name)
            if key in existing_keys:
                skipped += 1
                continue

            def _clean(val: object) -> str:
                s = str(val).strip()
                return "" if s == "nan" else s

            phone = _clean(row.get("phone", ""))
            rows_to_insert.append((
                name,
                phone,
                classify_phone(phone),
                _clean(row.get("website", "")),
                _clean(row.get("address", "")),
                _clean(row.get("category", "")),
                "imported",
            ))
            existing_keys.add(key)
            imported += 1

        if rows_to_insert:
            conn.executemany(
                """INSERT INTO companies
                   (name, phone, phone_type, website, address, category, sources)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                rows_to_insert,
            )
            conn.commit()

        return imported, skipped
