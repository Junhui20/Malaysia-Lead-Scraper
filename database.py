"""SQLite database layer for Lead Scraper."""

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

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        default_tags = [
            ("已打電話", "#3B82F6"),
            ("有興趣", "#10B981"),
            ("不要再打", "#EF4444"),
            ("待跟進", "#F59E0B"),
            ("重要客戶", "#8B5CF6"),
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


# ---- Companies ----


def save_companies(companies: list[dict], session_id: int) -> int:
    with _conn() as conn:
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
            )
            for c in companies
        ]
        conn.executemany(
            """INSERT INTO companies
               (name, phone, phone_type, website, address, category, company_size,
                rating, sources, google_maps_url, jobstreet_url, hiredly_url, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        return len(rows)


def get_all_companies() -> pd.DataFrame:
    with _conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM companies ORDER BY id DESC", conn
        )


def get_company_count() -> int:
    with _conn() as conn:
        return conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]


def update_company_field(company_id: int, field: str, value: str) -> None:
    allowed = {
        "name", "phone", "phone_type", "website", "address",
        "category", "company_size", "rating", "tags", "notes",
    }
    if field not in allowed or not is_safe_column_name(field):
        return
    with _conn() as conn:
        conn.execute(
            f"UPDATE companies SET {field} = ?, updated_at = ? WHERE id = ?",
            (value, datetime.now().isoformat(), company_id),
        )
        conn.commit()


def update_companies_from_df(df: pd.DataFrame) -> int:
    """Batch update companies from an edited DataFrame. Returns rows updated."""
    editable = {"name", "phone", "phone_type", "website", "address", "category", "tags", "notes"}
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


def delete_companies(company_ids: list[int]) -> int:
    if not company_ids:
        return 0
    with _conn() as conn:
        placeholders = ",".join("?" * len(company_ids))
        deleted = conn.execute(
            f"DELETE FROM companies WHERE id IN ({placeholders})", company_ids
        ).rowcount
        conn.commit()
        return deleted


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


def update_tag(old_name: str, new_name: str, color: str) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE tags SET name = ?, color = ? WHERE name = ?",
            (new_name, color, old_name),
        )
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
