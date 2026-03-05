from __future__ import annotations

import os
import sqlite3
from pathlib import Path

try:
    import psycopg
except Exception as exc:  # pragma: no cover
    raise SystemExit("psycopg가 필요합니다. requirements.txt를 설치하세요.") from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQLITE_PATH = REPO_ROOT / "afours" / "accounting.db"


def get_sqlite_path() -> Path:
    raw = os.environ.get("SQLITE_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_SQLITE_PATH


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row["name"] for row in rows]


def fetch_rows(conn: sqlite3.Connection, table: str, columns: list[str]) -> list[tuple]:
    cols = ", ".join(columns)
    cur = conn.execute(f"SELECT {cols} FROM {table}")
    return [tuple(row[c] for c in columns) for row in cur.fetchall()]


def bump_sequence(pg_conn, table: str) -> None:
    cur = pg_conn.execute(
        "SELECT pg_get_serial_sequence(%s, %s)",
        (table, "id"),
    )
    seq_row = cur.fetchone()
    if not seq_row or not seq_row[0]:
        return
    seq_name = seq_row[0]
    max_row = pg_conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table}").fetchone()
    max_id = int(max_row[0] or 0)
    pg_conn.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))


def main() -> None:
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        raise SystemExit("DATABASE_URL 환경변수가 필요합니다.")

    sqlite_path = get_sqlite_path()
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite 파일이 없습니다: {sqlite_path}")

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg.connect(db_url)

    tables = [
        "app_users",
        "accounts",
        "vouchers",
        "journal_entries",
        "monthly_closing",
        "bank_transactions",
        "voucher_status_overrides",
        "upload_batches",
        "products",
    ]

    for table in tables:
        if not table_exists(sqlite_conn, table):
            print(f"[skip] {table} (SQLite에 없음)")
            continue

        columns = sqlite_columns(sqlite_conn, table)
        rows = fetch_rows(sqlite_conn, table, columns)
        if not rows:
            print(f"[skip] {table} (데이터 없음)")
            continue

        placeholders = ", ".join(["%s"] * len(columns))
        col_sql = ", ".join(columns)
        insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        pg_conn.executemany(insert_sql, rows)
        print(f"[ok] {table} -> {len(rows)} rows")

    for table in ("app_users", "vouchers", "journal_entries", "bank_transactions", "upload_batches", "products"):
        bump_sequence(pg_conn, table)

    pg_conn.commit()
    pg_conn.close()
    sqlite_conn.close()
    print("완료: SQLite -> Postgres 마이그레이션 끝")


if __name__ == "__main__":
    main()
