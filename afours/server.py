from __future__ import annotations

import hashlib
import hmac
import os
import sqlite3
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from itsdangerous import BadSignature, BadTimeSignature, URLSafeTimedSerializer
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
try:
    import pandas as pd
except Exception:
    pd = None

try:
    from xlsx2csv import Xlsx2csv
except Exception:
    Xlsx2csv = None

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None
    dict_row = None

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "accounting.db"
UPLOAD_DIR = BASE_DIR / "uploads"
ALLOWED_EXTENSIONS = {"xlsx", "xls"}
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)
AUTH_COOKIE_NAME = "afours_auth"
AUTH_COOKIE_MAX_AGE = 60 * 60 * 24 * 30

COLUMN_CANDIDATES = {
    "date": ["발급일자", "날짜", "일자", "거래일", "date", "Date"],
    "type": ["영수/청구 구분", "영수청구구분", "영수/청구", "구분", "유형", "매입매출", "종류", "type", "Type"],
    "supply": ["품목공급가액", "품목공급가", "공급가액", "공급가", "금액", "amount", "Amount"],
    "vat": ["부가세", "세액", "vat", "VAT"],
    "description": ["적요", "내용", "description", "Description"],
    "partner": ["거래처", "상호", "partner", "Partner"],
}

BANK_COLUMN_CANDIDATES = {
    "date": ["거래일시", "거래일자", "거래일", "일자", "날짜", "date", "Date"],
    "partner": [
        "거래처",
        "입금자명",
        "출금처",
        "거래내용",
        "상대계좌예금주명",
        "적요",
        "내용",
        "상호",
        "partner",
        "Partner",
    ],
    "amount": ["거래금액", "금액", "amount", "Amount"],
    "out_amount": ["출금", "출금액"],
    "in_amount": ["입금", "입금액"],
    "io_type": ["입출금", "거래구분", "구분", "유형", "type", "Type"],
    "description": ["거래내용", "적요", "내용", "메모", "description", "Description"],
}

DEFAULT_ACCOUNTS = [
    ("1000", "현금", "자산"),
    ("1100", "보통예금", "자산"),
    ("1200", "외상매출금", "자산"),
    ("1300", "부가세대급금", "자산"),
    ("2000", "미지급금", "부채"),
    ("2100", "부가세예수금", "부채"),
    ("4000", "매출", "수익"),
    ("5000", "매입", "비용"),
]

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "afours-erp-secret-key")
app.config["UPLOAD_FOLDER"] = str(UPLOAD_DIR)


def get_auth_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.secret_key, salt="afours-auth-cookie")


def build_auth_cookie(username: str) -> str:
    return get_auth_serializer().dumps({"username": username})


def read_auth_cookie(token: str | None) -> str | None:
    if not token:
        return None
    try:
        payload = get_auth_serializer().loads(token, max_age=AUTH_COOKIE_MAX_AGE)
    except (BadSignature, BadTimeSignature):
        return None
    username = str(payload.get("username") or "").strip()
    return username or None


def get_login_username() -> str:
    return os.environ.get("ERP_LOGIN_ID", "admin")


def get_login_password() -> str:
    return os.environ.get("ERP_LOGIN_PASSWORD", "admin1234")


def is_safe_next_path(value: str | None) -> bool:
    if not value:
        return False
    return value.startswith("/") and not value.startswith("//")


def get_conn() -> Any:
    if USE_POSTGRES:
        if psycopg is None:
            raise RuntimeError("psycopg가 설치되지 않았습니다. requirements.txt를 확인하세요.")
        return psycopg.connect(DATABASE_URL, row_factory=dict_row)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def adapt_sql(sql: str) -> str:
    return sql.replace("?", "%s") if USE_POSTGRES else sql


def db_execute(conn: Any, sql: str, params: Iterable | None = None):
    if params is None:
        return conn.execute(adapt_sql(sql))
    return conn.execute(adapt_sql(sql), params)


def db_executemany(conn: Any, sql: str, params_seq: Iterable[Iterable]):
    if hasattr(conn, "executemany"):
        return conn.executemany(adapt_sql(sql), params_seq)
    with conn.cursor() as cur:
        cur.executemany(adapt_sql(sql), params_seq)
        return cur


def db_read_sql(conn: Any, sql: str, params: Iterable | None = None) -> pd.DataFrame:
    if pd is None:
        raise RuntimeError("pandas가 설치되지 않았습니다.")
    return pd.read_sql_query(adapt_sql(sql), conn, params=params)


def row_value(row: Any, key: str, idx: int = 0):
    try:
        return row[key]
    except Exception:
        return row[idx]


def get_table_columns(conn: Any, table: str) -> set[str]:
    if USE_POSTGRES:
        rows = db_execute(
            conn,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            """,
            (table,),
        ).fetchall()
        return {row_value(r, "column_name", 0) for r in rows}
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row_value(r, "name", 1) for r in rows}


def ensure_default_auth_user(conn: Any) -> None:
    id_column = "SERIAL PRIMARY KEY" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS app_users (
            id {id_column},
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    current_count = db_execute(conn, "SELECT COUNT(*) AS cnt FROM app_users").fetchone()["cnt"]
    if int(current_count) > 0:
        return

    db_execute(conn,
        """
        INSERT INTO app_users(username, password_hash, is_admin, is_active, created_at, updated_at)
        VALUES (?, ?, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (get_login_username(), generate_password_hash(get_login_password())),
    )


def get_current_user(conn: Any) -> Any | None:
    username = (session.get("auth_user") or "").strip()
    if not username:
        return None
    return db_execute(conn,
        """
        SELECT id, username, is_admin, is_active
        FROM app_users
        WHERE username = ?
        """,
        (username,),
    ).fetchone()


def verify_login_credentials(conn: Any, username: str, password: str) -> bool:
    row = db_execute(conn,
        """
        SELECT password_hash, is_active
        FROM app_users
        WHERE username = ?
        """,
        (username,),
    ).fetchone()
    if not row or int(row["is_active"]) != 1:
        return False
    return check_password_hash(str(row["password_hash"]), password)


def get_client_ip() -> str:
    forwarded_for = (request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return (request.remote_addr or "").strip()


def log_auth_event(conn: Any, username: str, event_type: str) -> None:
    db_execute(
        conn,
        """
        INSERT INTO auth_event_logs(username, event_type, request_path, ip_address, user_agent, created_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            username,
            event_type,
            request.path,
            get_client_ip(),
            (request.headers.get("User-Agent") or "")[:500],
        ),
    )


def init_db() -> None:
    UPLOAD_DIR.mkdir(exist_ok=True)
    conn = get_conn()
    id_column = "SERIAL PRIMARY KEY" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    db_execute(
        conn,
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_code TEXT PRIMARY KEY,
            account_name TEXT NOT NULL,
            account_type TEXT NOT NULL CHECK(account_type IN ('자산','부채','자본','수익','비용')),
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS auth_event_logs (
            id {id_column},
            username TEXT NOT NULL,
            event_type TEXT NOT NULL,
            request_path TEXT,
            ip_address TEXT,
            user_agent TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS vouchers (
            id {id_column},
            voucher_no TEXT NOT NULL,
            txn_date TEXT NOT NULL,
            year_month TEXT NOT NULL,
            txn_type TEXT NOT NULL,
            supply_amount REAL NOT NULL,
            vat_amount REAL NOT NULL,
            total_amount REAL NOT NULL,
            description TEXT,
            partner TEXT,
            source_file TEXT,
            voucher_hash TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS journal_entries (
            id {id_column},
            voucher_hash TEXT NOT NULL,
            voucher_no TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            txn_date TEXT NOT NULL,
            year_month TEXT NOT NULL,
            dr_cr TEXT NOT NULL CHECK(dr_cr IN ('차변','대변')),
            account_code TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            partner TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(voucher_hash, line_no)
        )
        """,
    )
    db_execute(
        conn,
        """
        CREATE TABLE IF NOT EXISTS monthly_closing (
            year_month TEXT PRIMARY KEY,
            is_closed INTEGER NOT NULL DEFAULT 1,
            note TEXT,
            closed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id {id_column},
            txn_date TEXT NOT NULL,
            year_month TEXT NOT NULL,
            partner TEXT,
            io_type TEXT NOT NULL,
            amount REAL NOT NULL,
            in_amount REAL NOT NULL DEFAULT 0,
            out_amount REAL NOT NULL DEFAULT 0,
            description TEXT,
            source_file TEXT,
            tx_hash TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    db_execute(
        conn,
        """
        CREATE TABLE IF NOT EXISTS voucher_status_overrides (
            voucher_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL CHECK(status IN ('지급완료', '미지급금')),
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS upload_batches (
            id {id_column},
            source_type TEXT NOT NULL CHECK(source_type IN ('세금계산서', '통장')),
            source_file TEXT,
            uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
            saved_count INTEGER DEFAULT 0
        )
        """,
    )
    db_execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS products (
            id {id_column},
            name TEXT NOT NULL,
            price INTEGER NOT NULL
        )
        """,
    )
    ensure_default_auth_user(conn)
    ensure_bank_table_columns(conn)
    ensure_batch_columns(conn)
    if USE_POSTGRES:
        db_executemany(
            conn,
            """
            INSERT INTO accounts(account_code, account_name, account_type)
            VALUES (?, ?, ?)
            ON CONFLICT (account_code) DO NOTHING
            """,
            DEFAULT_ACCOUNTS,
        )
    else:
        db_executemany(
            conn,
            "INSERT OR IGNORE INTO accounts(account_code, account_name, account_type) VALUES (?, ?, ?)",
            DEFAULT_ACCOUNTS,
        )
    conn.commit()
    conn.close()


def ensure_bank_table_columns(conn: Any) -> None:
    cols = get_table_columns(conn, "bank_transactions")
    if "io_type" not in cols:
        db_execute(conn, "ALTER TABLE bank_transactions ADD COLUMN io_type TEXT DEFAULT '출금'")
    if "in_amount" not in cols:
        db_execute(conn, "ALTER TABLE bank_transactions ADD COLUMN in_amount REAL DEFAULT 0")
    if "out_amount" not in cols:
        db_execute(conn, "ALTER TABLE bank_transactions ADD COLUMN out_amount REAL DEFAULT 0")


def ensure_batch_columns(conn: Any) -> None:
    voucher_cols = get_table_columns(conn, "vouchers")
    if "batch_id" not in voucher_cols:
        db_execute(conn, "ALTER TABLE vouchers ADD COLUMN batch_id INTEGER")

    journal_cols = get_table_columns(conn, "journal_entries")
    if "batch_id" not in journal_cols:
        db_execute(conn, "ALTER TABLE journal_entries ADD COLUMN batch_id INTEGER")

    bank_cols = get_table_columns(conn, "bank_transactions")
    if "batch_id" not in bank_cols:
        db_execute(conn, "ALTER TABLE bank_transactions ADD COLUMN batch_id INTEGER")


# Ensure schema exists when running under WSGI servers like gunicorn.
init_db()


def allowed_file(name: str) -> bool:
    return "." in name and name.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def find_column(df: pd.DataFrame, key: str) -> str | None:
    normalized_map: dict[str, str] = {}
    for col in df.columns:
        normalized_map[normalize_text(col)] = col
    for candidate in COLUMN_CANDIDATES[key]:
        hit = normalized_map.get(normalize_text(candidate))
        if hit is not None:
            return hit
    return None


def find_bank_column(df: pd.DataFrame, key: str) -> str | None:
    normalized_map: dict[str, str] = {}
    for col in df.columns:
        normalized_map[normalize_text(col)] = col
    for candidate in BANK_COLUMN_CANDIDATES[key]:
        hit = normalized_map.get(normalize_text(candidate))
        if hit is not None:
            return hit
    return None


def normalize_type(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if "영수" in text:
        return "매출"
    if "청구" in text:
        return "매입"
    if "매입" in text or "purchase" in text or "buy" in text:
        return "매입"
    if "매출" in text or "sale" in text or "sell" in text:
        return "매출"
    return None


def normalize_text(value: object) -> str:
    text = str(value).strip().lower()
    return "".join(ch for ch in text if ch.isalnum() or ("가" <= ch <= "힣"))


def extract_columns_with_header_detection(raw_df: pd.DataFrame) -> pd.DataFrame:
    required_keys = ["date", "type", "supply"]
    best_row = None
    best_score = -1
    scan_rows = min(len(raw_df), 15)

    for i in range(scan_rows):
        row_values = [normalize_text(v) for v in raw_df.iloc[i].tolist()]
        score = 0
        for key in required_keys:
            candidates = {normalize_text(c) for c in COLUMN_CANDIDATES[key]}
            if any(v in candidates for v in row_values):
                score += 1
        if score > best_score:
            best_score = score
            best_row = i

    if best_row is None or best_score < 2:
        df = raw_df.copy()
        df.columns = [str(c) for c in df.columns]
        return df

    header = raw_df.iloc[best_row].tolist()
    columns = []
    for idx, value in enumerate(header):
        name = str(value).strip() if pd.notna(value) else ""
        columns.append(name if name else f"COL_{idx}")

    df = raw_df.iloc[best_row + 1 :].copy().reset_index(drop=True)
    df.columns = columns
    return df


def extract_bank_columns_with_header_detection(raw_df: pd.DataFrame) -> pd.DataFrame:
    required_keys = ["date"]
    best_row = None
    best_score = -1
    scan_rows = min(len(raw_df), 15)

    for i in range(scan_rows):
        row_values = [normalize_text(v) for v in raw_df.iloc[i].tolist()]
        score = 0
        for key in required_keys:
            candidates = {normalize_text(c) for c in BANK_COLUMN_CANDIDATES[key]}
            if any(v in candidates for v in row_values):
                score += 1
        amount_candidates = (
            {normalize_text(c) for c in BANK_COLUMN_CANDIDATES["amount"]}
            | {normalize_text(c) for c in BANK_COLUMN_CANDIDATES["out_amount"]}
            | {normalize_text(c) for c in BANK_COLUMN_CANDIDATES["in_amount"]}
        )
        if any(v in amount_candidates for v in row_values):
            score += 1
        if score > best_score:
            best_score = score
            best_row = i

    if best_row is None or best_score < 1:
        df = raw_df.copy()
        df.columns = [str(c) for c in df.columns]
        return df

    header = raw_df.iloc[best_row].tolist()
    columns = []
    for idx, value in enumerate(header):
        name = str(value).strip() if pd.notna(value) else ""
        columns.append(name if name else f"COL_{idx}")

    df = raw_df.iloc[best_row + 1 :].copy().reset_index(drop=True)
    df.columns = columns
    return df


def to_numeric(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce")


def get_series(df: pd.DataFrame, col: str) -> pd.Series:
    selected = df[col]
    if isinstance(selected, pd.DataFrame):
        return selected.iloc[:, 0]
    return selected


def normalize_upload_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    if pd is None:
        raise RuntimeError("pandas/openpyxl 미설치로 엑셀 처리를 사용할 수 없습니다.")
    date_col = find_column(raw_df, "date")
    type_col = find_column(raw_df, "type")
    supply_col = find_column(raw_df, "supply")
    vat_col = find_column(raw_df, "vat")
    desc_col = find_column(raw_df, "description")
    partner_col = find_column(raw_df, "partner")

    if not date_col or not type_col or not supply_col:
        raise ValueError("필수 컬럼 부족: 날짜, 구분, 공급가액")

    df = pd.DataFrame()
    date_series = get_series(raw_df, date_col)
    type_series = get_series(raw_df, type_col)
    supply_series = get_series(raw_df, supply_col)
    vat_series = get_series(raw_df, vat_col) if vat_col else pd.Series([pd.NA] * len(raw_df))
    desc_series = get_series(raw_df, desc_col) if desc_col else pd.Series([""] * len(raw_df))

    df["txn_date"] = pd.to_datetime(date_series, errors="coerce")
    df["txn_type"] = type_series.apply(normalize_type)
    df["supply_amount"] = to_numeric(supply_series)
    df["vat_amount"] = to_numeric(vat_series) if vat_col else pd.NA
    df["description"] = desc_series.astype(str).str.strip()

    if partner_col:
        partner_series = get_series(raw_df, partner_col)
        df["partner"] = partner_series.astype(str).str.strip()
    else:
        df["partner"] = df["description"]

    df["vat_amount"] = df["vat_amount"].fillna((df["supply_amount"] * 0.1).round(0))
    df["total_amount"] = df["supply_amount"] + df["vat_amount"]
    df = df.dropna(subset=["txn_date", "txn_type", "supply_amount", "vat_amount"])
    df = df[df["supply_amount"] >= 0]
    df["txn_date"] = df["txn_date"].dt.strftime("%Y-%m-%d")
    df["year_month"] = pd.to_datetime(df["txn_date"]).dt.to_period("M").astype(str)
    return df[[
        "txn_date",
        "year_month",
        "txn_type",
        "supply_amount",
        "vat_amount",
        "total_amount",
        "description",
        "partner",
    ]]


def normalize_bank_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_bank_column(raw_df, "date")
    amount_col = find_bank_column(raw_df, "amount")
    out_col = find_bank_column(raw_df, "out_amount")
    in_col = find_bank_column(raw_df, "in_amount")
    partner_col = find_bank_column(raw_df, "partner")
    io_col = find_bank_column(raw_df, "io_type")
    desc_col = find_bank_column(raw_df, "description")

    if not date_col or (not amount_col and not out_col and not in_col):
        raise ValueError("통장 필수 컬럼 부족: 거래일시(또는 거래일자), 출금(또는 거래금액)")

    date_series = get_series(raw_df, date_col)
    amount_series = to_numeric(get_series(raw_df, amount_col)) if amount_col else pd.Series([0] * len(raw_df))
    out_series = to_numeric(get_series(raw_df, out_col)) if out_col else pd.Series([0] * len(raw_df))
    in_series = to_numeric(get_series(raw_df, in_col)) if in_col else pd.Series([0] * len(raw_df))
    partner_series = (
        get_series(raw_df, partner_col) if partner_col else pd.Series([""] * len(raw_df))
    )
    desc_series = (
        get_series(raw_df, desc_col) if desc_col else pd.Series([""] * len(raw_df))
    )

    base = pd.DataFrame()
    base["txn_date"] = pd.to_datetime(date_series, errors="coerce")
    base["partner"] = partner_series.astype(str).str.strip()
    base["description"] = desc_series.astype(str).str.strip()
    base.loc[base["partner"] == "", "partner"] = base["description"]

    rows: list[pd.DataFrame] = []
    if out_col or in_col:
        out_df = base.copy()
        out_df["io_type"] = "출금"
        out_df["out_amount"] = out_series.abs().fillna(0)
        out_df["in_amount"] = 0.0
        out_df["amount"] = out_df["out_amount"]
        out_df = out_df[out_df["out_amount"] > 0]
        rows.append(out_df)

        in_df = base.copy()
        in_df["io_type"] = "입금"
        in_df["in_amount"] = in_series.abs().fillna(0)
        in_df["out_amount"] = 0.0
        in_df["amount"] = in_df["in_amount"]
        in_df = in_df[in_df["in_amount"] > 0]
        rows.append(in_df)
    else:
        single = base.copy()
        if io_col:
            io_series = get_series(raw_df, io_col).astype(str).str.strip().str.lower()
            outflow_mask = io_series.str.contains("출금|지급|인출")
            single["io_type"] = outflow_mask.map({True: "출금", False: "입금"})
        else:
            single["io_type"] = (amount_series < 0).map({True: "출금", False: "입금"})
        single["amount"] = amount_series.abs().fillna(0)
        single["out_amount"] = single.apply(
            lambda x: x["amount"] if x["io_type"] == "출금" else 0.0, axis=1
        )
        single["in_amount"] = single.apply(
            lambda x: x["amount"] if x["io_type"] == "입금" else 0.0, axis=1
        )
        single = single[single["amount"] > 0]
        rows.append(single)

    df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "txn_date",
                "year_month",
                "partner",
                "io_type",
                "amount",
                "in_amount",
                "out_amount",
                "description",
            ]
        )

    df = df.dropna(subset=["txn_date", "amount"])
    df["txn_date"] = df["txn_date"].dt.strftime("%Y-%m-%d")
    df["year_month"] = pd.to_datetime(df["txn_date"]).dt.to_period("M").astype(str)
    return df[
        [
            "txn_date",
            "year_month",
            "partner",
            "io_type",
            "amount",
            "in_amount",
            "out_amount",
            "description",
        ]
    ]




def safe_read_excel(source, filename: str) -> pd.DataFrame:
    try:
        return pd.read_excel(source, header=None)
    except Exception as exc:
        message = str(exc)
        is_xlsx = filename.lower().endswith('.xlsx')
        if is_xlsx and 'styleId' in message and Xlsx2csv is not None:
            output = StringIO()
            if isinstance(source, (str, Path)):
                Xlsx2csv(str(source), outputencoding='utf-8').convert(output, sheetid=1)
            else:
                source.seek(0)
                data = source.read()
                Xlsx2csv(BytesIO(data), outputencoding='utf-8').convert(output, sheetid=1)
            output.seek(0)
            return pd.read_csv(output, header=None)
        raise

def hash_voucher(row: pd.Series) -> str:
    payload = "|".join(
        [
            str(row["txn_date"]),
            str(row["txn_type"]),
            f"{float(row['supply_amount']):.2f}",
            f"{float(row['vat_amount']):.2f}",
            str(row["description"]),
            str(row["partner"]),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def make_voucher_no(txn_date: str, voucher_hash: str) -> str:
    return f"V{txn_date.replace('-', '')}-{voucher_hash[:6].upper()}"


def is_month_closed(conn: Any, year_month: str) -> bool:
    row = db_execute(
        conn,
        "SELECT is_closed FROM monthly_closing WHERE year_month = ?",
        (year_month,),
    ).fetchone()
    return bool(row and row_value(row, "is_closed", 0) == 1)


def create_upload_batch(conn: Any, source_type: str, source_file: str) -> int:
    if USE_POSTGRES:
        cur = db_execute(
            conn,
            """
            INSERT INTO upload_batches(source_type, source_file, uploaded_at, saved_count)
            VALUES (?, ?, CURRENT_TIMESTAMP, 0)
            RETURNING id
            """,
            (source_type, source_file),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row_value(row, "id", 0))
    cur = db_execute(
        conn,
        """
        INSERT INTO upload_batches(source_type, source_file, uploaded_at, saved_count)
        VALUES (?, ?, CURRENT_TIMESTAMP, 0)
        """,
        (source_type, source_file),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_upload_batch_saved_count(conn: Any, batch_id: int, saved_count: int) -> None:
    db_execute(
        conn,
        "UPDATE upload_batches SET saved_count = ? WHERE id = ?",
        (int(saved_count), int(batch_id)),
    )
    conn.commit()


def create_journal_lines(txn_type: str, supply: float, vat: float) -> list[tuple[str, str, float]]:
    if txn_type == "매출":
        lines = [("차변", "1200", supply + vat), ("대변", "4000", supply)]
        if vat > 0:
            lines.append(("대변", "2100", vat))
        return lines

    lines = [("차변", "5000", supply), ("대변", "2000", supply + vat)]
    if vat > 0:
        lines.insert(1, ("차변", "1300", vat))
    return lines


def insert_uploaded_rows(
    conn: Any, df: pd.DataFrame, source_file: str, batch_id: int
) -> dict[str, int]:
    result = {"saved": 0, "duplicate": 0, "closed": 0}
    if USE_POSTGRES:
        insert_sql = """
            INSERT INTO vouchers (
                voucher_no, txn_date, year_month, txn_type,
                supply_amount, vat_amount, total_amount,
                description, partner, source_file, voucher_hash, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (voucher_hash) DO NOTHING
        """
    else:
        insert_sql = """
            INSERT OR IGNORE INTO vouchers (
                voucher_no, txn_date, year_month, txn_type,
                supply_amount, vat_amount, total_amount,
                description, partner, source_file, voucher_hash, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    for _, row in df.iterrows():
        year_month = str(row["year_month"])
        if is_month_closed(conn, year_month):
            result["closed"] += 1
            continue

        voucher_hash = hash_voucher(row)
        voucher_no = make_voucher_no(row["txn_date"], voucher_hash)
        cur = db_execute(
            conn,
            insert_sql,
            (
                voucher_no,
                row["txn_date"],
                row["year_month"],
                row["txn_type"],
                float(row["supply_amount"]),
                float(row["vat_amount"]),
                float(row["total_amount"]),
                row["description"],
                row["partner"],
                source_file,
                voucher_hash,
                batch_id,
            ),
        )
        if cur.rowcount != 1:
            result["duplicate"] += 1
            continue

        lines = create_journal_lines(
            str(row["txn_type"]), float(row["supply_amount"]), float(row["vat_amount"])
        )
        line_no = 1
        for dr_cr, account_code, amount in lines:
            db_execute(
                conn,
                """
                INSERT INTO journal_entries (
                    voucher_hash, voucher_no, line_no, txn_date, year_month,
                    dr_cr, account_code, amount, description, partner, batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    voucher_hash,
                    voucher_no,
                    line_no,
                    row["txn_date"],
                    row["year_month"],
                    dr_cr,
                    account_code,
                    float(amount),
                    row["description"],
                    row["partner"],
                    batch_id,
                ),
            )
            line_no += 1
        result["saved"] += 1

    conn.commit()
    return result


def hash_bank_txn(row: pd.Series) -> str:
    payload = "|".join(
        [
            str(row["txn_date"]),
            str(row["partner"]),
            str(row["io_type"]),
            f"{float(row['amount']):.2f}",
            str(row["description"]),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compact_partner_name(value: object) -> str:
    return normalize_text(value)


def longest_common_substring_len(a: str, b: str) -> int:
    if not a or not b:
        return 0
    dp = [0] * (len(b) + 1)
    best = 0
    for i in range(1, len(a) + 1):
        prev = 0
        for j in range(1, len(b) + 1):
            temp = dp[j]
            if a[i - 1] == b[j - 1]:
                dp[j] = prev + 1
                if dp[j] > best:
                    best = dp[j]
            else:
                dp[j] = 0
            prev = temp
    return best


def partner_match_score(voucher_partner: str, bank_partner: str) -> int:
    v = compact_partner_name(voucher_partner)
    b = compact_partner_name(bank_partner)
    if not v or not b:
        return 0
    if v == b:
        return 1000 + len(v)
    common_len = longest_common_substring_len(v, b)
    if common_len >= 2:
        return common_len
    return 0


def insert_bank_rows(
    conn: Any, df: pd.DataFrame, source_file: str, batch_id: int
) -> dict[str, int]:
    result = {"saved": 0, "duplicate": 0}
    if USE_POSTGRES:
        insert_sql = """
            INSERT INTO bank_transactions (
                txn_date, year_month, partner, io_type, amount, in_amount, out_amount,
                description, source_file, tx_hash, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tx_hash) DO NOTHING
        """
    else:
        insert_sql = """
            INSERT OR IGNORE INTO bank_transactions (
                txn_date, year_month, partner, io_type, amount, in_amount, out_amount,
                description, source_file, tx_hash, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    for _, row in df.iterrows():
        tx_hash = hash_bank_txn(row)
        cur = db_execute(
            conn,
            insert_sql,
            (
                row["txn_date"],
                row["year_month"],
                row["partner"],
                row["io_type"],
                float(row["amount"]),
                float(row["in_amount"]),
                float(row["out_amount"]),
                row["description"],
                source_file,
                tx_hash,
                batch_id,
            ),
        )
        if cur.rowcount == 1:
            result["saved"] += 1
        else:
            result["duplicate"] += 1
    conn.commit()
    return result


def apply_payable_status(conn: Any, rows: list[Any], end_date: str | None) -> list[dict]:
    payment_sql, payment_params = append_date_range_filters(
        """
        SELECT COALESCE(NULLIF(TRIM(partner), ''), '(미입력)') AS partner_name,
               SUM(amount) AS paid_amount
        FROM bank_transactions
        WHERE 1=1
          AND COALESCE(io_type, '출금') = '출금'
        GROUP BY partner_name
        """,
        [],
        "txn_date",
        None,
        end_date,
    )
    payment_rows = db_execute(conn, payment_sql, payment_params).fetchall()
    voucher_partner_names = {
        ((dict(r).get("partner") or "").strip() or "(미입력)") for r in rows
    }
    paid_map = {name: 0.0 for name in voucher_partner_names}

    for r in payment_rows:
        bank_partner_name = r["partner_name"]
        amount = float(r["paid_amount"] or 0)
        best_partner = None
        best_score = 0
        for voucher_partner in voucher_partner_names:
            score = partner_match_score(voucher_partner, bank_partner_name)
            if score > best_score:
                best_score = score
                best_partner = voucher_partner
        if best_partner is not None and best_score > 0:
            paid_map[best_partner] = paid_map.get(best_partner, 0.0) + amount

    partner_groups: dict[str, list[dict]] = {}
    for r in rows:
        item = dict(r)
        partner_name = (item.get("partner") or "").strip() or "(미입력)"
        item["partner_name"] = partner_name
        partner_groups.setdefault(partner_name, []).append(item)

    for partner_name, items in partner_groups.items():
        items.sort(key=lambda x: (x["txn_date"], x["id"]))
        remain_paid = paid_map.get(partner_name, 0.0)
        for item in items:
            if item["txn_type"] != "매입":
                item["paid_amount"] = 0.0
                item["unpaid_amount"] = 0.0
                item["payable_status"] = "-"
                continue
            total = float(item["total_amount"] or 0)
            paid = min(total, max(remain_paid, 0))
            unpaid = max(total - paid, 0)
            remain_paid -= paid
            item["paid_amount"] = paid
            item["unpaid_amount"] = unpaid
            item["payable_status"] = "지급완료" if unpaid <= 0.0001 else "미지급금"

    override_rows = db_execute(conn,
        "SELECT voucher_id, status FROM voucher_status_overrides"
    ).fetchall()
    override_map = {int(r["voucher_id"]): r["status"] for r in override_rows}

    merged: list[dict] = []
    for items in partner_groups.values():
        merged.extend(items)
    for item in merged:
        voucher_id = int(item.get("id", 0))
        item["manual_status"] = override_map.get(voucher_id)
        if item["manual_status"]:
            item["payable_status"] = item["manual_status"]
    merged.sort(key=lambda x: (x["txn_date"], x["id"]), reverse=True)
    return merged


def date_range_where() -> tuple[str | None, str | None]:
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    return start, end


def append_date_range_filters(
    sql: str,
    params: list[Any],
    date_column: str,
    start: str | None,
    end: str | None,
) -> tuple[str, list[Any]]:
    if start:
        sql += f"\n  AND {date_column} >= ?"
        params.append(start)
    if end:
        sql += f"\n  AND {date_column} <= ?"
        params.append(end)
    return sql, params


def build_export_dataframes(conn: Any) -> dict[str, pd.DataFrame]:
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    txn_type = (request.args.get("txn_type") or "").strip()
    status_filter = (request.args.get("status") or "").strip()
    io_type = (request.args.get("io_type") or "").strip()

    vouchers_sql, vouchers_params = append_date_range_filters(
        """
        SELECT id, voucher_no, txn_date, year_month, txn_type,
               supply_amount, vat_amount, total_amount, partner, description, source_file, created_at
        FROM vouchers
        WHERE 1=1
          AND (? = '' OR txn_type = ?)
        ORDER BY txn_date DESC, id DESC
        """,
        [txn_type, txn_type],
        "txn_date",
        start,
        end,
    )
    vouchers_rows = db_execute(conn, vouchers_sql, vouchers_params).fetchall()
    vouchers_items = apply_payable_status(conn, vouchers_rows, end)
    if status_filter:
        vouchers_items = [r for r in vouchers_items if str(r.get("payable_status", "")) == status_filter]
    vouchers_df = pd.DataFrame(vouchers_items)
    if not vouchers_df.empty:
        vouchers_df = vouchers_df[
            [
                "voucher_no",
                "txn_date",
                "year_month",
                "txn_type",
                "supply_amount",
                "vat_amount",
                "total_amount",
                "paid_amount",
                "unpaid_amount",
                "payable_status",
                "manual_status",
                "partner",
                "description",
                "source_file",
                "created_at",
            ]
        ]
        vouchers_df.columns = [
            "전표번호",
            "날짜",
            "년월",
            "구분",
            "공급가액",
            "부가세",
            "합계",
            "지급액",
            "미지급잔액",
            "상태",
            "수동상태",
            "거래처",
            "적요",
            "원본파일",
            "생성일시",
        ]

    journals_sql, journals_params = append_date_range_filters(
        """
        SELECT j.voucher_no AS 전표번호, j.txn_date AS 날짜, j.dr_cr AS 차대,
               j.account_code AS 계정코드, COALESCE(a.account_name, '(미등록)') AS 계정과목,
               j.amount AS 금액, j.partner AS 거래처, j.description AS 적요
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE 1=1
        ORDER BY j.txn_date DESC, j.voucher_no DESC, j.line_no ASC
        """,
        [],
        "j.txn_date",
        start,
        end,
    )
    journals_df = db_read_sql(conn, journals_sql, params=journals_params)

    bank_sql, bank_params = append_date_range_filters(
        """
        SELECT txn_date AS 거래일, io_type AS 구분, partner AS 거래처,
               in_amount AS 입금, out_amount AS 출금, amount AS 금액,
               description AS 내용, source_file AS 원본파일, created_at AS 생성일시
        FROM bank_transactions
        WHERE 1=1
          AND (? = '' OR io_type = ?)
        ORDER BY txn_date DESC, id DESC
        """,
        [io_type, io_type],
        "txn_date",
        start,
        end,
    )
    bank_df = db_read_sql(conn, bank_sql, params=bank_params)

    accounts_df = db_read_sql(
        conn,
        """
        SELECT account_code AS 계정코드, account_name AS 계정과목, account_type AS 계정구분
        FROM accounts
        ORDER BY account_code
        """,
    )

    batches_df = db_read_sql(
        conn,
        """
        SELECT id AS 배치ID, source_type AS 업로드유형, source_file AS 파일명,
               uploaded_at AS 업로드일시, saved_count AS 저장건수
        FROM upload_batches
        ORDER BY id DESC
        """,
    )

    return {
        "전표원장": vouchers_df,
        "분개장": journals_df,
        "통장입출금": bank_df,
        "계정과목": accounts_df,
        "업로드이력": batches_df,
    }


@app.context_processor
def inject_top_rollback_batches():
    conn = get_conn()
    batches = db_execute(conn,
        """
        SELECT id, uploaded_at
        FROM upload_batches
        ORDER BY id DESC
        LIMIT 200
        """
    ).fetchall()
    user = get_current_user(conn)
    conn.close()
    return {
        "top_rollback_batches": batches,
        "auth_user": session.get("auth_user"),
        "auth_is_admin": bool(user and int(user["is_admin"]) == 1),
    }


@app.before_request
def require_login():
    endpoint = request.endpoint or ""
    if endpoint in {"login", "static"}:
        return None
    if session.get("auth_user"):
        return None
    remembered_username = read_auth_cookie(request.cookies.get(AUTH_COOKIE_NAME))
    if remembered_username:
        conn = get_conn()
        try:
            row = db_execute(
                conn,
                """
                SELECT username, is_active
                FROM app_users
                WHERE username = ?
                """,
                (remembered_username,),
            ).fetchone()
            if row and int(row["is_active"]) == 1:
                session["auth_user"] = remembered_username
                session["logged_in_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_auth_event(conn, remembered_username, "session_restored")
                conn.commit()
                return None
        finally:
            conn.close()
    next_path = request.full_path if request.query_string else request.path
    return redirect(url_for("login", next=next_path))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        next_path = (request.form.get("next") or "").strip()
        conn = get_conn()
        ok = verify_login_credentials(conn, username, password)
        if ok:
            session.clear()
            session["auth_user"] = username
            session["logged_in_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_auth_event(conn, username, "login_success")
            conn.commit()
            conn.close()
            response = redirect(next_path) if is_safe_next_path(next_path) else redirect(url_for("index"))
            response.set_cookie(
                AUTH_COOKIE_NAME,
                build_auth_cookie(username),
                max_age=AUTH_COOKIE_MAX_AGE,
                httponly=True,
                samesite="Lax",
            )
            return response
        conn.close()
        flash("로그인 실패: 아이디 또는 비밀번호를 확인하세요.", "error")

    if session.get("auth_user"):
        return redirect(url_for("index"))
    next_path = (request.args.get("next") or "").strip()
    if not is_safe_next_path(next_path):
        next_path = ""
    return render_template("login.html", next_path=next_path)


@app.route("/logout", methods=["POST"])
def logout():
    username = (session.get("auth_user") or "").strip()
    if username:
        conn = get_conn()
        log_auth_event(conn, username, "logout")
        conn.commit()
        conn.close()
    session.clear()
    flash("로그아웃되었습니다.", "success")
    response = redirect(url_for("login"))
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response


@app.route("/api/products")
def products():
    if not USE_POSTGRES:
        return jsonify({"error": "DATABASE_URL이 설정되지 않았습니다."}), 500

    conn = get_conn()
    try:
        rows = db_execute(conn, "SELECT id, name, price FROM products ORDER BY id").fetchall()
        data = []
        for row in rows:
            item = dict(row)
            price = item.get("price")
            data.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "price": float(price) if price is not None else None,
                }
            )
        return jsonify(data)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@app.route("/settings/users", methods=["GET", "POST"])
def settings_users():
    conn = get_conn()
    user = get_current_user(conn)
    if not user:
        conn.close()
        return redirect(url_for("login", next=request.path))

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "create_user":
            if int(user["is_admin"]) != 1:
                conn.close()
                flash("권한 없음: 관리자만 신규 ID를 생성할 수 있습니다.", "error")
                return redirect(url_for("settings_users"))

            username = (request.form.get("new_username") or "").strip()
            password = request.form.get("new_password") or ""
            password_confirm = request.form.get("new_password_confirm") or ""
            if len(username) < 3:
                flash("신규 ID는 3자 이상으로 입력하세요.", "error")
            elif len(password) < 6:
                flash("비밀번호는 6자 이상으로 입력하세요.", "error")
            elif not hmac.compare_digest(password, password_confirm):
                flash("신규 비밀번호 확인값이 일치하지 않습니다.", "error")
            else:
                exists = db_execute(conn,
                    "SELECT 1 FROM app_users WHERE username = ?",
                    (username,),
                ).fetchone()
                if exists:
                    flash("이미 존재하는 ID입니다.", "error")
                else:
                    db_execute(conn,
                        """
                        INSERT INTO app_users(username, password_hash, is_admin, is_active, created_at, updated_at)
                        VALUES (?, ?, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (username, generate_password_hash(password)),
                    )
                    conn.commit()
                    flash("신규 ID가 등록되었습니다.", "success")

        elif action == "change_password":
            current_password = request.form.get("current_password") or ""
            new_password = request.form.get("change_password") or ""
            new_password_confirm = request.form.get("change_password_confirm") or ""
            if not verify_login_credentials(conn, str(user["username"]), current_password):
                flash("현재 비밀번호가 올바르지 않습니다.", "error")
            elif len(new_password) < 6:
                flash("새 비밀번호는 6자 이상으로 입력하세요.", "error")
            elif not hmac.compare_digest(new_password, new_password_confirm):
                flash("새 비밀번호 확인값이 일치하지 않습니다.", "error")
            else:
                db_execute(conn,
                    """
                    UPDATE app_users
                    SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (generate_password_hash(new_password), int(user["id"])),
                )
                conn.commit()
                flash("비밀번호가 변경되었습니다.", "success")
        else:
            flash("잘못된 요청입니다.", "error")

        conn.close()
        return redirect(url_for("settings_users"))

    users = db_execute(conn,
        """
        SELECT username, is_admin, is_active, created_at, updated_at
        FROM app_users
        ORDER BY id DESC
        """
    ).fetchall()
    auth_logs = db_execute(
        conn,
        """
        SELECT username, event_type, request_path, ip_address, user_agent, created_at
        FROM auth_event_logs
        ORDER BY id DESC
        LIMIT 50
        """,
    ).fetchall()
    conn.close()
    return render_template("settings_users.html", users=users, auth_logs=auth_logs)


@app.route("/export-xlsx")
def export_xlsx():
    if pd is None:
        flash("엑셀 다운로드는 pandas/openpyxl 설치 후 사용할 수 있습니다.", "error")
        return redirect(request.referrer or url_for("index"))

    conn = get_conn()
    dataframes = build_export_dataframes(conn)
    conn.close()

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in dataframes.items():
            export_df = df if not df.empty else pd.DataFrame([{"안내": "데이터가 없습니다."}])
            export_df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    output.seek(0)

    now_text = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        output,
        as_attachment=True,
        download_name=f"afours_export_{now_text}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/")
def index():
    conn = get_conn()
    start, end = date_range_where()
    quick = (request.args.get("quick") or "").strip()
    today = datetime.now().date()
    if quick in ("60", "90"):
        days = int(quick)
        start = (today - timedelta(days=days - 1)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
    else:
        if not start:
            start = today.replace(day=1).strftime("%Y-%m-%d")
        if not end:
            end = today.strftime("%Y-%m-%d")

    monthly_sql, monthly_params = append_date_range_filters(
        """
        SELECT year_month,
               SUM(CASE WHEN txn_type='매출' THEN supply_amount ELSE 0 END) AS sales,
               SUM(CASE WHEN txn_type='매입' THEN supply_amount ELSE 0 END) AS purchases,
               SUM(CASE WHEN txn_type='매출' THEN supply_amount ELSE 0 END)
               - SUM(CASE WHEN txn_type='매입' THEN supply_amount ELSE 0 END) AS profit
        FROM vouchers
        WHERE 1=1
        GROUP BY year_month
        ORDER BY year_month DESC
        """,
        [],
        "txn_date",
        start,
        end,
    )
    monthly = db_execute(conn, monthly_sql, monthly_params).fetchall()

    totals_sql, totals_params = append_date_range_filters(
        """
        SELECT
          COALESCE(SUM(CASE WHEN txn_type='매출' THEN supply_amount ELSE 0 END), 0) AS sales,
          COALESCE(SUM(CASE WHEN txn_type='매입' THEN supply_amount ELSE 0 END), 0) AS purchases
        FROM vouchers
        WHERE 1=1
        """,
        [],
        "txn_date",
        start,
        end,
    )
    totals = db_execute(conn, totals_sql, totals_params).fetchone()

    conn.close()
    sales = float(totals["sales"])
    purchases = float(totals["purchases"])
    profit = sales - purchases
    return render_template(
        "index.html",
        monthly=monthly,
        sales=sales,
        purchases=purchases,
        profit=profit,
        start=start,
        end=end,
        quick=quick,
    )


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if pd is None:
        flash("현재 서버는 엑셀 업로드 비활성 모드입니다. pandas/openpyxl 설치 후 사용하세요.", "error")
        return render_template("upload.html", upload_enabled=False)
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("엑셀 파일을 선택하세요.", "error")
            return redirect(url_for("upload"))
        if not allowed_file(file.filename):
            flash("xlsx/xls 파일만 업로드 가능합니다.", "error")
            return redirect(url_for("upload"))

        filename = secure_filename(file.filename)
        path = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
        file.save(path)

        try:
            raw_df = safe_read_excel(path, filename)
            raw_df = extract_columns_with_header_detection(raw_df)
            normalized = normalize_upload_dataframe(raw_df)
            conn = get_conn()
            batch_id = create_upload_batch(conn, "세금계산서", filename)
            result = insert_uploaded_rows(conn, normalized, filename, batch_id)
            update_upload_batch_saved_count(conn, batch_id, result["saved"])
            conn.close()
            flash(
                f"업로드 완료: 저장 {result['saved']}건 / 중복 {result['duplicate']}건 / 마감월 제외 {result['closed']}건",
                "success",
            )
        except Exception as exc:
            flash(f"처리 실패: {exc}", "error")

        return redirect(url_for("upload"))

    return render_template("upload.html", upload_enabled=True)


@app.route("/bank-upload", methods=["GET", "POST"])
def bank_upload():
    if pd is None:
        flash("현재 서버는 엑셀 업로드 비활성 모드입니다. pandas/openpyxl 설치 후 사용하세요.", "error")
        return render_template("bank_upload.html", upload_enabled=False)
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("통장 엑셀 파일을 선택하세요.", "error")
            return redirect(url_for("bank_upload"))
        if not allowed_file(file.filename):
            flash("xlsx/xls 파일만 업로드 가능합니다.", "error")
            return redirect(url_for("bank_upload"))

        filename = secure_filename(file.filename)
        path = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
        file.save(path)

        try:
            raw_df = safe_read_excel(path, filename)
            raw_df = extract_bank_columns_with_header_detection(raw_df)
            normalized = normalize_bank_dataframe(raw_df)
            conn = get_conn()
            batch_id = create_upload_batch(conn, "통장", filename)
            result = insert_bank_rows(conn, normalized, filename, batch_id)
            update_upload_batch_saved_count(conn, batch_id, result["saved"])
            conn.close()
            flash(
                f"통장 업로드 완료: 저장 {result['saved']}건 / 중복 {result['duplicate']}건",
                "success",
            )
        except Exception as exc:
            flash(f"처리 실패: {exc}", "error")
        return redirect(url_for("bank_upload"))

    return render_template("bank_upload.html", upload_enabled=True)


@app.route("/bank-transactions")
def bank_transactions():
    conn = get_conn()
    start, end = date_range_where()
    io_type = (request.args.get("io_type") or "").strip()
    rows_sql, rows_params = append_date_range_filters(
        """
        SELECT txn_date, io_type, partner, in_amount, out_amount, amount, description, source_file
        FROM bank_transactions
        WHERE 1=1
          AND (? = '' OR io_type = ?)
        ORDER BY txn_date DESC, id DESC
        LIMIT 1000
        """,
        [io_type, io_type],
        "txn_date",
        start,
        end,
    )
    rows = db_execute(conn, rows_sql, rows_params).fetchall()
    conn.close()
    return render_template(
        "bank_transactions.html", rows=rows, start=start, end=end, io_type=io_type
    )


@app.route("/rollback", methods=["GET", "POST"])
def rollback():
    conn = get_conn()
    if request.method == "POST":
        selected_batch_id = request.form.get("batch_id", "").strip()
        next_url = (request.form.get("next") or "").strip()
        if not selected_batch_id.isdigit():
            conn.close()
            flash("롤백 실패: 업로드 기준을 선택하세요.", "error")
            return redirect(next_url if next_url.startswith("/") else url_for("rollback"))

        cutoff = int(selected_batch_id)
        deleting_voucher_ids = [
            int(r["id"])
            for r in db_execute(conn,
                "SELECT id FROM vouchers WHERE batch_id IS NOT NULL AND batch_id > ?",
                (cutoff,),
            ).fetchall()
        ]
        if deleting_voucher_ids:
            placeholders = ",".join("?" for _ in deleting_voucher_ids)
            db_execute(conn,
                f"DELETE FROM voucher_status_overrides WHERE voucher_id IN ({placeholders})",
                deleting_voucher_ids,
            )

        db_execute(conn,"DELETE FROM journal_entries WHERE batch_id IS NOT NULL AND batch_id > ?", (cutoff,))
        db_execute(conn,"DELETE FROM vouchers WHERE batch_id IS NOT NULL AND batch_id > ?", (cutoff,))
        db_execute(conn,
            "DELETE FROM bank_transactions WHERE batch_id IS NOT NULL AND batch_id > ?",
            (cutoff,),
        )
        db_execute(conn,"DELETE FROM upload_batches WHERE id > ?", (cutoff,))
        conn.commit()
        conn.close()
        flash("선택한 업로드 기준 시점으로 롤백되었습니다.", "success")
        return redirect(next_url if next_url.startswith("/") else url_for("upload"))

    batches = db_execute(conn,
        """
        SELECT id, source_type, source_file, uploaded_at, saved_count
        FROM upload_batches
        ORDER BY id DESC
        LIMIT 300
        """
    ).fetchall()
    conn.close()
    return render_template("rollback.html", batches=batches)


@app.route("/vouchers")
def vouchers():
    conn = get_conn()
    start, end = date_range_where()
    txn_type = (request.args.get("txn_type") or "").strip()
    status_filter = (request.args.get("status") or "").strip()
    rows_sql, rows_params = append_date_range_filters(
        """
        SELECT id, voucher_no, txn_date, year_month, txn_type,
               supply_amount, vat_amount, total_amount, partner, description, source_file
        FROM vouchers
        WHERE 1=1
          AND (? = '' OR txn_type = ?)
        ORDER BY txn_date DESC, id DESC
        LIMIT 500
        """,
        [txn_type, txn_type],
        "txn_date",
        start,
        end,
    )
    rows = db_execute(conn, rows_sql, rows_params).fetchall()
    rows = apply_payable_status(conn, rows, end)
    if status_filter:
        rows = [r for r in rows if str(r.get("payable_status", "")) == status_filter]
    conn.close()
    return render_template(
        "vouchers.html",
        rows=rows,
        start=start,
        end=end,
        txn_type=txn_type,
        status_filter=status_filter,
    )


@app.route("/vouchers/status", methods=["POST"])
def update_voucher_status():
    voucher_id = request.form.get("voucher_id", "").strip()
    status = request.form.get("status", "").strip()
    start = request.form.get("start", "").strip()
    end = request.form.get("end", "").strip()
    txn_type = request.form.get("txn_type", "").strip()
    status_filter = request.form.get("status_filter", "").strip()
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if txn_type:
        params["txn_type"] = txn_type
    if status_filter:
        params["status"] = status_filter

    if not voucher_id.isdigit():
        flash("상태 변경 실패: 잘못된 전표 식별자", "error")
        return redirect(url_for("vouchers", **params))

    conn = get_conn()
    if status in ("지급완료", "미지급금"):
        db_execute(conn,
            """
            INSERT INTO voucher_status_overrides(voucher_id, status, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(voucher_id) DO UPDATE SET
              status=excluded.status,
              updated_at=CURRENT_TIMESTAMP
            """,
            (int(voucher_id), status),
        )
        conn.commit()
        flash("상태가 수동으로 저장되었습니다.", "success")
    else:
        db_execute(conn,
            "DELETE FROM voucher_status_overrides WHERE voucher_id = ?",
            (int(voucher_id),),
        )
        conn.commit()
        flash("수동 상태가 해제되어 자동 계산으로 복귀했습니다.", "success")
    conn.close()
    return redirect(url_for("vouchers", **params))


@app.route("/journals")
def journals():
    conn = get_conn()
    start, end = date_range_where()
    account_code = (request.args.get("account_code") or "").strip()
    partner = (request.args.get("partner") or "").strip()

    account_options = db_execute(conn,
        """
        SELECT DISTINCT j.account_code, COALESCE(a.account_name, '(미등록)') AS account_name
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        ORDER BY j.account_code
        """
    ).fetchall()
    partner_options = db_execute(conn,
        """
        SELECT DISTINCT COALESCE(NULLIF(TRIM(partner), ''), '(미입력)') AS partner_name
        FROM journal_entries
        ORDER BY partner_name
        """
    ).fetchall()

    rows_sql, rows_params = append_date_range_filters(
        """
        SELECT j.voucher_no, j.txn_date, j.dr_cr, j.account_code, a.account_name, j.amount, j.partner, j.description
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE 1=1
          AND (? = '' OR j.account_code = ?)
          AND (
            ? = ''
            OR (? = '(미입력)' AND COALESCE(NULLIF(TRIM(j.partner), ''), '(미입력)') = '(미입력)')
            OR (COALESCE(NULLIF(TRIM(j.partner), ''), '(미입력)') = ?)
          )
        ORDER BY j.txn_date DESC, j.voucher_no DESC, j.line_no ASC
        LIMIT 1000
        """,
        [
            account_code,
            account_code,
            partner,
            partner,
            partner,
        ],
        "j.txn_date",
        start,
        end,
    )
    rows = db_execute(conn, rows_sql, rows_params).fetchall()
    conn.close()
    return render_template(
        "journals.html",
        rows=rows,
        start=start,
        end=end,
        account_code=account_code,
        partner=partner,
        account_options=account_options,
        partner_options=partner_options,
    )


@app.route("/reports")
def reports():
    conn = get_conn()
    start, end = date_range_where()

    tb_sql, tb_params = append_date_range_filters(
        """
        SELECT j.account_code, COALESCE(a.account_name, '(미등록)') AS account_name,
               SUM(CASE WHEN j.dr_cr='차변' THEN j.amount ELSE 0 END) AS debit,
               SUM(CASE WHEN j.dr_cr='대변' THEN j.amount ELSE 0 END) AS credit
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE 1=1
        GROUP BY j.account_code, a.account_name
        ORDER BY j.account_code
        """,
        [],
        "j.txn_date",
        start,
        end,
    )
    tb_rows = db_execute(conn, tb_sql, tb_params).fetchall()

    pl_sql, pl_params = append_date_range_filters(
        """
        SELECT a.account_type, j.account_code, a.account_name,
               SUM(
                 CASE
                   WHEN a.account_type='수익' AND j.dr_cr='대변' THEN j.amount
                   WHEN a.account_type='수익' AND j.dr_cr='차변' THEN -j.amount
                   WHEN a.account_type='비용' AND j.dr_cr='차변' THEN j.amount
                   WHEN a.account_type='비용' AND j.dr_cr='대변' THEN -j.amount
                   ELSE 0
                 END
               ) AS amount
        FROM journal_entries j
        JOIN accounts a ON a.account_code = j.account_code
        WHERE a.account_type IN ('수익','비용')
        GROUP BY a.account_type, j.account_code, a.account_name
        HAVING ABS(amount) > 0.00001
        ORDER BY a.account_type, j.account_code
        """,
        [],
        "j.txn_date",
        start,
        end,
    )
    pl_rows = db_execute(conn, pl_sql, pl_params).fetchall()

    revenue = sum(float(r["amount"]) for r in pl_rows if r["account_type"] == "수익")
    expense = sum(float(r["amount"]) for r in pl_rows if r["account_type"] == "비용")
    profit = revenue - expense

    conn.close()
    return render_template(
        "reports.html",
        tb_rows=tb_rows,
        pl_rows=pl_rows,
        revenue=revenue,
        expense=expense,
        profit=profit,
        start=start,
        end=end,
    )


@app.route("/closing", methods=["GET", "POST"])
def closing():
    conn = get_conn()
    if request.method == "POST":
        year_month = request.form.get("year_month", "").strip()
        action = request.form.get("action")
        note = request.form.get("note", "").strip()
        if year_month:
            is_closed = 1 if action == "close" else 0
            db_execute(conn,
                """
                INSERT INTO monthly_closing(year_month, is_closed, note, closed_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(year_month) DO UPDATE SET
                  is_closed=excluded.is_closed,
                  note=excluded.note,
                  closed_at=CURRENT_TIMESTAMP
                """,
                (year_month, is_closed, note),
            )
            conn.commit()
            flash(f"{year_month} {'마감' if is_closed else '마감해제'} 처리 완료", "success")
        return redirect(url_for("closing"))

    months = db_execute(conn,"SELECT DISTINCT year_month FROM vouchers ORDER BY year_month DESC").fetchall()
    rows = db_execute(conn,
        "SELECT year_month, is_closed, note, closed_at FROM monthly_closing ORDER BY year_month DESC"
    ).fetchall()
    conn.close()
    return render_template("closing.html", months=months, rows=rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
