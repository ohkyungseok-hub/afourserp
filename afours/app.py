from __future__ import annotations

import hashlib
import sqlite3
from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path("accounting.db")

COLUMN_CANDIDATES = {
    "date": ["발급일자", "날짜", "일자", "거래일", "date", "Date"],
    "type": ["영수/청구 구분", "영수청구구분", "영수/청구", "구분", "유형", "매입매출", "종류", "type", "Type"],
    "supply": ["품목공급가액", "품목공급가", "공급가액", "공급가", "금액", "amount", "Amount"],
    "vat": ["부가세", "세액", "vat", "VAT"],
    "description": ["적요", "내용", "description", "Description"],
    "partner": ["거래처", "상호", "partner", "Partner"],
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
    ("5100", "임차료", "비용"),
    ("5200", "급여", "비용"),
    ("5300", "광고선전비", "비용"),
    ("5400", "지급수수료", "비용"),
]


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_code TEXT PRIMARY KEY,
            account_name TEXT NOT NULL,
            account_type TEXT NOT NULL CHECK(account_type IN ('자산','부채','자본','수익','비용')),
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vouchers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS journal_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_closing (
            year_month TEXT PRIMARY KEY,
            is_closed INTEGER NOT NULL DEFAULT 1,
            note TEXT,
            closed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fixed_cost_rules (
            vendor TEXT PRIMARY KEY,
            decision TEXT NOT NULL CHECK(decision IN ('확정', '제외')),
            decided_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    ensure_default_accounts(conn)
    migrate_legacy_transactions(conn)
    return conn


def ensure_default_accounts(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT COUNT(*) FROM accounts")
    count = cur.fetchone()[0]
    if count > 0:
        return
    conn.executemany(
        "INSERT INTO accounts(account_code, account_name, account_type) VALUES (?, ?, ?)",
        DEFAULT_ACCOUNTS,
    )
    conn.commit()


def migrate_legacy_transactions(conn: sqlite3.Connection) -> None:
    has_legacy = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='transactions'
        """
    ).fetchone()
    if not has_legacy:
        return

    has_new_data = conn.execute("SELECT COUNT(*) FROM vouchers").fetchone()[0]
    if has_new_data > 0:
        return

    legacy_df = pd.read_sql_query(
        """
        SELECT
            txn_date,
            year_month,
            txn_type,
            supply_amount,
            vat_amount,
            total_amount,
            COALESCE(description, '') AS description,
            COALESCE(description, '') AS partner,
            source_file
        FROM transactions
        ORDER BY txn_date, id
        """,
        conn,
    )
    if legacy_df.empty:
        return

    for _, row in legacy_df.iterrows():
        voucher_hash = hash_voucher(row)
        voucher_no = make_voucher_no(str(row["txn_date"]), voucher_hash)
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO vouchers (
                voucher_no, txn_date, year_month, txn_type,
                supply_amount, vat_amount, total_amount,
                description, partner, source_file, voucher_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
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
                row["source_file"],
                voucher_hash,
            ),
        )
        if cur.rowcount != 1:
            continue

        lines = create_journal_lines(
            str(row["txn_type"]), float(row["supply_amount"]), float(row["vat_amount"])
        )
        line_no = 1
        for dr_cr, account_code, amount in lines:
            conn.execute(
                """
                INSERT INTO journal_entries (
                    voucher_hash, voucher_no, line_no, txn_date, year_month,
                    dr_cr, account_code, amount, description, partner
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            line_no += 1

    conn.commit()


def find_column(df: pd.DataFrame, key: str) -> str | None:
    normalized_map: dict[str, str] = {}
    for col in df.columns:
        normalized_map[normalize_text(col)] = col
    for candidate in COLUMN_CANDIDATES[key]:
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


def to_numeric(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce")


def get_series(df: pd.DataFrame, col: str) -> pd.Series:
    selected = df[col]
    if isinstance(selected, pd.DataFrame):
        return selected.iloc[:, 0]
    return selected


def normalize_upload_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(raw_df, "date")
    type_col = find_column(raw_df, "type")
    supply_col = find_column(raw_df, "supply")
    vat_col = find_column(raw_df, "vat")
    desc_col = find_column(raw_df, "description")
    partner_col = find_column(raw_df, "partner")

    if not date_col or not type_col or not supply_col:
        raise ValueError("필수 컬럼 부족: 날짜, 구분(매입/매출), 공급가액(또는 금액)")

    df = pd.DataFrame()
    date_series = get_series(raw_df, date_col)
    type_series = get_series(raw_df, type_col)
    supply_series = get_series(raw_df, supply_col)
    vat_series = get_series(raw_df, vat_col) if vat_col else pd.Series([pd.NA] * len(raw_df))

    df["txn_date"] = pd.to_datetime(date_series, errors="coerce")
    df["txn_type"] = type_series.apply(normalize_type)
    df["supply_amount"] = to_numeric(supply_series)
    df["vat_amount"] = to_numeric(vat_series) if vat_col else pd.NA

    if partner_col:
        partner_series = get_series(raw_df, partner_col)
        df["partner"] = partner_series.astype(str).str.strip()
    elif desc_col:
        desc_for_partner = get_series(raw_df, desc_col)
        df["partner"] = desc_for_partner.astype(str).str.strip()
    else:
        df["partner"] = ""

    if desc_col:
        desc_series = get_series(raw_df, desc_col)
        df["description"] = desc_series.astype(str).str.strip()
    else:
        df["description"] = ""
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


def is_month_closed(conn: sqlite3.Connection, year_month: str) -> bool:
    row = conn.execute(
        "SELECT is_closed FROM monthly_closing WHERE year_month = ?",
        (year_month,),
    ).fetchone()
    return bool(row and row[0] == 1)


def create_journal_lines(txn_type: str, supply: float, vat: float) -> list[tuple[str, str, float]]:
    if txn_type == "매출":
        lines = [
            ("차변", "1200", supply + vat),
            ("대변", "4000", supply),
        ]
        if vat > 0:
            lines.append(("대변", "2100", vat))
        return lines

    lines = [
        ("차변", "5000", supply),
        ("대변", "2000", supply + vat),
    ]
    if vat > 0:
        lines.insert(1, ("차변", "1300", vat))
    return lines


def insert_from_upload(conn: sqlite3.Connection, df: pd.DataFrame, source_file: str) -> dict[str, int]:
    result = {
        "saved_vouchers": 0,
        "saved_lines": 0,
        "skipped_duplicate": 0,
        "skipped_closed": 0,
    }

    for _, row in df.iterrows():
        year_month = str(row["year_month"])
        if is_month_closed(conn, year_month):
            result["skipped_closed"] += 1
            continue

        voucher_hash = hash_voucher(row)
        voucher_no = make_voucher_no(row["txn_date"], voucher_hash)
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO vouchers (
                voucher_no, txn_date, year_month, txn_type,
                supply_amount, vat_amount, total_amount,
                description, partner, source_file, voucher_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                voucher_no,
                row["txn_date"],
                year_month,
                row["txn_type"],
                float(row["supply_amount"]),
                float(row["vat_amount"]),
                float(row["total_amount"]),
                row["description"],
                row["partner"],
                source_file,
                voucher_hash,
            ),
        )

        if cur.rowcount != 1:
            result["skipped_duplicate"] += 1
            continue

        lines = create_journal_lines(
            str(row["txn_type"]), float(row["supply_amount"]), float(row["vat_amount"])
        )
        line_no = 1
        for dr_cr, account_code, amount in lines:
            conn.execute(
                """
                INSERT INTO journal_entries (
                    voucher_hash, voucher_no, line_no, txn_date, year_month,
                    dr_cr, account_code, amount, description, partner
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    voucher_hash,
                    voucher_no,
                    line_no,
                    row["txn_date"],
                    year_month,
                    dr_cr,
                    account_code,
                    float(amount),
                    row["description"],
                    row["partner"],
                ),
            )
            line_no += 1

        result["saved_vouchers"] += 1
        result["saved_lines"] += len(lines)

    conn.commit()
    return result


def load_date_bounds(conn: sqlite3.Connection) -> tuple[date | None, date | None]:
    row = conn.execute("SELECT MIN(txn_date), MAX(txn_date) FROM vouchers").fetchone()
    if not row or not row[0] or not row[1]:
        return None, None
    return date.fromisoformat(row[0]), date.fromisoformat(row[1])


def load_monthly_summary(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT
            year_month,
            SUM(CASE WHEN txn_type = '매입' THEN supply_amount ELSE 0 END) AS 매입_공급가액,
            SUM(CASE WHEN txn_type = '매입' THEN vat_amount ELSE 0 END) AS 매입_부가세,
            SUM(CASE WHEN txn_type = '매출' THEN supply_amount ELSE 0 END) AS 매출_공급가액,
            SUM(CASE WHEN txn_type = '매출' THEN vat_amount ELSE 0 END) AS 매출_부가세,
            SUM(CASE WHEN txn_type = '매출' THEN vat_amount ELSE 0 END)
              - SUM(CASE WHEN txn_type = '매입' THEN vat_amount ELSE 0 END) AS 납부예정부가세,
            SUM(CASE WHEN txn_type = '매출' THEN supply_amount ELSE 0 END)
              - SUM(CASE WHEN txn_type = '매입' THEN supply_amount ELSE 0 END) AS 월손익_공급가기준
        FROM vouchers
        WHERE (:start_date IS NULL OR txn_date >= :start_date)
          AND (:end_date IS NULL OR txn_date <= :end_date)
        GROUP BY year_month
        ORDER BY year_month
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )
    if df.empty:
        return df

    for col in [
        "매입_공급가액",
        "매입_부가세",
        "매출_공급가액",
        "매출_부가세",
        "납부예정부가세",
        "월손익_공급가기준",
    ]:
        df[f"누적_{col}"] = df[col].cumsum()

    return df


def load_partner_summary(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            CASE WHEN TRIM(COALESCE(partner, '')) = '' THEN '(미입력)' ELSE partner END AS 거래처,
            SUM(CASE WHEN txn_type = '매입' THEN supply_amount ELSE 0 END) AS 매입_공급가액,
            SUM(CASE WHEN txn_type = '매입' THEN vat_amount ELSE 0 END) AS 매입_부가세,
            SUM(CASE WHEN txn_type = '매출' THEN supply_amount ELSE 0 END) AS 매출_공급가액,
            SUM(CASE WHEN txn_type = '매출' THEN vat_amount ELSE 0 END) AS 매출_부가세,
            SUM(CASE WHEN txn_type = '매출' THEN supply_amount ELSE 0 END)
              - SUM(CASE WHEN txn_type = '매입' THEN supply_amount ELSE 0 END) AS 손익_공급가기준
        FROM vouchers
        WHERE (:start_date IS NULL OR txn_date >= :start_date)
          AND (:end_date IS NULL OR txn_date <= :end_date)
        GROUP BY 거래처
        ORDER BY 손익_공급가기준 DESC, 거래처
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )


def load_voucher_ledger(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            voucher_no AS 전표번호,
            txn_date AS 날짜,
            year_month AS 년월,
            txn_type AS 구분,
            supply_amount AS 공급가액,
            vat_amount AS 부가세,
            total_amount AS 합계,
            partner AS 거래처,
            description AS 적요,
            source_file AS 원본파일,
            created_at AS 저장일시
        FROM vouchers
        WHERE (:start_date IS NULL OR txn_date >= :start_date)
          AND (:end_date IS NULL OR txn_date <= :end_date)
        ORDER BY txn_date DESC, id DESC
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )


def load_journal_lines(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            j.voucher_no AS 전표번호,
            j.txn_date AS 날짜,
            j.dr_cr AS 차대,
            j.account_code AS 계정코드,
            a.account_name AS 계정과목,
            j.amount AS 금액,
            j.partner AS 거래처,
            j.description AS 적요
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE (:start_date IS NULL OR j.txn_date >= :start_date)
          AND (:end_date IS NULL OR j.txn_date <= :end_date)
        ORDER BY j.txn_date DESC, j.voucher_no DESC, j.line_no ASC
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )


def load_trial_balance(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            j.account_code AS 계정코드,
            COALESCE(a.account_name, '(미등록)') AS 계정과목,
            COALESCE(a.account_type, '(미분류)') AS 계정구분,
            SUM(CASE WHEN j.dr_cr = '차변' THEN j.amount ELSE 0 END) AS 차변합계,
            SUM(CASE WHEN j.dr_cr = '대변' THEN j.amount ELSE 0 END) AS 대변합계,
            SUM(CASE WHEN j.dr_cr = '차변' THEN j.amount ELSE -j.amount END) AS 차대차이
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE (:start_date IS NULL OR j.txn_date >= :start_date)
          AND (:end_date IS NULL OR j.txn_date <= :end_date)
        GROUP BY j.account_code, a.account_name, a.account_type
        ORDER BY j.account_code
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )


def load_profit_loss(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> tuple[pd.DataFrame, float, float, float]:
    df = pd.read_sql_query(
        """
        SELECT
            a.account_type AS 계정구분,
            j.account_code AS 계정코드,
            a.account_name AS 계정과목,
            SUM(
                CASE
                    WHEN a.account_type = '수익' AND j.dr_cr = '대변' THEN j.amount
                    WHEN a.account_type = '수익' AND j.dr_cr = '차변' THEN -j.amount
                    WHEN a.account_type = '비용' AND j.dr_cr = '차변' THEN j.amount
                    WHEN a.account_type = '비용' AND j.dr_cr = '대변' THEN -j.amount
                    ELSE 0
                END
            ) AS 금액
        FROM journal_entries j
        JOIN accounts a ON a.account_code = j.account_code
        WHERE a.account_type IN ('수익', '비용')
          AND (:start_date IS NULL OR j.txn_date >= :start_date)
          AND (:end_date IS NULL OR j.txn_date <= :end_date)
        GROUP BY a.account_type, j.account_code, a.account_name
        HAVING ABS(금액) > 0.00001
        ORDER BY a.account_type, j.account_code
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )
    revenue = float(df[df["계정구분"] == "수익"]["금액"].sum()) if not df.empty else 0.0
    expense = float(df[df["계정구분"] == "비용"]["금액"].sum()) if not df.empty else 0.0
    profit = revenue - expense
    return df, revenue, expense, profit


def load_accounts(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT account_code AS 계정코드, account_name AS 계정과목, account_type AS 계정구분, is_active AS 사용여부
        FROM accounts
        ORDER BY account_code
        """,
        conn,
    )


def add_account(conn: sqlite3.Connection, code: str, name: str, account_type: str) -> None:
    conn.execute(
        """
        INSERT INTO accounts(account_code, account_name, account_type, is_active)
        VALUES (?, ?, ?, 1)
        """,
        (code, name, account_type),
    )
    conn.commit()


def load_closing_status(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT c.year_month AS 년월,
               c.is_closed AS 마감여부,
               c.note AS 비고,
               c.closed_at AS 처리일시
        FROM monthly_closing c
        ORDER BY c.year_month DESC
        """,
        conn,
    )


def upsert_closing(conn: sqlite3.Connection, year_month: str, close: bool, note: str = "") -> None:
    conn.execute(
        """
        INSERT INTO monthly_closing(year_month, is_closed, note, closed_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(year_month) DO UPDATE SET
            is_closed = excluded.is_closed,
            note = excluded.note,
            closed_at = CURRENT_TIMESTAMP
        """,
        (year_month, 1 if close else 0, note),
    )
    conn.commit()


def load_fixed_cost_candidates(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    monthly_df = pd.read_sql_query(
        """
        SELECT
            year_month,
            CASE WHEN TRIM(COALESCE(partner, '')) = '' THEN '(미입력)' ELSE partner END AS 거래처,
            SUM(supply_amount) AS 월매입공급가액
        FROM vouchers
        WHERE txn_type = '매입'
          AND (:start_date IS NULL OR txn_date >= :start_date)
          AND (:end_date IS NULL OR txn_date <= :end_date)
        GROUP BY year_month, 거래처
        ORDER BY 거래처, year_month
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )
    if monthly_df.empty:
        return monthly_df

    records: list[dict[str, float | int | str]] = []
    for vendor, g in monthly_df.groupby("거래처"):
        months = pd.PeriodIndex(g["year_month"], freq="M")
        min_m = months.min()
        max_m = months.max()
        month_span = (max_m.year - min_m.year) * 12 + (max_m.month - min_m.month) + 1
        active_months = int(g["year_month"].nunique())
        regularity = active_months / month_span if month_span > 0 else 0.0

        avg_amount = float(g["월매입공급가액"].mean())
        std_amount = float(g["월매입공급가액"].std(ddof=0))
        cv = std_amount / avg_amount if avg_amount > 0 else 999.0

        score = (regularity * 0.6 + max(0.0, 1 - min(cv, 1.0)) * 0.4) * 100
        is_candidate = active_months >= 3 and regularity >= 0.6 and cv <= 0.2
        if not is_candidate:
            continue

        records.append(
            {
                "거래처": vendor,
                "활성월수": active_months,
                "월범위": month_span,
                "반복률": round(regularity, 3),
                "평균매입공급가액": round(avg_amount, 2),
                "표준편차": round(std_amount, 2),
                "변동계수(CV)": round(cv, 3),
                "고정비신뢰도점수": round(score, 1),
            }
        )

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records).sort_values(
        by=["고정비신뢰도점수", "평균매입공급가액"], ascending=[False, False]
    )


def load_fixed_cost_rules(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT vendor AS 거래처, decision AS 판정, decided_at AS 판정일시
        FROM fixed_cost_rules
        ORDER BY 판정일시 DESC
        """,
        conn,
    )


def save_fixed_cost_decision(conn: sqlite3.Connection, vendors: list[str], decision: str) -> int:
    if not vendors:
        return 0
    changed = 0
    for vendor in vendors:
        cur = conn.execute(
            """
            INSERT INTO fixed_cost_rules(vendor, decision, decided_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(vendor) DO UPDATE SET
                decision = excluded.decision,
                decided_at = CURRENT_TIMESTAMP
            """,
            (vendor, decision),
        )
        if cur.rowcount >= 1:
            changed += 1
    conn.commit()
    return changed


def clear_fixed_cost_decision(conn: sqlite3.Connection, vendors: list[str]) -> int:
    if not vendors:
        return 0
    placeholders = ",".join("?" for _ in vendors)
    cur = conn.execute(
        f"DELETE FROM fixed_cost_rules WHERE vendor IN ({placeholders})",
        vendors,
    )
    conn.commit()
    return cur.rowcount


def apply_fixed_rule_status(candidates_df: pd.DataFrame, rules_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df
    if rules_df.empty:
        result = candidates_df.copy()
        result["판정"] = "미판정"
        return result
    merged = candidates_df.merge(rules_df[["거래처", "판정"]], on="거래처", how="left")
    merged["판정"] = merged["판정"].fillna("미판정")
    return merged


def build_export_file(
    monthly_df: pd.DataFrame,
    partner_df: pd.DataFrame,
    voucher_df: pd.DataFrame,
    journal_df: pd.DataFrame,
    trial_df: pd.DataFrame,
    pl_df: pd.DataFrame,
    fixed_df: pd.DataFrame,
    fixed_rule_df: pd.DataFrame,
    closing_df: pd.DataFrame,
    account_df: pd.DataFrame,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        monthly_df.to_excel(writer, sheet_name="월별요약", index=False)
        partner_df.to_excel(writer, sheet_name="거래처별집계", index=False)
        voucher_df.to_excel(writer, sheet_name="전표원장", index=False)
        journal_df.to_excel(writer, sheet_name="분개장", index=False)
        trial_df.to_excel(writer, sheet_name="합계잔액시산표", index=False)
        pl_df.to_excel(writer, sheet_name="손익계산", index=False)
        fixed_df.to_excel(writer, sheet_name="고정비후보", index=False)
        fixed_rule_df.to_excel(writer, sheet_name="고정비판정", index=False)
        closing_df.to_excel(writer, sheet_name="월마감상태", index=False)
        account_df.to_excel(writer, sheet_name="계정과목", index=False)
    return output.getvalue()


def main() -> None:
    st.set_page_config(page_title="ERP 회계 시스템", layout="wide")
    st.title("ERP 회계 시스템 (전표/분개/월마감)")
    st.caption("엑셀 업로드 -> 전표 생성 -> 분개 반영 -> 시산표/손익/고정비/월마감")

    conn = get_connection()

    with st.expander("엑셀 형식 안내", expanded=False):
        st.markdown(
            """
            필수 컬럼:
            - 날짜, 구분(매입/매출), 공급가액

            선택 컬럼:
            - 부가세(없으면 공급가액의 10% 자동 계산)
            - 거래처(없으면 적요를 거래처로 사용)
            - 적요

            업로드 시 자동 분개:
            - 매출: 차변 외상매출금 / 대변 매출, 부가세예수금
            - 매입: 차변 매입, 부가세대급금 / 대변 미지급금
            """
        )

    st.subheader("계정과목 관리")
    account_df = load_accounts(conn)
    st.dataframe(account_df, use_container_width=True)

    with st.form("account_add_form"):
        c1, c2, c3 = st.columns([1, 2, 1])
        with c1:
            new_code = st.text_input("계정코드")
        with c2:
            new_name = st.text_input("계정과목")
        with c3:
            new_type = st.selectbox("계정구분", ["자산", "부채", "자본", "수익", "비용"])
        add_submit = st.form_submit_button("계정과목 추가")
        if add_submit:
            try:
                if not new_code.strip() or not new_name.strip():
                    st.error("계정코드/계정과목을 입력하세요.")
                else:
                    add_account(conn, new_code.strip(), new_name.strip(), new_type)
                    st.success("계정과목이 추가되었습니다.")
                    st.rerun()
            except sqlite3.IntegrityError:
                st.error("동일한 계정코드가 이미 존재합니다.")

    st.subheader("엑셀 업로드")
    files = st.file_uploader(
        "엑셀 파일 업로드 (.xlsx, .xls)",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )

    if files:
        total_saved_vouchers = 0
        total_saved_lines = 0
        total_duplicate = 0
        total_closed = 0

        for file in files:
            try:
                raw_df = pd.read_excel(file, header=None)
                raw_df = extract_columns_with_header_detection(raw_df)
                normalized = normalize_upload_dataframe(raw_df)
                result = insert_from_upload(conn, normalized, file.name)
                total_saved_vouchers += result["saved_vouchers"]
                total_saved_lines += result["saved_lines"]
                total_duplicate += result["skipped_duplicate"]
                total_closed += result["skipped_closed"]
                st.success(
                    f"{file.name}: 전표 {result['saved_vouchers']}건 저장, "
                    f"중복 {result['skipped_duplicate']}건, 마감월 제외 {result['skipped_closed']}건"
                )
            except Exception as e:
                st.error(f"{file.name}: 처리 실패 - {e}")

        st.info(
            f"총 저장 전표 {total_saved_vouchers}건 / 분개 {total_saved_lines}라인 | "
            f"중복 {total_duplicate}건 | 마감월 제외 {total_closed}건"
        )

    min_date, max_date = load_date_bounds(conn)
    selected_start = min_date
    selected_end = max_date
    if min_date and max_date:
        st.subheader("기간 필터")
        f1, f2 = st.columns(2)
        with f1:
            selected_start = st.date_input(
                "시작일",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
            )
        with f2:
            selected_end = st.date_input(
                "종료일",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
            )
        if selected_start > selected_end:
            st.error("시작일은 종료일보다 늦을 수 없습니다.")
            conn.close()
            return

    monthly_df = load_monthly_summary(conn, selected_start, selected_end)
    partner_df = load_partner_summary(conn, selected_start, selected_end)
    voucher_df = load_voucher_ledger(conn, selected_start, selected_end)
    journal_df = load_journal_lines(conn, selected_start, selected_end)
    trial_df = load_trial_balance(conn, selected_start, selected_end)
    pl_df, revenue, expense, profit = load_profit_loss(conn, selected_start, selected_end)
    fixed_candidates_df = load_fixed_cost_candidates(conn, selected_start, selected_end)
    fixed_rule_df = load_fixed_cost_rules(conn)
    fixed_df = apply_fixed_rule_status(fixed_candidates_df, fixed_rule_df)
    closing_df = load_closing_status(conn)

    m1, m2, m3 = st.columns(3)
    m1.metric("기간 매출(공급가)", f"{monthly_df['매출_공급가액'].sum():,.0f}" if not monthly_df.empty else "0")
    m2.metric("기간 매입(공급가)", f"{monthly_df['매입_공급가액'].sum():,.0f}" if not monthly_df.empty else "0")
    m3.metric("기간 순이익(공급가)", f"{profit:,.0f}")

    top1, top2 = st.columns([2, 1])
    with top1:
        st.subheader("월별 요약")
        if monthly_df.empty:
            st.write("데이터가 없습니다.")
        else:
            st.dataframe(monthly_df, use_container_width=True)

    with top2:
        if monthly_df.empty and voucher_df.empty:
            st.caption("내보낼 데이터가 없습니다.")
        else:
            export_bytes = build_export_file(
                monthly_df,
                partner_df,
                voucher_df,
                journal_df,
                trial_df,
                pl_df,
                fixed_df,
                fixed_rule_df,
                closing_df,
                account_df,
            )
            st.download_button(
                "엑셀로 내보내기",
                data=export_bytes,
                file_name="erp_accounting_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        if st.button("전체 데이터 초기화", type="secondary", use_container_width=True):
            conn.execute("DELETE FROM journal_entries")
            conn.execute("DELETE FROM vouchers")
            conn.execute("DELETE FROM monthly_closing")
            conn.execute("DELETE FROM fixed_cost_rules")
            conn.commit()
            st.warning("전표/분개/마감/고정비 판정 데이터가 삭제되었습니다.")
            st.rerun()

    tabs = st.tabs(["전표 원장", "분개장", "시산표/손익", "거래처", "고정비", "월마감"])

    with tabs[0]:
        st.dataframe(voucher_df, use_container_width=True)

    with tabs[1]:
        st.dataframe(journal_df, use_container_width=True)

    with tabs[2]:
        st.markdown("**합계잔액시산표**")
        st.dataframe(trial_df, use_container_width=True)
        st.markdown("**손익계산(기간 기준)**")
        p1, p2, p3 = st.columns(3)
        p1.metric("수익", f"{revenue:,.0f}")
        p2.metric("비용", f"{expense:,.0f}")
        p3.metric("순이익", f"{profit:,.0f}")
        st.dataframe(pl_df, use_container_width=True)

    with tabs[3]:
        st.dataframe(partner_df, use_container_width=True)

    with tabs[4]:
        st.caption("기준: 매입 거래 중 활성월 3개월 이상 + 반복률 60% 이상 + CV 0.2 이하")
        if fixed_df.empty:
            st.write("고정비 후보가 없습니다.")
        else:
            selected_vendors = st.multiselect(
                "판정할 거래처 선택",
                options=fixed_df["거래처"].tolist(),
                help="선택 후 확정/제외/해제 버튼으로 저장하세요.",
            )
            a1, a2, a3 = st.columns(3)
            with a1:
                if st.button("선택 거래처 확정", use_container_width=True):
                    changed = save_fixed_cost_decision(conn, selected_vendors, "확정")
                    st.success(f"{changed}건 확정 저장")
                    st.rerun()
            with a2:
                if st.button("선택 거래처 제외", use_container_width=True):
                    changed = save_fixed_cost_decision(conn, selected_vendors, "제외")
                    st.success(f"{changed}건 제외 저장")
                    st.rerun()
            with a3:
                if st.button("선택 거래처 판정 해제", use_container_width=True):
                    changed = clear_fixed_cost_decision(conn, selected_vendors)
                    st.success(f"{changed}건 판정 해제")
                    st.rerun()
            st.dataframe(fixed_df, use_container_width=True)

        st.markdown("**저장된 고정비 판정**")
        st.dataframe(fixed_rule_df, use_container_width=True)

    with tabs[5]:
        months_df = pd.read_sql_query(
            "SELECT DISTINCT year_month FROM vouchers ORDER BY year_month DESC", conn
        )
        month_options = months_df["year_month"].tolist()
        if not month_options:
            st.write("마감할 월이 없습니다.")
        else:
            target_month = st.selectbox("대상 년월", month_options)
            note = st.text_input("비고", value="")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("월 마감", use_container_width=True):
                    upsert_closing(conn, target_month, True, note)
                    st.success(f"{target_month} 마감 처리 완료")
                    st.rerun()
            with c2:
                if st.button("마감 해제", use_container_width=True):
                    upsert_closing(conn, target_month, False, note)
                    st.success(f"{target_month} 마감 해제 완료")
                    st.rerun()
        st.markdown("**월마감 상태**")
        st.dataframe(closing_df, use_container_width=True)

    conn.close()


if __name__ == "__main__":
    main()
