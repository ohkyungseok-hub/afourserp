import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "accounting.db"
UPLOAD_DIR = BASE_DIR / "uploads"
ALLOWED_EXTENSIONS = {"xlsx", "xls"}

COLUMN_CANDIDATES = {
    "date": ["날짜", "일자", "거래일", "date", "Date"],
    "type": ["구분", "유형", "매입매출", "종류", "type", "Type"],
    "supply": ["공급가액", "공급가", "금액", "amount", "Amount"],
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
]

app = Flask(__name__)
app.secret_key = "afours-erp-secret-key"
app.config["UPLOAD_FOLDER"] = str(UPLOAD_DIR)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    UPLOAD_DIR.mkdir(exist_ok=True)
    conn = get_conn()
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
    conn.executemany(
        "INSERT OR IGNORE INTO accounts(account_code, account_name, account_type) VALUES (?, ?, ?)",
        DEFAULT_ACCOUNTS,
    )
    conn.commit()
    conn.close()


def allowed_file(name: str) -> bool:
    return "." in name and name.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def find_column(df: pd.DataFrame, key: str) -> str | None:
    for candidate in COLUMN_CANDIDATES[key]:
        if candidate in df.columns:
            return candidate
    return None


def normalize_type(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if "매입" in text or "purchase" in text or "buy" in text:
        return "매입"
    if "매출" in text or "sale" in text or "sell" in text:
        return "매출"
    return None


def to_numeric(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce")


def normalize_upload_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(raw_df, "date")
    type_col = find_column(raw_df, "type")
    supply_col = find_column(raw_df, "supply")
    vat_col = find_column(raw_df, "vat")
    desc_col = find_column(raw_df, "description")
    partner_col = find_column(raw_df, "partner")

    if not date_col or not type_col or not supply_col:
        raise ValueError("필수 컬럼 부족: 날짜, 구분, 공급가액")

    df = pd.DataFrame()
    df["txn_date"] = pd.to_datetime(raw_df[date_col], errors="coerce")
    df["txn_type"] = raw_df[type_col].apply(normalize_type)
    df["supply_amount"] = to_numeric(raw_df[supply_col])
    df["vat_amount"] = to_numeric(raw_df[vat_col]) if vat_col else pd.NA
    df["description"] = raw_df[desc_col].astype(str).str.strip() if desc_col else ""

    if partner_col:
        df["partner"] = raw_df[partner_col].astype(str).str.strip()
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
        lines = [("차변", "1200", supply + vat), ("대변", "4000", supply)]
        if vat > 0:
            lines.append(("대변", "2100", vat))
        return lines

    lines = [("차변", "5000", supply), ("대변", "2000", supply + vat)]
    if vat > 0:
        lines.insert(1, ("차변", "1300", vat))
    return lines


def insert_uploaded_rows(conn: sqlite3.Connection, df: pd.DataFrame, source_file: str) -> dict[str, int]:
    result = {"saved": 0, "duplicate": 0, "closed": 0}

    for _, row in df.iterrows():
        year_month = str(row["year_month"])
        if is_month_closed(conn, year_month):
            result["closed"] += 1
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
                row["year_month"],
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
            result["duplicate"] += 1
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
        result["saved"] += 1

    conn.commit()
    return result


def date_range_where() -> tuple[str | None, str | None]:
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    return start, end


@app.route("/")
def index():
    conn = get_conn()
    start, end = date_range_where()

    monthly = conn.execute(
        """
        SELECT year_month,
               SUM(CASE WHEN txn_type='매출' THEN supply_amount ELSE 0 END) AS sales,
               SUM(CASE WHEN txn_type='매입' THEN supply_amount ELSE 0 END) AS purchases,
               SUM(CASE WHEN txn_type='매출' THEN supply_amount ELSE 0 END)
               - SUM(CASE WHEN txn_type='매입' THEN supply_amount ELSE 0 END) AS profit
        FROM vouchers
        WHERE (? IS NULL OR txn_date >= ?)
          AND (? IS NULL OR txn_date <= ?)
        GROUP BY year_month
        ORDER BY year_month DESC
        """,
        (start, start, end, end),
    ).fetchall()

    totals = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN txn_type='매출' THEN supply_amount ELSE 0 END), 0) AS sales,
          COALESCE(SUM(CASE WHEN txn_type='매입' THEN supply_amount ELSE 0 END), 0) AS purchases
        FROM vouchers
        WHERE (? IS NULL OR txn_date >= ?)
          AND (? IS NULL OR txn_date <= ?)
        """,
        (start, start, end, end),
    ).fetchone()

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
    )


@app.route("/upload", methods=["GET", "POST"])
def upload():
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
            raw_df = pd.read_excel(path)
            normalized = normalize_upload_dataframe(raw_df)
            conn = get_conn()
            result = insert_uploaded_rows(conn, normalized, filename)
            conn.close()
            flash(
                f"업로드 완료: 저장 {result['saved']}건 / 중복 {result['duplicate']}건 / 마감월 제외 {result['closed']}건",
                "success",
            )
        except Exception as exc:
            flash(f"처리 실패: {exc}", "error")

        return redirect(url_for("upload"))

    return render_template("upload.html")


@app.route("/vouchers")
def vouchers():
    conn = get_conn()
    start, end = date_range_where()
    rows = conn.execute(
        """
        SELECT voucher_no, txn_date, year_month, txn_type,
               supply_amount, vat_amount, total_amount, partner, description, source_file
        FROM vouchers
        WHERE (? IS NULL OR txn_date >= ?)
          AND (? IS NULL OR txn_date <= ?)
        ORDER BY txn_date DESC, id DESC
        LIMIT 500
        """,
        (start, start, end, end),
    ).fetchall()
    conn.close()
    return render_template("vouchers.html", rows=rows, start=start, end=end)


@app.route("/journals")
def journals():
    conn = get_conn()
    start, end = date_range_where()
    rows = conn.execute(
        """
        SELECT j.voucher_no, j.txn_date, j.dr_cr, j.account_code, a.account_name, j.amount, j.partner, j.description
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE (? IS NULL OR j.txn_date >= ?)
          AND (? IS NULL OR j.txn_date <= ?)
        ORDER BY j.txn_date DESC, j.voucher_no DESC, j.line_no ASC
        LIMIT 1000
        """,
        (start, start, end, end),
    ).fetchall()
    conn.close()
    return render_template("journals.html", rows=rows, start=start, end=end)


@app.route("/reports")
def reports():
    conn = get_conn()
    start, end = date_range_where()

    tb_rows = conn.execute(
        """
        SELECT j.account_code, COALESCE(a.account_name, '(미등록)') AS account_name,
               SUM(CASE WHEN j.dr_cr='차변' THEN j.amount ELSE 0 END) AS debit,
               SUM(CASE WHEN j.dr_cr='대변' THEN j.amount ELSE 0 END) AS credit
        FROM journal_entries j
        LEFT JOIN accounts a ON a.account_code = j.account_code
        WHERE (? IS NULL OR j.txn_date >= ?)
          AND (? IS NULL OR j.txn_date <= ?)
        GROUP BY j.account_code, a.account_name
        ORDER BY j.account_code
        """,
        (start, start, end, end),
    ).fetchall()

    pl_rows = conn.execute(
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
          AND (? IS NULL OR j.txn_date >= ?)
          AND (? IS NULL OR j.txn_date <= ?)
        GROUP BY a.account_type, j.account_code, a.account_name
        HAVING ABS(amount) > 0.00001
        ORDER BY a.account_type, j.account_code
        """,
        (start, start, end, end),
    ).fetchall()

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
            conn.execute(
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

    months = conn.execute("SELECT DISTINCT year_month FROM vouchers ORDER BY year_month DESC").fetchall()
    rows = conn.execute(
        "SELECT year_month, is_closed, note, closed_at FROM monthly_closing ORDER BY year_month DESC"
    ).fetchall()
    conn.close()
    return render_template("closing.html", months=months, rows=rows)


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
