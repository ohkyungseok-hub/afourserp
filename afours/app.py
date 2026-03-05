import hashlib
from io import BytesIO
import sqlite3
from pathlib import Path
from datetime import date

import pandas as pd
import streamlit as st

DB_PATH = Path("accounting.db")

COLUMN_CANDIDATES = {
    "date": ["날짜", "일자", "거래일", "date", "Date"],
    "type": ["구분", "유형", "매입매출", "종류", "type", "Type"],
    "supply": ["공급가액", "공급가", "금액", "amount", "Amount"],
    "vat": ["부가세", "세액", "vat", "VAT"],
    "description": ["적요", "내용", "거래처", "description", "Description"],
}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            txn_date TEXT NOT NULL,
            year_month TEXT NOT NULL,
            txn_type TEXT NOT NULL,
            supply_amount REAL NOT NULL,
            vat_amount REAL NOT NULL,
            total_amount REAL NOT NULL,
            description TEXT,
            source_file TEXT,
            row_hash TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
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
    return conn


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


def normalize_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(raw_df, "date")
    type_col = find_column(raw_df, "type")
    supply_col = find_column(raw_df, "supply")
    vat_col = find_column(raw_df, "vat")
    desc_col = find_column(raw_df, "description")

    if not date_col or not type_col or not supply_col:
        raise ValueError(
            "필수 컬럼 부족: 날짜, 구분(매입/매출), 공급가액(또는 금액) 컬럼이 필요합니다."
        )

    df = pd.DataFrame()
    df["txn_date"] = pd.to_datetime(raw_df[date_col], errors="coerce")
    df["txn_type"] = raw_df[type_col].apply(normalize_type)
    df["supply_amount"] = to_numeric(raw_df[supply_col])

    if vat_col:
        df["vat_amount"] = to_numeric(raw_df[vat_col])
    else:
        df["vat_amount"] = pd.NA

    df["description"] = raw_df[desc_col].astype(str).str.strip() if desc_col else ""
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
    ]]


def hash_row(row: pd.Series) -> str:
    payload = "|".join(
        [
            str(row["txn_date"]),
            str(row["txn_type"]),
            f"{float(row['supply_amount']):.2f}",
            f"{float(row['vat_amount']):.2f}",
            str(row["description"]),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def insert_transactions(conn: sqlite3.Connection, df: pd.DataFrame, source_file: str) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    rows = []
    for _, row in df.iterrows():
        row_hash = hash_row(row)
        rows.append(
            (
                row["txn_date"],
                row["year_month"],
                row["txn_type"],
                float(row["supply_amount"]),
                float(row["vat_amount"]),
                float(row["total_amount"]),
                row["description"],
                source_file,
                row_hash,
            )
        )

    for row in rows:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO transactions (
                txn_date, year_month, txn_type,
                supply_amount, vat_amount, total_amount,
                description, source_file, row_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )
        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    return inserted, skipped


def load_date_bounds(conn: sqlite3.Connection) -> tuple[date | None, date | None]:
    row = conn.execute(
        "SELECT MIN(txn_date) AS min_date, MAX(txn_date) AS max_date FROM transactions"
    ).fetchone()
    if not row or not row[0] or not row[1]:
        return None, None
    return date.fromisoformat(row[0]), date.fromisoformat(row[1])


def load_monthly_summary(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    query = """
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
    FROM transactions
    WHERE (:start_date IS NULL OR txn_date >= :start_date)
      AND (:end_date IS NULL OR txn_date <= :end_date)
    GROUP BY year_month
    ORDER BY year_month
    """
    params = {
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
    }
    df = pd.read_sql_query(query, conn, params=params)
    if df.empty:
        return df

    cumulative_columns = [
        "매입_공급가액",
        "매입_부가세",
        "매출_공급가액",
        "매출_부가세",
        "납부예정부가세",
        "월손익_공급가기준",
    ]
    for col in cumulative_columns:
        df[f"누적_{col}"] = df[col].cumsum()

    return df


def load_recent_transactions(
    conn: sqlite3.Connection,
    limit: int = 200,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT txn_date AS 날짜, txn_type AS 구분, supply_amount AS 공급가액,
               vat_amount AS 부가세, total_amount AS 합계, description AS 적요,
               source_file AS 원본파일
        FROM transactions
        WHERE (:start_date IS NULL OR txn_date >= :start_date)
          AND (:end_date IS NULL OR txn_date <= :end_date)
        ORDER BY txn_date DESC, id DESC
        LIMIT :limit
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "limit": limit,
        },
    )


def load_partner_summary(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    query = """
    SELECT
        CASE
            WHEN TRIM(COALESCE(description, '')) = '' THEN '(미입력)'
            ELSE description
        END AS 거래처,
        SUM(CASE WHEN txn_type = '매입' THEN supply_amount ELSE 0 END) AS 매입_공급가액,
        SUM(CASE WHEN txn_type = '매입' THEN vat_amount ELSE 0 END) AS 매입_부가세,
        SUM(CASE WHEN txn_type = '매출' THEN supply_amount ELSE 0 END) AS 매출_공급가액,
        SUM(CASE WHEN txn_type = '매출' THEN vat_amount ELSE 0 END) AS 매출_부가세,
        SUM(CASE WHEN txn_type = '매출' THEN supply_amount ELSE 0 END)
          - SUM(CASE WHEN txn_type = '매입' THEN supply_amount ELSE 0 END) AS 손익_공급가기준
    FROM transactions
    WHERE (:start_date IS NULL OR txn_date >= :start_date)
      AND (:end_date IS NULL OR txn_date <= :end_date)
    GROUP BY 거래처
    ORDER BY 손익_공급가기준 DESC, 거래처
    """
    return pd.read_sql_query(
        query,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )


def load_all_transactions(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT txn_date AS 날짜, year_month AS 년월, txn_type AS 구분,
               supply_amount AS 공급가액, vat_amount AS 부가세, total_amount AS 합계,
               description AS 적요, source_file AS 원본파일, created_at AS 저장일시
        FROM transactions
        WHERE (:start_date IS NULL OR txn_date >= :start_date)
          AND (:end_date IS NULL OR txn_date <= :end_date)
        ORDER BY txn_date ASC, id ASC
        """,
        conn,
        params={
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )


def load_fixed_cost_candidates(
    conn: sqlite3.Connection, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    monthly_df = pd.read_sql_query(
        """
        SELECT
            year_month,
            CASE
                WHEN TRIM(COALESCE(description, '')) = '' THEN '(미입력)'
                ELSE description
            END AS 거래처,
            SUM(supply_amount) AS 월매입공급가액
        FROM transactions
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

    result_df = pd.DataFrame(records).sort_values(
        by=["고정비신뢰도점수", "평균매입공급가액"], ascending=[False, False]
    )
    return result_df.reset_index(drop=True)


def load_fixed_cost_rules(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT vendor AS 거래처, decision AS 판정, decided_at AS 판정일시
        FROM fixed_cost_rules
        ORDER BY 판정일시 DESC, 거래처
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
    merged = candidates_df.merge(
        rules_df[["거래처", "판정"]], on="거래처", how="left"
    )
    merged["판정"] = merged["판정"].fillna("미판정")
    return merged


def build_export_file(
    summary_df: pd.DataFrame,
    partner_df: pd.DataFrame,
    ledger_df: pd.DataFrame,
    fixed_df: pd.DataFrame,
    fixed_rule_df: pd.DataFrame,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="월별요약", index=False)
        partner_df.to_excel(writer, sheet_name="거래처별집계", index=False)
        ledger_df.to_excel(writer, sheet_name="거래원장", index=False)
        fixed_df.to_excel(writer, sheet_name="고정비후보", index=False)
        fixed_rule_df.to_excel(writer, sheet_name="고정비판정", index=False)
    return output.getvalue()


def main() -> None:
    st.set_page_config(page_title="회계 프로그램", layout="wide")
    st.title("엑셀 기반 매입/매출 누적 관리")
    st.caption("엑셀 업로드 -> 매입/매출/VAT 계산 -> 월별 누적 집계")

    conn = get_connection()

    with st.expander("엑셀 형식 안내", expanded=False):
        st.markdown(
            """
            필수 컬럼(이름 유사해도 자동 인식):
            - 날짜(예: 날짜, 일자, 거래일)
            - 구분(매입 또는 매출)
            - 공급가액(또는 금액)

            선택 컬럼:
            - 부가세(없으면 공급가액의 10% 자동 계산)
            - 적요/거래처 (거래처별 집계에서 거래처명으로 사용)
            """
        )

    files = st.file_uploader(
        "엑셀 파일 업로드 (.xlsx, .xls)",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )

    if files:
        total_inserted = 0
        total_skipped = 0
        for file in files:
            try:
                raw_df = pd.read_excel(file)
                normalized = normalize_dataframe(raw_df)
                inserted, skipped = insert_transactions(conn, normalized, file.name)
                total_inserted += inserted
                total_skipped += skipped
                st.success(f"{file.name}: {inserted}건 저장, {skipped}건 중복 스킵")
            except Exception as e:
                st.error(f"{file.name}: 처리 실패 - {e}")

        st.info(f"총 저장 {total_inserted}건, 총 중복 스킵 {total_skipped}건")

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

    summary_df = load_monthly_summary(conn, selected_start, selected_end)
    partner_df = load_partner_summary(conn, selected_start, selected_end)
    recent_df = load_recent_transactions(conn, 200, selected_start, selected_end)
    ledger_df = load_all_transactions(conn, selected_start, selected_end)
    fixed_candidate_df = load_fixed_cost_candidates(conn, selected_start, selected_end)
    fixed_rule_df = load_fixed_cost_rules(conn)
    fixed_df = apply_fixed_rule_status(fixed_candidate_df, fixed_rule_df)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("월별 집계 + 누적 + 손익")
        if summary_df.empty:
            st.write("데이터가 없습니다. 엑셀을 업로드하세요.")
        else:
            st.dataframe(summary_df, use_container_width=True)

    with col2:
        if summary_df.empty:
            st.caption("내보낼 데이터가 없습니다.")
        else:
            export_bytes = build_export_file(
                summary_df, partner_df, ledger_df, fixed_df, fixed_rule_df
            )
            st.download_button(
                "엑셀로 내보내기",
                data=export_bytes,
                file_name="accounting_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        if st.button("전체 데이터 초기화", type="secondary", use_container_width=True):
            conn.execute("DELETE FROM transactions")
            conn.execute("DELETE FROM fixed_cost_rules")
            conn.commit()
            st.warning("모든 거래/고정비 판정 데이터가 삭제되었습니다.")

    st.subheader("거래처별 집계")
    if partner_df.empty:
        st.write("거래처 집계 데이터가 없습니다.")
    else:
        st.dataframe(partner_df, use_container_width=True)

    st.subheader("고정비 자동 추출(후보)")
    st.caption("기준: 매입 거래 중 월 3회 이상 반복 + 반복률 60% 이상 + 금액 변동계수 0.2 이하")
    if fixed_df.empty:
        st.write("선택 기간에서 고정비 후보가 없습니다.")
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

    st.subheader("저장된 고정비 판정")
    if fixed_rule_df.empty:
        st.write("저장된 판정이 없습니다.")
    else:
        st.dataframe(fixed_rule_df, use_container_width=True)

    st.subheader("최근 거래 내역")
    st.dataframe(recent_df, use_container_width=True)

    conn.close()


if __name__ == "__main__":
    main()
