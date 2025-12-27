from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


def log_progress(message: str, log_path: str = "etl_project_log.txt") -> None:
    """
    Append a timestamped log line to a local log file.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{ts} : {message}\n")


def _fetch_html(url: str, timeout: int = 20) -> str:
    headers = {
        # Wikipedia occasionally blocks/limits unknown clients; a UA helps.
        "User-Agent": "largest-banks-etl/0.1 (learning project; contact: none)"
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def _find_market_cap_table_html(html: str) -> str:
    """
    Locate the table right after the 'By market capitalization' heading.
    Returns the table HTML string.
    """
    soup = BeautifulSoup(html, "lxml")

    # Best case: Wikipedia heading has id="By_market_capitalization"
    anchor = soup.find(id="By_market_capitalization")

    # Fallback: search for a heading that matches the text
    if anchor is None:
        headline = soup.find(
            lambda tag: tag.name in {"h2", "h3", "span"}
            and tag.get_text(strip=True).lower() == "by market capitalization"
        )
        anchor = headline

    if anchor is None:
        raise ValueError("Could not find the 'By market capitalization' section on the page.")

    # Move forward to find the next wikitable
    table = anchor.find_next("table", class_=lambda c: c and "wikitable" in c)
    if table is None:
        raise ValueError("Found the heading but could not find the following wikitable.")

    return str(table)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _pick_name_and_cap_columns(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Heuristic: pick a name-like column and a market-cap column.
    """
    cols = list(df.columns)
    lower = [c.lower() for c in cols]

    # Name column candidates
    name_candidates = []
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ["bank", "company", "name"]):
            name_candidates.append(c)
    name_col = name_candidates[0] if name_candidates else cols[0]

    # Market cap candidates
    cap_candidates = []
    for c in cols:
        cl = c.lower()
        if "market cap" in cl and any(k in cl for k in ["us$", "usd", "us$ billion", "billion"]):
            cap_candidates.append(c)

    if cap_candidates:
        cap_col = cap_candidates[0]
        return name_col, cap_col

    # Fallback: first numeric-ish column (after cleaning)
    # We'll try each column and see if it becomes numeric for many rows.
    best_col = None
    best_score = -1
    for c in cols:
        ser = df[c].astype(str)
        ser = ser.str.replace(r"\[.*?\]", "", regex=True)
        ser = ser.str.replace(",", "", regex=False)
        ser = ser.str.replace(r"[^\d.\-]", "", regex=True)
        nums = pd.to_numeric(ser, errors="coerce")
        score = int(nums.notna().sum())
        if score > best_score:
            best_score = score
            best_col = c

    if best_col is None:
        raise ValueError("Could not detect a market cap column.")

    return name_col, best_col


def extract(url: str, table_attribs: Optional[list[str]] = None, log_path: str = "etl_project_log.txt") -> pd.DataFrame:
    """
    Extract the 'By market capitalization' table from Wikipedia LIVE page,
    and return a dataframe with columns:
      - company
      - MC_USD_Billion
    """
    log_progress("Starting extraction", log_path)

    html = _fetch_html(url)
    table_html = _find_market_cap_table_html(html)

    tables = pd.read_html(table_html)
    if not tables:
        raise ValueError("read_html returned no tables.")

    raw = _normalize_columns(tables[0])

    name_col, cap_col = _pick_name_and_cap_columns(raw)

    df = raw[[name_col, cap_col]].copy()
    df.columns = ["company", "MC_USD_Billion"]

    # Clean MC_USD_Billion values (remove footnotes, commas, currency symbols)
    s = df["MC_USD_Billion"].astype(str)
    s = s.str.replace(r"\[.*?\]", "", regex=True)
    s = s.str.replace(",", "", regex=False)
    s = s.str.strip()
    s = s.str.replace(r"[^\d.\-]", "", regex=True)  # keep digits/dot/minus only

    df["MC_USD_Billion"] = pd.to_numeric(s, errors="coerce")
    df["company"] = df["company"].astype(str).str.replace(r"\[.*?\]", "", regex=True).str.strip()

    df = df.dropna(subset=["MC_USD_Billion"])
    df = df[df["company"].ne("")]

    # Keep top 10 (by market cap)
    df = df.sort_values("MC_USD_Billion", ascending=False).head(10).reset_index(drop=True)

    log_progress(f"Extraction complete: {len(df)} rows", log_path)
    return df


def transform(df: pd.DataFrame, csv_path: str, log_path: str = "etl_project_log.txt") -> pd.DataFrame:
    """
    Read exchange rates from CSV (Currency, Rate), and add:
      - MC_GBP_Billion
      - MC_EUR_Billion
      - MC_INR_Billion
    """
    log_progress("Starting transformation", log_path)

    df_rate = pd.read_csv(csv_path)
    df_rate["Currency"] = df_rate["Currency"].astype(str).str.strip()
    df_rate["Rate"] = pd.to_numeric(df_rate["Rate"], errors="coerce")

    rates: Dict[str, float] = df_rate.set_index("Currency")["Rate"].dropna().to_dict()

    for cur in ["GBP", "EUR", "INR"]:
        if cur not in rates:
            raise ValueError(f"Missing exchange rate for {cur} in {csv_path}")

    out = df.copy()
    out["MC_USD_Billion"] = pd.to_numeric(out["MC_USD_Billion"], errors="coerce")

    out["MC_GBP_Billion"] = (out["MC_USD_Billion"] * float(rates["GBP"])).round(2)
    out["MC_EUR_Billion"] = (out["MC_USD_Billion"] * float(rates["EUR"])).round(2)
    out["MC_INR_Billion"] = (out["MC_USD_Billion"] * float(rates["INR"])).round(2)

    log_progress("Transformation complete", log_path)
    return out


def load_to_csv(df: pd.DataFrame, output_path: str, log_path: str = "etl_project_log.txt") -> None:
    log_progress(f"Saving CSV to {output_path}", log_path)
    df.to_csv(output_path, index=False)
    log_progress("CSV saved", log_path)


def load_to_db(df: pd.DataFrame, sql_connection: sqlite3.Connection, table_name: str, log_path: str = "etl_project_log.txt") -> None:
    log_progress(f"Loading to DB table: {table_name}", log_path)
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)
    log_progress("DB load complete", log_path)


def run_query(query_statement: str, sql_connection: sqlite3.Connection, log_path: str = "etl_project_log.txt") -> pd.DataFrame:
    log_progress(f"Running query: {query_statement}", log_path)
    result = pd.read_sql_query(query_statement, sql_connection)
    return result
