from __future__ import annotations

import argparse
import os
import sqlite3

from largest_banks_etl.pipeline import (
    extract,
    load_to_csv,
    load_to_db,
    log_progress,
    run_query,
    transform,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Largest Banks ETL (Wikipedia → CSV + SQLite)")
    parser.add_argument("--url", default="https://en.wikipedia.org/wiki/List_of_largest_banks")
    parser.add_argument("--rates", default="data/exchange_rates.csv")
    parser.add_argument("--out-csv", default="outputs/largest_banks.csv")
    parser.add_argument("--db", default="outputs/largest_banks.db")
    parser.add_argument("--table", default="Largest_banks")
    parser.add_argument("--log", default="etl_project_log.txt")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    os.makedirs(os.path.dirname(args.db), exist_ok=True)

    log_progress("Preliminaries complete. Initiating ETL process", args.log)

    df = extract(args.url, table_attribs=None, log_path=args.log)
    df = transform(df, args.rates, log_path=args.log)

    load_to_csv(df, args.out_csv, log_path=args.log)

    conn = sqlite3.connect(args.db)
    try:
        load_to_db(df, conn, args.table, log_path=args.log)

        # Example queries (your course-style outputs)
        q_all = f"SELECT * FROM {args.table};"
        q_london = f"SELECT company, MC_GBP_Billion FROM {args.table};"
        q_berlin = f"SELECT company, MC_EUR_Billion FROM {args.table};"
        q_delhi = f"SELECT company, MC_INR_Billion FROM {args.table};"

        print("\n--- FULL TABLE ---")
        print(run_query(q_all, conn, log_path=args.log).to_string(index=False))

        print("\n--- London Office (GBP) ---")
        print(run_query(q_london, conn, log_path=args.log).to_string(index=False))

        print("\n--- Berlin Office (EUR) ---")
        print(run_query(q_berlin, conn, log_path=args.log).to_string(index=False))

        print("\n--- New Delhi Office (INR) ---")
        print(run_query(q_delhi, conn, log_path=args.log).to_string(index=False))

        log_progress("Process Complete.", args.log)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
