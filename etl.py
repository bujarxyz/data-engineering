"""
ETL DAG: scans /opt/airflow/data/incoming for CSV files, loads each one into
the BMW star schema (fact_car_listing + dimension lookup functions), and
audits every file in t_file with rows_inserted / rows_rejected.

Files end up in:
  - processed/   if the load committed successfully
  - bad/         if the load raised an exception (whole file failed)
  - bad/<f>.bad.csv  sidecar containing only the rows rejected by validation
"""

import logging
import os
import shutil
from datetime import datetime

import pandas as pd
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

# Module-level logger -- preferred over print() because Airflow captures it
# per task instance and shows it in the UI.
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INCOMING_DIR = "/opt/airflow/data/incoming"
PROCESSED_DIR = "/opt/airflow/data/processed"
BAD_DIR = "/opt/airflow/data/bad"

# Postgres connection registered in Airflow > Admin > Connections.
POSTGRES_CONN_ID = "cdr"
ALERT_EMAIL = "data@test.com"

# Columns we coerce to numeric. Any row with a NaN here after coercion is
# considered "bad" and is excluded from the insert.
NUMERIC_COLS = ["price", "mileage", "tax", "mpg", "engineSize", "year"]
# Columns we just trim whitespace on.
STRING_COLS = ["model", "transmission", "fuelType"]

# Make sure the directories exist on first run so shutil.move() never fails
# because of a missing destination folder.
for d in (INCOMING_DIR, PROCESSED_DIR, BAD_DIR):
    os.makedirs(d, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_move(src: str, dst_dir: str, suffix: str = "") -> str:
    """Move `src` into `dst_dir`, creating the directory if needed.

    Guarded with os.path.exists() so a retry that has already moved the file
    doesn't blow up the task.
    """
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, os.path.basename(src) + suffix)
    if os.path.exists(src):
        shutil.move(src, dst)
    return dst


def _resolve_dim_ids(cur, func: str, values) -> dict:
    """Batch dimension lookups.

    Instead of calling get_<dim>_id() once per row (N round-trips), we call
    it once per *distinct* value in the column and build a {value: id} map.
    The caller then translates rows in pure Python.
    """
    distinct = sorted({v for v in values if v is not None and not pd.isna(v)})
    mapping = {}
    for v in distinct:
        cur.execute(f"SELECT {func}(%s)", (v,))
        mapping[v] = cur.fetchone()[0]
    return mapping


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,            # triggered manually
    catchup=False,            # don't backfill missed runs
    max_active_runs=1,        # prevent two runs racing on the incoming/ folder
    tags=["bmw", "etl"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: list the CSVs to process
    # -----------------------------------------------------------------------
    @task
    def scan_directory() -> list[str]:
        """Return absolute paths of every *.csv in INCOMING_DIR (sorted)."""
        files = sorted(
            os.path.join(INCOMING_DIR, f)
            for f in os.listdir(INCOMING_DIR)
            if f.lower().endswith(".csv")
        )
        log.info("Found %d CSV file(s) in %s", len(files), INCOMING_DIR)
        return files

    # -----------------------------------------------------------------------
    # Task 2: process one CSV (this task is dynamically mapped per file)
    # -----------------------------------------------------------------------
    @task
    def process_file(file_path: str) -> dict:
        """Load a single CSV in its own transaction.

        Returns a small dict so the downstream notify task can summarize
        the run without needing to query the database.
        """
        filename = os.path.basename(file_path)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        file_id = None
        try:
            # 1. Audit row -- created up front so we always have a file_id to
            #    reference in fact_car_listing, even if the load is partial.
            cur.execute(
                """
                INSERT INTO t_file(filename, start_date)
                VALUES (%s, CURRENT_TIMESTAMP)
                RETURNING file_id
                """,
                (filename,),
            )
            file_id = cur.fetchone()[0]
            conn.commit()

            # 2. Load + clean the CSV in pandas.
            df = pd.read_csv(file_path)

            for col in STRING_COLS:
                df[col] = df[col].astype(str).str.strip()
            for col in NUMERIC_COLS:
                # errors="coerce" turns unparseable values into NaN so we
                # can detect them with a single .isna() pass below.
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # 3. Split good vs. bad rows.
            bad_mask = df[NUMERIC_COLS].isna().any(axis=1)
            bad_rows = df[bad_mask]
            good = df[~bad_mask]

            # 4. Build dimension lookup maps and assemble fact records.
            inserted = 0
            if not good.empty:
                model_ids = _resolve_dim_ids(cur, "get_model_id", good["model"])
                year_ids = _resolve_dim_ids(
                    cur, "get_year_id", good["year"].astype(int)
                )
                trans_ids = _resolve_dim_ids(
                    cur, "get_transmission_id", good["transmission"]
                )
                fuel_ids = _resolve_dim_ids(
                    cur, "get_fuel_type_id", good["fuelType"]
                )

                # Translate each row using the maps -- no DB round-trips here.
                records = [
                    (
                        model_ids[r["model"]],
                        year_ids[int(r["year"])],
                        trans_ids[r["transmission"]],
                        fuel_ids[r["fuelType"]],
                        float(r["price"]),
                        int(r["mileage"]),
                        int(r["tax"]),
                        float(r["mpg"]),
                        float(r["engineSize"]),
                        int(file_id),
                    )
                    for _, r in good.iterrows()
                ]

                # Single multi-row INSERT (much faster than executemany).
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_car_listing(
                        model_id, year_id, transmission_id, fuel_type_id,
                        price, mileage, tax, mpg, engine_size, file_id
                    ) VALUES %s
                    """,
                    records,
                )
                inserted = len(records)

            # 5. Persist rejected rows to a sidecar CSV for inspection.
            rejected = len(bad_rows)
            if rejected:
                bad_rows.to_csv(
                    os.path.join(BAD_DIR, filename + ".bad.csv"), index=False
                )

            # 6. Update audit row with final counts and end timestamp.
            cur.execute(
                """
                UPDATE t_file
                SET end_date = CURRENT_TIMESTAMP,
                    rows_inserted = %s,
                    rows_rejected = %s
                WHERE file_id = %s
                """,
                (inserted, rejected, file_id),
            )
            conn.commit()

            # 7. Only move to processed/ AFTER the commit succeeds, so a
            #    failure mid-transaction leaves the CSV in incoming/ for
            #    the next run.
            _safe_move(file_path, PROCESSED_DIR)

            return {
                "file": filename,
                "file_id": file_id,
                "inserted": inserted,
                "rejected": rejected,
            }

        except Exception as exc:
            # Anything raised above: roll back the partial transaction,
            # log the traceback, and quarantine the file.
            conn.rollback()
            log.exception("Failed to process %s", filename)
            _safe_move(file_path, BAD_DIR, suffix=".bad")
            return {
                "file": filename,
                "file_id": file_id,
                "inserted": 0,
                "rejected": 0,
                "error": str(exc),
            }
        finally:
            cur.close()
            conn.close()

    # -----------------------------------------------------------------------
    # Task 3: send a single summary email
    # -----------------------------------------------------------------------
    # trigger_rule="all_done" -> run even if some process_file tasks failed,
    # so the operator still gets a report.
    @task(trigger_rule="all_done")
    def notify(results: list[dict]) -> None:
        if not results:
            log.info("No files processed; skipping notification.")
            return

        # Anything with an error or any rejected rows is worth reporting.
        problems = [r for r in results if r.get("error") or r.get("rejected", 0) > 0]
        if not problems:
            log.info("All %d file(s) processed cleanly.", len(results))
            return

        rows = "".join(
            f"<tr><td>{r['file']}</td>"
            f"<td>{r.get('inserted', 0)}</td><td>{r.get('rejected', 0)}</td>"
            f"<td>{r.get('error', '')}</td></tr>"
            for r in problems
        )
        html = (
            "<p>ETL completed with issues.</p>"
            "<table border='1' cellpadding='4'>"
            "<tr><th>File</th><th>Inserted</th><th>Rejected</th><th>Error</th></tr>"
            f"{rows}</table>"
        )
        send_email(
            to=[ALERT_EMAIL],
            subject=f"[etl] {len(problems)} file(s) need attention",
            html_content=html,
        )

    # -----------------------------------------------------------------------
    # Wiring: scan -> map process_file across the file list -> notify
    # process_file.expand() creates one task instance per file, so retries
    # and logs are isolated per CSV.
    # -----------------------------------------------------------------------
    files = scan_directory()
    results = process_file.expand(file_path=files)
    notify(results)