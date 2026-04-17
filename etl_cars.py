import os
import shutil
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime

INCOMING_DIR = "/opt/airflow/data/incoming"
PROCESSED_DIR = "/opt/airflow/data/processed"
BAD_DIR = "/opt/airflow/data/bad"

with DAG(
    dag_id="etl_bmw_directory",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    tags=["bmw","etl"],
) as dag:

    # ------------------------
    # 1. Scan directory
    # ------------------------
    def scan_directory(ti):
        files = [os.path.join(INCOMING_DIR,f) for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")]
        ti.xcom_push(key="csv_files", value=files)

    scan_task = PythonOperator(
        task_id="scan_directory",
        python_callable=scan_directory
    )

    # ------------------------
    # 2. Process each CSV
    # ------------------------
    def process_file(ti):
        files = ti.xcom_pull(task_ids="scan_directory", key="csv_files")
        if not files:
            print("No CSV files found")
            ti.xcom_push(key="bad_files", value=[])
            return

        hook = PostgresHook(postgres_conn_id="cdr")
        conn = hook.get_conn()
        cur = conn.cursor()
        
        bad_files = []  # Track files with bad rows

        try:
            for file_path in files:
                try:
                    # Skip empty files
                    if os.path.getsize(file_path) == 0:
                        print(f"Skipping empty file: {file_path}")
                        continue

                    df = pd.read_csv(file_path)
                    df["model"] = df["model"].str.strip()
                    df["transmission"] = df["transmission"].str.strip()
                    df["fuelType"] = df["fuelType"].str.strip()

                    # Convert numeric columns, collect bad rows
                    for col in ["price","mileage","tax","mpg","engineSize","year"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    bad_rows = df[df[["price","mileage","tax","mpg","engineSize","year"]].isna().any(axis=1)]
                    df = df.dropna(subset=["price","mileage","tax","mpg","engineSize","year"])

                    if not df.empty:
                        # Insert t_file audit
                        cur.execute("""
                            INSERT INTO t_file(filename, start_date, rows, status)
                            VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
                            RETURNING file_id
                        """, (os.path.basename(file_path), len(df), "RUNNING"))
                        file_id = cur.fetchone()[0]

                        # Insert fact table using dimension functions
                        from psycopg2.extras import execute_values
                        records = []
                        for _, r in df.iterrows():
                            cur.execute("SELECT get_model_id(%s)", (r["model"],))
                            model_id = cur.fetchone()[0]
                            cur.execute("SELECT get_year_id(%s)", (int(r["year"]),))
                            year_id = cur.fetchone()[0]
                            cur.execute("SELECT get_transmission_id(%s)", (r["transmission"],))
                            trans_id = cur.fetchone()[0]
                            cur.execute("SELECT get_fuel_type_id(%s)", (r["fuelType"],))
                            fuel_id = cur.fetchone()[0]

                            records.append((
                                model_id, year_id, trans_id, fuel_id,
                                float(r["price"]), int(r["mileage"]), int(r["tax"]),
                                float(r["mpg"]), float(r["engineSize"]), int(file_id)
                            ))
                        sql = """
                            INSERT INTO fact_car_listing(
                                model_id, year_id, transmission_id, fuel_type_id,
                                price, mileage, tax, mpg, engine_size, file_id
                            ) VALUES %s
                        """
                        execute_values(cur, sql, records)
                        conn.commit()

                        # Update t_file audit
                        cur.execute("""
                            UPDATE t_file
                            SET end_date = CURRENT_TIMESTAMP,
                                rows_inserted = %s,
                                rows_rejected = %s,
                                status = %s
                            WHERE file_id = %s
                        """, (len(df), len(bad_rows), "SUCCESS" if bad_rows.empty else "PARTIAL", file_id))
                        conn.commit()

                    # ------------------------
                    # Move files
                    # ------------------------
                    if bad_rows.empty:
                        shutil.move(file_path, os.path.join(PROCESSED_DIR, os.path.basename(file_path)))
                    else:
                        bad_file = os.path.join(BAD_DIR, os.path.basename(file_path)+".bad")
                        bad_rows.to_csv(bad_file, index=False)
                        bad_files.append(os.path.basename(file_path))

                except Exception as e:
                    # Move the file to bad folder if processing fails
                    shutil.move(file_path, os.path.join(BAD_DIR, os.path.basename(file_path)+".bad"))
                    print(f"Error processing {file_path}: {e}")
                    continue

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

        # Push bad files list for email task
        ti.xcom_push(key="bad_files", value=bad_files)

    process_task = PythonOperator(
        task_id="process_files",
        python_callable=process_file
    )

    # ------------------------
    # 3. Send summary email (separate task)
    # ------------------------
    def send_summary_email(ti):
        bad_files = ti.xcom_pull(task_ids="process_files", key="bad_files")
        
        if not bad_files:
            subject = "ETL Complete: All files processed successfully"
            html_content = "All CSV files were processed without rejected rows."
        else:
            subject = f"ETL Alert: {len(bad_files)} file(s) with rejected rows"
            html_content = f"""
                <h3>Files with rejected rows:</h3>
                <ul>
                    {''.join(f'<li>{f}</li>' for f in bad_files)}
                </ul>
                <p>Bad rows have been saved to: {BAD_DIR}</p>
            """

        email = EmailOperator(
            task_id="send_summary_email",
            to="data@test.com",
            subject=subject,
            html_content=html_content
        )
        email.execute(context=None)

    email_task = PythonOperator(
        task_id="send_summary",
        python_callable=send_summary_email,
        trigger_rule="all_done"  # Run even if upstream fails
    )

    scan_task >> process_task >> email_task