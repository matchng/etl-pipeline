# dags/etl_pipeline.py
import pandas as pd
import os
import sqlite3
import json

def run_etl():
    with open("/opt/airflow/raw_data/data.json", "r") as f: # Import json file
        raw = json.load(f)
        users_df = pd.DataFrame(raw["users"])
        txn_df = pd.DataFrame(raw["transactions"])
    
    # Clean, Parse date, Change dtype and Merge 
    users_df["name"] = users_df["name"].str.strip()
    users_df = users_df.replace("", pd.NA).dropna(subset=["user_id", "name"])
    users_df = users_df.drop_duplicates(subset=["user_id", "name"])
    users_df["signup_date"] = pd.to_datetime(users_df["signup_date"])
    txn_df["timestamp"] = pd.to_datetime(txn_df["timestamp"])
    txn_df["amount"] = pd.to_numeric(txn_df["amount"], errors='coerce')
    txn_df = txn_df.dropna(subset=["txn_id", "user_id", "amount"])


    master_df = pd.merge(users_df, txn_df, on="user_id", how="inner")

    # Save JSON and CSV
    os.makedirs("/opt/airflow/output/cleaned", exist_ok=True)
    master_df.to_json("/opt/airflow/output/cleaned/master.json", orient="records", date_format="iso", indent=2)

    # Write to SQLite
    conn = sqlite3.connect("/opt/airflow/output/etl_pipeline.db")
    master_df.to_sql("master", conn, if_exists="replace", index=False)
    conn.close()


def export_sql_query():
    db_path = "/opt/airflow/output/etl_pipeline.db"  # Database path
    conn = sqlite3.connect(db_path)

    # Queries
    spend_per_user = pd.read_sql("""
        SELECT user_id, name, SUM(amount) AS total_spend
        FROM master
        GROUP BY user_id, name
        ORDER BY total_spend DESC
    """, conn)

    weekly_revenue = pd.read_sql("""
        SELECT strftime('%Y', timestamp) AS year,
               strftime('%W', timestamp) AS week,
               SUM(amount) AS revenue
        FROM master
        GROUP BY year, week
        ORDER BY year, week
    """, conn)

    # Output directory
    output_dir = "/opt/airflow/output/summary"
    os.makedirs(output_dir, exist_ok=True)

    # Save to CSV files
    spend_per_user.to_csv(f"{output_dir}/total_spend_per_user.csv", index=False)
    weekly_revenue.to_csv(f"{output_dir}/weekly_revenue.csv", index=False)
    
    # Close connection
    conn.close()

    