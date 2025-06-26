# Fake ETL Pipeline: Pandas Skills Reinforcement

## 1. Overview

This mini project simulates an end‑to‑end data engineering pipeline using synthetic data. It is designed to reinforce skill using Pandas for:

* **Data cleaning**: stripping whitespace, handling missing values (`dropna`, `fillna`).
* **Data transformation & reshaping**: pivot tables (`pivot_table`), unpivoting (`melt`).
* **Type conversion & validation**: numeric coercion (`pd.to_numeric`), date parsing.

## 2. Data Files

All raw and intermediate data live under `raw_data/` and `cleaned/`. Final outputs are in `output_summary/`.

### 2.1 Raw Data (`raw_data/`)

* `users.csv`: synthetic user profiles (user\_id, name, signup\_date)
* `transactions.csv`: synthetic transaction log (txn\_id, user\_id, amount, timestamp)

### 2.2 Cleaned Data (`cleaned/`)

* `users_clean.csv`
* `transactions_clean.csv`
* `master.csv`: merged users + transactions

### 2.3 Final Outputs (`outputs/`)

* `user_monthly_spend.csv`: wide pivot of monthly spend per user
* `user_monthly_spend_long.csv`: long form of the monthly spend summary
* `weekly_revenue.csv`: weekly revenue totals across all users

## 3. Requirements

```bash
pip install pandas
```

## 4. Usage

1. Open the Jupyter notebook `etl_challenge.ipynb`.
2. Run each cell sequentially to: load raw CSVs, clean, merge, reshape, and export.
3. All outputs will be generated under the `cleaned/` and `output_summary/` folders.

## 5. Project Structure

```
etl_pipeline/
├── README.md
├── cleaned
│   ├── master.csv
│   ├── transactions_clean.csv
│   └── users_clean.csv
├── etl_challenge.ipynb
├── output_summary
│   ├── user_monthly_spend.csv
│   ├── user_monthly_spend_long.csv
│   └── weekly_revenue.csv
└── raw_data
    ├── data.json
    ├── transactions.csv
    └── users.csv
```

## 6. Next Steps

* Extend to JSON input/output.
* Deploy as a scheduled script using Airflow or Prefect.
* Integrate with a real SQL database for storage and querying.
