import sqlite3
import pandas as pd

# List your SQLite files
db_files = ["X:\Deepak\orders update\DB\DB_ASG\orders.sqlite", "X:\Deepak\orders update\DB\DB_DRV\orders.sqlite", "DB/DB_MHR/orders_MHR.sqlite"]

all_users = []

# Read user_name from each DB
for db in db_files:
    conn = sqlite3.connect(db)
    df = pd.read_sql_query("SELECT user_name FROM orders", conn)
    conn.close()
    all_users.append(df)

# Combine all results
combined_df = pd.concat(all_users, ignore_index=True)

# Keep unique user names only
unique_users = combined_df["user_name"].dropna().drop_duplicates()

# Write to CSV file
unique_users.to_csv("unique_user_names.csv", index=False, header=True)

print(f"Total unique user_names written to CSV: {len(unique_users)}")
