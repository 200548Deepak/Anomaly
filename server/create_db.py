# create_and_populate_db.py
import sqlite3
import csv

# Connect to SQLite
conn = sqlite3.connect("users.db")
c = conn.cursor()

# Create table
c.execute('''
CREATE TABLE IF NOT EXISTS users (
    userNo TEXT PRIMARY KEY,
    registerDays INTEGER,
    firstOrderDays INTEGER,
    avgReleaseTimeOfLatest30day REAL,
    avgPayTimeOfLatest30day REAL,
    finishRateLatest30day REAL,
    completedOrderNumOfLatest30day INTEGER,
    completedBuyOrderNumOfLatest30day INTEGER,
    completedSellOrderNumOfLatest30day INTEGER,
    completedOrderTotalBtcAmountOfLatest30day REAL,
    completedOrderNum INTEGER,
    completedBuyOrderNum INTEGER,
    completedSellOrderNum INTEGER,
    completedBuyOrderTotalBtcAmount REAL,
    completedSellOrderTotalBtcAmount REAL,
    completedOrderTotalBtcAmount REAL,
    counterpartyCount INTEGER
)
''')

# Read CSV and insert into database
with open(r"X:\Deepak\orders update\user_details_output2.csv", newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        c.execute('''
        INSERT OR REPLACE INTO users VALUES (
            :userNo, :registerDays, :firstOrderDays, :avgReleaseTimeOfLatest30day, :avgPayTimeOfLatest30day,
            :finishRateLatest30day, :completedOrderNumOfLatest30day, :completedBuyOrderNumOfLatest30day,
            :completedSellOrderNumOfLatest30day, :completedOrderTotalBtcAmountOfLatest30day,
            :completedOrderNum, :completedBuyOrderNum, :completedSellOrderNum,
            :completedBuyOrderTotalBtcAmount, :completedSellOrderTotalBtcAmount,
            :completedOrderTotalBtcAmount, :counterpartyCount
        )
        ''', row)

conn.commit()
conn.close()
print("Database created and populated successfully.")
