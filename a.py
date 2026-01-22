import csv
import requests
import time

# Input CSV with user_name column
INPUT_CSV = "orders_output.csv"

# Output CSV file
OUTPUT_CSV = "user_stats_output.csv"

# Binance API URL template
API_URL = "https://c2c.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={}"

# Headers for CSV (from userStatsRet in sample)
HEADERS = [
    "userNo",
    "registerDays",
    "firstOrderDays",
    "avgReleaseTimeOfLatest30day",
    "avgPayTimeOfLatest30day",
    "finishRateLatest30day",
    "completedOrderNumOfLatest30day",
    "completedBuyOrderNumOfLatest30day",
    "completedSellOrderNumOfLatest30day",
    "completedOrderTotalBtcAmountOfLatest30day",
    "completedOrderNum",
    "completedBuyOrderNum",
    "completedSellOrderNum",
    "completedBuyOrderTotalBtcAmount",
    "completedSellOrderTotalBtcAmount",
    "completedOrderTotalBtcAmount",
    "counterpartyCount"
]

# Read user_name column from input CSV
with open(INPUT_CSV, newline="", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    user_ids = [row["user_name"] for row in reader]

# Open output CSV and write headers
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=HEADERS)
    writer.writeheader()

    for user_no in user_ids:
        url = API_URL.format(user_no)
        try:
            response = requests.get(url, timeout=10)
            data = response.json()

            if data.get("success") and "userDetailVo" in data["data"]:
                user_stats = data["data"]["userDetailVo"].get("userStatsRet", {})
                user_stats["userNo"] = user_no

                # Write row to CSV
                writer.writerow({h: user_stats.get(h) for h in HEADERS})
            else:
                print(f"No stats for user: {user_no}")

        except Exception as e:
            print(f"Error fetching user {user_no}: {e}")

        time.sleep(0.2)  # prevent rate-limiting

print(f"Saved all user stats to {OUTPUT_CSV}")
