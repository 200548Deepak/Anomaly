import csv
import threading
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

RATE_LIMIT_PER_SEC = 5


class RateLimiter:
    def __init__(self, rate_per_sec):
        self._interval = 1.0 / max(rate_per_sec, 1)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
            self._next_allowed = time.monotonic() + self._interval


rate_limiter = RateLimiter(RATE_LIMIT_PER_SEC)

def Anomaly_points(user_name):
    points = 0
    API_URL = f"https://c2c.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={user_name}"
    
    try:
        rate_limiter.wait()
        response = requests.get(API_URL, timeout=10)
        data = response.json()
        user_stats = data["data"]["userDetailVo"].get("userStatsRet", {})
    except:
        return None
    
    if user_stats.get('completedSellOrderNum', 0) == 0:
        points += 10  
    else:
        buy_sell_ratio = user_stats.get('completedBuyOrderNum', 0) / max(user_stats.get('completedSellOrderNum', 1), 1)
        if buy_sell_ratio >= 8:
            points += 10
    if user_stats.get('registerDays', 0) == 0:
        day_avg = 0
    else:
        day_avg = user_stats.get('completedBuyOrderNum', 0) / max(user_stats.get('registerDays', 1), 1)
    if 2 < day_avg < 3:
        points += 30
    elif day_avg >= 3:
        points += 40

    completed_last_30 = user_stats.get('completedBuyOrderNumOfLatest30day', 0)
    if 60 <= completed_last_30 < 90:
        points += 30
    elif completed_last_30 >= 90:
        points += 40

    counterparty = user_stats.get('counterpartyCount', 0)
    if counterparty == 0:
        count_party_avg2 = 0
    else:
        count_party_avg2 = user_stats.get('completedOrderNum', 0) / counterparty
    
    if 2 < count_party_avg2 < 2.5:
        points += 10
    elif 2.5 <= count_party_avg2 < 3:
        points += 15
    elif 3 <= count_party_avg2 < 4:
        points += 25
    elif count_party_avg2 >= 4:
        points += 40

    return points

# -------------------------
# Load users and logs
# -------------------------
user_file = 'Users_names.csv'
log_file = 'anomaly_log6.csv'

df_users = pd.read_csv(user_file)
all_users = df_users['user_name'].tolist()

try:
    df_log = pd.read_csv(log_file)
    processed_users = set(df_log['user_name'].tolist())
except FileNotFoundError:
    df_log = pd.DataFrame(columns=['user_name', 'points', 'anomaly_flag'])
    processed_users = set()

# Filter out already processed users
users_to_process = [u for u in all_users if u not in processed_users]

batch_rows = []
anomaly_count = df_log['anomaly_flag'].sum() if not df_log.empty else 0
processed_count = 0

# -------------------------
# Parallel fetching
# -------------------------
MAX_THREADS = 30  # adjust depending on your network/API limits

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    future_to_user = {executor.submit(Anomaly_points, user): user for user in users_to_process}
    
    for future in as_completed(future_to_user):
        user = future_to_user[future]
        points = future.result()
        if points is None:
            continue
        
        anomaly_flag = points >= 40
        if anomaly_flag:
            anomaly_count += 1
        
        batch_rows.append([user, points, anomaly_flag])
        processed_count += 1

        # Save every 100 users
        if processed_count % 100 == 0:
            with open(log_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if f.tell() == 0:
                    writer.writerow(['user_name', 'points', 'anomaly_flag'])
                writer.writerows(batch_rows)
            batch_rows = []
            print(f"✅ Saved {processed_count} users to CSV file...")

# Save remaining users
if batch_rows:
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(['user_name', 'points', 'anomaly_flag'])
        writer.writerows(batch_rows)
    print(f"✅ Saved final batch of {len(batch_rows)} users to CSV file.")

# -------------------------
# Final stats
# -------------------------
df_log = pd.read_csv(log_file)
anomaly_users = (df_log['anomaly_flag'] == True).sum()
total_users = len(df_log)
percent = (anomaly_users / total_users) * 100
print(f"Processing complete! Anomaly percentage: {percent:.2f}%")
