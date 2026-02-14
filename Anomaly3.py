import csv
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Create session with connection pooling for faster requests
session = requests.Session()
retry_strategy = Retry(
    total=2,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
session.mount("http://", adapter)
session.mount("https://", adapter)

def Anomaly_points(user_name):
    points = 0
    API_URL = f"https://c2c.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={user_name}"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.get(API_URL, timeout=8)
            
            # Handle rate limiting
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)
                    continue
                else:
                    return None
            
            data = response.json()
            user_stats = data["data"]["userDetailVo"].get("userStatsRet", {})
            break
        except Exception as e:
            if attempt < max_retries - 1:
                import time
                time.sleep(0.5)
                continue
            else:
                return None
    
    if user_stats.get('completedSellOrderNum', 0) == 0:
        points += 10  
    else:
        buy_sell_ratio = user_stats.get('completedBuyOrderNum', 0) / max(user_stats.get('completedSellOrderNum', 1), 1)
        if buy_sell_ratio >= 8:
            points += 10
            
    day_avg = user_stats.get('completedBuyOrderNum', 0) / max(user_stats.get('registerDays', 1), 1)
    if 2 < day_avg < 3:
        points += 20
    elif day_avg >= 3:
        points += 30

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
        points += 15
    elif 2.5 <= count_party_avg2 < 3:
        points += 20
    elif 3 <= count_party_avg2 < 4:
        points += 30
    elif count_party_avg2 >= 4:
        points += 40

    return points

# -------------------------
# Load users and logs
# -------------------------
user_file = 'Users_names.csv'
log_file = 'anomaly_log5.csv'

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

print(f"📊 Total users in {user_file}: {len(all_users)}")
print(f"✅ Already processed: {len(processed_users)}")
print(f"⏳ Remaining to fetch: {len(users_to_process)}")
print(f"🚀 Starting to fetch remaining users...\n")

batch_rows = []
anomaly_count = df_log['anomaly_flag'].sum() if not df_log.empty else 0
processed_count = 0

# -------------------------
# Parallel fetching
# -------------------------
MAX_THREADS = 80  # increased for faster parallel fetching

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    future_to_user = {executor.submit(Anomaly_points, user): user for user in users_to_process}
    
    for future in as_completed(future_to_user):
        user = future_to_user[future]
        points = future.result()
        if points is None:
            continue
        
        anomaly_flag = points >= 30
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
