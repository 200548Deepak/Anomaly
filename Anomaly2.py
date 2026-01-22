import csv
import requests
import pandas as pd
import time

def Anomaly_points(user_name):
    points = 0
    API_URL = f"https://c2c.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={user_name}"
    
    try:
        response = requests.get(API_URL, timeout=10)
        data = response.json()
        user_stats = data["data"]["userDetailVo"].get("userStatsRet", {})
    except Exception as e:
        print(f"Error fetching {user_name}: {e}")
        return False
    
    # Example scoring logic (your original code)
    if user_stats['completedSellOrderNum']==0:
            points+=10  

    else:
        buy_sell_ratio=user_stats['completedBuyOrderNum']/user_stats['completedSellOrderNum']
        if buy_sell_ratio >= 8:
            points+=10
            
    day_avg = user_stats['completedBuyOrderNum']/user_stats['registerDays']
    if day_avg > 2 and day_avg < 3:
        points+=20
    elif day_avg >= 3:
         points += 30

    if user_stats['completedBuyOrderNumOfLatest30day'] >=60 and user_stats['completedBuyOrderNumOfLatest30day'] < 90:
         points += 20
    elif user_stats['completedBuyOrderNumOfLatest30day'] >=90:
         points += 30

    if user_stats['counterpartyCount']==0:
            count_party_avg2=0
    else:
        count_party_avg2=user_stats['completedOrderNum']/user_stats['counterpartyCount']
    if count_party_avg2 > 2 and count_party_avg2 < 2.5:
            points+=10
    elif count_party_avg2 > 2.5 and count_party_avg2 < 3:
            points+=15
    elif count_party_avg2 > 3 and count_party_avg2 < 4:
            points+=20
    elif count_party_avg2 > 4:
            points+=30

    return points > 30

# -------------------------
# Resume logging
# -------------------------

user_file = 'unique_user_names.csv'
log_file = 'anomaly_log3.csv'

# Load all users
df_users = pd.read_csv(user_file)
all_users = df_users['user_name'].tolist()

# Load existing log if exists
try:
    df_log = pd.read_csv(log_file)
    processed_users = set(df_log['user_name'].tolist())
except FileNotFoundError:
    # No log yet
    df_log = pd.DataFrame(columns=['user_name', 'anomaly_flag'])
    processed_users = set()

# Open log file in append mode
with open(log_file, 'a', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    # If file was empty, write header
    if f.tell() == 0:
        writer.writerow(['user_name', 'anomaly_flag'])

    anomaly_users = df_log['anomaly_flag'].sum() if not df_log.empty else 0

    # Loop over users and skip already processed
    for user in all_users:
        if user in processed_users:
            continue  # skip already processed
        
        is_anomaly = Anomaly_points(user)
        if is_anomaly:
            anomaly_users += 1
            print(f"{user} --> Anomaly #{anomaly_users}")
        
        # Write to log
        writer.writerow([user, is_anomaly])
        
        # Optional: avoid API throttling
        #time.sleep(0.2)

total_users = len(all_users)
percent = (anomaly_users / total_users) * 100
print(f"Processing complete! Anomaly percentage: {percent:.2f}%")
