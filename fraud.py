import requests
import pandas as pd

def get_user(user_name):
    API_URL = f"https://c2c.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={user_name}"
    response = requests.get(API_URL, timeout=10)
    data = response.json()
    user_stats = data["data"]["userDetailVo"].get("userStatsRet", {})

    cols_to_show = [
    'firstOrderDays',
    'completedBuyOrderNum',
    'completedSellOrderNum',
    'counterpartyCount',
    'completedBuyOrderNumOfLatest30day',
    ]
    filtered_stats = {k: user_stats.get(k) for k in cols_to_show}
    print(filtered_stats)

users = pd.read_csv("anomaly_log3.csv")

fraud = users[users['anomaly_flag']]
users_name = fraud['user_name']

for a in users_name:
    get_user(a)
