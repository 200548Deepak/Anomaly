import requests
import pandas as pd

def get_user(user_name):
    API_URL = f"https://c2c.binance.com/bapi/c2c/v2/friendly/c2c/user/profile-and-ads-list?userNo={user_name}"
    response = requests.get(API_URL, timeout=10)
    data = response.json()
    user_stats = data["data"]["userDetailVo"].get("userStatsRet", {})

    cols_to_show = [
    'registerDays',
    'completedBuyOrderNum',
    'completedSellOrderNum',
    'counterpartyCount',
    'completedBuyOrderNumOfLatest30day',
    'userNo',
    ]

    print(user_stats)

get_user('')