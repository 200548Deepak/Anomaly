import requests
import pandas as pd

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