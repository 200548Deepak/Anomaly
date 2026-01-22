import csv
import requests
import time
import pandas as pd

# | Signal                                  | Points |
# | --------------------------------------- | ------ |
# | Buy/Sell ratio > 10:1                   | +25    |
# | counterpartyCount / orders < 0.4       | +20    |
# | registerDays < 60 with high volume      | +15    |
# | avgPayTime < 2 min                      | +10    |
# | avgReleaseTime < 1 min                  | +10    |
# | Near-perfect finish rate with imbalance | +10    |

# {
#     'registerDays': 51, 
#  'firstOrderDays': 51,
#   'avgReleaseTimeOfLatest30day': 0.0, 'avgPayTimeOfLatest30day': 83.09, 'finishRateLatest30day': 0.804, 
#   'completedOrderNumOfLatest30day': 45, 'completedBuyOrderNumOfLatest30day': 45, 'completedSellOrderNumOfLatest30day': 0,
#     'completedOrderTotalBtcAmountOfLatest30day': 0, 'completedOrderNum': 87, 'completedBuyOrderNum': 87, 'completedSellOrderNum': 0, 
#     'completedBuyOrderTotalBtcAmount': 0, 'completedSellOrderTotalBtcAmount': 0, 'completedOrderTotalBtcAmount': 0, 
#     'counterpartyCount': 42}

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
        print('completedSellOrderNum==0 : points+=10')

    else:
        buy_sell_ratio=user_stats['completedBuyOrderNum']/user_stats['completedSellOrderNum']
        if buy_sell_ratio >= 8:
            points+=10
            print('Buy_sell_ratio >=8: points += 10')

    day_avg = user_stats['completedBuyOrderNum']/user_stats['registerDays']
    if day_avg > 2 and day_avg < 3:
        points+=20
        print('day_avg > 2 and < 3 : points=20 ')
    elif day_avg > 3:
          points +=30
          print('day_avg > 3 : points += 30')

    if user_stats['completedBuyOrderNumOfLatest30day'] >=60 and user_stats['completedBuyOrderNumOfLatest30day'] < 90:
         points += 20
         print('completedBuyOrderNumOfLatest30day >=60 and <=90 points += 20')
    elif user_stats['completedBuyOrderNumOfLatest30day'] >=90:
         points += 30
         print('completedBuyOrderNumOfLatest30day >=90 points += 30')

    if user_stats['counterpartyCount']==0:
            count_party_avg2=0
    else:
        count_party_avg2=user_stats['completedOrderNum']/user_stats['counterpartyCount']
    if count_party_avg2 > 2 and count_party_avg2 < 2.5:
            points+=10
            print('count_party_avg2 > 2 and count_party_avg2 < 2.5: points+=10')
    elif count_party_avg2 > 2.5 and count_party_avg2 < 3:
            points+=15
            print('count_party_avg2 > 2.5 and count_party_avg2 < 3 : points+=15')
    elif count_party_avg2 > 3 and count_party_avg2 < 4:
            points+=20
            print('count_party_avg2 > 3 and count_party_avg2 < 4 : points+=20')
    elif count_party_avg2 > 4:
            points+=30
            print('count_party_avg2 > 4 : points+=30')

    return points 

print(Anomaly_points('sa17f89001ba833bc8da33e3c706345aa'))


