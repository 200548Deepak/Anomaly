import pandas as pd

# userNo,registerDays,firstOrderDays,avgReleaseTimeOfLatest30day,avgPayTimeOfLatest30day,finishRateLatest30day,
# completedOrderNumOfLatest30day,completedBuyOrderNumOfLatest30day,completedSellOrderNumOfLatest30day,
# completedOrderTotalBtcAmountOfLatest30day,completedOrderNum,completedBuyOrderNum,completedSellOrderNum,
# completedBuyOrderTotalBtcAmount,completedSellOrderTotalBtcAmount,completedOrderTotalBtcAmount,counterpartyCount

df = pd.read_csv("user_stats_output.csv")

filtered = df[
    (df['registerDays'] >= 50) &
    (df['completedBuyOrderNum'] >= 0) &
    (df['completedSellOrderNum'] <= 0 ) &
    (df['counterpartyCount'] >= 0 )
]

filtered = filtered.sort_values(by='registerDays', ascending=True)

cols_to_show = [
    'registerDays',
    'completedBuyOrderNum',
    'completedSellOrderNum',
    'counterpartyCount',
    'completedBuyOrderNumOfLatest30day',
    'userNo',
]

pd.set_option('display.max_rows', None)

print(len(filtered))
print(filtered[cols_to_show])

