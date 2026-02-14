import pandas as pd
df = pd.read_csv('anomaly_log5.csv')
print(f'Total users: {len(df)}')
print(f'Anomaly users: {(df["anomaly_flag"] == True).sum()}')
print(f'Percentage: {(df["anomaly_flag"] == True).sum() / len(df) * 100:.2f}%')