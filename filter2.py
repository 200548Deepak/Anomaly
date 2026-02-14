import pandas as pd

users = pd.read_csv("anomaly_log5.csv")

fraud = users[users['points'] == 30]
pd.set_option('display.max_rows', None)
print(fraud)
print(len(fraud))