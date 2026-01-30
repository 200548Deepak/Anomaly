import pandas as pd

users = pd.read_csv("anomaly_log3.csv")

fraud = users[users['anomaly_flag']]
pd.set_option('display.max_rows', None)
print(fraud)