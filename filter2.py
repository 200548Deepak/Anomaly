import pandas as pd

users = pd.read_csv("anomaly_log6.csv")

#fraud1 = users[users['points'] == 30] 
fraud2 = users[users['points'] == 35]
print(fraud2.head(50))
#fraud3 = users[users['points'] == 40]
# pd.set_option('display.max_rows', None)
# fraud = pd.concat([fraud1, fraud2, fraud3], ignore_index=True)
# print(fraud)
# ano_count=len(fraud)
# percent=(ano_count/len(users))*100
# print(f"Total Anomalies: {ano_count}")
# print(f"Percentage of Anomalies: {percent:.2f}%")