import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV files
user_stats = pd.read_csv("X:\Deepak\orders update\user_details_output2.csv")  # your first CSV
anomalies = pd.read_csv("X:\Deepak\orders update\anomaly_log3.csv")    # your second CSV

# Merge on userNo / user_name
data = user_stats.merge(anomalies, left_on='userNo', right_on='user_name', how='left')

# Fill missing anomaly_flag with False
data['anomaly_flag'] = data['anomaly_flag'].fillna(False)

# Set style
sns.set(style="whitegrid")

# 1️⃣ Distribution of completedOrderNum (normal vs anomaly)
plt.figure(figsize=(10,6))
sns.histplot(data=data, x='completedOrderNum', hue='anomaly_flag', bins=30, kde=True, palette={True:'red', False:'green'})
plt.title('Completed Orders Distribution (Red=Anomaly, Green=Normal)')
plt.xlabel('Completed Orders')
plt.ylabel('Count')
plt.show()

# 2️⃣ Scatter plot: registerDays vs completedOrderNum
plt.figure(figsize=(10,6))
sns.scatterplot(data=data, x='registerDays', y='completedOrderNum', hue='anomaly_flag', palette={True:'red', False:'blue'})
plt.title('Register Days vs Completed Orders')
plt.xlabel('Register Days')
plt.ylabel('Completed Orders')
plt.show()

# 3️⃣ Heatmap of correlations
plt.figure(figsize=(12,8))
corr = data.select_dtypes(include=['float64', 'int64']).corr()
sns.heatmap(corr, annot=True, fmt=".2f", cmap='coolwarm')
plt.title('Correlation Heatmap')
plt.show()

# 4️⃣ Boxplot: avgPayTimeOfLatest30day for anomalies vs normal
plt.figure(figsize=(10,6))
sns.boxplot(x='anomaly_flag', y='avgPayTimeOfLatest30day', data=data, palette={True:'red', False:'green'})
plt.title('Average Pay Time in Latest 30 Days (Anomaly vs Normal)')
plt.xlabel('Anomaly Flag')
plt.ylabel('Average Pay Time (days)')
plt.show()
