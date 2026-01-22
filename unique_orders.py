import sqlite3
import csv

# Connect to your database
conn = sqlite3.connect('orders.sqlite')
cursor = conn.cursor()

# Execute your query
cursor.execute("SELECT DISTINCT user_name FROM orders")
rows = cursor.fetchall()

# Write to CSV
with open('orders_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Write header row (optional)
    writer.writerow([description[0] for description in cursor.description])
    
    # Write all rows
    writer.writerows(rows)

print("CSV file saved successfully!")
conn.close()
