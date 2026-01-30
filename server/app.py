from flask import Flask, jsonify, render_template
import sqlite3

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/users")
def get_users():
    conn = sqlite3.connect("users.db")
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    # Select only the required columns
    c.execute("""
        SELECT 
            userNo,
            registerDays,
            firstOrderDays,
            completedOrderNumOfLatest30day,
            completedBuyOrderNumOfLatest30day,
            completedSellOrderNumOfLatest30day,
            completedOrderNum,
            completedBuyOrderNum,
            completedSellOrderNum,
            counterpartyCount
        FROM users
    """)
    rows = c.fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

if __name__ == "__main__":
    app.run(debug=True)
