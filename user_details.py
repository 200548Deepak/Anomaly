import requests
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_FILE = r"X:\Deepak\orders update\unique_user_names.csv"
OUTPUT_FILE = "user_details_output2.csv"

BATCH_SIZE = 100
MAX_WORKERS = 10   # safe range: 8–15

# Load all user IDs
all_users = pd.read_csv(INPUT_FILE)['user_name'].tolist()

# Load already fetched users (for resume)
if os.path.exists(OUTPUT_FILE):
    done_df = pd.read_csv(OUTPUT_FILE, usecols=['userNo'])
    done_users = set(done_df['userNo'].astype(str))
    print(f"🔁 Resuming: {len(done_users)} users already fetched")
else:
    done_users = set()

# Remaining users only
pending_users = [u for u in all_users if str(u) not in done_users]
total = len(pending_users)

print(f"🚀 Fetching {total} remaining users")

session = requests.Session()

def get_user(user_name):
    url = (
        "https://c2c.binance.com/bapi/c2c/v2/friendly/"
        f"c2c/user/profile-and-ads-list?userNo={user_name}"
    )
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        stats = data["data"]["userDetailVo"].get("userStatsRet", {})
        if stats:
            stats["userNo"] = user_name
            return stats
    except Exception:
        return None


def save_batch(batch):
    df = pd.DataFrame(batch)
    if os.path.exists(OUTPUT_FILE):
        df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)
    else:
        df.to_csv(OUTPUT_FILE, index=False)


batch = []
completed = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(get_user, uid): uid for uid in pending_users}

    for future in as_completed(futures):
        result = future.result()
        completed += 1

        if result:
            batch.append(result)

        if completed % BATCH_SIZE == 0:
            save_batch(batch)
            batch = []
            print(f"⚡ Progress: {completed}/{total}")

# Save leftovers
if batch:
    save_batch(batch)

print("🎉 DONE — resumed & completed successfully")
