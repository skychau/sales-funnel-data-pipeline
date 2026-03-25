from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import random

# === CONFIG ===
PROJECT_ID = "sales-funnel-data-pipeline"
DATASET_ID = "analytics"
TABLE_ID = "raw_leads"

# === GENERATE SAMPLE DATA ===

domains = ["company.com", "startup.io", "enterprise.org"]
sources = ["web", "ads", "email", "event"]

data = []
start_date = datetime(2024, 1, 1)

for i in range(1, 31):  # 30 rows
    domain = random.choice(domains)
    email = f"user{i}@{domain}"

    row = {
        "lead_id": i,
        "email": email,
        "source": random.choice(sources),
        "created_at": (start_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
    }

    data.append(row)

# === ADD EDGE CASES ===

# duplicate email (simulate same person multiple leads)
data.append({
    "lead_id": 31,
    "email": "user1@company.com",
    "source": "web",
    "created_at": "2024-01-15"
})

# null source
data.append({
    "lead_id": 32,
    "email": "null_source@startup.io",
    "source": None,
    "created_at": "2024-01-20"
})

# future date (data quality issue)
data.append({
    "lead_id": 33,
    "email": "future@enterprise.org",
    "source": "ads",
    "created_at": "2025-12-01"
})

df = pd.DataFrame(data)

# === INIT BIGQUERY CLIENT ===
client = bigquery.Client(project=PROJECT_ID)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# === LOAD TO BIGQUERY ===
job = client.load_table_from_dataframe(df, table_ref)
job.result()

print(f"✅ Loaded {len(df)} rows into {table_ref}")