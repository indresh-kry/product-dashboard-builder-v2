import os
from google.cloud import bigquery
import pandas as pd

# Auth and client setup (assumes GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT set)
client = bigquery.Client(location="EU")

query = """
SELECT *
FROM `gc-prod-459709.fm_ingest.highway_racer_v1`
WHERE event_name IN ('af_purchase', 'buy_car1', 'af_ad_revenue')
LIMIT 500
"""

print("Running revenue query...")
df = client.query(query, location='EU').to_dataframe()
df.to_csv("sql-revenue-results.csv", index=False)
print(f"Extracted {len(df)} rows to sql-revenue-results.csv.")
