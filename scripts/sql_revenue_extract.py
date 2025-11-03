import os
from google.cloud import bigquery
import pandas as pd

# Auth and client setup (assumes GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT set)
client = bigquery.Client(location="EU")

query = """
SELECT DISTINCT DATE(event_time), event_name, sum(event_revenue_usd) as daily_rev, count(distinct appsflyer_id) as users
-- SELECT country_code, sum(event_revenue_usd) as daily_rev, count(distinct appsflyer_id) as users
FROM `gc-prod-459709.fm_ingest.highway_racer_v1`
WHERE DATE(event_time) >= '2025-08-15'
AND DATE(event_time) <= '2025-08-30'
AND event_revenue_usd IS NOT NULL
group by 1,2
order by 1
"""

print("Running revenue query...")
df = client.query(query, location='EU').to_dataframe()
df.to_csv("sql-revenue-results.csv", index=False)
print(f"Extracted {len(df)} rows to sql-revenue-results.csv.")
