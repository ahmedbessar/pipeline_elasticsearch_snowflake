import requests
import json
import snowflake.connector
import re
from datetime import datetime
import os
import logging
from dotenv import load_dotenv
import pandas as pd

# Elasticsearch endpoint
url = "https://vpc-waseet-prod-ma4lk6ppczqcqiepfb2q2ayimy.eu-west-1.es.amazonaws.com/classifieds/_search"
headers = {'Content-Type': 'application/json'}

# Load environment variables
load_dotenv()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Snowflake connection
try:
    snowflake_conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse="COMPUTE_WH",
        database="WASEET",
        schema="RAW"
    )
    logging.info("Connected to Snowflake successfully.")
except Exception as e:
    logging.error(f"Failed to connect to Snowflake: {e}")
    raise

cursor = snowflake_conn.cursor()

# Step 1: Get the last processed timestamp from Snowflake
cursor = snowflake_conn.cursor()
cursor.execute("SELECT COALESCE(MAX(created), '1970-01-01T00:00:00Z') FROM classifieds;")
last_processed_timestamp = cursor.fetchone()[0]
print(f"Last processed timestamp: {last_processed_timestamp}")

# Step 2: Query Elasticsearch for new or updated records
query = {
    "query": {
        "range": {
            "created": {"gt": last_processed_timestamp}
        }
    }
}
response = url.search(index="classifieds", body=query, size=10000)  # Adjust the size as needed

# Step 3: Convert Elasticsearch response to DataFrame
data = [hit["_source"] for hit in response["hits"]["hits"]]
df = pd.DataFrame(data)
print(f"Fetched {len(df)} new/updated records from Elasticsearch.")

# Step 4: Upsert data into Snowflake
if not df.empty:
    # Create a temporary staging table
    cursor.execute("CREATE OR REPLACE TEMP TABLE temp_classifieds LIKE classifieds;")

    # Write DataFrame to Snowflake staging table
    snowflake.write_pandas(snowflake_conn, df, "temp_classifieds")

    # Merge data into main table
    merge_query = """
        MERGE INTO classifieds t
        USING temp_classifieds s
        ON t.nid = s.nid
        WHEN MATCHED THEN
            UPDATE SET
                t.title = s.title,
                t.price = s.price,
                t.updated = s.updated
        WHEN NOT MATCHED THEN
            INSERT (nid, title, price, created, updated)
            VALUES (s.nid, s.title, s.price, s.created, s.updated);
    """
    cursor.execute(merge_query)
    print("Upsert completed.")

else:
    print("No new records to process.")

# Close Snowflake connection
cursor.close()
snowflake_conn.close()
