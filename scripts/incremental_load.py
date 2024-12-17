import requests
import json
import snowflake.connector
import re
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv

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

# Initialize state
state_file = "last_run_state.txt"
default_start_date = "2024-01-01T00:00:00Z"  # Fallback if no state exists

# Helper function to load the last run timestamp
def get_last_run_time():
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return f.read().strip()
    return default_start_date

# Helper function to save the last run timestamp
def save_last_run_time(timestamp):
    with open(state_file, "w") as f:
        f.write(timestamp)

# Get last run timestamp
last_run_time = get_last_run_time()
last_run_timestamp = int(datetime.fromisoformat(last_run_time.replace("Z", "+00:00")).timestamp())

logging.info(f"Last run time: {last_run_timestamp}")

# Helper functions
def convert_unix_to_datetime(unix_timestamp):
    try:
        return datetime.fromtimestamp(int(unix_timestamp)) if unix_timestamp and str(unix_timestamp).isdigit() else None
    except (ValueError, TypeError):
        return None

def extract_numeric_id(id_value):
    numeric_part = re.search(r'\d+', id_value)
    return int(numeric_part.group()) if numeric_part else None

# Initialize Elasticsearch query variables
page_size = 100  # Number of records per page
search_after_value = None
all_results = []

# Fetch and process data from Elasticsearch using search_after
while True:
    payload = {
        "_source": {
            "excludes": ["images", "description", "image_log", "translated_description", "videos", "title"]
        },
        "query": {
            "bool": {
                "must": [
                    {"match": {"country": "Kuwait"}},
                    {"range": {"created": {"gte": last_run_timestamp}}}  # Pass as Unix timestamp
                ]
            }
        },
        "size": page_size,
        "sort": [{"created": "asc"}, {"nid": "asc"}]
    }

    # Add search_after if available
    if search_after_value:
        payload["search_after"] = search_after_value

    response = requests.get(url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        logging.error(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    hits = data['hits']['hits']
    if not hits:
        break  # Stop if there are no more hits

    # Helper function to handle boolean conversion
    def handle_boolean(value):
        if value == '' or value is None:
            return None  # or False if you prefer
        return bool(value)

    for result in hits:
        source = result['_source']
        
        # Convert boolean fields from empty string '' to None (NULL) or False
        negotiable = None if source.get("negotiable") == '' else source.get("negotiable")
        is_muted = None if source.get("is_muted") == '' else source.get("is_muted")
        has_images = None if source.get("has_images") == '' else source.get("has_images")
        rejected = None if source.get("rejected") == '' else source.get("rejected")
        enabled = None if source.get("enabled") == '' else source.get("enabled")
        paid = None if source.get("paid") == '' else source.get("paid")
        
        # Ensure the "price" field and others are set to proper data types
        price = None if source.get("price") == '' else source.get("price")
        
        # Prepare extracted data
        extracted_data = [
            source.get("country"), 
            source.get("agent"), 
            has_images, 
            source.get("nid"),
            negotiable, 
            is_muted, 
            source.get("global_type"), 
            source.get("approved"),
            price, 
            source.get("translated_title"), 
            extract_numeric_id(source.get("id")), 
            source.get("paid_for_taxonomy_id"),
            convert_unix_to_datetime(source.get("created")), 
            source.get("created_by"), 
            source.get("phone_number"), 
            source.get("status"),
            source.get("handled_by"), 
            source.get("request_type"), 
            source.get("city")[0] if isinstance(source.get("city"), list) else None,
            rejected, 
            source.get("sub_category"), 
            enabled, 
            source.get("ad_source"),
            source.get("advertisement_type"), 
            source.get("bundle"), 
            source.get("leaf_taxonomy_id"), 
            source.get("taxonomy_status"),
            source.get("data_type"), 
            paid, 
            source.get("category"), 
            source.get("username"),
            convert_unix_to_datetime(source.get("approved_date")), 
            convert_unix_to_datetime(source.get("original_creation")),
            convert_unix_to_datetime(source.get("sticky_payment_start_time")), 
            convert_unix_to_datetime(source.get("sticky_expiry"))
        ]

        # Insert data into Snowflake
        insert_query = """
        INSERT INTO classifieds (
            country, agent, has_images, nid, negotiable, is_muted, global_type, approved, price,
            translated_title, id, paid_for_taxonomy_id, created, created_by, phone_number, status,
            handled_by, request_type, city, rejected, sub_category, enabled, ad_source,
            advertisement_type, bundle, leaf_taxonomy_id, taxonomy_status, data_type, paid, category,
            username, approved_date, original_creation, sticky_payment_start_time, sticky_expiry
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, extracted_data)

    # Update search_after for the next batch
    search_after_value = hits[-1]["sort"]

    # Update the last run timestamp
    last_run_time = hits[-1]['_source']['created']

# Save the last run timestamp for the next run
save_last_run_time(last_run_time)
logging.info("Incremental load completed successfully.")