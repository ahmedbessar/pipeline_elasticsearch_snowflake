import os
import logging
from datetime import datetime
import requests
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASS")

# Configure loggings
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Snowflake connection
try:
    snowflake_conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    logging.info("Connected to Snowflake successfully.")
except Exception as e:
    logging.error(f"Failed to connect to Snowflake: {e}")
    raise

cursor = snowflake_conn.cursor()

# Ensure table schema matches expected structure
create_elasticsearch_table_query = """
CREATE OR REPLACE TABLE CLASSIFIED_LOG_REPORT_ALL_ELS (
    EntityID INT,
    EntityTitle STRING,
    Breadcrumb STRING,
    BreadcrumbTitleEnglish STRING,
    BreadcrumbTitleArabic STRING,
    CallAndroidCount INT,
    CallHuaweiCount INT,
    CallIOSCount INT,
    CallWebCount INT,
    ChatAndroidCount INT,
    ChatHuaweiCount INT,
    ChatIOSCount INT,
    ChatWebCount INT,
    CountryAlias STRING,
    CreatedAt TIMESTAMP_NTZ,
    Day TIMESTAMP_NTZ,
    EntityType STRING,
    IsExpired BOOLEAN,
    IsFree BOOLEAN,
    IsPaid BOOLEAN,
    IsSticky BOOLEAN,
    MainTaxonomy INT,
    MainTaxonomyTitleEnglish STRING,
    MainTaxonomyTitleArabic STRING,
    OwnerPhone STRING,
    UpdatedAt TIMESTAMP_NTZ,
    VisitAndroidCount INT,
    VisitHuaweiCount INT,
    VisitIOSCount INT,
    VisitWebCount INT,
    WhatsappAndroidCount INT,
    WhatsappHuaweiCount INT,
    WhatsappIOSCount INT,
    WhatsappWebCount INT
);
"""
cursor.execute(create_elasticsearch_table_query)
logging.info("Ensured table schema exists in Snowflake.")

# Elasticsearch query setup
headers = {
    'kbn-xsrf': 'reporting',
    'Content-Type': 'application/json',
}

auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
page_size = 10000  # Number of records per page
search_after_value = None  # Initialize search_after


# List of Elasticsearch indices
indices = [
    # "log-classified-report-day-kw",
    "log-classified-report-day-ae",
    "log-classified-report-day-bh",
    "log-classified-report-day-eg",
    "log-classified-report-day-lb",
    "log-classified-report-day-om",
    "log-classified-report-day-qa",
    "log-classified-report-day-sa"
]

# Iterate over Elasticsearch indices
for index in indices:
    logging.info(f"Processing index: {index}")
    
    base_url = f'https://elasticsearch1.waseet.net/{index}/_search'
    search_after_value = None  # Reset pagination for each index
    
    while True:
        # Build query payload
        json_data = {
            'size': page_size,
            'query': {
                'bool': {
                    'filter': [
                        {'range': {'created_at': {'gte': '2024-10-01T00:00:00.230Z', 'lte': '2025-01-25T00:00:00.230Z'}}}
                    ]
                }
            },
            'sort': [{'created_at': 'asc'}, {'entity_id': 'asc'}],  # Sort order for pagination
        }

        # Add search_after for subsequent pages
        if search_after_value:
            json_data['search_after'] = search_after_value

        # Fetch data from Elasticsearch
        response = requests.get(base_url, headers=headers, json=json_data, auth=auth)
        
        # Check response status
        if response.status_code != 200:
            logging.error(f"Error for index {index}: {response.status_code} - {response.text}")
            break

        data = response.json()
        hits = data['hits']['hits']
        
        # Break if no more hits are returned
        if not hits:
            logging.info(f"No more records to process for index: {index}")
            break

        # Process hits
        rows_to_insert = []
        for hit in hits:
            source = hit['_source']
            main_taxonomy_title = source.get('main_taxonomy_title', {})
            main_taxonomy_title_en = ''
            main_taxonomy_title_ar = ''

            if isinstance(main_taxonomy_title, dict):
                main_taxonomy_title_en = main_taxonomy_title.get('en', '')
                main_taxonomy_title_ar = main_taxonomy_title.get('ar', '')
            elif isinstance(main_taxonomy_title, list):
                main_taxonomy_title_en = ', '.join(item.get('en', '') for item in main_taxonomy_title if isinstance(item, dict))
                main_taxonomy_title_ar = ', '.join(item.get('ar', '') for item in main_taxonomy_title if isinstance(item, dict))

            breadcrumb_title = source.get('breadcrumb_title', {})
            breadcrumb_title_english = breadcrumb_title.get('en', '') if isinstance(breadcrumb_title, dict) else ''
            breadcrumb_title_arabic = breadcrumb_title.get('ar', '') if isinstance(breadcrumb_title, dict) else ''

            rows_to_insert.append((
                source.get('entity_id'),
                source.get('entity_title'),
                ', '.join(map(str, source.get('breadcrumb', []))),
                breadcrumb_title_english,
                breadcrumb_title_arabic,
                source.get('call_android_count'),
                source.get('call_huawei_count'),
                source.get('call_ios_count'),
                source.get('call_web_count'),
                source.get('chat_android_count'),
                source.get('chat_huawei_count'),
                source.get('chat_ios_count'),
                source.get('chat_web_count'),
                source.get('country_alias'),
                source.get('created_at'),
                source.get('day'),
                source.get('entity_type'),
                source.get('is_expired'),
                source.get('is_free'),
                source.get('is_paid'),
                source.get('is_sticky'),
                source.get('main_taxonomy'),
                main_taxonomy_title_en,
                main_taxonomy_title_ar,
                source.get('owner_phone'),
                source.get('updated_at'),
                source.get('visit_android_count'),
                source.get('visit_huawei_count'),
                source.get('visit_ios_count'),
                source.get('visit_web_count'),
                source.get('whatsapp_android_count'),
                source.get('whatsapp_huawei_count'),
                source.get('whatsapp_ios_count'),
                source.get('whatsapp_web_count')
            ))

        # Bulk insert into Snowflake
        insert_query = """
        INSERT INTO CLASSIFIED_LOG_REPORT_ALL_ELS (
            EntityID, EntityTitle, Breadcrumb, BreadcrumbTitleEnglish, BreadcrumbTitleArabic,
            CallAndroidCount, CallHuaweiCount, CallIOSCount, CallWebCount, ChatAndroidCount,
            ChatHuaweiCount, ChatIOSCount, ChatWebCount, CountryAlias, CreatedAt, Day,
            EntityType, IsExpired, IsFree, IsPaid, IsSticky, MainTaxonomy, MainTaxonomyTitleEnglish,
            MainTaxonomyTitleArabic, OwnerPhone, UpdatedAt, VisitAndroidCount, VisitHuaweiCount,
            VisitIOSCount, VisitWebCount, WhatsappAndroidCount, WhatsappHuaweiCount,
            WhatsappIOSCount, WhatsappWebCount
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s
        )
        """
        cursor.executemany(insert_query, rows_to_insert)
        snowflake_conn.commit()
        logging.info(f"Inserted {len(rows_to_insert)} records into Snowflake for index: {index}.")

        # Set search_after for next iteration
        search_after_value = hits[-1]['sort']
        logging.info(f"Processed {len(hits)} records for index: {index}.")
        
logging.info("All indices processed successfully.")
