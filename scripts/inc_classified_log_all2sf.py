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

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Snowflake connection
try:
    snowflake_conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    logging.info("Connected to Snowflake successfully.")
except Exception as e:
    logging.error(f"Failed to connect to Snowflake: {e}")
    raise

cursor = snowflake_conn.cursor()

# List of Elasticsearch indices
indices = [
    "log-classified-report-day-ae",
    "log-classified-report-day-bh",
    "log-classified-report-day-eg",
    "log-classified-report-day-lb",
    "log-classified-report-day-om",
    "log-classified-report-day-qa",
    "log-classified-report-day-sa"
]

# Loop through each index
for index in indices:
    logging.info(f"Processing index: {index}")

    country_alias = index.split("-")[-1]
    # Get the last day from Snowflake
    try:
        cursor.execute(f"SELECT MAX(createdat) FROM CLASSIFIED_LOG_REPORT_ALL_ELS WHERE COUNTRYALIAS = '{country_alias}'")
        result = cursor.fetchone()
        last_day_in_db = result[0] if result and result[0] else '2024-10-01T00:00:00.230Z'
        logging.info(f"Last day in Snowflake database for index {index} (country: {country_alias}): {last_day_in_db}")
    except Exception as e:
        logging.error(f"Failed to retrieve the last day from Snowflake for index {index} (country: {country_alias}): {e}")
        raise

    if isinstance(last_day_in_db, datetime):
        last_day_in_db = last_day_in_db.isoformat()

    headers = {
        'kbn-xsrf': 'reporting',
        'Content-Type': 'application/json',
    }
    base_url = f'https://elasticsearch1.waseet.net/{index}/_search'
    auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
    page_size = 10000
    search_after_value = None
    max_updated_at = last_day_in_db if last_day_in_db else datetime.min

    while True:
        json_data = {
            'size': page_size,
            'query': {
                'bool': {
                    'filter': [
                        {'range': {'created_at': {'gte': last_day_in_db}}}
                    ]
                }
            },
            'sort': [{'created_at': 'asc'}, {'entity_id': 'asc'}],
        }
        if search_after_value:
            json_data['search_after'] = search_after_value

        response = requests.get(base_url, headers=headers, json=json_data, auth=auth)

        if response.status_code != 200:
            logging.error(f"Error: {response.status_code} - {response.text}")
            break

        data = response.json()
        hits = data['hits']['hits']
        if not hits:
            break

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

            rows_to_insert.append((
                source.get('entity_id'),
                source.get('entity_title'),
                ', '.join(map(str, source.get('breadcrumb', []))),
                source.get('breadcrumb_title', {}).get('en', ''),
                source.get('breadcrumb_title', {}).get('ar', ''),
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
        logging.info(f"Inserted {len(rows_to_insert)} records into Snowflake for index {index}.")

        search_after_value = hits[-1]['sort']

    if max_updated_at:
        cursor.execute("""
            MERGE INTO etl_metadata AS target
            USING (SELECT %s AS etl_name, %s AS last_updated_at) AS source
            ON target.etl_name = source.etl_name
            WHEN MATCHED THEN UPDATE SET target.last_updated_at = source.last_updated_at
            WHEN NOT MATCHED THEN INSERT (etl_name, last_updated_at) VALUES (source.etl_name, source.last_updated_at)
        """, (f'classified_log_report_all_els_{index}', max_updated_at))
        snowflake_conn.commit()
        logging.info(f"Updated metadata table for index {index}.")

# Close connection
cursor.close()
snowflake_conn.close()
logging.info("Snowflake connection closed.")
