import os
import requests
import json
import time
from datetime import datetime
import logging
import snowflake.connector
from dotenv import load_dotenv
import concurrent.futures
# import threading

# Load environment variables
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_URL_CLASSIFIED_KW = os.getenv("ELASTICSEARCH_URL_CLASSIFIED_KW")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASS")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# # Snowflake connection setup
# try:
#     db_connection = snowflake.connector.connect(
#         user=SNOWFLAKE_USER,
#         password=SNOWFLAKE_PASSWORD,
#         account=SNOWFLAKE_ACCOUNT,
#         database=SNOWFLAKE_DATABASE,
#         schema=SNOWFLAKE_SCHEMA,
#         warehouse=SNOWFLAKE_WAREHOUSE,
#     )
#     cursor = db_connection.cursor()
#     logger.info("Snowflake database connection established successfully.")
# except snowflake.connector.Error as e:
#     logger.error(f"Error connecting to Snowflake: {e}")
#     raise

# # Elasticsearch query setup
# headers = {
#     'kbn-xsrf': 'reporting',
#     'Content-Type': 'application/json',
# }
# base_url = ELASTICSEARCH_URL_CLASSIFIED_KW
# auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
# page_size = 10000
# search_after_value = None

# # Create the attributes table if it doesn't exist
# create_attributes_table_query = """
# CREATE TABLE IF NOT EXISTS prod_posts_kw_attributes (
#     EntityID INT,
#     AttributeName STRING,
#     AttributeValue STRING,
#     CreatedAt TIMESTAMP,
#     UpdatedAt TIMESTAMP,
#     ExpiredAt TIMESTAMP,
#     PublishedAt TIMESTAMP,
#     DeletedAt TIMESTAMP,
#     StickyFrom TIMESTAMP,
#     StickyTo TIMESTAMP,
#     RepostedAt TIMESTAMP,
#     RepostCount INT
# );
# """
# try:
#     cursor.execute(create_attributes_table_query)
#     logger.info("Attributes table created successfully (if not already existing).")
# except snowflake.connector.Error as e:
#     logger.error(f"Error creating attributes table: {e}")
#     raise

# # Metadata tracking
# state_file = "last_run_state.txt"
# default_start_date = "2024-01-01T00:00:00Z"

# # Load last processed timestamps
# def load_last_processed_timestamps():
#     try:
#         with open(state_file, "r") as file:
#             data = json.load(file)
#             return data.get("CreatedAt", default_start_date), data.get("UpdatedAt", default_start_date)
#     except FileNotFoundError:
#         return default_start_date, default_start_date

# # Save last processed timestamps
# def save_last_processed_timestamps(last_created_at, last_updated_at):
#     with open(state_file, "w") as file:
#         json.dump({"CreatedAt": last_created_at, "UpdatedAt": last_updated_at}, file)

# last_created_at, last_updated_at = load_last_processed_timestamps()

# # Process and save attributes
# def process_and_save_attributes(entity_id, attributes, timestamps):
#     if isinstance(attributes, dict):
#         for attr_name, attr_value in attributes.items():
#             if isinstance(attr_value, dict):
#                 attr_value = attr_value.get('value')
#             if attr_value is None:
#                 continue
            
#             created_at = timestamps.get("CreatedAt")
#             updated_at = timestamps.get("UpdatedAt")
#             expired_at = timestamps.get("ExpiredAt")
#             published_at = timestamps.get("PublishedAt")
#             deleted_at = timestamps.get("DeletedAt")
#             sticky_from = timestamps.get("StickyFrom")
#             sticky_to = timestamps.get("StickyTo")
#             reposted_at = timestamps.get("RepostedAt")
#             repost_count = timestamps.get("RepostCount")
            
#             try:
#                 cursor.execute("""
#                     INSERT INTO prod_posts_kw_attributes (
#                         EntityID, AttributeName, AttributeValue
#                     )
#                     VALUES (%s, %s, %s)
#                 """, (
#                     entity_id, attr_name, attr_value
#                 ))
#             except snowflake.connector.Error as e:
#                 logger.error(f"Error inserting attribute {attr_name} for entity {entity_id}: {e}")

# # Process Elasticsearch data
# while True:
#     json_data = {
#         "_source": "*",
#         "size": page_size,
#         "query": {
#             "bool": {
#                 "should": [
#                     {"range": {"created_at": {"gte": last_created_at}}},
#                     {
#                         "bool": {
#                             "should": [
#                                 {"range": {"updated_at": {"gte": last_updated_at}}},
#                                 {"exists": {"field": "expired_at"}},
#                                 {"exists": {"field": "published_at"}},
#                                 {"exists": {"field": "deleted_at"}},
#                                 {"exists": {"field": "sticky_from"}},
#                                 {"exists": {"field": "sticky_to"}},
#                                 {"exists": {"field": "reposted_at"}},
#                                 {"exists": {"field": "repost_count"}}
#                             ]
#                         }
#                     }
#                 ]
#             }
#         },
#         "sort": [{"created_at": "asc"}, {"entity_id": "asc"}]
#     }

#     if search_after_value:
#         json_data['search_after'] = search_after_value

#     try:
#         response = requests.get(base_url, headers=headers, json=json_data, auth=auth)
#         response.raise_for_status()
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error querying Elasticsearch: {e}")
#         break

#     data = response.json()
#     hits = data['hits']['hits']

#     if not hits:
#         break

#     for hit in hits:
#         source = hit['_source']
#         entity_id = source.get('entity_id')
#         attributes = source.get('attributes', {})
#         timestamps = {
#             "CreatedAt": source.get("created_at"),
#             "UpdatedAt": source.get("updated_at"),
#             "ExpiredAt": source.get("expired_at"),
#             "PublishedAt": source.get("published_at"),
#             "DeletedAt": source.get("deleted_at"),
#             "StickyFrom": source.get("sticky_from"),
#             "StickyTo": source.get("sticky_to"),
#             "RepostedAt": source.get("reposted_at"),
#             "RepostCount": source.get("repost_count")
#         }

#         process_and_save_attributes(entity_id, attributes, timestamps)

#     db_connection.commit()
#     search_after_value = hits[-1]['sort']
#     last_created_at = hits[-1]['_source']['created_at']
#     last_updated_at = hits[-1]['_source']['updated_at']
#     save_last_processed_timestamps(last_created_at, last_updated_at)

#     logger.info(f"Processed {len(hits)} records.")

# logger.info("Incremental load to (prod_posts_kw_attributes) completed.")




############################################################################



# Snowflake connection setup
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
        )
        logger.info("Snowflake database connection established successfully.")
        return conn
    except snowflake.connector.Error as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

# Get the last date for incremental load
def get_last_load_dates(cursor, country):
    query = """
        SELECT 
            MAX(COALESCE(CreatedAt, '2024-01-01 00:00:00')) AS last_created_at,
            MAX(COALESCE(UpdatedAt, '2024-01-01 00:00:00')) AS last_updated_at,
            MAX(COALESCE(ExpiredAt, '2024-01-01 00:00:00')) AS last_expired_at,
            MAX(COALESCE(PublishedAt, '2024-01-01 00:00:00')) AS last_published_at,
            MAX(COALESCE(DeletedAt, '2024-01-01 00:00:00')) AS last_deleted_at,
            MAX(COALESCE(StickyFrom, '2024-01-01')) AS last_sticky_from,
            MAX(COALESCE(StickyTo, '2024-01-01')) AS last_sticky_to,
            MAX(COALESCE(RepostedAt, '2024-01-01 00:00:00')) AS last_reposted_at
        FROM WASEET.RAW.prod_posts_all_countries_els
        where Country = %s;
    """
    try:
        cursor.execute(query, (country,))
        result = cursor.fetchone()
        print(f'Results: >>>{result}')

        if result:
            return {
                "created_at": result[0].strftime('%Y-%m-%dT%H:%M:%S') if result[0] else "2024-01-01T00:00:00",
                "updated_at": result[1].strftime('%Y-%m-%dT%H:%M:%S') if result[1] else "2024-01-01T00:00:00",
                "expired_at": result[2].strftime('%Y-%m-%dT%H:%M:%S') if result[2] else "2024-01-01T00:00:00",
                "published_at": result[3].strftime('%Y-%m-%dT%H:%M:%S') if result[3] else "2024-01-01T00:00:00",
                "deleted_at": result[4].strftime('%Y-%m-%dT%H:%M:%S') if result[4] else "2024-01-01T00:00:00",
                "sticky_from": result[5].strftime('%Y-%m-%d') if result[5] else "2024-01-01",
                "sticky_to": result[6].strftime('%Y-%m-%d') if result[6] else "2024-01-01",
                "reposted_at": result[7].strftime('%Y-%m-%dT%H:%M:%S') if result[7] else "2024-01-01T00:00:00"
            }
    except Exception as e:
        print(f"Error fetching last load dates: {e}")
        return {
            "created_at": "2024-10-01T00:00:00",
            "updated_at": "2024-10-01T00:00:00",
            "expired_at": "2024-10-01T00:00:00",
            "published_at": "2024-10-01T00:00:00",
            "deleted_at": "2024-10-01T00:00:00",
            "sticky_from": "2024-10-01",
            "sticky_to": "2024-10-01",
            "reposted_at": "2024-10-01T00:00:00"
        }

# Function to process nested fields
def process_nested_field(field_data, name_key='name', slug_key='slug'):
    if isinstance(field_data, dict):
        # Convert datetime objects to strings
        for key, value in field_data.items():
            if isinstance(value, datetime):
                field_data[key] = value.isoformat()

        name = field_data.get(name_key, {})
        if isinstance(name, dict):
            name_value = f"{name.get('ar', '')} ({name.get('en', '')})"
        else:
            name_value = ''
        slug_value = field_data.get(slug_key, '')
        return json.dumps(field_data), name_value, slug_value
    return '{}', '', ''

# Implement retry logic for the database query to handle transient locking issues
def execute_with_retry(func, retries=3, delay=5, *args, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                raise  # Re-raise the exception if out of retries

# Function to map index name to country
def get_country_from_index(index_name):
    country_mapping = {
        # "prod-posts-kw": "Kuwait"
        "prod-posts-ae": "UAE",
        "prod-posts-bh": "Bahrain",
        "prod-posts-eg": "Egypt",
        "prod-posts-lb": "Lebanon",
        "prod-posts-om": "Oman",
        "prod-posts-qa": "Qatar",
        "prod-posts-sa": "Saudi Arabia"
    }
    return country_mapping.get(index_name, "Unknown")

# Elasticsearch indices to process
indices = [
    # "prod-posts-kw", 
    "prod-posts-ae", 
    "prod-posts-bh", 
    "prod-posts-eg", "prod-posts-lb", "prod-posts-om", 
    "prod-posts-qa", "prod-posts-sa"
]

# Elasticsearch query setup
headers = {
    'kbn-xsrf': 'reporting',
    'Content-Type': 'application/json',
}

base_url = ELASTICSEARCH_URL
auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
page_size = 10000

# Incremental Load Logic
def incremental_load():
    try:
        for index in indices:
            country = get_country_from_index(index)
            logger.info(f"Processing index: {index} for country: {country}")
            index_url = f"{base_url}/{index}/_search"
            print(index_url)

            db_connection = get_snowflake_connection()
            cursor = db_connection.cursor()

            # Fetch last load dates
            last_dates = get_last_load_dates(cursor, country)

            # Build Elasticsearch query
            json_data_ = {
                "_source": {
                    "excludes": ["featured_image", "description_en", "description_ar"]
                },
                "size": page_size,
                "query": {
                    "bool": {
                        "should": [
                            {"range": {"created_at": {"gte": last_dates["created_at"]}}},
                            {"range": {"updated_at": {"gte": last_dates["updated_at"]}}},
                            {"range": {"expired_at": {"gte": last_dates["expired_at"]}}},
                            {"range": {"published_at": {"gte": last_dates["published_at"]}}},
                            {"range": {"deleted_at": {"gte": last_dates["deleted_at"]}}},
                            {"range": {"sticky_from": {"gte": last_dates["sticky_from"]}}},
                            {"range": {"sticky_to": {"gte": last_dates["sticky_to"]}}},
                            {"range": {"reposted_at": {"gte": last_dates["reposted_at"]}}}
                        ]
                    }
                },
                "sort": [{"created_at": "asc"}, {"entity_id": "asc"}]
            }

            # Log the query
            print("Elasticsearch query:", json.dumps(json_data_, indent=4))

            # Use ThreadPoolExecutor for parallel processing
            # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            #     futures = []
            search_after_value = None

            # Process Elasticsearch data and load into Snowflake
            while True:
                if search_after_value:
                    json_data_["search_after"] = search_after_value

                try:
                    response = requests.get(index_url, headers=headers, json=json_data_, auth=auth)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error querying Elasticsearch: {e}")
                    break

                data = response.json()
                hits = data['hits']['hits']

                if not hits:
                    break

                # Submit tasks to the thread pool
                for hit in hits:
                    # futures.append(executor.submit(process_record, hit, cursor, db_connection, country))
                    try:
                        process_record(hit, cursor, db_connection, country)
                    except Exception as e:
                        logger.error(f"Error processing record: {e}")


                search_after_value = hits[-1]['sort']
                logger.info(f"Fetching page after: {search_after_value}")
                logger.info(f"Processed {len(hits)} records.")

                # Wait for all tasks to complete
                # concurrent.futures.wait(futures)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        db_connection.close()
        logger.info("Database connection closed.")

# Function to process a single record
def process_record(hit, cursor, db_connection, country):
    try:
        source = hit['_source']

        # Extract and process fields
        entity_id = source.get('entity_id')
        title_ar = source.get('title_ar')
        title_en = source.get('title_en')
        areas = ', '.join([f"{area['name']['ar']} ({area['name']['en']})" for area in source.get('areas', [])])
        cities = ', '.join([f"{city['name']['ar']} ({city['name']['en']})" for city in source.get('cities', [])])
        breadcrumb = ', '.join([f"{b['name']['ar']} ({b['name']['en']})" for b in source.get('breadcrumb', [])])
        price = source.get('price')
        is_free = source.get('is_free')
        is_negotiable = source.get('is_negotiable')
        created_at = source.get('created_at')
        updated_at = source.get('updated_at')
        rejected_at = source.get('rejected_at')
        rejected_reason_id = source.get('rejected_reason_id')
        approved_at = source.get('approved_at')
        approved_by = source.get('approved_by')
        expired_at = source.get('expired_at')
        username = source.get('username')
        phone_number = source.get('phone_number')
        is_approved = source.get('is_approved')
        is_enabled_call = source.get('is_enabled_call')
        is_enabled_chat = source.get('is_enabled_chat')
        # latitude = source.get('latitude')
        # longitude = source.get('longitude')
        media_count = source.get('media_count')
        platform = source.get('platform')
        published_at = source.get('published_at')
        deleted_at = source.get('deleted_at')
        sticky_from = source.get('sticky_from')
        sticky_to = source.get('sticky_to')
        slug = source.get('slug')
        # collection = source.get('collection')
        breadcrumb_data, breadcrumb_name, breadcrumb_slug = process_nested_field(source.get('breadcrumb', [])[0] if source.get('breadcrumb') else {})
        config_category_attribute_name, config_category_attribute_slug = process_nested_field(source.get('config_category_attribute'))[1:]
        leaf_category_name, leaf_category_slug = process_nested_field(source.get('leaf_category'))[1:]
        handled_by = source.get('handled_by')
        has_map = source.get('has_map')
        is_auto_approved = source.get('is_auto_approved')
        is_images_reviewed = source.get('is_images_reviewed')
        is_price_hidden = source.get('is_price_hidden')
        is_spam = source.get('is_spam')
        is_sticky = source.get('is_sticky')
        is_videos_reviewed = source.get('is_videos_reviewed')
        repost_count = source.get('repost_count')
        reposted_at = source.get('reposted_at')

        # Check if the record already exists and whether it needs updating
        select_query_updated = f"""
            SELECT CreatedAt, UpdatedAt, ExpiredAt, PublishedAt, DeletedAt, StickyFrom, StickyTo, RepostedAt 
            FROM WASEET.RAW.prod_posts_all_countries_els
            WHERE EntityID = %s
        """
        logging.debug(f"Executing query: {select_query_updated} with EntityID: {entity_id}")
        cursor.execute(select_query_updated, (entity_id,))
        existing_record_updated = cursor.fetchone()
        logging.debug(f"Query result: {existing_record_updated}")

        # Function to wrap cursor execution
        def execute_query(cursor, query, params):
            def query_execution():
                cursor.execute(query, params)
            execute_with_retry(query_execution)

        select_query_insert = """
            SELECT EntityID FROM WASEET.RAW.prod_posts_all_countries_els WHERE EntityID = %s
        """
        cursor.execute(select_query_insert, (entity_id,))
        existing_record_inserted = cursor.fetchone()

        changes_needed = False
        if existing_record_updated:
            datetime_fields = [
                ("UpdatedAt", updated_at),
                ("ExpiredAt", expired_at),
                ("PublishedAt", published_at),
                ("DeletedAt", deleted_at),
                ("StickyFrom", sticky_from),
                ("StickyTo", sticky_to),
                ("RepostedAt", reposted_at)
            ]

            for index, (field_name, input_value) in enumerate(datetime_fields):
                print(f"This Index: {index} for {entity_id}, Field Name is {field_name} AND Input value is {input_value}")
                
                if input_value:  # Only process if input_value is not None
                    try:
                        input_datetime = datetime.strptime(input_value, "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            # If the first format fails, try the second format
                            input_datetime = datetime.strptime(input_value, "%Y-%m-%d")
                        except ValueError:
                            logging.error(f"Invalid datetime format for field {field_name}: {input_value}")
                            continue  # Skip this iteration if input_value cannot be parsed

                    db_value = existing_record_updated[index]
                    if db_value is None:
                        db_datetime = None
                    else:
                        db_datetime = db_value if isinstance(db_value, datetime) else datetime.strptime(db_value, "%Y-%m-%d %H:%M:%S")

                    if db_datetime != input_datetime:
                        logging.info(f"Field {field_name} has changed. Database: {db_datetime}, Input: {input_datetime}")
                        changes_needed = True 

            if changes_needed:
                update_query = """
                    UPDATE WASEET.RAW.prod_posts_all_countries_els
                    SET 
                        TitleArabic = %s, TitleEnglish = %s, Areas = %s, Cities = %s, Price = %s, 
                        IsFree = %s, IsNegotiable = %s, CreatedAt = %s, UpdatedAt = %s, RejectedAt = %s, RejectedReasonID = %s, 
                        ApprovedAt = %s, ApprovedBy = %s, ExpiredAt = %s, UserName = %s, PhoneNumber = %s, IsApproved = %s,
                        IsEnabledCall = %s, IsEnabledChat = %s, MediaCount = %s, Platform = %s,
                        PublishedAt = %s, DeletedAt = %s, StickyFrom = %s, StickyTo = %s, Slug = %s, Breadcrumb = %s, MainCategory = %s, MainCategory_Slug = %s, 
                        SubCategory = %s, SubCategory_Slug = %s, Extra_SubCategory = %s, 
                        Extra_SubCategory_Slug = %s, HandledBy = %s, HasMap = %s, IsAutoApproved = %s, IsImagesReviewed = %s, IsPriceHidden = %s, 
                        IsSpam = %s, IsSticky = %s, IsVideosReviewed = %s, RepostCount = %s, RepostedAt = %s
                    WHERE EntityID = %s
                """
                execute_query(cursor, update_query, (
                        title_ar, title_en, areas, cities, price, is_free, is_negotiable, 
                        created_at, updated_at, rejected_at, rejected_reason_id, approved_at, 
                        approved_by, expired_at, username, phone_number, is_approved, 
                        is_enabled_call, is_enabled_chat, media_count, platform, 
                        published_at, deleted_at, sticky_from, sticky_to, slug,
                        breadcrumb, breadcrumb_name, breadcrumb_slug, config_category_attribute_name, 
                        config_category_attribute_slug, leaf_category_name, leaf_category_slug, handled_by, 
                        has_map, is_auto_approved, is_images_reviewed, is_price_hidden, is_spam, is_sticky, 
                        is_videos_reviewed, repost_count, reposted_at, entity_id))
                logger.info(f"Record with EntityID {entity_id} updated.")
        else:
            logging.info("Record is up-to-date.")
            if not existing_record_inserted:
                print(f"why is this not: {existing_record_inserted}")
                insert_query = """
                    INSERT INTO WASEET.RAW.prod_posts_all_countries_els (
                        EntityID, TitleArabic, TitleEnglish, Areas, Cities, Price, 
                        IsFree, IsNegotiable, CreatedAt, UpdatedAt, RejectedAt, RejectedReasonID, 
                        ApprovedAt, ApprovedBy, ExpiredAt, UserName, PhoneNumber, IsApproved,
                        IsEnabledCall, IsEnabledChat, MediaCount, Platform,
                        PublishedAt, DeletedAt, StickyFrom, StickyTo, Slug, Breadcrumb, MainCategory, MainCategory_Slug, 
                        SubCategory, SubCategory_Slug, Extra_SubCategory, 
                        Extra_SubCategory_Slug, HandledBy, HasMap, IsAutoApproved, IsImagesReviewed, IsPriceHidden, 
                        IsSpam, IsSticky, IsVideosReviewed, RepostCount, RepostedAt, Country
                    ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                execute_query(cursor, insert_query, (
                    entity_id, title_ar, title_en, areas, cities, price,
                    is_free, is_negotiable, created_at, updated_at, rejected_at, rejected_reason_id,
                    approved_at, approved_by, expired_at, username, phone_number, is_approved,
                    is_enabled_call, is_enabled_chat, media_count, platform,
                    published_at, deleted_at, sticky_from, sticky_to, slug, breadcrumb, breadcrumb_name, breadcrumb_slug,
                    config_category_attribute_name, config_category_attribute_slug,
                    leaf_category_name, leaf_category_slug, handled_by, has_map, is_auto_approved, 
                    is_images_reviewed, is_price_hidden, is_spam, is_sticky, is_videos_reviewed, 
                    repost_count, reposted_at, country
                ))
                logger.info(f"New record with EntityID {entity_id} inserted.")

        db_connection.commit()
    except Exception as e:
        logger.error(f"Error processing record {hit}: {e}")

if __name__ == "__main__":
    incremental_load()
