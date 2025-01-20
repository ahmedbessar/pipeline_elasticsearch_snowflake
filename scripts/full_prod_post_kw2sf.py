import os
import requests
import json
import datetime
import logging
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
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_URL_CLASSIFIED_KW = os.getenv("ELASTICSEARCH_URL_CLASSIFIED_KW")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASS")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Snowflake connection setup
try:
    db_connection = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    cursor = db_connection.cursor()
    logger.info("Snowflake database connection established successfully.")
except snowflake.connector.Error as e:
    logger.error(f"Error connecting to Snowflake: {e}")
    raise

# # Elasticsearch query setup
# headers = {
#     'kbn-xsrf': 'reporting',
#     'Content-Type': 'application/json',
# }

# base_url = ELASTICSEARCH_URL_CLASSIFIED_KW
# auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
# page_size = 10000
# search_after_value = None

## Step 1: (attributes field) This structure ensures that all nested fields in attributes are stored dynamically, 
# regardless of their depth or key names.

# # Create the attributes table if it doesn't exist
# create_attributes_table_query = """
# CREATE TABLE IF NOT EXISTS prod_posts_kw_attributes (
#     EntityID INT,
#     AttributeName STRING,
#     AttributeValue STRING
# );
# """
# try:
#     cursor.execute(create_attributes_table_query)
#     logger.info("Attributes table created successfully (if not already existing).")
# except snowflake.connector.Error as e:
#     logger.error(f"Error creating attributes table: {e}")
#     raise

# # Function to process attributes and insert into the new table
# def process_and_save_attributes(entity_id, attributes):
#     if isinstance(attributes, dict):
#         for attr_name, attr_value in attributes.items():
#             # Check if 'value' key exists and extract it
#             if isinstance(attr_value, dict):
#                 attr_value = attr_value.get('value', None)  # Extract only the 'value' part
            
#             # If 'value' is still None, continue to the next iteration
#             if attr_value is None:
#                 continue
            
#             try:
#                 cursor.execute("""
#                     INSERT INTO prod_posts_kw_attributes (EntityID, AttributeName, AttributeValue)
#                     VALUES (%s, %s, %s)
#                 """, (entity_id, attr_name, attr_value))
#             except snowflake.connector.Error as e:
#                 logger.error(f"Error inserting attribute {attr_name} for entity {entity_id}: {e}")
#                 continue

# today = datetime.datetime.today().strftime('%Y-%m-%d')
# # Process Elasticsearch data
# while True:
#     # Elasticsearch query
#     # json_data = {
#     #     "_source": "*",
#     #     "size": page_size,
#     #     "query": {
#     #         "match_all": {}
#     #     },
#     #     "sort": [{"created_at": "asc"}, {"entity_id": "asc"}]
#     # }

#     json_data = {
#         "_source": "*",
#         "size": page_size,
#         "query": {
#             "bool": {
#                 "filter": {
#                     "range": {
#                         "created_at": {
#                             "gte": f"{today}T00:00:00.000Z",  # Greater than or equal to midnight today
#                             "lt": f"{today}T23:59:59.999Z"   # Less than the end of today
#                         }
#                     }
#                 }
#             }
#         },
#         "sort": [{"created_at": "asc"}, {"entity_id": "asc"}]
#     }

#     if search_after_value:
#         json_data['search_after'] = search_after_value

#     try:
#         response = requests.get(base_url, headers=headers, json=json_data, auth=auth)
#         response.raise_for_status()  # Will raise HTTPError for bad responses
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error querying Elasticsearch: {e}")
#         break

#     data = response.json()
#     hits = data['hits']['hits']

#     if not hits:
#         break

#     # Process each hit (record) one by one
#     for hit in hits:
#         source = hit['_source']
#         entity_id = source.get('entity_id')
#         attributes = source.get('attributes', {})

#         # Process and save attributes
#         process_and_save_attributes(entity_id, attributes)

#     # Commit after processing the entire batch of attributes
#     db_connection.commit()

#     # Update the search_after_value for pagination
#     search_after_value = hits[-1]['sort']  # For pagination

#     logger.info(f"Processed {len(hits)} records for attributes.")



## Step 2: (prod_posts_kw_els) table, This ensures all nested fields are extracted and stored properly in the database. 
# Create table for Elasticsearch data if it does not exist
create_table_query = f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_SCHEMA}.PROD_POSTS_KW_ELS (
    EntityID INT PRIMARY KEY,
    TitleArabic STRING,
    TitleEnglish STRING,
    Areas STRING,
    Cities STRING,
    Price FLOAT,
    IsFree BOOLEAN,
    IsNegotiable BOOLEAN,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP,
    RejectedAt TIMESTAMP,
    RejectedReasonID INT,
    ApprovedAt TIMESTAMP,
    ApprovedBy STRING,
    ExpiredAt TIMESTAMP,
    UserName STRING,
    PhoneNumber STRING,
    IsApproved BOOLEAN,
    IsEnabledCall BOOLEAN,
    IsEnabledChat BOOLEAN,
    MediaCount INT,
    Platform STRING,
    PublishedAt TIMESTAMP,
    DeletedAt TIMESTAMP,
    StickyFrom TIMESTAMP,
    StickyTo TIMESTAMP,
    Slug STRING,
    Breadcrumb STRING,
    MainCategory STRING,
    MainCategory_Slug STRING,
    SubCategory STRING,
    SubCategory_Slug STRING,
    Extra_SubCategory STRING,
    Extra_SubCategory_Slug STRING,
    HandledBy STRING,
    HasMap BOOLEAN,
    IsAutoApproved BOOLEAN,
    IsImagesReviewed BOOLEAN,
    IsPriceHidden BOOLEAN,
    IsSpam BOOLEAN,
    IsSticky BOOLEAN,
    IsVideosReviewed BOOLEAN,
    RepostCount INT,
    RepostedAt TIMESTAMP,
    Country STRING
)
"""
try:
    cursor.execute(create_table_query)
    logger.info("Elasticsearch data table created successfully (or replaced if already existing).")
except snowflake.connector.Error as e:
    logger.error(f"Error creating Elasticsearch data table: {e}")
    raise

# Function to process nested fields
def process_nested_field(field_data, name_key='name', slug_key='slug'):
    if isinstance(field_data, dict):
        name = field_data.get(name_key, {})
        if isinstance(name, dict):
            name_value = f"{name.get('ar', '')} ({name.get('en', '')})"
        else:
            name_value = ''
        slug_value = field_data.get(slug_key, '')
        return json.dumps(field_data), name_value, slug_value
    return '{}', '', ''

# Function to map index name to country
def get_country_from_index(index_name):
    country_mapping = {
        "prod-posts-kw": "Kuwait"
        # "prod-posts-ae": "UAE",
        # "prod-posts-bh": "Bahrain",
        # "prod-posts-eg": "Egypt",
        # "prod-posts-lb": "Lebanon",
        # "prod-posts-om": "Oman",
        # "prod-posts-qa": "Qatar",
        # "prod-posts-sa": "Saudi Arabia"
    }
    return country_mapping.get(index_name, "Unknown")

# Elasticsearch indices to process
indices = [
    "prod-posts-kw"
    # "prod-posts-ae", "prod-posts-bh", 
    # "prod-posts-eg", "prod-posts-lb", "prod-posts-om", 
    # "prod-posts-qa", "prod-posts-sa"
]

# Elasticsearch query setup
headers = {
    'kbn-xsrf': 'reporting',
    'Content-Type': 'application/json',
}

base_url = ELASTICSEARCH_URL
auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
page_size = 10000


# Process Elasticsearch data and insert into the database
try:
    for index in indices:
        country = get_country_from_index(index)
        logger.info(f"Processing index: {index} for country: {country}")
        index_url = f"{base_url}/{index}/_search"
        search_after_value = None
        print(index_url)
        # Process Elasticsearch data and load into Snowflake
        while True:
            json_data_ = {
                "_source": "*",
                "size": page_size,
                "query": {
                    "bool": {
                        "should": [
                            {
                                "range": {
                                    "created_at": {
                                        "gte": "now-3M/M",
                                        "lt": "now"
                                    }
                                }
                            },
                            {
                                "range": {
                                    "updated_at": {
                                        "gte": "now-3M/M",
                                        "lt": "now"
                                    }
                                }
                            },
                            {
                                "range": {
                                    "reposted_at": {
                                        "gte": "now-3M/M",
                                        "lt": "now"
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
                "sort": [
                    {"created_at": "asc"},
                    {"entity_id": "asc"}
                ]
            }

            if search_after_value:
                json_data_['search_after'] = search_after_value

            try:
                response = requests.get(index_url, headers=headers, json=json_data_, auth=auth)
                response.raise_for_status()  # Will raise HTTPError for bad responses
                data = response.json()
                hits = data["hits"]["hits"]
            except requests.exceptions.RequestException as e:
                logger.error(f"Error querying Elasticsearch: {e}")
                break

            # data = response.json()
            # hits = data['hits']['hits']

            if not hits:
                logger.info(f"No more hits for index {index}.")
                break

            for hit in hits:
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

                # Insert into Snowflake
                try:
                    insert_query = """
                        INSERT INTO PROD_POSTS_KW_ELS (
                            EntityID, TitleArabic, TitleEnglish, Areas, Cities, Price, 
                            IsFree, IsNegotiable, CreatedAt, UpdatedAt, RejectedAt, RejectedReasonID, 
                            ApprovedAt, ApprovedBy, ExpiredAt, UserName, PhoneNumber, IsApproved,
                            IsEnabledCall, IsEnabledChat, MediaCount, Platform,
                            PublishedAt, DeletedAt, StickyFrom, StickyTo, Slug, Breadcrumb, MainCategory, MainCategory_Slug, 
                            SubCategory, SubCategory_Slug, Extra_SubCategory, 
                            Extra_SubCategory_Slug, HandledBy, HasMap, IsAutoApproved, IsImagesReviewed, IsPriceHidden, 
                            IsSpam, IsSticky, IsVideosReviewed, RepostCount, RepostedAt, Country
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
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
                except snowflake.connector.errors.ProgrammingError as e:
                    logger.error(f"Database insert error: {e}")
                    db_connection.rollback()
                else:
                    db_connection.commit()
            
            search_after_value = hits[-1]['sort']  # For pagination
            logger.info(f"Fetching page after: {search_after_value}")
            logger.info(f"Processed {len(hits)} records.")

except Exception as e:
    logger.error(f"Unexpected error: {e}")
finally:
    db_connection.close()
    logger.info("Database connection closed.")