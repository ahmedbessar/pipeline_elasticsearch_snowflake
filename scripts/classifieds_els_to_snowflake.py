import requests
import json
import snowflake.connector
import re
from datetime import datetime
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

# Load JSON data for taxonomy
category_tree_file = 'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/scripts/category_treeV2.json'

with open(category_tree_file, 'r', encoding='utf-8') as file:
    category_tree_data  = json.load(file)

# SQL table creation for category_hierarchy
create_category_hierarchy_table_query = """
CREATE OR REPLACE TABLE category_hierarchy (
    ID INT PRIMARY KEY,
    ParentID INT,
    CategoryNameEn STRING,
    CategoryNameAr STRING,
    ParentNameEn STRING,
    ParentNameAr STRING
);
"""

cursor.execute(create_category_hierarchy_table_query)
snowflake_conn.commit()

# Helper function to build category hierarchy
def build_category_hierarchy(tree):
    category_hierarchy = []

    def process_node(node, parent=None):
        # Append current node with its parent's details
        category_hierarchy.append({
            'ID': node['id'],
            'ParentID': node['parent_id'],
            'CategoryNameEn': node['name']['en'],
            'CategoryNameAr': node['name']['ar'],
            'ParentNameEn': parent['name']['en'] if parent else None,
            'ParentNameAr': parent['name']['ar'] if parent else None
        })

        # Process children recursively
        for child in node.get('children', []):
            process_node(child, node)

    # Start processing from the root nodes
    for root_node in tree:
        process_node(root_node)

    return category_hierarchy

# Build category hierarchy from category_tree.json
category_hierarchy_results = build_category_hierarchy(category_tree_data)

# Insert category hierarchy into the database
insert_category_hierarchy_query = """
INSERT INTO category_hierarchy (ID, ParentID, CategoryNameEn, CategoryNameAr, ParentNameEn, ParentNameAr)
VALUES (%s, %s, %s, %s, %s, %s)
"""
for row in category_hierarchy_results:
    cursor.execute(
        insert_category_hierarchy_query,
        (row['ID'], row['ParentID'], row['CategoryNameEn'], row['CategoryNameAr'], row['ParentNameEn'], row['ParentNameAr'])
    )

snowflake_conn.commit()
print("category_hierarchy data inserted into the database.")


# SQL table creation
create_table_query = """
CREATE OR REPLACE TABLE classifieds (
    country STRING,
    agent STRING,
    has_images BOOLEAN,
    nid INT,
    negotiable BOOLEAN,
    is_muted BOOLEAN,
    global_type STRING,
    approved BOOLEAN,
    price FLOAT,
    translated_title STRING,
    id INT,
    paid_for_taxonomy_id INT,
    created TIMESTAMP,
    created_by STRING,
    phone_number STRING,
    status STRING,
    handled_by STRING,
    request_type STRING,
    city STRING,
    rejected BOOLEAN,
    sub_category STRING,
    enabled BOOLEAN,
    ad_source STRING,
    advertisement_type STRING,
    bundle STRING,
    leaf_taxonomy_id INT,
    taxonomy_status STRING,
    data_type STRING,
    paid FLOAT,
    category STRING,
    username STRING,
    approved_date TIMESTAMP,
    original_creation TIMESTAMP,
    sticky_payment_start_time TIMESTAMP,
    sticky_expiry TIMESTAMP
);
"""
cursor.execute(create_table_query)
snowflake_conn.commit()


# Initialize an empty list to hold all results
all_results = []
page_size = 100  # Number of records per page
search_after_value = None

# Helper functions
def convert_unix_to_datetime(unix_timestamp):
    try:
        return datetime.fromtimestamp(int(unix_timestamp)) if unix_timestamp and str(unix_timestamp).isdigit() else None
    except (ValueError, TypeError):
        return None

def extract_numeric_id(id_value):
    numeric_part = re.search(r'\d+', id_value)
    return int(numeric_part.group()) if numeric_part else None

# Insert data into SQL Server
insert_query = """
INSERT INTO classifieds (
    country, agent, has_images, nid, negotiable, is_muted, global_type, approved, price,
    translated_title, id, paid_for_taxonomy_id, created, created_by, phone_number, status,
    handled_by, request_type, city, rejected, sub_category, enabled, ad_source,
    advertisement_type, bundle, leaf_taxonomy_id, taxonomy_status, data_type, paid, category,
    username, approved_date, original_creation, sticky_payment_start_time, sticky_expiry
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

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
                    {"range": {"created": {
                        "gte": 1722470400,  # August 1, 2024, 00:00:00 UTC
                        "lte": 1730419200    # November 1, 2024, 00:00:00 UTC (exclusive) 1730419200
                        }}}
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
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    hits = data['hits']['hits']
    if not hits:
        break  # Stop if there are no more hits

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
        
        # Construct the extracted_data list with the corrected values
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
        
        # Execute the insertion query
        cursor.execute(insert_query, extracted_data)


        # # Commit every page to avoid data loss
        # snowflake_conn.commit()

    # Set search_after for the next batch
    search_after_value = hits[-1]["sort"]
    # print(f"Fetched {len(hits)} records. Total fetched so far: {len(all_results)}")
    all_results.extend(hits)  # Accumulate results
    print(f"Processed {len(hits)} records. Continuing...")

print("Data migration to the database is complete.")



# Load JSON data for taxonomy
category_tree_file = 'category_treeV2.json' 
kw_file = 'kw.json'

with open(category_tree_file, 'r', encoding='utf-8') as file:
    category_tree_data  = json.load(file)

with open(kw_file, 'r', encoding='utf-8') as f:
    kw_data = json.load(f)

# SQL table creation for taxonomy_tree
create_taxonomy_table_query = """
CREATE OR REPLACE TABLE taxonomy_tree (
    ParentID INT,
    ID INT,
    CategoryNameEn STRING,
    CategoryNameAr STRING
);
"""

cursor.execute(create_taxonomy_table_query)
snowflake_conn.commit()

# Helper function to search for children by parent_id
def find_children_by_parent_id(tree, target_ids):
    results = []
    for node in tree:
        if node['parent_id'] in target_ids:
            results.append({
                'Parent ID': node['parent_id'],
                'Child ID': node['id'],
                'Child Name (English)': node['name']['en'],
                'Child Name (Arabic)': node['name']['ar']
            })
        # Recurse into children if they exist
        if 'children' in node:
            results.extend(find_children_by_parent_id(node['children'], target_ids))
    return results

# Helper function to find root nodes (parent_id = null) and their children
def find_null_parent_nodes_with_children(tree):
    results = []
    for node in tree:
        if node['parent_id'] is None:  # Check if parent_id is null (root node)
            parent_id = node['id']
            parent_name_en = node['name']['en']
            parent_name_ar = node['name']['ar']
            
            # Include the root node itself as a parent entry
            results.append({
                'Parent ID': None,
                'Child ID': parent_id,
                'Child Name (English)': parent_name_en,
                'Child Name (Arabic)': parent_name_ar
            })
            
            # Include each child of this root node
            for child in node.get('children', []):
                results.append({
                    'Parent ID': parent_id,
                    'Child ID': child['id'],
                    'Child Name (English)': child['name']['en'],
                    'Child Name (Arabic)': child['name']['ar']
                })
        # Recurse into children if they exist
        if 'children' in node:
            results.extend(find_null_parent_nodes_with_children(node['children']))
    return results

# Define target parent IDs for vehicles and real estate
target_ids_vehicles = [34, 33, 31, 30, 32]
target_ids_real_estate = [142, 143, 141, 144]

# Extract data for vehicles and real estate
taxonomy_results_vehicles = find_children_by_parent_id(category_tree_data, target_ids_vehicles)
taxonomy_results_real_estate = find_children_by_parent_id(category_tree_data, target_ids_real_estate)

# Extract data for root nodes (parent_id = null) and their children
taxonomy_results_null_parents_with_children = find_null_parent_nodes_with_children(category_tree_data)

# Combine all results
taxonomy_results = taxonomy_results_vehicles + taxonomy_results_real_estate + taxonomy_results_null_parents_with_children

# Insert combined taxonomy into database
taxonomy_insert_query = """
INSERT INTO taxonomy_tree (ParentID, ID, CategoryNameEn, CategoryNameAr)
VALUES (%s, %s, %s, %s)
"""
for row in taxonomy_results:
    cursor.execute(taxonomy_insert_query, (row['Parent ID'], row['Child ID'], row['Child Name (English)'], row['Child Name (Arabic)']))
snowflake_conn.commit()
print("taxonomy_tree data inserted into the database.")


# SQL table creation for kw.json
create_kw_table_query = """
CREATE OR REPLACE TABLE taxonomy (
    ID INT PRIMARY KEY,
    Name_English STRING,
    Slug STRING,
    Breadcrumbs STRING
);
"""

try:
    cursor.execute(create_kw_table_query)
    snowflake_conn.commit()
    print("Table dbo.kw_data ensured to exist.")
except Exception as e:
    print(f"Error ensuring table dbo.kw_data exists: {e}")

# Process data from kw.json
kw_results = []
for kw_id, kw_info in kw_data.items():
    kw_results.append({
        'ID': int(kw_id),  # Ensure the ID is an integer for SQL insertion
        'Name_English': kw_info['name']['en'],
        'Slug': kw_info['slug'],
        'Breadcrumbs': str(kw_info['breadcrumbs'])  # Convert list to string for SQL
    })

# Insert data from kw.json into dbo.taxonomy
kw_insert_query = """
INSERT INTO taxonomy (ID, Name_English, Slug, Breadcrumbs)
VALUES (%s, %s, %s, %s)
"""
for row in kw_results:
    cursor.execute(kw_insert_query, (row['ID'], row['Name_English'], row['Slug'], row['Breadcrumbs']))
snowflake_conn.commit()
print("taxonomy data inserted into the database.")

# Close the database connection
cursor.close()
snowflake_conn.close()