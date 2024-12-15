import requests
import json
import pyodbc
import re
from datetime import datetime
import os


# Elasticsearch endpoint
url = "https://vpc-waseet-prod-ma4lk6ppczqcqiepfb2q2ayimy.eu-west-1.es.amazonaws.com/classifieds/_search"
headers = {'Content-Type': 'application/json'}

# Database connection
db_connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=AHMED-BISAR;'
    'DATABASE=analytics_db;'
    'UID=bisar;'  
    'PWD=welcome1234;'   
    'Trusted_Connection=yes;'
)
cursor = db_connection.cursor()

# Load JSON data for taxonomy
category_tree_file = 'category_treeV2.json' 

with open(category_tree_file, 'r', encoding='utf-8') as file:
    category_tree_data  = json.load(file)

# SQL table creation for category_hierarchy
create_category_hierarchy_table_query = """
IF OBJECT_ID('dbo.category_hierarchy', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.category_hierarchy (
        ID INT PRIMARY KEY,
        ParentID INT,
        CategoryNameEn NVARCHAR(255),
        CategoryNameAr NVARCHAR(255),
        ParentNameEn NVARCHAR(255),
        ParentNameAr NVARCHAR(255)
    )
END
"""
cursor.execute(create_category_hierarchy_table_query)
db_connection.commit()

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
INSERT INTO dbo.category_hierarchy (ID, ParentID, CategoryNameEn, CategoryNameAr, ParentNameEn, ParentNameAr)
VALUES (?, ?, ?, ?, ?, ?)
"""
for row in category_hierarchy_results:
    cursor.execute(
        insert_category_hierarchy_query,
        row['ID'], row['ParentID'], row['CategoryNameEn'], row['CategoryNameAr'], row['ParentNameEn'], row['ParentNameAr']
    )
db_connection.commit()
print("category_hierarchy data inserted into the database.")


# SQL table creation
create_table_query = """
IF OBJECT_ID('dbo.classifieds', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.classifieds (
        country NVARCHAR(100),
        agent NVARCHAR(100),
        has_images BIT,
        nid INT,
        negotiable BIT,
        is_muted BIT,
        global_type NVARCHAR(50),
        approved BIT,
        price FLOAT,
        translated_title NVARCHAR(MAX),
        id INT,
        paid_for_taxonomy_id INT,
        created DATETIME,
        created_by NVARCHAR(100),
        phone_number NVARCHAR(100),
        status NVARCHAR(50),
        handled_by NVARCHAR(100),
        request_type NVARCHAR(50),
        city NVARCHAR(100),
        rejected BIT,
        sub_category NVARCHAR(100),
        enabled BIT,
        ad_source NVARCHAR(100),
        advertisement_type NVARCHAR(50),
        bundle NVARCHAR(100),
        leaf_taxonomy_id INT,
        taxonomy_status NVARCHAR(50),
        data_type NVARCHAR(50),
        paid FLOAT,
        category NVARCHAR(100),
        username NVARCHAR(100),
        approved_date DATETIME,
        original_creation DATETIME,
        sticky_payment_start_time DATETIME, -- New field
        sticky_expiry DATETIME              -- New field
    )
END
"""
cursor.execute(create_table_query)
db_connection.commit()


# Load JSON data for taxonomy
category_tree_file = 'category_treeV2.json' 
kw_file = 'kw.json'

with open(category_tree_file, 'r', encoding='utf-8') as file:
    category_tree_data  = json.load(file)

with open(kw_file, 'r', encoding='utf-8') as f:
    kw_data = json.load(f)

# SQL table creation for taxonomy_tree
create_taxonomy_table_query = """
IF OBJECT_ID('dbo.taxonomy_tree', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.taxonomy_tree (
        ParentID INT,
        ID INT,
        CategoryNameEn NVARCHAR(255),
        CategoryNameAr NVARCHAR(255)
    )
END
"""
cursor.execute(create_taxonomy_table_query)
db_connection.commit()

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
INSERT INTO dbo.taxonomy_tree (ParentID, ID, CategoryNameEn, CategoryNameAr)
VALUES (?, ?, ?, ?)
"""
for row in taxonomy_results:
    cursor.execute(taxonomy_insert_query, row['Parent ID'], row['Child ID'], row['Child Name (English)'], row['Child Name (Arabic)'])
db_connection.commit()
print("taxonomy_tree data inserted into the database.")


# SQL table creation for kw.json
create_kw_table_query = """
IF OBJECT_ID('dbo.taxonomy', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.taxonomy (
        ID INT PRIMARY KEY,
        Name_English NVARCHAR(255),
        Slug NVARCHAR(255),
        Breadcrumbs NVARCHAR(MAX)
    )
END
"""

try:
    cursor.execute(create_kw_table_query)
    db_connection.commit()
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
INSERT INTO dbo.taxonomy (ID, Name_English, Slug, Breadcrumbs)
VALUES (?, ?, ?, ?)
"""
for row in kw_results:
    cursor.execute(kw_insert_query, row['ID'], row['Name_English'], row['Slug'], row['Breadcrumbs'])
db_connection.commit()
print("taxonomy data inserted into the database.")


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
INSERT INTO dbo.classifieds (
    country, agent, has_images, nid, negotiable, is_muted, global_type, approved, price,
    translated_title, id, paid_for_taxonomy_id, created, created_by, phone_number, status,
    handled_by, request_type, city, rejected, sub_category, enabled, ad_source,
    advertisement_type, bundle, leaf_taxonomy_id, taxonomy_status, data_type, paid, category,
    username, approved_date, original_creation, sticky_payment_start_time, sticky_expiry
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
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
                        "lte": 1730419200    # November 1, 2024, 00:00:00 UTC (exclusive)
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
        extracted_data = {
            "country": source.get("country"),
            "agent": source.get("agent"),
            "has_images": source.get("has_images"),
            "nid": source.get("nid"),
            "negotiable": source.get("negotiable"),
            "is_muted": source.get("is_muted"),
            "global_type": source.get("global_type"),
            "approved": source.get("approved"),
            "price": source.get("price"),
            "translated_title": source.get("translated_title"),
            "id": extract_numeric_id(source.get("id")),
            "paid_for_taxonomy_id": source.get("paid_for_taxonomy_id"),
            "created": convert_unix_to_datetime(source.get("created")),
            "created_by": source.get("created_by"),
            "phone_number": source.get("phone_number"),
            "status": source.get("status"),
            "handled_by": source.get("handled_by"),
            "request_type": source.get("request_type"),
            "city": source.get("city")[0] if isinstance(source.get("city"), list) and source.get("city") else None,
            "rejected": source.get("rejected"),
            "sub_category": source.get("sub_category"),
            "enabled": source.get("enabled"),
            "ad_source": source.get("ad_source"),
            "advertisement_type": source.get("advertisement_type"),
            "bundle": source.get("bundle"),
            "leaf_taxonomy_id": source.get("leaf_taxonomy_id"),
            "taxonomy_status": source.get("taxonomy_status"),
            "data_type": source.get("data_type"),
            "paid": source.get("paid"),
            "category": source.get("category"),
            "username": source.get("username"),
            "approved_date": convert_unix_to_datetime(source.get("approved_date")),
            "original_creation": convert_unix_to_datetime(source.get("original_creation")),
            "sticky_payment_start_time": convert_unix_to_datetime(source.get("sticky_payment_start_time")),  # New field
            "sticky_expiry": convert_unix_to_datetime(source.get("sticky_expiry"))     # New field
        }
        
        # Insert the record
        cursor.execute(insert_query, (
            extracted_data["country"], extracted_data["agent"], extracted_data["has_images"], extracted_data["nid"],
            extracted_data["negotiable"], extracted_data["is_muted"], extracted_data["global_type"], extracted_data["approved"],
            extracted_data["price"], extracted_data["translated_title"], extracted_data["id"], extracted_data["paid_for_taxonomy_id"],
            extracted_data["created"], extracted_data["created_by"], extracted_data["phone_number"], extracted_data["status"],
            extracted_data["handled_by"], extracted_data["request_type"], extracted_data["city"], extracted_data["rejected"],
            extracted_data["sub_category"], extracted_data["enabled"], extracted_data["ad_source"],
            extracted_data["advertisement_type"], extracted_data["bundle"], extracted_data["leaf_taxonomy_id"],
            extracted_data["taxonomy_status"], extracted_data["data_type"], extracted_data["paid"], extracted_data["category"],
            extracted_data["username"], extracted_data["approved_date"], extracted_data["original_creation"],
            extracted_data["sticky_payment_start_time"], extracted_data["sticky_expiry"]
        ))

    # Commit every page to avoid data loss
    db_connection.commit()

    # Set search_after for the next batch
    search_after_value = hits[-1]["sort"]
    print(f"Fetched {len(hits)} records. Total fetched so far: {len(all_results)}")

print("Data migration to the database is complete.")

# Close the database connection
cursor.close()
db_connection.close()

# # Serialize datetime fields for JSON output
# extracted_results = serialize_datetimes(extracted_results)

# # Save results to a JSON file with a timestamp
# timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# file_name = f"classifieds_results_extracted_{timestamp}.json"
# with open(file_name, 'w') as f:
#     json.dump(extracted_results, f, indent=2)

# print(f"Extracted results saved to {file_name}.")