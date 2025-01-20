import requests
import json
import pyodbc
import re
from datetime import datetime
import os


# Elasticsearch endpoint
# url = "https://vpc-waseet-prod-ma4lk6ppczqcqiepfb2q2ayimy.eu-west-1.es.amazonaws.com/classifieds/_search"
# headers = {'Content-Type': 'application/json'}

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
category_tree_file = 'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/scripts/category_treeV2.json' 
print(category_tree_file)
kw_file = 'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/scripts/kw.json'

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


# Load JSON data for taxonomy
category_tree_file = 'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/scripts/category_treeV2.json'

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