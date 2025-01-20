import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

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

# Load JSON data for taxonomy
category_tree_file = 'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/scripts/category_treeV2.json'

try:
    with open(category_tree_file, 'r', encoding='utf-8') as file:
        category_tree_data = json.load(file)
    logger.info("Category tree JSON file loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load category tree JSON file: {e}")
    raise

# Create Snowflake table for category_hierarchy
create_category_hierarchy_table_query = """
CREATE OR REPLACE TABLE category_hierarchy (
    ID INT,
    ParentID INT,
    CategoryNameEn STRING,
    CategoryNameAr STRING,
    ParentNameEn STRING,
    ParentNameAr STRING
);
"""
try:
    cursor.execute(create_category_hierarchy_table_query)
    logger.info("Table 'category_hierarchy' created or replaced successfully in Snowflake.")
except Exception as e:
    logger.error(f"Failed to create table in Snowflake: {e}")
    raise

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

# Insert category hierarchy into Snowflake
insert_category_hierarchy_query = """
INSERT INTO category_hierarchy (ID, ParentID, CategoryNameEn, CategoryNameAr, ParentNameEn, ParentNameAr)
VALUES (%s, %s, %s, %s, %s, %s)
"""
try:
    for row in category_hierarchy_results:
        cursor.execute(
            insert_category_hierarchy_query,
            (row['ID'], row['ParentID'], row['CategoryNameEn'], row['CategoryNameAr'], row['ParentNameEn'], row['ParentNameAr'])
        )
    db_connection.commit()
    logger.info("Category hierarchy data inserted into Snowflake successfully.")
except Exception as e:
    logger.error(f"Failed to insert data into Snowflake: {e}")
    raise

# Close the Snowflake connection
try:
    cursor.close()
    db_connection.close()
    logger.info("Snowflake database connection closed.")
except Exception as e:
    logger.error(f"Failed to close Snowflake connection: {e}")
    raise
