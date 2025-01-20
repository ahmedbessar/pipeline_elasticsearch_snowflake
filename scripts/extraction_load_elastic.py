import requests
import pyodbc
import json
from datetime import datetime

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

# Ensure table schema matches expected structure
create_elasticsearch_table_query = """
IF OBJECT_ID('dbo.classified_log_report_els', 'U') IS NULL 
BEGIN
    CREATE TABLE dbo.classified_log_report_els (   
        EntityID INT,
        EntityTitle NVARCHAR(255),
        Breadcrumb NVARCHAR(MAX),
        BreadcrumbTitleEnglish NVARCHAR(255),
        BreadcrumbTitleArabic NVARCHAR(255),
        CallAndroidCount INT,
        CallHuaweiCount INT,
        CallIOSCount INT,
        CallWebCount INT,
        ChatAndroidCount INT,
        ChatHuaweiCount INT,
        ChatIOSCount INT,
        ChatWebCount INT,
        CountryAlias NVARCHAR(50),
        CreatedAt DATETIME,
        Day DATETIME,
        EntityType NVARCHAR(50),
        IsExpired BIT,
        IsFree BIT,
        IsPaid BIT,
        IsSticky BIT,
        MainTaxonomy INT,
        MainTaxonomyTitleEnglish NVARCHAR(255),
        MainTaxonomyTitleArabic NVARCHAR(255),
        OwnerPhone NVARCHAR(50),
        UpdatedAt DATETIME,
        VisitAndroidCount INT,
        VisitHuaweiCount INT,
        VisitIOSCount INT,
        VisitWebCount INT,
        WhatsappAndroidCount INT,
        WhatsappHuaweiCount INT,
        WhatsappIOSCount INT,
        WhatsappWebCount INT
    )
END
"""
cursor.execute(create_elasticsearch_table_query)
db_connection.commit()

# Elasticsearch query setup
headers = {
    'kbn-xsrf': 'reporting',
    'Content-Type': 'application/json',
}

base_url = 'https://elasticsearch1.waseet.net/log-classified-report-day-kw/_search'
auth = ('bisar', '6gvMZJ4zTeBR82R')
page_size = 10000  # Number of records per page
search_after_value = None  # Initialize search_after

while True:
    # Build query payload
    json_data = {
        'size': page_size,
        'query': {
            'bool': {
                'filter': [
                    {'term': {'country_alias': 'kw'}},
                    {'range': {'day': {'gte': '2024-10-01T00:00:00.230Z', 'lte': '2024-11-01T00:00:00.230Z'}}}
                ]
            }
        },
        'sort': [{'day': 'asc'}, {'entity_id': 'asc'}],  # Sort order for pagination
    }

    # json_data = {
    # 'size': page_size,
    # 'query': {
    #     'match_all': {}  # Replace with match_all to include all documents
    # },
    # 'sort': [{'day': 'asc'}, {'entity_id': 'asc'}]  # Sort order for pagination
    # }

    # Add search_after for subsequent pages
    if search_after_value:
        json_data['search_after'] = search_after_value

    response = requests.get(base_url, headers=headers, json=json_data, auth=auth)

    # Check response status
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    hits = data['hits']['hits']
    if not hits:
        break  # Stop if no more hits are returned

    # Process hits
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

        # Correctly extract Breadcrumb Titles from 'breadcrumb_title'
        breadcrumb_title = source.get('breadcrumb_title', {})
        breadcrumb_title_english = breadcrumb_title.get('en', '') if isinstance(breadcrumb_title, dict) else ''  # Extract English breadcrumb title
        breadcrumb_title_arabic = breadcrumb_title.get('ar', '') if isinstance(breadcrumb_title, dict) else ''  # Extract English breadcrumb title
        # breadcrumb_title_arabic = breadcrumb_title.get('ar', '')   # Extract Arabic breadcrumb title

        # Insert data into database
        cursor.execute("""
            INSERT INTO dbo.classified_log_report_els (
                EntityID, EntityTitle, Breadcrumb, BreadcrumbTitleEnglish, BreadcrumbTitleArabic, 
                CallAndroidCount, CallHuaweiCount, CallIOSCount, CallWebCount, ChatAndroidCount, 
                ChatHuaweiCount, ChatIOSCount, ChatWebCount, CountryAlias, CreatedAt, Day, 
                EntityType, IsExpired, IsFree, IsPaid, IsSticky, MainTaxonomy, MainTaxonomyTitleEnglish, 
                MainTaxonomyTitleArabic, OwnerPhone, UpdatedAt, VisitAndroidCount, VisitHuaweiCount, 
                VisitIOSCount, VisitWebCount, WhatsappAndroidCount, WhatsappHuaweiCount, 
                WhatsappIOSCount, WhatsappWebCount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
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

    db_connection.commit()  # Commit changes after each page

    # Set search_after for next iteration
    search_after_value = hits[-1]['sort']
    print(f"Processed {len(hits)} records.")

db_connection.close()