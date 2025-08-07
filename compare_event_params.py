from google.cloud import bigquery
from datetime import datetime, timedelta
from google.api_core.exceptions import Conflict
from config import (
    RAW_PROJECT_ID,
    WRITE_PROJECT_ID,
    RAW_DATASET,
    RAW_TABLE_PATTERN,
    PROCESSED_TABLE_ID,
    TEMP_DATASET,
    TEMP_TABLE,
    DAYS_TO_LOOK_BACK
)

def compare_event_params_and_store_schema_diff(request):
    print("Initializing BigQuery clients.")
    raw_client = bigquery.Client(project=RAW_PROJECT_ID)
    write_client = bigquery.Client(project=WRITE_PROJECT_ID)

    # -------------------------------
    # Delete Temp Table If Already Exists
    # -------------------------------
    table_id = f"{WRITE_PROJECT_ID}.{TEMP_DATASET}.{TEMP_TABLE}"
    print(f"Deleting temp table (if exists): {table_id}")
    write_client.delete_table(table_id, not_found_ok=True)

    # -------------------------------
    # Generate List of Raw Table Suffixes
    # -------------------------------
    today = datetime.utcnow().date()
    suffixes = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(DAYS_TO_LOOK_BACK)]
    suffix_filter = ",".join([f"'{s}'" for s in suffixes])
    print(f"Suffixes for raw table filtering: {suffix_filter}")

    # -------------------------------
    # Query Raw Event Params
    # -------------------------------
    raw_query = f"""
        SELECT
            param.key AS event_param_key,
            CASE
                WHEN param.value.string_value IS NOT NULL THEN 'STRING'
                WHEN param.value.int_value IS NOT NULL THEN 'INT64'
                WHEN param.value.double_value IS NOT NULL THEN 'FLOAT64'
                WHEN param.value.float_value IS NOT NULL THEN 'FLOAT64'
                ELSE 'STRING'
            END AS inferred_type
        FROM `{RAW_PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE_PATTERN}`,
             UNNEST(event_params) AS param
        WHERE _TABLE_SUFFIX IN ({suffix_filter})
    """
    print("Executing raw event parameter query.")
    try:
        raw_keys_result = raw_client.query(raw_query)
    except Exception as e:
        print(f"BigQuery raw query failed: {e}")
        raise

    # -------------------------------
    # Build Raw Key-Type Mapping
    # -------------------------------
    raw_key_type_map = {}
    for row in raw_keys_result:
        key = row.event_param_key
        inferred_type = row.inferred_type
        if key not in raw_key_type_map:
            raw_key_type_map[key] = inferred_type
    print(f"Extracted {len(raw_key_type_map)} unique keys from raw data.")

    # -------------------------------
    # Fetch Processed Table Schema
    # -------------------------------
    print("Fetching schema from processed table.")
    processed_table = write_client.get_table(PROCESSED_TABLE_ID)
    processed_fields = set(
        field.name.replace("_event_param", "")
        for field in processed_table.schema
        if field.name.endswith("_event_param")
    )
    print(f"Found {len(processed_fields)} processed parameter fields.")

    # -------------------------------
    # Core parameters that should not be added to custom array
    # -------------------------------
    core_params = {
        # Batch parameters
        'batch_ordering_id', 'batch_page_id',
        # Page parameters  
        'page_location', 'page_referrer', 'page_title',
        # Session parameters
        'ga_session_id', 'ga_session_number', 'engagement_time_msec', 'session_engaged',
        'engaged_session_event', 'entrances', 'ignore_referrer', 'synthetic_bundle',
        # Content parameters
        'content_group', 'content_id', 'content_type', 'content',
        # Traffic source parameters
        'medium', 'campaign', 'source', 'term', 'campaign_info_source',
        # Click IDs
        'gclid', 'dclid', 'srsltid', 'aclid', 'cp1', 'anid', 'click_timestamp',
        # Ecommerce parameters
        'currency', 'shipping', 'tax', 'value', 'transaction_id', 'coupon', 
        'payment_type', 'shipping_tier', 'item_list_id', 'item_list_name',
        'creative_name', 'creative_slot', 'promotion_id', 'promotion_name', 'item_name',
        # Link tracking
        'link_classes', 'link_domain', 'link_id', 'link_text', 'link_url', 'outbound',
        # Video tracking
        'video_current_time', 'video_duration', 'video_percent', 'video_provider',
        'video_title', 'video_url',
        # App parameters
        'app_version', 'method', 'fatal', 'timestamp',
        # Other core parameters
        'reward_type', 'reward_value', 'label', 'language', 'percent_scrolled',
        'search_term', 'file_extension', 'file_name', 'screen_resolution'
    }
    
    # -------------------------------
    # Identify Missing Keys (excluding core parameters)
    # -------------------------------
    missing_keys = [
        (key, raw_key_type_map[key])
        for key in raw_key_type_map
        if key not in processed_fields and key not in core_params
    ]
    
    # Log skipped core parameters
    skipped_core = [key for key in raw_key_type_map if key in core_params]
    if skipped_core:
        print(f"Skipped {len(skipped_core)} core parameters: {skipped_core}")
    
    print(f"Identified {len(missing_keys)} missing keys to be added.")

    rows_to_insert = [
        {"field_name": key, "field_type": dtype}
        for key, dtype in missing_keys
    ]

    # -------------------------------
    # Handle Case: No Missing Fields
    # -------------------------------
    if not rows_to_insert:
        print("No missing fields to insert into temp table.")
        return {
            "written_table": table_id,
            "missing_count": 0,
            "fields": []
        }

    # -------------------------------
    # Create Temp Table with found mismatches
    # -------------------------------
    table_schema = [
        bigquery.SchemaField("field_name", "STRING"),
        bigquery.SchemaField("field_type", "STRING"),
    ]
    print(f"Creating temp table: {table_id}")
    table_ref = bigquery.Table(table_id, schema=table_schema)

    try:
        write_client.create_table(table_ref)
        print(f"Created temp table: {table_id}")
    except Conflict:
        print(f"Temp table {table_id} already exists. Skipping creation.")
    except Exception as e:
        print(f"Failed to create temp table {table_id}: {e}")
        raise

    # -------------------------------
    # Insert Missing Fields into Temp Table
    # -------------------------------
    print(f"Inserting {len(rows_to_insert)} rows into temp table.")
    errors = write_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("Insert errors:", errors)

    return {
        "written_table": table_id,
        "missing_count": len(missing_keys),
        "fields": rows_to_insert,
        "skipped_core_params": len([key for key in raw_key_type_map if key in core_params])
    }
