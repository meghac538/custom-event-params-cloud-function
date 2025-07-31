from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config import WRITE_PROJECT_ID, TEMP_TABLE, TEMP_DATASET, PROCESSED_TABLE_ID

def alter_processed_table_with_missing_event_params():
    print("Initializing BigQuery client.")
    client = bigquery.Client(project=WRITE_PROJECT_ID)

    # -------------------------------
    # Data Type Mapping
    # -------------------------------
    TYPE_MAPPING = {
        "STRING": "STRING",
        "INT64": "INT64",
        "INTEGER": "INT64",
        "FLOAT64": "FLOAT64",
        "FLOAT": "FLOAT64",
        "BOOL": "BOOL",
        "BOOLEAN": "BOOL"
    }

    # -------------------------------
    # Check if Temporary Table Exists
    # -------------------------------
    temp_table_ref_str = f"{WRITE_PROJECT_ID}.{TEMP_DATASET}.{TEMP_TABLE}"
    print(f"Checking if temp table exists: {temp_table_ref_str}")
    try:
        client.get_table(temp_table_ref_str)
    except NotFound:
        print(f"Temp table not found: {temp_table_ref_str}")
        return {
            "status": "Error",
            "message": f"Temp table `{temp_table_ref_str}` not found"
        }

    # -------------------------------
    # Read Schema Differences
    # -------------------------------
    print("Reading missing field definitions from temp table.")
    query = f"""
        SELECT field_name, field_type
        FROM `{temp_table_ref_str}`
        WHERE field_name IS NOT NULL AND field_type IS NOT NULL
    """
    try:
        result = client.query(query).result()
    except Exception as e:
        print(f"Failed to query temp table: {e}")
        raise

    alter_statements = []
    skipped_fields = []

    # -------------------------------
    # Process Each Missing Field
    # -------------------------------
    for row in result:
        raw_name = row["field_name"]
        raw_type = row["field_type"].upper().strip()

        if raw_type not in TYPE_MAPPING:
            print(f"Skipping unknown type: {raw_type} for field: {raw_name}")
            skipped_fields.append(f"{raw_name} (type: {raw_type})")
            continue

        new_column = f"{raw_name}_event_param"
        mapped_type = TYPE_MAPPING[raw_type]
        alter_stmt = f"ADD COLUMN IF NOT EXISTS `{new_column}` {mapped_type}"
        alter_statements.append(alter_stmt)

    if not alter_statements:
        msg = "No valid new fields to add."
        if skipped_fields:
            print(f"Skipped fields: {', '.join(skipped_fields)}")
            msg += f" Skipped: {', '.join(skipped_fields)}"
        return {
            "status": "No changes",
            "message": msg
        }

    # -------------------------------
    # Construct ALTER TABLE Statement
    # -------------------------------
    newline_indent = ',\n    '
    alter_sql = f"""
        ALTER TABLE `{PROCESSED_TABLE_ID}`
        {newline_indent.join(alter_statements)}
    """
    print("Executing ALTER TABLE statement:")
    print(alter_sql)

    try:
        job = client.query(alter_sql)
        job.result()
    except Exception as e:
        print(f"ALTER TABLE job failed: {e}")
        raise

    print(f"Successfully altered table: {PROCESSED_TABLE_ID}")
    print(f"BigQuery Job ID: {job.job_id}")
    if skipped_fields:
        print(f"Skipped fields: {skipped_fields}")

    return {
        "status": "Success",
        "added_fields": len(alter_statements),
        "executed_sql": alter_sql,
        "skipped_fields": skipped_fields,
        "job_id": job.job_id
    }
