import traceback
import base64
import json

from compare_event_params import compare_event_params_and_store_schema_diff
from alter_table_event_params import alter_processed_table_with_missing_event_params
from update_dataform_config import update_config_file_with_new_params

def main(event, context):
    try:
        print("Incoming Pub/Sub message.")

        # -----------------------
        # Decode and parse message
        # -----------------------
        if 'data' not in event:
            print("Missing 'data' in Pub/Sub message.")
            return

        try:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            print(f"Decoded message data: {message_data}")
            payload = json.loads(message_data)
        except Exception as decode_error:
            print(f"Error decoding message: {decode_error}")
            traceback.print_exc()
            return

        # -----------------------
        # Step 1: Compare Schemas
        # -----------------------
        print("Starting schema comparison...")
        compare_result = compare_event_params_and_store_schema_diff(payload)
        print(f"Schema comparison result:\n{compare_result}")

        if compare_result["missing_count"] == 0:
            print("No mismatches found. Nothing to update.")
            return

        # -----------------------
        # Step 2: Alter Table
        # -----------------------
        print("Starting table alteration...")
        alter_result = alter_processed_table_with_missing_event_params()
        print(f"Table alteration result:\n{alter_result}")

        # -----------------------
        # Step 3: Update Config
        # -----------------------
        print("Starting config update...")
        config_update_result = update_config_file_with_new_params()
        print(f"Config update result:\n{config_update_result}")

    except Exception as e:
        print(f"Unhandled exception occurred: {e}")
        traceback.print_exc()
