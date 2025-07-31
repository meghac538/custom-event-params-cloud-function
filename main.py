import traceback
import base64
from flask import Request, jsonify

from compare_event_params import compare_event_params_and_store_schema_diff
from alter_table_event_params import alter_processed_table_with_missing_event_params
from update_dataform_config import update_config_file_with_new_params

def app(request: Request):
    try:
        print("Incoming request to Cloud Run function.")

        # -----------------------
        # Validate Request Payload
        # -----------------------
        request_json = request.get_json(silent=True)
        print(f"Parsed request JSON: {request_json}")

        if not request_json or 'message' not in request_json:
            print("Invalid Pub/Sub message: missing 'message' field.")
            return "Bad Request: Invalid Pub/Sub message format.", 400

        message_data_base64 = request_json['message'].get('data')
        if not message_data_base64:
            print("Missing 'data' in Pub/Sub message.")
            return "Bad Request: No data in Pub/Sub message.", 400

        try:
            message_data = base64.b64decode(message_data_base64).decode('utf-8')
            print(f"Decoded message data: {message_data}")
        except Exception as decode_error:
            print(f"Error decoding base64 data: {decode_error}")
            traceback.print_exc()
            return "Bad Request: Failed to decode Pub/Sub message.", 400

        # -----------------------
        # Step 1: Compare Schemas
        # -----------------------
        print("Starting schema comparison...")
        compare_result = compare_event_params_and_store_schema_diff(request)
        print(f"Schema comparison result:\n{compare_result}")

        # Exit if no mismatches found
        if compare_result["missing_count"] == 0:
            print("No mismatches found. The processed Dataform table is up-to-date.")
            return jsonify({
                "status": "No Action Needed",
                "message": "The processed Dataform table already contains all event parameters.",
                "compare_result": compare_result
            }), 200

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

        return jsonify({
            "compare_result": compare_result,
            "alter_result": alter_result,
            "config_update_result": config_update_result
        }), 200

    except Exception as e:
        print(f"Unhandled exception occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
