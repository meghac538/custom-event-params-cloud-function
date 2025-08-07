# -------------------------------
# GCP Configuration
# -------------------------------
RAW_PROJECT_ID       = "oneorigin-ga4-raw"
WRITE_PROJECT_ID     = "ga4-dataform"    
PROJECT_ID           = WRITE_PROJECT_ID
REGION               = "us-west1" 

# -------------------------------
# Raw Table Configuration
# -------------------------------
RAW_DATASET          = "analytics_374935609"
RAW_TABLE_PATTERN    = "events_*"

# -------------------------------
# Processed Table (Dataform)
# -------------------------------
PROCESSED_TABLE_ID   = "ga4-dataform.GA4Dataform_374935609.ga4_events"

# -------------------------------
# Temporary Output Table
# -------------------------------
TEMP_DATASET         = "GA4Dataform_374935609"
TEMP_TABLE           = "missing_event_params_schema"

# -------------------------------
# Schema Comparison Settings
# -------------------------------
DAYS_TO_LOOK_BACK    = 7

# -------------------------------
# GitHub Configuration
# -------------------------------
REPO                 = "Reshma-San/OneOrigin-GA4-Dataform"
FILE_PATH            = "includes/custom/config.js"
BRANCH               = "main"
COMMIT_MESSAGE       = "Update CUSTOM_EVENT_PARAMS_ARRAY in config.js"

# -------------------------------
# Dataform API Execution Constants
# -------------------------------
REPO_ID              = "OneOrigin-GA4-Dataform"      # Repository ID in Dataform
RELEASE_ID           = "custom_event_params_1"             # Release config name
WORKFLOW_ID          = "automation_test"                 # Workflow config name
