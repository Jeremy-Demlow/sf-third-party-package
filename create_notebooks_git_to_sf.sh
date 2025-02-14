#!/bin/bash

# Base Snowflake setup script
# This script provides core functions for Snowflake operations

# Enable error handling
set -e

# Default values
CONNECTION="${SNOWFLAKE_CONNECTION:-sf_third_party_package}"
SF_NBS="${SF_NBS:-sf_nbs}"
DATABASE="${DATABASE:-PACKAGES}"
SCHEMA="${SCHEMA:-DEMO}"
BRANCH="${BRANCH:-main}"
WAREHOUSE="${WAREHOUSE:-DS_WH_XS}"
REPOSITORY="${REPOSITORY:-sf_third_party_package}"
USE_CONTAINER_RUNTIME=false
COMPUTE_POOL="${COMPUTE_POOL:-SIMPLE_DS_POOL}"
MIN_NODES="${MIN_NODES:-1}"
MAX_NODES="${MAX_NODES:-2}"
INSTANCE_FAMILY="${INSTANCE_FAMILY:-CPU_X64_L}"
SECRET_NAME="${SECRET_NAME:-GH_SECRET}"
API_INTEGRATION_NAME="${API_INTEGRATION_NAME:-default_git_api_integration}"
GIT_USERNAME="${GIT_USERNAME:-''}"
GIT_PASSWORD="${GIT_PASSWORD:-''}"
GITHUB_PREFIX="${GITHUB_PREFIX:-'https://github.com'}"

# Configure logging
log_info() {
echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2
}

log_warning() {
    echo "[WARNING] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2
}

# Function to execute Snowflake SQL commands with masked output
execute_sql() {
    local query="$1"
    local suppress_log="${2:-false}"
    
    if [ "$suppress_log" = false ]; then
        # Create masked version for logging
        local masked_query
        masked_query=$(echo "$query" | sed 's/PASSWORD.*$/PASSWORD = ******;/g' | sed 's/USERNAME.*PASSWORD/USERNAME = ****** PASSWORD/g')
        log_info "Executing SQL: $masked_query"
    fi
    
    snow sql -c "$CONNECTION" -q "$query"
}

# Function to check if object exists
check_object_exists() {
    local object_type="$1"
    local object_name="$2"
    local result
    
    case $object_type in
        "SECRET")
            result=$(execute_sql "SHOW SECRETS LIKE '$object_name';" true)
            ;;
        "API_INTEGRATION")
            result=$(execute_sql "SHOW API INTEGRATIONS LIKE '$object_name';" true)
            ;;
        "GIT_REPOSITORY")
            result=$(execute_sql "SHOW GIT REPOSITORIES LIKE '\"$object_name\"';" true)
            ;;
    esac
    
    if [ -n "$result" ]; then
        return 0
    else
        return 1
    fi
}

# Function to parse command line arguments
parse_arguments() {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            --connection) CONNECTION="$2"; shift ;;
            --sf_nbs) SF_NBS="$2"; shift ;;
            --database) DATABASE="$2"; shift ;;
            --schema) SCHEMA="$2"; shift ;;
            --branch) BRANCH="$2"; shift ;;
            --warehouse) WAREHOUSE="$2"; shift ;;
            --repository) REPOSITORY="$2"; shift ;;
            --use_container_runtime) USE_CONTAINER_RUNTIME=true ;;
            --compute_pool) COMPUTE_POOL="$2"; shift ;;
            --min_nodes) MIN_NODES="$2"; shift ;;
            --max_nodes) MAX_NODES="$2"; shift ;;
            --instance_family) INSTANCE_FAMILY="$2"; shift ;;
            --secret_name) SECRET_NAME="$2"; shift ;;
            --api_integration_name) API_INTEGRATION_NAME="$2"; shift ;;
            --git_username) GIT_USERNAME="$2"; shift ;;
            --git_password) GIT_PASSWORD="$2"; shift ;;
            --github_prefix) GITHUB_PREFIX="$2"; shift ;;
            *) log_error "Unknown parameter passed: $1"; return 1 ;;
        esac
        shift
    done
}

# Function to create database and schema if they don't exist
create_database_and_schema() {
    log_info "Creating database and schema if they don't exist..."
    execute_sql "CREATE DATABASE IF NOT EXISTS $DATABASE;"
    execute_sql "CREATE SCHEMA IF NOT EXISTS $DATABASE.$SCHEMA;"
}

# Function to create secret if it doesn't exist
create_secret_if_not_exists() {
    if ! check_object_exists "SECRET" "$SECRET_NAME"; then
        log_info "Creating secret $SECRET_NAME..."
        execute_sql "
CREATE SECRET IF NOT EXISTS $SECRET_NAME
    TYPE = password
    USERNAME = '$GIT_USERNAME'
    PASSWORD = '$GIT_PASSWORD';" true
    else
        log_info "Secret $SECRET_NAME already exists, skipping creation"
    fi
}

# Function to create API integration if it doesn't exist
create_api_integration_if_not_exists() {
    if ! check_object_exists "API_INTEGRATION" "$API_INTEGRATION_NAME"; then
        log_info "Creating API integration $API_INTEGRATION_NAME..."
        execute_sql "
CREATE API INTEGRATION IF NOT EXISTS $API_INTEGRATION_NAME
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('$GITHUB_PREFIX')
    ALLOWED_AUTHENTICATION_SECRETS = all
    ENABLED = TRUE;"
    else
        log_info "API integration $API_INTEGRATION_NAME already exists, skipping creation"
    fi
}

# Function to create git repository if it doesn't exist
create_git_repo_if_not_exists() {
    if ! check_object_exists "GIT_REPOSITORY" "$REPOSITORY"; then
        log_info "Creating git repository $REPOSITORY..."
        execute_sql "
CREATE GIT REPOSITORY IF NOT EXISTS \"$DATABASE\".\"$SCHEMA\".\"$REPOSITORY\"
    API_INTEGRATION = $API_INTEGRATION_NAME
    GIT_CREDENTIALS = $SECRET_NAME
    ORIGIN = '$GITHUB_PREFIX/$REPOSITORY';"
    else
        log_info "Git repository $REPOSITORY already exists, skipping creation"
    fi
}

# Function to fetch from git repository
fetch_git_repository() {
    log_info "Fetching latest from Git repository..."
    execute_sql "ALTER GIT REPOSITORY \"${DATABASE}\".\"${SCHEMA}\".\"${REPOSITORY}\" FETCH;"
}

# Function to configure compute pool
configure_compute_pool() {
    if [ "$USE_CONTAINER_RUNTIME" = true ] ; then
        log_info "Configuring compute pool..."
        execute_sql "
        ALTER COMPUTE POOL IF EXISTS ${COMPUTE_POOL} STOP ALL;
        DROP COMPUTE POOL IF EXISTS ${COMPUTE_POOL};
        CREATE COMPUTE POOL IF NOT EXISTS ${COMPUTE_POOL}
        MIN_NODES = ${MIN_NODES}
        MAX_NODES = ${MAX_NODES}
        INSTANCE_FAMILY = ${INSTANCE_FAMILY}
        AUTO_RESUME = true
        AUTO_SUSPEND_SECS = 60;
        "
    fi
}

# Function to create or replace notebooks
create_notebooks() {
    log_info "Creating or replacing notebooks..."
    for notebook in $SF_NBS/*.ipynb; do
        filename=$(basename "$notebook")
        identifier="\"${DATABASE}\".\"${SCHEMA}\".\"${filename%.*}\""
        file_path="@\"${DATABASE}\".\"${SCHEMA}\".\"${REPOSITORY}\"/branches/${BRANCH}/${SF_NBS}/$filename"
        log_info "Creating or replacing notebook: $file_path"
        
        # Create or replace the notebook
        snow notebook create "$identifier" --notebook-file "$file_path" --connection "$CONNECTION"

        # Prepare the ALTER NOTEBOOK command
        alter_notebook_cmd="
        ALTER NOTEBOOK IF EXISTS $identifier SET
          EXTERNAL_ACCESS_INTEGRATIONS = ('allow_all_eai')"

        # Add runtime-specific settings
        if [ "$USE_CONTAINER_RUNTIME" = true ] ; then
            alter_notebook_cmd+="
            COMPUTE_POOL = '${COMPUTE_POOL}'
            RUNTIME_NAME = 'SYSTEM\$BASIC_RUNTIME'"
        else
            alter_notebook_cmd+="
            QUERY_WAREHOUSE = '${WAREHOUSE}'"
        fi

        # Execute the ALTER NOTEBOOK command
        execute_sql "${alter_notebook_cmd};"

        log_info "Created or replaced notebook: $identifier"
    done
}

# Main function to run the entire process
run_setup() {
    log_info "Starting setup process..."
    
    # Create initial database and schema
    create_database_and_schema
    
    # Create secret if it doesn't exist and credentials are provided
    if [ ! -z "$GIT_USERNAME" ] && [ ! -z "$GIT_PASSWORD" ]; then
        create_secret_if_not_exists
    fi
    
    # Create API integration if it doesn't exist
    create_api_integration_if_not_exists
    
    # Create git repository if it doesn't exist
    create_git_repo_if_not_exists
    
    # Fetch from git repository
    fetch_git_repository
    
    # Configure compute pool if using container runtime
    configure_compute_pool
    
    # Create or replace notebooks
    create_notebooks
    
    log_info "Setup process completed."
}

# Only execute if script is run directly (not sourced)
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    # Parse command line arguments
    if ! parse_arguments "$@"; then
        exit 1
    fi
    
    # Run the setup
    run_setup
fi