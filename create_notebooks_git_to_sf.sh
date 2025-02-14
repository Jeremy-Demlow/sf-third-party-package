#!/bin/bash

# Base Snowflake setup script
# This script provides core functions for Snowflake operations

# Enable error handling
set -e
CONNECTION="${SNOWFLAKE_CONNECTION:-sf_third_party_package}"
SF_NBS="${SF_NBS:-sf_nbs}"
DATABASE="${DATABASE:-PACKAGES}"
SCHEMA="${SCHEMA:-DEMO}"
BRANCH="${BRANCH:-main}"
WAREHOUSE="${WAREHOUSE:-DS_WH_XS}"
REPOSITORY="${REPOSITORY:-sf_third_party_package}"
USE_CONTAINER_RUNTIME=false
COMPUTE_POOL="${COMPUTE_POOL:-SIMPLE_DS_POOL}" # Not covered in this demo
MIN_NODES="${MIN_NODES:-1}" # Not covered in this demo
MAX_NODES="${MAX_NODES:-2}" # Not covered in this demo
INSTANCE_FAMILY="${INSTANCE_FAMILY:-CPU_X64_L}" # Not covered in this demo

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

# Function to execute Snowflake SQL commands
execute_sql() {
    local query="$1"
    snow sql -c "$CONNECTION" -q "$query"
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
            --use_container_runtime) 
                USE_CONTAINER_RUNTIME=true
                ;;
            --compute_pool) COMPUTE_POOL="$2"; shift ;;
            --min_nodes) MIN_NODES="$2"; shift ;;
            --max_nodes) MAX_NODES="$2"; shift ;;
            --instance_family) INSTANCE_FAMILY="$2"; shift ;;
            *) log_error "Unknown parameter passed: $1"; return 1 ;;
        esac
        shift
    done
}

# Function to check if a secret exists
check_secret_exists() {
    local secret_name="$1"
    execute_sql "DESCRIBE SECRET $secret_name" &>/dev/null
}

# Function to check if an API integration exists
check_api_integration_exists() {
    local integration_name="$1"
    execute_sql "SHOW API INTEGRATIONS LIKE '$integration_name'" &>/dev/null
}

# Function to check if a git repository exists
check_git_repo_exists() {
    local repo_name="$1"
    execute_sql "SHOW GIT REPOSITORIES LIKE '$repo_name'" &>/dev/null
}

# Function to fetch from git repository
fetch_git_repository() {
    log_info "Fetching latest from Git repository..."
    execute_sql "
    ALTER GIT REPOSITORY ${DATABASE}.${SCHEMA}.${REPOSITORY} FETCH;
    "
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
        identifier="${DATABASE}.${SCHEMA}.${filename%.*}"
        file_path="@${DATABASE}.${SCHEMA}.${REPOSITORY}/branches/${BRANCH}/${SF_NBS}/$filename"
        log_info "Creating or replacing notebook: $file_path"
        
        # Create or replace the notebook
        snow notebook create "$identifier" --notebook-file "$file_path" --connection "$CONNECTION"

        # Prepare the ALTER NOTEBOOK command
        alter_notebook_cmd="
        ALTER NOTEBOOK IF EXISTS $identifier SET
          EXTERNAL_ACCESS_INTEGRATIONS = ('allow_all_eai')
        "

        # Add runtime-specific settings
        if [ "$USE_CONTAINER_RUNTIME" = true ] ; then
            alter_notebook_cmd+="
            COMPUTE_POOL = '${COMPUTE_POOL}'
            RUNTIME_NAME = 'SYSTEM\$BASIC_RUNTIME'
            "
        else
            alter_notebook_cmd+="
            QUERY_WAREHOUSE = '${WAREHOUSE}'
            "
        fi

        # Execute the ALTER NOTEBOOK command
        execute_sql "${alter_notebook_cmd};"

        # Uncomment if needed:
        # execute_sql "
        # ALTER NOTEBOOK ${identifier} ADD LIVE VERSION FROM LAST;
        # "

        log_info "Created or replaced notebook: $identifier"
    done
}

# Function to validate required environment variables
validate_environment() {
    local required_vars=("$@")
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            missing_vars+=("$var")
        fi
    done
    
    if [ ${#missing_vars[@]} -ne 0 ]; then
        log_error "Missing required environment variables: ${missing_vars[*]}"
        return 1
    fi
}

# Main function to run the entire process
run_notebook_setup() {
    log_info "Starting notebook setup process..."
    
    # Fetch from git repository
    fetch_git_repository
    
    # Configure compute pool if using container runtime
    configure_compute_pool
    
    # Create or replace notebooks
    create_notebooks
    
    log_info "Notebook setup process completed."
}

# Only execute if script is run directly (not sourced)
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    # Parse command line arguments
    if ! parse_arguments "$@"; then
        exit 1
    fi
    
    # Run the setup
    run_notebook_setup
fi