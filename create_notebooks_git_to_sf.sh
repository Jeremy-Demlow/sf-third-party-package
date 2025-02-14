#!/bin/bash
set -euo pipefail  # Fail fast and explicitly

# Core settings with sensible defaults
settings() {
    CONNECTION="${SNOWFLAKE_CONNECTION:-sf_third_party_package}"
    SF_NBS="${SF_NBS:-sf_nbs}"
    DATABASE="${DATABASE:-PACKAGES}"
    SCHEMA="${SCHEMA:-DEMO}"
    BRANCH="${BRANCH:-main}"
    WAREHOUSE="${WAREHOUSE:-DS_WH_XS}"
    REPOSITORY="${REPOSITORY:-sf_third_party_package}"
    USE_CONTAINER_RUNTIME="${USE_CONTAINER_RUNTIME:-false}"
    COMPUTE_POOL="${COMPUTE_POOL:-SIMPLE_DS_POOL}"
    MIN_NODES="${MIN_NODES:-1}"
    MAX_NODES="${MAX_NODES:-2}"
    INSTANCE_FAMILY="${INSTANCE_FAMILY:-CPU_X64_L}"
    SECRET_NAME="${SECRET_NAME:-GH_SECRET}"
    API_INTEGRATION_NAME="${API_INTEGRATION_NAME:-default_git_api_integration}"
    GITHUB_PREFIX="${GITHUB_PREFIX:-'https://github.com'}"
}

# Simple logging
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1"; }
fail() { log "ERROR: $1"; exit 1; }

# Run SQL with masked output and error handling
run_sql() {
    local query="$1"
    local masked_query
    
    # More comprehensive masking of sensitive data
    masked_query=$(echo "$query" | \
        sed -E 's/(PASSWORD|TOKEN).*$/\1 = ******;/g' | \
        sed -E 's/(USERNAME|USER).*PASSWORD/\1 = ****** PASSWORD/g' | \
        sed -E 's/'"$GIT_PASSWORD"'/******/g' | \
        sed -E 's/'"$GIT_USERNAME"'/******/g')
    
    log "Running SQL: $masked_query"
    snow sql -c "$CONNECTION" -q "$query" 2>&1 | \
        sed -E 's/'"$GIT_PASSWORD"'/******/g' | \
        sed -E 's/'"$GIT_USERNAME"'/******/g'
}

# Check if Snowflake object exists
check_exists() {
    local type="$1" name="$2"
    local cmd="SHOW ${type}S LIKE '\"$name\"';"
    snow sql -c "$CONNECTION" -q "$cmd" 2>/dev/null | grep -q "$name"
}

# Core setup functions
setup_database() {
    log "Setting up database and schema..."
    run_sql "CREATE DATABASE IF NOT EXISTS $DATABASE;"
    run_sql "CREATE SCHEMA IF NOT EXISTS $DATABASE.$SCHEMA;"
}

setup_git_auth() {
    [[ -z "${GIT_USERNAME:-}" ]] && fail "GIT_USERNAME required"
    [[ -z "${GIT_PASSWORD:-}" ]] && fail "GIT_PASSWORD required"
    
    if ! check_exists "SECRET" "$SECRET_NAME"; then
        run_sql "
        CREATE SECRET IF NOT EXISTS $SECRET_NAME
            TYPE = password
            USERNAME = '$GIT_USERNAME'
            PASSWORD = '$GIT_PASSWORD';"
    fi
}

setup_git_integration() {
    if ! check_exists "API_INTEGRATION" "$API_INTEGRATION_NAME"; then
        run_sql "
        CREATE API INTEGRATION IF NOT EXISTS $API_INTEGRATION_NAME
            API_PROVIDER = git_https_api
            API_ALLOWED_PREFIXES = ('$GITHUB_PREFIX')
            ALLOWED_AUTHENTICATION_SECRETS = all
            ENABLED = TRUE;"
    fi
}

setup_git_repo() {
    local repo="\"$DATABASE\".\"$SCHEMA\".\"$REPOSITORY\""
    if ! check_exists "GIT_REPOSITORY" "$REPOSITORY"; then
        run_sql "
        CREATE GIT REPOSITORY IF NOT EXISTS $repo
            API_INTEGRATION = $API_INTEGRATION_NAME
            GIT_CREDENTIALS = $SECRET_NAME
            ORIGIN = '$GITHUB_PREFIX/$REPOSITORY';"
    fi
    run_sql "ALTER GIT REPOSITORY $repo FETCH;"
}

setup_compute() {
    [[ "$USE_CONTAINER_RUNTIME" != "true" ]] && return
    
    run_sql "
    ALTER COMPUTE POOL IF EXISTS $COMPUTE_POOL STOP ALL;
    DROP COMPUTE POOL IF EXISTS $COMPUTE_POOL;
    CREATE COMPUTE POOL IF NOT EXISTS $COMPUTE_POOL
        MIN_NODES = $MIN_NODES
        MAX_NODES = $MAX_NODES
        INSTANCE_FAMILY = $INSTANCE_FAMILY
        AUTO_RESUME = true
        AUTO_SUSPEND_SECS = 60;"
}

setup_notebooks() {
    log "Setting up notebooks..."
    for nb in $SF_NBS/*.ipynb; do
        local name=$(basename "$nb" .ipynb)
        local id="\"$DATABASE\".\"$SCHEMA\".\"$name\""
        local path="@\"$DATABASE\".\"$SCHEMA\".\"$REPOSITORY\"/branches/$BRANCH/$SF_NBS/$(basename "$nb")"
        
        snow notebook create "$id" --notebook-file "$path" --connection "$CONNECTION"
        
        local alter_cmd="ALTER NOTEBOOK IF EXISTS $id SET
            EXTERNAL_ACCESS_INTEGRATIONS = ('allow_all_eai')"
        
        if [[ "$USE_CONTAINER_RUNTIME" == "true" ]]; then
            alter_cmd+=" COMPUTE_POOL = '$COMPUTE_POOL'
                RUNTIME_NAME = 'SYSTEM\$BASIC_RUNTIME'"
        else
            alter_cmd+=" QUERY_WAREHOUSE = '$WAREHOUSE'"
        fi
        
        run_sql "$alter_cmd"
    done
}

# Parse arguments with validation
parse_args() {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            --connection) CONNECTION="$2"; shift ;;
            --sf_nbs) SF_NBS="$2"; shift ;;
            --database) DATABASE="$2"; shift ;;
            --schema) SCHEMA="$2"; shift ;;
            --branch) BRANCH="$2"; shift ;;
            --warehouse) WAREHOUSE="$2"; shift ;;
            --repository) REPOSITORY="$2"; shift ;;
            --compute_pool) COMPUTE_POOL="$2"; shift ;;
            --min_nodes) MIN_NODES="$2"; shift ;;
            --max_nodes) MAX_NODES="$2"; shift ;;
            --instance_family) INSTANCE_FAMILY="$2"; shift ;;
            --secret_name) SECRET_NAME="$2"; shift ;;
            --api_integration_name) API_INTEGRATION_NAME="$2"; shift ;;
            --git_username) GIT_USERNAME="$2"; shift ;;
            --git_password) GIT_PASSWORD="$2"; shift ;;
            --github_prefix) GITHUB_PREFIX="$2"; shift ;;
            --use_container_runtime) USE_CONTAINER_RUNTIME=true ;;
            *) fail "Unknown parameter: $1" ;;
        esac
        shift
    done
}


# Main execution
main() {
    settings
    parse_args "$@"
    
    local steps=(
        setup_database
        setup_git_auth
        setup_git_integration
        setup_git_repo
        setup_compute
        setup_notebooks
    )
    
    for step in "${steps[@]}"; do
        $step || fail "Step $step failed"
    done
    
    log "Setup completed successfully"
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"
