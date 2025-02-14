#!/bin/bash
set -euo pipefail  # Fail fast and explicitly

# Core settings with sensible defaults
settings() {
    STAGE_NAME="${STAGE_NAME:-PYTHON_PACKAGES}"
    SCHEMA_NAME="${SCHEMA_NAME:-DEMO}"
    DATABASE_NAME="${DATABASE_NAME:-PACKAGES}"
    REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-extra-requirements.txt}"
    OVERWRITE="${OVERWRITE:-true}"
    CONNECTION="${CONNECTION:-sf_third_party_package}"
    WAREHOUSE="${WAREHOUSE:-DS_WH_XS}"
}

# Simple logging
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1"; }
fail() { log "ERROR: $1"; exit 1; }

# Run SQL with error handling
run_sql() {
    local query="$1"
    log "Running SQL: $query"
    snow sql -c "$CONNECTION" -q "$query" || fail "SQL failed: $query"
}

# Core setup functions
setup_database() {
    log "Setting up Snowflake environment..."
    run_sql "
        USE WAREHOUSE $WAREHOUSE;
        CREATE DATABASE IF NOT EXISTS $DATABASE_NAME;
        USE DATABASE $DATABASE_NAME;
        CREATE SCHEMA IF NOT EXISTS $SCHEMA_NAME;
        USE SCHEMA $SCHEMA_NAME;"
}

# Package handling functions
normalize_package() {
    local pkg="$1"
    echo "$pkg" | sed -E 's/([a-zA-Z0-9.-]+)(==|>=|<=|!=|~=|>|<)?.*/\1/' | tr '-' '_'
}

clean_version() {
    local dep="$1"
    local name=$(echo "$dep" | sed -E 's/([a-zA-Z0-9._-]+).*/\1/' | tr '[:upper:]' '[:lower:]')
    local version=$(echo "$dep" | sed -E 's/[a-zA-Z0-9._-]+//')
    
    if [ -z "$version" ]; then
        echo "${name}=*"
    elif echo "$version" | grep -q '[<>]'; then
        echo "${name}=*"
    else
        local ver=$(echo "$version" | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')
        [ -n "$ver" ] && echo "${name}=${ver}" || echo "${name}=*"
    fi
}

update_environment() {
    local main_pkg="$1" deps="$2" env_file="sf_nbs/environment.yml"
    
    mkdir -p sf_nbs
    [ ! -f "$env_file" ] && echo "name: app_environment
channels:
  - snowflake
dependencies:" > "$env_file"
    
    log "Updating $env_file..."
    echo "$deps" | while IFS= read -r dep; do
        [[ -z "$dep" || "$dep" =~ (following|Anaconda|included) ]] && continue
        [[ "$dep" == *"$main_pkg"* ]] && continue
        local clean_dep=$(clean_version "$dep")
        grep -q "  - $clean_dep" "$env_file" || echo "  - $clean_dep" >> "$env_file"
    done
}

process_package() {
    local pkg="$1"
    [[ $pkg =~ ^[[:space:]]*$ || $pkg == \#* ]] && return
    
    local norm_pkg=$(normalize_package "$pkg")
    log "Processing package: $pkg"
    
    local pkg_output
    if ! pkg_output=$(snow snowpark package create "$pkg" --allow-shared-libraries 2>&1); then
        fail "Failed to create package: $pkg"
    fi
    
    echo "$pkg_output"
    
    if echo "$pkg_output" | grep -q "depends on the following"; then
        local deps=$(echo "$pkg_output" | sed -n '/depends on the following/,/Successfully created package/p')
        update_environment "$pkg" "$deps"
    fi
    
    local full_stage="${DATABASE_NAME}.${SCHEMA_NAME}.${STAGE_NAME}"
    local zip_file="${norm_pkg}.zip"
    
    local upload_cmd="snow snowpark package upload -f $zip_file -s $full_stage"
    [ "$OVERWRITE" = true ] && upload_cmd+=" -o"
    
    if ! eval "$upload_cmd"; then
        fail "Failed to upload $zip_file"
    fi
    
    log "Successfully processed $pkg"
    rm -f "$zip_file"
}

# Parse arguments
parse_args() {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            -s|--stage) STAGE_NAME="$2"; shift ;;
            -c|--schema) SCHEMA_NAME="$2"; shift ;;
            -d|--database) DATABASE_NAME="$2"; shift ;;
            -r|--requirements) REQUIREMENTS_FILE="$2"; shift ;;
            -o|--overwrite) OVERWRITE=true ;;
            --connection) CONNECTION="$2"; shift ;;
            --warehouse) WAREHOUSE="$2"; shift ;;
            *) fail "Unknown parameter: $1" ;;
        esac
        shift
    done
}

# Main execution
main() {
    settings
    parse_args "$@"
    
    local req_path=$(realpath "$REQUIREMENTS_FILE")
    [ ! -f "$req_path" ] && fail "Requirements file not found: $req_path"
    
    log "Configuration:
  Stage: $STAGE_NAME
  Schema: $SCHEMA_NAME
  Database: $DATABASE_NAME
  Requirements: $req_path
  Overwrite: $OVERWRITE
  Connection: $CONNECTION
  Warehouse: $WAREHOUSE"
    
    setup_database
    
    log "Creating stage $STAGE_NAME..."
    snow stage create "$STAGE_NAME" \
        --schema="$SCHEMA_NAME" \
        --database="$DATABASE_NAME" || fail "Failed to create stage"
    
    while IFS= read -r package || [[ -n "$package" ]]; do
        process_package "$package"
    done < "$req_path"
    
    log "All packages processed successfully"
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"
