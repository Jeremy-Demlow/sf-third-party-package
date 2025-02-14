#!/bin/bash
set -e

# Default values
DEFAULT_STAGE_NAME="PYTHON_PACKAGES";
DEFAULT_SCHEMA_NAME="DEMO"; 
DEFAULT_DATABASE_NAME="PACKAGES"
DEFAULT_REQUIREMENTS_FILE="extra-requirements.txt"; 
DEFAULT_OVERWRITE=true; 
DEFAULT_CONNECTION="sf_third_party_package"
DEFAULT_WAREHOUSE="DS_WH_XS"

log_info() { echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1"; }
log_error() { echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2; }

execute_sql() {
    local query="$1"; local suppress_log="${2:-false}"
    [ "$suppress_log" = false ] && log_info "Executing SQL: $query"
    snow sql -c "$CONNECTION" -q "$query"
}

setup_snowflake_env() {
    log_info "Setting up Snowflake environment..."
    execute_sql "USE WAREHOUSE $WAREHOUSE; CREATE DATABASE IF NOT EXISTS $DATABASE_NAME; USE DATABASE $DATABASE_NAME; CREATE SCHEMA IF NOT EXISTS $SCHEMA_NAME; USE SCHEMA $SCHEMA_NAME;"
}

normalize_package_name() {
    local package="$1"
    local base_package=$(echo "$package" | sed -E 's/([a-zA-Z0-9.-]+)(==|>=|<=|!=|~=|>|<)?.*/\1/')
    echo "$base_package" | tr '-' '_'
}

clean_version_specifier() {
    local dep="$1"
    local package_name=$(echo "$dep" | sed -E 's/([a-zA-Z0-9._-]+).*/\1/' | tr '[:upper:]' '[:lower:]')
    local version_part=$(echo "$dep" | sed -E 's/[a-zA-Z0-9._-]+//')
    [ -z "$version_part" ] && echo "${package_name}=*" && return
    if echo "$version_part" | grep -q '[<>]'; then echo "${package_name}=*"
    else
        local version=$(echo "$version_part" | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')
        [ -n "$version" ] && echo "${package_name}=${version}" || echo "${package_name}=*"
    fi
}

append_to_environment_yml() {
    local main_package="$1"; local dependencies="$2"; local env_file="sf_nbs/environment.yml"
    
    if [ ! -f "$env_file" ]; then
        mkdir -p sf_nbs
        echo "name: app_environment
channels:
  - snowflake
dependencies:" > "$env_file"
    fi
    
    echo "Appending dependencies to $env_file..."
    echo "$dependencies" | while IFS= read -r dep; do
        [[ -z "$dep" || "$dep" == *"following"* || "$dep" == *"Anaconda"* || "$dep" == *"included"* ]] && continue
        [[ "$dep" == *"$main_package"* ]] && continue
        local cleaned_dep=$(clean_version_specifier "$dep")
        grep -q "  - $cleaned_dep" "$env_file" || echo "  - $cleaned_dep" >> "$env_file"
    done
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--stage) STAGE_NAME="$2"; shift 2 ;;
        -c|--schema) SCHEMA_NAME="$2"; shift 2 ;;
        -d|--database) DATABASE_NAME="$2"; shift 2 ;;
        -r|--requirements) REQUIREMENTS_FILE="$2"; shift 2 ;;
        -o|--overwrite) OVERWRITE=true; shift ;;
        --connection) CONNECTION="$2"; shift 2 ;;
        --warehouse) WAREHOUSE="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Set default values
STAGE_NAME=${STAGE_NAME:-$DEFAULT_STAGE_NAME}; SCHEMA_NAME=${SCHEMA_NAME:-$DEFAULT_SCHEMA_NAME}
DATABASE_NAME=${DATABASE_NAME:-$DEFAULT_DATABASE_NAME}; REQUIREMENTS_FILE=${REQUIREMENTS_FILE:-$DEFAULT_REQUIREMENTS_FILE}
OVERWRITE=${OVERWRITE:-$DEFAULT_OVERWRITE}; CONNECTION=${CONNECTION:-$DEFAULT_CONNECTION}
WAREHOUSE=${WAREHOUSE:-$DEFAULT_WAREHOUSE}

REQUIREMENTS_FILE_FULL_PATH=$(realpath "$REQUIREMENTS_FILE")

# Print configuration
echo "Configuration:
  Stage Name:        $STAGE_NAME
  Schema Name:       $SCHEMA_NAME
  Database Name:     $DATABASE_NAME
  Requirements File: $REQUIREMENTS_FILE_FULL_PATH
  Overwrite:        $OVERWRITE
  Connection:       $CONNECTION
  Warehouse:        $WAREHOUSE"

[ ! -f "$REQUIREMENTS_FILE_FULL_PATH" ] && log_error "Requirements file not found: $REQUIREMENTS_FILE_FULL_PATH" && exit 1

echo "Requirements file found. Contents of $REQUIREMENTS_FILE_FULL_PATH:"
cat "$REQUIREMENTS_FILE_FULL_PATH"

setup_snowflake_env

log_info "Creating stage $STAGE_NAME..."
snow stage create "$STAGE_NAME" --schema="$SCHEMA_NAME" --database="$DATABASE_NAME"

# Process packages
while IFS= read -r package || [[ -n "$package" ]]; do
    [[ $package =~ ^[[:space:]]*$ || $package == \#* ]] && continue
    
    ORIGINAL_PACKAGE="$package"
    NORMALIZED_PACKAGE=$(normalize_package_name "$package")
    
    echo "Creating Snowpark package for $ORIGINAL_PACKAGE..."
    PACKAGE_OUTPUT=$(snow snowpark package create "$ORIGINAL_PACKAGE" --allow-shared-libraries 2>&1)
    CREATION_STATUS=$?
    
    # Explicitly echo the package creation output
    echo "$PACKAGE_OUTPUT"
    
    if echo "$PACKAGE_OUTPUT" | grep -q "depends on the following"; then
        DEPENDENCIES=$(echo "$PACKAGE_OUTPUT" | sed -n '/depends on the following/,/Successfully created package/p')
        append_to_environment_yml "$ORIGINAL_PACKAGE" "$DEPENDENCIES"
    fi
    
    if [ $CREATION_STATUS -eq 0 ]; then
        echo "Successfully created package for $ORIGINAL_PACKAGE"
        FULL_STAGE_NAME="${DATABASE_NAME}.${SCHEMA_NAME}.${STAGE_NAME}"
        ZIP_FILE="${NORMALIZED_PACKAGE}.zip"
        
        UPLOAD_CMD="snow snowpark package upload -f $ZIP_FILE -s $FULL_STAGE_NAME"
        [ "$OVERWRITE" = true ] && UPLOAD_CMD="$UPLOAD_CMD -o"
        
        echo "Executing: $UPLOAD_CMD"
        if eval "$UPLOAD_CMD"; then
            echo "Successfully uploaded $ZIP_FILE to stage $FULL_STAGE_NAME"
            rm "$ZIP_FILE"
        else
            echo "Error: Failed to upload $ZIP_FILE to stage $FULL_STAGE_NAME"
        fi
    else
        echo "Error: Failed to create package for $ORIGINAL_PACKAGE"
        continue
    fi
done < "$REQUIREMENTS_FILE_FULL_PATH"

echo "Packages created and staged to Snowflake."
echo "Remember to add the packages to imports in your procedure or function definition."