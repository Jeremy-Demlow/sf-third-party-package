#!/bin/bash

# Default values
DEFAULT_STAGE_NAME="PYTHON_PACKAGES"
DEFAULT_SCHEMA_NAME="TECHSTYLE_MMM"
DEFAULT_DATABASE_NAME="DATASCIENCE"
DEFAULT_REQUIREMENTS_FILE="extra-requirements.txt"
DEFAULT_OVERWRITE=true

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [options]

Options:
    -s, --stage        Stage name (default: $DEFAULT_STAGE_NAME)
    -c, --schema       Schema name (default: $DEFAULT_SCHEMA_NAME)
    -d, --database     Database name (default: $DEFAULT_DATABASE_NAME)
    -r, --requirements Requirements file path (default: $DEFAULT_REQUIREMENTS_FILE)
    -o, --overwrite    Overwrite existing files in stage (default: true)
    -h, --help        Show this help message

Example: 
    $0 -s Wheels -c techstyle -d datascience -r extra-requirements.txt -o
    $0 --stage Wheels --schema techstyle --database datascience --requirements extra-requirements.txt --overwrite
EOF
    exit 1
}

# Function to normalize package names
normalize_package_name() {
    local package="$1"
    # Remove version specifier if present and convert to underscore format
    local base_package=$(echo "$package" | sed -E 's/([a-zA-Z0-9.-]+)(==|>=|<=|!=|~=|>|<)?.*/\1/')
    # Convert hyphens to underscores
    echo "$base_package" | tr '-' '_'
}

clean_version_specifier() {
    local dep="$1"
    # Extract package name and version specifier
    local package_name=$(echo "$dep" | sed -E 's/([a-zA-Z0-9._-]+).*/\1/' | tr '[:upper:]' '[:lower:]')
    local version_part=$(echo "$dep" | sed -E 's/[a-zA-Z0-9._-]+//')
    
    # Skip if no version specified
    if [ -z "$version_part" ]; then
        echo "${package_name}=*"
        return
    fi
    
    # Check if there are version constraints using grep
    if echo "$version_part" | grep -q '[<>]'; then
        # If there are complex version constraints, use =*
        echo "${package_name}=*"
    else
        # For simple version numbers, use exact version
        # Extract the version number
        local version=$(echo "$version_part" | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')
        if [ -n "$version" ]; then
            echo "${package_name}=${version}"
        else
            echo "${package_name}=*"
        fi
    fi
}

append_to_environment_yml() {
    local main_package="$1"
    local dependencies="$2"
    local env_file="sf_nbs/environment.yml"
    
    # Create environment.yml with header if it doesn't exist
    if [ ! -f "$env_file" ]; then
        cat > "$env_file" << EOF
name: app_environment
channels:
  - snowflake
dependencies:
EOF
    fi
    
    echo "Appending dependencies to $env_file..."
    
    # Parse and add dependencies
    echo "$dependencies" | while IFS= read -r dep; do
        # Skip empty lines and header lines
        [[ -z "$dep" ]] && continue
        [[ "$dep" == *"following"* ]] && continue
        [[ "$dep" == *"Anaconda"* ]] && continue
        [[ "$dep" == *"included"* ]] && continue
        
        # Skip if this is the main package being uploaded
        if [[ "$dep" == *"$main_package"* ]]; then
            continue
        fi
        
        # Clean the dependency version specifiers
        local cleaned_dep=$(clean_version_specifier "$dep")
        
        # Check if cleaned dependency already exists
        if ! grep -q "  - $cleaned_dep" "$env_file"; then
            # Add dependency with proper indentation
            echo "  - $cleaned_dep" >> "$env_file"
        fi
    done
    
    echo "Updated $env_file with new dependencies"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -s|--stage)
            STAGE_NAME="$2"
            shift 2
            ;;
        -c|--schema)
            SCHEMA_NAME="$2"
            shift 2
            ;;
        -d|--database)
            DATABASE_NAME="$2"
            shift 2
            ;;
        -r|--requirements)
            REQUIREMENTS_FILE="$2"
            shift 2
            ;;
        -o|--overwrite)
            OVERWRITE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Set default values if not provided
STAGE_NAME=${STAGE_NAME:-$DEFAULT_STAGE_NAME}
SCHEMA_NAME=${SCHEMA_NAME:-$DEFAULT_SCHEMA_NAME}
DATABASE_NAME=${DATABASE_NAME:-$DEFAULT_DATABASE_NAME}
REQUIREMENTS_FILE=${REQUIREMENTS_FILE:-$DEFAULT_REQUIREMENTS_FILE}
OVERWRITE=${OVERWRITE:-$DEFAULT_OVERWRITE}

# Get the full path of the requirements file
REQUIREMENTS_FILE_FULL_PATH=$(realpath "$REQUIREMENTS_FILE")

# Print configuration
echo "Configuration:"
echo "  Stage Name:        $STAGE_NAME"
echo "  Schema Name:       $SCHEMA_NAME"
echo "  Database Name:     $DATABASE_NAME"
echo "  Requirements File: $REQUIREMENTS_FILE_FULL_PATH"
echo "  Overwrite:        $OVERWRITE"
echo

# Check if the requirements file exists
if [ ! -f "$REQUIREMENTS_FILE_FULL_PATH" ]; then
    echo "Error: Requirements file $REQUIREMENTS_FILE_FULL_PATH not found."
    exit 1
fi

echo "Requirements file found. Contents of $REQUIREMENTS_FILE_FULL_PATH:"
cat "$REQUIREMENTS_FILE_FULL_PATH"

# Create the stage in Snowflake if it doesn't exist
echo "Creating stage $STAGE_NAME in schema $SCHEMA_NAME and database $DATABASE_NAME..."
snow stage create "$STAGE_NAME" --schema="$SCHEMA_NAME" --database="$DATABASE_NAME"

# Read packages from the requirements file
while IFS= read -r package || [[ -n "$package" ]]; do
    # Skip empty lines and comments
    [[ $package =~ ^[[:space:]]*$ || $package == \#* ]] && continue

    # Get both the original package name and the normalized version
    ORIGINAL_PACKAGE="$package"
    NORMALIZED_PACKAGE=$(normalize_package_name "$package")

    echo "Creating Snowpark package for $ORIGINAL_PACKAGE..."
    
    # Capture the output of the package creation command
    PACKAGE_OUTPUT=$(snow snowpark package create "$ORIGINAL_PACKAGE" --allow-shared-libraries 2>&1)
    CREATION_STATUS=$?
    
    echo "$PACKAGE_OUTPUT"
    
    # Check if the output contains dependency information
    if echo "$PACKAGE_OUTPUT" | grep -q "depends on the following"; then
        # Extract dependencies section
        DEPENDENCIES=$(echo "$PACKAGE_OUTPUT" | sed -n '/depends on the following/,/Successfully created package/p')
        append_to_environment_yml "$ORIGINAL_PACKAGE" "$DEPENDENCIES"
    fi
    
    if [ $CREATION_STATUS -eq 0 ]; then
        echo "Successfully created package for $ORIGINAL_PACKAGE"
        
        # Construct the fully qualified stage name
        FULL_STAGE_NAME="${DATABASE_NAME}.${SCHEMA_NAME}.${STAGE_NAME}"
        
        # Look for the zip file using the normalized name
        ZIP_FILE="${NORMALIZED_PACKAGE}.zip"
        
        # Construct the upload command
        UPLOAD_CMD="snow snowpark package upload -f $ZIP_FILE -s $FULL_STAGE_NAME"
        
        # Add overwrite flag if specified
        if [ "$OVERWRITE" = true ]; then
            UPLOAD_CMD="$UPLOAD_CMD -o"
        fi
        
        echo "Executing: $UPLOAD_CMD"
        
        # Upload the package to the stage
        if eval "$UPLOAD_CMD"; then
            echo "Successfully uploaded $ZIP_FILE to stage $FULL_STAGE_NAME"
            
            # Clean up the zip file
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