#!/bin/bash

# Default values
CONNECTION="${SNOWFLAKE_CONNECTION:-datascience_techstyle_mmm}"
SF_NBS="${SF_NBS:-sf_nbs}"
DATABASE="${DATABASE:-DATASCIENCE}"
SCHEMA="${SCHEMA:-TECHSTYLE_MMM}"
BRANCH="${BRANCH:-mmm}"
WAREHOUSE="${WAREHOUSE:-DS_WH_XS}"
REPOSITORY="${REPOSITORY:-DS_TEMPLATE}"
USE_CONTAINER_RUNTIME=false
COMPUTE_POOL="${COMPUTE_POOL:-SIMPLE_DS_POOL}"
MIN_NODES="${MIN_NODES:-1}"
MAX_NODES="${MAX_NODES:-2}"
INSTANCE_FAMILY="${INSTANCE_FAMILY:-CPU_X64_L}"

# Parse command-line arguments
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
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done


# Function to execute SQL commands using snow sql
execute_sql() {
    # echo "Executing SQL command: $1"
    snow sql -c $CONNECTION -q "$1"
}

# Fetch latest from Git repository
execute_sql "
ALTER GIT REPOSITORY ${DATABASE}.${SCHEMA}.${REPOSITORY} FETCH;
"

# Create or configure compute pool if using container runtime
if [ "$USE_CONTAINER_RUNTIME" = true ] ; then
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

# Create or replace notebooks from sf_nbs folder
for notebook in $SF_NBS/*.ipynb; do
    filename=$(basename "$notebook")
    identifier="${DATABASE}.${SCHEMA}.${filename%.*}"
    file_path="@${DATABASE}.${SCHEMA}.${REPOSITORY}/branches/${BRANCH}/${SF_NBS}/$filename"
    echo "Creating or replacing notebook: $file_path"
    
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

    # # Add live version TODO: look into this more to figure out when I need this versus not
    # execute_sql "
    # ALTER NOTEBOOK ${identifier} ADD LIVE VERSION FROM LAST;
    # "

    echo "Created or replaced notebook: $identifier"
done

echo "Notebook creation process completed."

# bash create_notebooks_git_to_sf.sh --use_container_runtime --compute_pool SIMPLE_DS_POOL --min_nodes 1 --max_nodes 2 --instance_family CPU_X64_S --use_container_runtime true