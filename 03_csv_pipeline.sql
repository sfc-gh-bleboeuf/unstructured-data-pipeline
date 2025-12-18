-- =============================
-- CSV PROCESSING PIPELINE
-- =============================
-- This file creates the infrastructure for handling CSV files from the document stage
-- Uses Snowflake's INFER_SCHEMA to dynamically detect and load CSV data
--
-- PIPELINE FLOW:
-- 1. CSV files land in S3 → Directory Table auto-refreshes → Stream captures new files
-- 2. csv_ingest_task triggers → Detects CSV files, infers schema, loads to tables
-- 3. CSV data is stored in structured tables with metadata tracking
--
-- SUPPORTED FORMATS: .csv

USE ROLE ACCOUNTADMIN;
USE DATABASE document_db;
USE SCHEMA s3_documents;
USE WAREHOUSE COMPUTE_WH;

-- =============================
-- FILE FORMAT FOR CSV
-- =============================
-- Create a file format for CSV files with header parsing enabled
CREATE OR REPLACE FILE FORMAT document_db.s3_documents.csv_infer_format
    TYPE = CSV
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'CSV format with header parsing for schema inference';

-- =============================
-- TABLES
-- =============================

-- Table to track CSV files that have been processed
CREATE OR REPLACE TABLE document_db.s3_documents.csv_file_registry (
    registry_id VARCHAR(150) PRIMARY KEY,
    file_path VARCHAR(1000) NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_size NUMBER,
    target_table_name VARCHAR(500),
    column_count NUMBER,
    row_count NUMBER,
    inferred_schema VARIANT,
    processing_status VARCHAR(50) DEFAULT 'pending',  -- pending, processing, completed, error
    error_message VARCHAR(2000),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    processed_timestamp TIMESTAMP
)
COMMENT = 'Registry tracking CSV files processed through the pipeline';

-- Master table to store all CSV data with flexible schema
-- Uses VARIANT for dynamic column storage
CREATE OR REPLACE TABLE document_db.s3_documents.csv_data_master (
    record_id VARCHAR(150) PRIMARY KEY,
    registry_id VARCHAR(150) NOT NULL,
    file_path VARCHAR(1000) NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    row_number NUMBER,
    row_data VARIANT,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (registry_id) REFERENCES document_db.s3_documents.csv_file_registry(registry_id)
)
COMMENT = 'Master table storing all CSV data with dynamic schema support';

-- Table to store inferred column metadata for each CSV
CREATE OR REPLACE TABLE document_db.s3_documents.csv_column_metadata (
    metadata_id VARCHAR(150) PRIMARY KEY,
    registry_id VARCHAR(150) NOT NULL,
    column_name VARCHAR(500),
    column_type VARCHAR(100),
    column_order NUMBER,
    nullable BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (registry_id) REFERENCES document_db.s3_documents.csv_file_registry(registry_id)
)
COMMENT = 'Stores inferred column metadata for each CSV file';


-- =============================
-- PROCEDURE: REGISTER CSV FILES
-- =============================
-- Registers new CSV files from the stream for processing
CREATE OR REPLACE PROCEDURE document_db.s3_documents.register_csv_files()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    registered_count INTEGER := 0;
    stream_count INTEGER := 0;
BEGIN
    -- Capture stream data into temp table (stream can only be read once)
    CREATE OR REPLACE TEMPORARY TABLE document_db.s3_documents.temp_csv_stream AS
    SELECT 
        relative_path,
        size,
        file_url,
        METADATA$ACTION
    FROM document_db.s3_documents.new_documents_stream
    WHERE METADATA$ACTION = 'INSERT'
        AND relative_path IS NOT NULL
        AND relative_path != ''
        AND UPPER(relative_path) LIKE '%.CSV';
    
    SELECT COUNT(*) INTO stream_count FROM document_db.s3_documents.temp_csv_stream;
    
    IF (stream_count = 0) THEN
        DROP TABLE IF EXISTS document_db.s3_documents.temp_csv_stream;
        RETURN 'No new CSV files in stream';
    END IF;
    
    -- Register CSV files that haven't been processed yet
    INSERT INTO document_db.s3_documents.csv_file_registry
    (registry_id, file_path, file_name, file_size, processing_status)
    SELECT
        CONCAT('CSV_', ABS(HASH(relative_path)), '_', 
               REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING, ' ', '_'), ':', '')) as registry_id,
        relative_path as file_path,
        REGEXP_SUBSTR(relative_path, '[^/]+$') as file_name,
        size as file_size,
        'pending' as processing_status
    FROM document_db.s3_documents.temp_csv_stream
    WHERE relative_path NOT IN (
        SELECT file_path FROM document_db.s3_documents.csv_file_registry WHERE file_path IS NOT NULL
    );
    
    registered_count := SQLROWCOUNT;
    
    DROP TABLE IF EXISTS document_db.s3_documents.temp_csv_stream;
    
    RETURN 'Registered ' || registered_count || ' new CSV files for processing';
END;
$$;


-- =============================
-- PROCEDURE: INFER AND LOAD CSV
-- =============================
-- Main procedure that infers schema and loads CSV data
CREATE OR REPLACE PROCEDURE document_db.s3_documents.process_csv_files()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_csvs'
AS
$$
import json
from snowflake.snowpark import Session

def process_csvs(session: Session) -> str:
    """
    Process pending CSV files by inferring schema and loading data.
    """
    
    STAGE_NAME = "@document_db.s3_documents.document_stage"
    FILE_FORMAT = "document_db.s3_documents.csv_infer_format"
    
    processed_count = 0
    error_count = 0
    total_rows = 0
    
    try:
        # Get pending CSV files
        pending_query = """
        SELECT 
            registry_id,
            file_path,
            file_name,
            file_size
        FROM document_db.s3_documents.csv_file_registry
        WHERE processing_status = 'pending'
        ORDER BY created_timestamp ASC
        LIMIT 10
        """
        
        pending_files = session.sql(pending_query).collect()
        
        if not pending_files:
            return "No pending CSV files to process"
        
        for csv_file in pending_files:
            registry_id = csv_file['REGISTRY_ID']
            file_path = csv_file['FILE_PATH']
            file_name = csv_file['FILE_NAME']
            
            try:
                # Update status to processing
                session.sql(f"""
                    UPDATE document_db.s3_documents.csv_file_registry
                    SET processing_status = 'processing', 
                        processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                # Step 1: Infer schema from the CSV file
                infer_query = f"""
                SELECT *
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '{STAGE_NAME}/{file_path}',
                        FILE_FORMAT => '{FILE_FORMAT}'
                    )
                )
                ORDER BY ORDER_ID
                """
                
                schema_result = session.sql(infer_query).collect()
                
                if not schema_result:
                    raise Exception("Could not infer schema from CSV file")
                
                # Store column metadata
                column_count = len(schema_result)
                schema_json = []
                
                for col in schema_result:
                    col_name = col['COLUMN_NAME']
                    col_type = col['TYPE']
                    col_order = col['ORDER_ID']
                    nullable = col['NULLABLE']
                    
                    # Insert column metadata
                    escaped_col_name = col_name.replace("'", "''")
                    metadata_id = f"{registry_id}_COL_{col_order}"
                    
                    session.sql(f"""
                        INSERT INTO document_db.s3_documents.csv_column_metadata
                        (metadata_id, registry_id, column_name, column_type, column_order, nullable)
                        VALUES (
                            '{metadata_id}',
                            '{registry_id}',
                            '{escaped_col_name}',
                            '{col_type}',
                            {col_order},
                            {nullable}
                        )
                    """).collect()
                    
                    schema_json.append({
                        'column_name': col_name,
                        'type': col_type,
                        'order': col_order,
                        'nullable': nullable
                    })
                
                # Step 2: Generate a sanitized table name for this CSV
                # Clean the filename to create a valid table name
                table_name_base = file_name.rsplit('.', 1)[0]  # Remove extension
                table_name_base = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name_base)
                table_name_base = table_name_base.upper()
                if table_name_base[0].isdigit():
                    table_name_base = 'T_' + table_name_base
                target_table = f"CSV_{table_name_base}"
                
                # Step 3: Create a dynamic table using the inferred schema
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS document_db.s3_documents.{target_table}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                        'COLUMN_NAME', COLUMN_NAME,
                        'TYPE', TYPE,
                        'NULLABLE', NULLABLE
                    ))
                    WITHIN GROUP (ORDER BY ORDER_ID)
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '{STAGE_NAME}/{file_path}',
                            FILE_FORMAT => '{FILE_FORMAT}'
                        )
                    )
                )
                """
                
                try:
                    session.sql(create_table_query).collect()
                except Exception as create_err:
                    # Table might already exist with different schema, that's OK
                    print(f"Note: {str(create_err)[:100]}")
                
                # Step 4: Load data into the target table using COPY INTO with MATCH_BY_COLUMN_NAME
                copy_query = f"""
                COPY INTO document_db.s3_documents.{target_table}
                FROM '{STAGE_NAME}/{file_path}'
                FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
                """
                
                copy_result = session.sql(copy_query).collect()
                
                # Get row count from copy result
                rows_loaded = 0
                if copy_result and len(copy_result) > 0:
                    rows_loaded = copy_result[0]['rows_loaded'] if 'rows_loaded' in copy_result[0].as_dict() else 0
                
                # Step 5: Also load into the master table with VARIANT for flexible querying
                # First, load into a temp table then transform
                load_master_query = f"""
                INSERT INTO document_db.s3_documents.csv_data_master
                (record_id, registry_id, file_path, file_name, row_number, row_data)
                SELECT
                    CONCAT('{registry_id}', '_ROW_', ROW_NUMBER() OVER (ORDER BY 1)),
                    '{registry_id}',
                    '{file_path.replace("'", "''")}',
                    '{file_name.replace("'", "''")}',
                    ROW_NUMBER() OVER (ORDER BY 1),
                    OBJECT_CONSTRUCT(*)
                FROM document_db.s3_documents.{target_table}
                WHERE NOT EXISTS (
                    SELECT 1 FROM document_db.s3_documents.csv_data_master 
                    WHERE registry_id = '{registry_id}'
                )
                """
                
                try:
                    session.sql(load_master_query).collect()
                except Exception as master_err:
                    print(f"Master table load note: {str(master_err)[:100]}")
                
                # Count actual rows in target table
                count_query = f"SELECT COUNT(*) as cnt FROM document_db.s3_documents.{target_table}"
                count_result = session.sql(count_query).collect()
                row_count = count_result[0]['CNT'] if count_result else 0
                
                # Update registry with success
                escaped_schema = json.dumps(schema_json).replace("'", "''")
                session.sql(f"""
                    UPDATE document_db.s3_documents.csv_file_registry
                    SET processing_status = 'completed',
                        target_table_name = '{target_table}',
                        column_count = {column_count},
                        row_count = {row_count},
                        inferred_schema = PARSE_JSON('{escaped_schema}'),
                        processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                processed_count += 1
                total_rows += row_count
                
            except Exception as e:
                error_msg = str(e)[:1900].replace("'", "''")
                session.sql(f"""
                    UPDATE document_db.s3_documents.csv_file_registry
                    SET processing_status = 'error',
                        error_message = '{error_msg}',
                        processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                error_count += 1
        
        return f"Processed: {processed_count}, Rows: {total_rows}, Errors: {error_count}"
        
    except Exception as e:
        return f"Error: {str(e)[:200]}"
$$;


-- =============================
-- COMBINED PROCEDURE: REGISTER AND PROCESS
-- =============================
-- Single procedure that registers and processes CSV files
CREATE OR REPLACE PROCEDURE document_db.s3_documents.ingest_csv_files()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    register_result STRING;
    process_result STRING;
BEGIN
    -- Step 1: Register any new CSV files from the stream
    CALL document_db.s3_documents.register_csv_files() INTO register_result;
    
    -- Step 2: Process registered CSV files
    CALL document_db.s3_documents.process_csv_files() INTO process_result;
    
    RETURN register_result || ' | ' || process_result;
END;
$$;


-- =============================
-- TASK: CSV INGESTION
-- =============================
-- Task runs after the main document pipeline parse task
-- CSV files are registered by parse_new_documents() procedure
-- This task only processes the registered CSV files
CREATE OR REPLACE TASK document_db.s3_documents.csv_ingest_task
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Process registered CSV files - infer schema and load to structured tables'
    AFTER document_db.s3_documents.parse_documents_task
AS
    CALL document_db.s3_documents.process_csv_files();

-- Resume the task
-- =============================
-- VIEWS FOR CSV DATA ACCESS
-- =============================

-- View to show all processed CSV files with their schemas
CREATE OR REPLACE VIEW document_db.s3_documents.csv_processing_status AS
SELECT 
    r.registry_id,
    r.file_path,
    r.file_name,
    r.file_size,
    ROUND(r.file_size / 1024.0, 2) as size_kb,
    r.target_table_name,
    r.column_count,
    r.row_count,
    r.processing_status,
    r.error_message,
    r.created_timestamp,
    r.processed_timestamp,
    r.inferred_schema
FROM document_db.s3_documents.csv_file_registry r
ORDER BY r.created_timestamp DESC;

-- View to show column details for all CSV files
CREATE OR REPLACE VIEW document_db.s3_documents.csv_schema_details AS
SELECT 
    r.file_name,
    r.target_table_name,
    m.column_name,
    m.column_type,
    m.column_order,
    m.nullable,
    r.processing_status
FROM document_db.s3_documents.csv_file_registry r
JOIN document_db.s3_documents.csv_column_metadata m
    ON r.registry_id = m.registry_id
ORDER BY r.file_name, m.column_order;

-- View to query all CSV data from the master table with flattened columns
CREATE OR REPLACE VIEW document_db.s3_documents.csv_data_flattened AS
SELECT 
    d.record_id,
    d.file_name,
    d.file_path,
    d.row_number,
    f.key as column_name,
    f.value::STRING as column_value,
    d.created_timestamp
FROM document_db.s3_documents.csv_data_master d,
     LATERAL FLATTEN(INPUT => d.row_data) f
ORDER BY d.file_name, d.row_number, f.key;


-- =============================
-- UPDATE PIPELINE STATUS VIEW
-- =============================
-- Add CSV processing status to the main pipeline status view
CREATE OR REPLACE VIEW document_db.s3_documents.pipeline_status AS
SELECT 
    'Stream' as stage,
    COUNT(*) as count,
    NULL as status
FROM document_db.s3_documents.new_documents_stream
UNION ALL
SELECT 
    'Parsed Documents' as stage,
    COUNT(*) as count,
    status
FROM document_db.s3_documents.parsed_documents
GROUP BY status
UNION ALL
SELECT 
    'Classifications' as stage,
    COUNT(*) as count,
    CASE 
        WHEN TRY_PARSE_JSON(document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(document_class):labels[0]::STRING
        ELSE document_class
    END as status
FROM document_db.s3_documents.document_classifications
GROUP BY status
UNION ALL
SELECT 
    'Extractions' as stage,
    COUNT(DISTINCT document_id) as count,
    'extracted' as status
FROM document_db.s3_documents.document_extractions
UNION ALL
SELECT 
    'Chunks' as stage,
    COUNT(*) as count,
    'chunked' as status
FROM document_db.s3_documents.document_chunks
UNION ALL
SELECT 
    'Large Documents' as stage,
    COUNT(*) as count,
    split_status as status
FROM document_db.s3_documents.large_document_registry
GROUP BY split_status
UNION ALL
SELECT 
    'CSV Files' as stage,
    COUNT(*) as count,
    processing_status as status
FROM document_db.s3_documents.csv_file_registry
GROUP BY processing_status;


-- =============================
-- VALIDATION QUERIES
-- =============================

-- Check CSV files in the stage
SELECT relative_path, size, last_modified
FROM DIRECTORY(@document_db.s3_documents.document_stage)
WHERE UPPER(relative_path) LIKE '%.CSV'
ORDER BY last_modified DESC;

-- Check CSV file registry
SELECT * FROM document_db.s3_documents.csv_file_registry;

-- Check column metadata
SELECT * FROM document_db.s3_documents.csv_column_metadata;

-- Check CSV processing status
SELECT * FROM document_db.s3_documents.csv_processing_status;

-- Check schema details
SELECT * FROM document_db.s3_documents.csv_schema_details;

-- View sample data from master table
SELECT * FROM document_db.s3_documents.csv_data_master LIMIT 100;

-- View flattened CSV data
SELECT * FROM document_db.s3_documents.csv_data_flattened LIMIT 100;

;

-- Verify task status
DESC TASK document_db.s3_documents.csv_ingest_task;


-- =============================
-- MANUAL EXECUTION (FOR TESTING)
-- =============================
-- Use these commands to manually run the CSV pipeline:

-- Step 1: Register any CSV files found in the stage (reads from directory, not stream)
-- CALL document_db.s3_documents.register_csv_files();

-- Step 2: Process registered CSV files (infer schema and load)
-- CALL document_db.s3_documents.process_csv_files();

-- Or run both together:
-- CALL document_db.s3_documents.ingest_csv_files();

-- Step 3: Check processing status
-- SELECT * FROM document_db.s3_documents.csv_processing_status;

-- Step 4: Query data from a specific CSV table
-- SELECT * FROM document_db.s3_documents.CSV_<YOUR_FILE_NAME> LIMIT 10;

-- Step 5: Query all CSV data via the master table
-- SELECT * FROM document_db.s3_documents.csv_data_flattened WHERE file_name = 'your_file.csv';


-- =============================
-- HELPER: MANUAL CSV FILE REGISTRATION
-- =============================
-- Use this procedure to manually register CSV files from the directory
-- (useful when stream has already been consumed)
CREATE OR REPLACE PROCEDURE document_db.s3_documents.register_csv_files_from_directory()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    registered_count INTEGER := 0;
BEGIN
    -- Register CSV files from directory that haven't been processed yet
    INSERT INTO document_db.s3_documents.csv_file_registry
    (registry_id, file_path, file_name, file_size, processing_status)
    SELECT
        CONCAT('CSV_', ABS(HASH(relative_path)), '_', 
               REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING, ' ', '_'), ':', '')) as registry_id,
        relative_path as file_path,
        REGEXP_SUBSTR(relative_path, '[^/]+$') as file_name,
        size as file_size,
        'pending' as processing_status
    FROM DIRECTORY(@document_db.s3_documents.document_stage)
    WHERE UPPER(relative_path) LIKE '%.CSV'
        AND relative_path NOT IN (
            SELECT file_path FROM document_db.s3_documents.csv_file_registry WHERE file_path IS NOT NULL
        );
    
    registered_count := SQLROWCOUNT;
    
    RETURN 'Registered ' || registered_count || ' CSV files from directory';
END;
$$;

ALTER TASK document_db.s3_documents.parse_documents_task SUSPEND;