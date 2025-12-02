-- =============================
-- EXCEL TO PDF CONVERSION PIPELINE
-- =============================
-- This file creates the infrastructure for handling Excel files (.xlsx, .xlsm)
-- by converting them to PDF format for processing through the existing document pipeline.
--
-- PIPELINE FLOW:
-- 1. Excel files land in S3 → Directory Table auto-refreshes → Stream captures new files
-- 2. parse_documents_task registers Excel files to excel_document_registry
-- 3. convert_excel_to_pdf_task triggers → Python procedure converts all sheets to PDF
-- 4. Converted PDFs are uploaded back to the SAME stage (converted_excel folder)
-- 5. The PDFs trigger the stream and get processed normally by the AI pipeline!
--
-- SUPPORTED FORMATS: .xlsx, .xlsm

USE ROLE ACCOUNTADMIN;
USE DATABASE document_db;
USE SCHEMA s3_documents;
USE WAREHOUSE COMPUTE_WH;

-- =============================
-- TABLES
-- =============================

-- Table to track Excel documents that need PDF conversion
CREATE OR REPLACE TABLE document_db.s3_documents.excel_document_registry (
  registry_id VARCHAR(150) PRIMARY KEY,
  original_file_path VARCHAR(1000) NOT NULL,
  original_file_name VARCHAR(500) NOT NULL,
  original_file_size NUMBER,
  sheet_count NUMBER,
  conversion_status VARCHAR(500) DEFAULT 'pending',  -- pending, converting, converted, error
  converted_pdf_path VARCHAR(1000),
  error_message VARCHAR(2000),
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  processed_timestamp TIMESTAMP
)
COMMENT = 'Registry tracking Excel documents (.xlsx, .xlsm) that require PDF conversion';

-- Table to store metadata about converted Excel sheets
CREATE OR REPLACE TABLE document_db.s3_documents.excel_sheet_metadata (
  sheet_id VARCHAR(150) PRIMARY KEY,
  registry_id VARCHAR(150) NOT NULL,
  sheet_name VARCHAR(500) NOT NULL,
  sheet_index NUMBER NOT NULL,
  row_count NUMBER,
  column_count NUMBER,
  has_data BOOLEAN DEFAULT TRUE,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (registry_id) REFERENCES document_db.s3_documents.excel_document_registry(registry_id)
)
COMMENT = 'Stores metadata about individual sheets from converted Excel files';


-- =============================
-- SNOWPARK PYTHON PROCEDURE - CONVERT EXCEL TO PDF
-- =============================
-- This procedure is part of the EVENT-DRIVEN pipeline:
-- 1. Queries excel_document_registry for pending Excel files
-- 2. Reads each Excel file using SnowflakeFile
-- 3. Extracts all sheets and converts them to a single PDF
-- 4. Uploads the PDF back to the SAME external stage using session.file.put()
-- 5. The PDF triggers the stream and gets processed normally!
--
-- This ensures ALL Excel documents that land in S3 get processed through the AI pipeline.

CREATE OR REPLACE PROCEDURE document_db.s3_documents.convert_excel_to_pdf()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'openpyxl', 'reportlab', 'pandas')
HANDLER = 'convert_excel_files'
AS
$$
import io
import os
import tempfile
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.files import SnowflakeFile
import openpyxl
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT


def convert_excel_files(session: Session) -> str:
    """
    Convert Excel files (.xlsx, .xlsm) to PDF format and upload back to stage.
    The converted PDFs will trigger the stream and be processed by the normal AI pipeline.
    
    This is part of the EVENT-DRIVEN pipeline - runs automatically via task.
    """
    
    STAGE_NAME = "document_db.s3_documents.document_stage"
    EXTERNAL_STAGE = f"@{STAGE_NAME}"
    CONVERTED_FOLDER = "converted_excel"
    
    processed_count = 0
    converted_count = 0
    error_count = 0
    
    try:
        # Query for pending Excel documents
        pending_query = """
        SELECT 
            registry_id,
            original_file_path,
            original_file_name,
            original_file_size
        FROM document_db.s3_documents.excel_document_registry
        WHERE conversion_status = 'pending'
        ORDER BY created_timestamp ASC
        LIMIT 10
        """
        
        pending_docs = session.sql(pending_query).collect()
        
        if not pending_docs:
            return "No pending Excel documents to convert"
        
        for doc in pending_docs:
            registry_id = doc['REGISTRY_ID']
            file_path = doc['ORIGINAL_FILE_PATH']
            file_name = doc['ORIGINAL_FILE_NAME']
            
            try:
                # Update status to converting
                session.sql(f"""
                    UPDATE document_db.s3_documents.excel_document_registry
                    SET conversion_status = 'converting', processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                # Build the full stage path for SnowflakeFile
                stage_file_path = f"{EXTERNAL_STAGE}/{file_path}"
                
                # Read Excel file using SnowflakeFile
                with SnowflakeFile.open(stage_file_path, 'rb', require_scoped_url=False) as sf:
                    excel_bytes = sf.read()
                
                # Load workbook from bytes
                excel_stream = io.BytesIO(excel_bytes)
                workbook = openpyxl.load_workbook(excel_stream, data_only=True)
                sheet_names = workbook.sheetnames
                sheet_count = len(sheet_names)
                
                # Update sheet count in registry
                session.sql(f"""
                    UPDATE document_db.s3_documents.excel_document_registry
                    SET sheet_count = {sheet_count}
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                # Create PDF in memory using temp directory
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # Generate PDF filename
                    base_name = file_name.rsplit('.', 1)[0]
                    pdf_name = f"{base_name}_converted.pdf"
                    pdf_path = os.path.join(tmp_dir, pdf_name)
                    
                    # Create PDF document with landscape orientation for better table fit
                    doc = SimpleDocTemplate(
                        pdf_path,
                        pagesize=landscape(letter),
                        rightMargin=0.5*inch,
                        leftMargin=0.5*inch,
                        topMargin=0.5*inch,
                        bottomMargin=0.5*inch
                    )
                    
                    # Build PDF content
                    elements = []
                    styles = getSampleStyleSheet()
                    
                    # Custom styles
                    title_style = ParagraphStyle(
                        'CustomTitle',
                        parent=styles['Heading1'],
                        fontSize=16,
                        alignment=TA_CENTER,
                        spaceAfter=20
                    )
                    
                    sheet_title_style = ParagraphStyle(
                        'SheetTitle',
                        parent=styles['Heading2'],
                        fontSize=14,
                        alignment=TA_LEFT,
                        spaceAfter=10,
                        textColor=colors.darkblue
                    )
                    
                    # Document title
                    elements.append(Paragraph(f"Excel Document: {file_name}", title_style))
                    elements.append(Paragraph(f"Converted on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
                    elements.append(Paragraph(f"Total Sheets: {sheet_count}", styles['Normal']))
                    elements.append(Spacer(1, 20))
                    
                    # Process each sheet
                    for sheet_idx, sheet_name in enumerate(sheet_names):
                        worksheet = workbook[sheet_name]
                        
                        # Get sheet dimensions
                        max_row = worksheet.max_row or 0
                        max_col = worksheet.max_column or 0
                        
                        # Store sheet metadata
                        sheet_id = f"{registry_id}_SHEET_{sheet_idx + 1}"
                        escaped_sheet_name = sheet_name.replace("'", "''")
                        has_data = max_row > 0 and max_col > 0
                        
                        session.sql(f"""
                            INSERT INTO document_db.s3_documents.excel_sheet_metadata
                            (sheet_id, registry_id, sheet_name, sheet_index, row_count, column_count, has_data)
                            VALUES (
                                '{sheet_id}', '{registry_id}', '{escaped_sheet_name}',
                                {sheet_idx + 1}, {max_row}, {max_col}, {has_data}
                            )
                        """).collect()
                        
                        # Add page break between sheets (except first)
                        if sheet_idx > 0:
                            elements.append(PageBreak())
                        
                        # Sheet header
                        elements.append(Paragraph(f"Sheet {sheet_idx + 1}: {sheet_name}", sheet_title_style))
                        elements.append(Paragraph(f"Rows: {max_row}, Columns: {max_col}", styles['Normal']))
                        elements.append(Spacer(1, 10))
                        
                        if max_row == 0 or max_col == 0:
                            elements.append(Paragraph("(Empty sheet)", styles['Italic']))
                            continue
                        
                        # Read data using pandas for better handling
                        excel_stream.seek(0)  # Reset stream position
                        try:
                            df = pd.read_excel(excel_stream, sheet_name=sheet_name, header=None)
                            
                            if df.empty:
                                elements.append(Paragraph("(No data in sheet)", styles['Italic']))
                                continue
                            
                            # Limit rows for very large sheets (to fit in PDF reasonably)
                            max_rows_per_sheet = 500
                            if len(df) > max_rows_per_sheet:
                                df = df.head(max_rows_per_sheet)
                                elements.append(Paragraph(
                                    f"(Showing first {max_rows_per_sheet} rows of {max_row} total)",
                                    styles['Italic']
                                ))
                            
                            # Convert DataFrame to list of lists for ReportLab table
                            # Replace NaN with empty string and convert all to strings
                            table_data = []
                            for row_idx, row in df.iterrows():
                                row_data = []
                                for val in row:
                                    if pd.isna(val):
                                        row_data.append('')
                                    else:
                                        # Truncate long cell values
                                        str_val = str(val)
                                        if len(str_val) > 100:
                                            str_val = str_val[:97] + '...'
                                        row_data.append(str_val)
                                table_data.append(row_data)
                            
                            if not table_data:
                                elements.append(Paragraph("(No data to display)", styles['Italic']))
                                continue
                            
                            # Calculate column widths (distribute evenly, max 10 columns visible)
                            num_cols = min(len(table_data[0]), 15) if table_data else 0
                            if num_cols > 0:
                                # Truncate columns if too many
                                if len(table_data[0]) > 15:
                                    table_data = [row[:15] for row in table_data]
                                    elements.append(Paragraph(
                                        f"(Showing first 15 of {max_col} columns)",
                                        styles['Italic']
                                    ))
                                
                                available_width = 9.5 * inch  # Landscape letter width minus margins
                                col_width = available_width / num_cols
                                
                                # Create table
                                table = Table(table_data, colWidths=[col_width] * num_cols)
                                
                                # Style the table
                                table_style = TableStyle([
                                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
                                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                    ('FONTSIZE', (0, 0), (-1, -1), 7),
                                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                                    ('WORDWRAP', (0, 0), (-1, -1), True),
                                ])
                                
                                # Alternate row colors
                                for i in range(1, len(table_data)):
                                    if i % 2 == 0:
                                        table_style.add('BACKGROUND', (0, i), (-1, i), colors.Color(0.95, 0.95, 0.95))
                                
                                table.setStyle(table_style)
                                elements.append(table)
                            
                        except Exception as sheet_error:
                            elements.append(Paragraph(f"(Error reading sheet: {str(sheet_error)[:100]})", styles['Italic']))
                    
                    # Build PDF
                    doc.build(elements)
                    
                    # Upload to external stage
                    stage_target = f"{EXTERNAL_STAGE}/{CONVERTED_FOLDER}"
                    put_result = session.file.put(
                        pdf_path,
                        stage_target,
                        auto_compress=False,
                        overwrite=True
                    )
                    
                    # Update registry with success
                    converted_pdf_stage_path = f"{CONVERTED_FOLDER}/{pdf_name}"
                    escaped_pdf_path = converted_pdf_stage_path.replace("'", "''")
                    session.sql(f"""
                        UPDATE document_db.s3_documents.excel_document_registry
                        SET conversion_status = 'converted',
                            converted_pdf_path = '{escaped_pdf_path}',
                            processed_timestamp = CURRENT_TIMESTAMP()
                        WHERE registry_id = '{registry_id}'
                    """).collect()
                    
                    converted_count += 1
                
            except Exception as e:
                # Store error in registry
                error_msg = str(e)[:1900].replace("'", "''")
                session.sql(f"""
                    UPDATE document_db.s3_documents.excel_document_registry
                    SET conversion_status = 'error',
                        error_message = '{error_msg}',
                        processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                error_count += 1
            
            processed_count += 1
        
        return f"Processed:{processed_count} Converted:{converted_count} Errors:{error_count}"
        
    except Exception as e:
        return f"Error: {str(e)[:200]}"
$$;


-- =============================
-- PROCEDURE TO REGISTER EXCEL FILES
-- =============================
-- This procedure can be called from parse_new_documents or run separately
-- to register Excel files for conversion

CREATE OR REPLACE PROCEDURE document_db.s3_documents.register_excel_files_for_conversion()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  registered_count INTEGER := 0;
BEGIN
  
  -- Register Excel files from the stream that aren't already registered
  INSERT INTO document_db.s3_documents.excel_document_registry
  (registry_id, original_file_path, original_file_name, original_file_size, conversion_status)
  SELECT
    CONCAT('EXCEL_', ABS(HASH(relative_path)), '_', 
           REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':','')) as registry_id,
    relative_path as original_file_path,
    REGEXP_SUBSTR(relative_path, '[^/]+$') as original_file_name,
    size as original_file_size,
    'pending' as conversion_status
  FROM DIRECTORY(@document_db.s3_documents.document_stage)
  WHERE (
      UPPER(relative_path) LIKE '%.XLSX'
      OR UPPER(relative_path) LIKE '%.XLSM'
    )
    -- DEDUPLICATION: Skip files already registered
    AND relative_path NOT IN (
      SELECT original_file_path 
      FROM document_db.s3_documents.excel_document_registry 
      WHERE original_file_path IS NOT NULL
    )
    -- Skip files already in converted folder (avoid re-processing)
    AND relative_path NOT LIKE 'converted_excel/%';
  
  registered_count := SQLROWCOUNT;
  
  RETURN 'Registered ' || registered_count || ' new Excel files for conversion';
END;
$$;


-- =============================
-- TASK - EXCEL TO PDF CONVERSION
-- =============================
-- This task runs after the main document pipeline to convert Excel files
-- The converted PDFs will trigger the stream and be processed in the next pipeline run

CREATE OR REPLACE TASK document_db.s3_documents.register_excel_files_task
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Register new Excel files from the directory for PDF conversion'
  AFTER document_db.s3_documents.parse_documents_task
AS
  CALL document_db.s3_documents.register_excel_files_for_conversion();


CREATE OR REPLACE TASK document_db.s3_documents.convert_excel_to_pdf_task
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Convert Excel files to PDF and upload back to stage for AI processing'
  AFTER document_db.s3_documents.register_excel_files_task
AS
  CALL document_db.s3_documents.convert_excel_to_pdf();


-- Resume the Excel tasks (must resume child tasks before parent)
ALTER TASK document_db.s3_documents.convert_excel_to_pdf_task RESUME;
ALTER TASK document_db.s3_documents.register_excel_files_task RESUME;


-- =============================
-- EXCEL CONVERSION STATUS VIEW
-- =============================
-- View to monitor Excel document conversion status

CREATE OR REPLACE VIEW document_db.s3_documents.excel_conversion_status AS
SELECT 
    edr.registry_id,
    edr.original_file_path,
    edr.original_file_name,
    edr.original_file_size,
    ROUND(edr.original_file_size / 1048576.0, 2) as size_mb,
    edr.sheet_count,
    edr.conversion_status,
    edr.converted_pdf_path,
    edr.error_message,
    edr.created_timestamp,
    edr.processed_timestamp,
    COUNT(esm.sheet_id) as sheets_recorded
FROM document_db.s3_documents.excel_document_registry edr
LEFT JOIN document_db.s3_documents.excel_sheet_metadata esm
    ON edr.registry_id = esm.registry_id
GROUP BY 
    edr.registry_id,
    edr.original_file_path,
    edr.original_file_name,
    edr.original_file_size,
    edr.sheet_count,
    edr.conversion_status,
    edr.converted_pdf_path,
    edr.error_message,
    edr.created_timestamp,
    edr.processed_timestamp
ORDER BY edr.created_timestamp DESC;


-- =============================
-- UPDATE PIPELINE STATUS VIEW
-- =============================
-- Add Excel conversion status to the main pipeline status view

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
    'Excel Conversions' as stage,
    COUNT(*) as count,
    conversion_status as status
FROM document_db.s3_documents.excel_document_registry
GROUP BY conversion_status;


-- =============================
-- VALIDATION QUERIES
-- =============================

-- Check Excel files in the stage
SELECT relative_path, size, last_modified
FROM DIRECTORY(@document_db.s3_documents.document_stage)
WHERE UPPER(relative_path) LIKE '%.XLSX'
   OR UPPER(relative_path) LIKE '%.XLSM'
ORDER BY last_modified DESC;

-- Check Excel conversion registry
SELECT * FROM document_db.s3_documents.excel_document_registry;

-- Check sheet metadata
SELECT * FROM document_db.s3_documents.excel_sheet_metadata;

-- Check Excel conversion status view
SELECT * FROM document_db.s3_documents.excel_conversion_status;

-- Check converted PDFs in the stage
SELECT relative_path, size, last_modified
FROM DIRECTORY(@document_db.s3_documents.document_stage)
WHERE relative_path LIKE 'converted_excel/%'
ORDER BY last_modified DESC;

-- Verify task status
DESC TASK document_db.s3_documents.register_excel_files_task;
DESC TASK document_db.s3_documents.convert_excel_to_pdf_task;


-- =============================
-- MANUAL EXECUTION (FOR TESTING)
-- =============================
-- Use these commands to manually run the Excel conversion:

-- Step 1: Register any Excel files found in the stage
-- CALL document_db.s3_documents.register_excel_files_for_conversion();

-- Step 2: Convert registered Excel files to PDF
-- CALL document_db.s3_documents.convert_excel_to_pdf();

-- Step 3: Check conversion status
-- SELECT * FROM document_db.s3_documents.excel_conversion_status;

-- Step 4: Verify the converted PDFs are in the stage
-- SELECT relative_path, size FROM DIRECTORY(@document_db.s3_documents.document_stage)
-- WHERE relative_path LIKE 'converted_excel/%';

