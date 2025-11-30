-- =============================
-- DOCUMENT PROCESSING PIPELINE SETUP
-- =============================
-- This file creates all tables, procedures, tasks, and services for the document AI pipeline
-- Run this file after completing the S3 integration setup
-- 
-- PIPELINE FLOW:
-- 1. S3 files land → Directory Table auto-refreshes → Stream captures new files
-- 2. parse_documents_task triggers → AI_PARSE_DOCUMENT extracts text (handles large docs)
-- 3. classify_documents_task triggers → AI_CLASSIFY categorizes documents
-- 4. extract_documents_task triggers → AI_EXTRACT pulls structured attributes
-- 5. chunk_documents_task triggers → SPLIT_TEXT_RECURSIVE_CHARACTER creates chunks
-- 6. Cortex Search Service auto-updates every 2 minutes
--
-- LARGE DOCUMENT HANDLING:
-- Documents >100MB or >500 pages are split using Snowpark Python before processing

USE ROLE ACCOUNTADMIN;
USE DATABASE document_db;
USE SCHEMA s3_documents;
USE WAREHOUSE COMPUTE_WH;

-- =============================
-- TABLES
-- =============================

-- Table to track large documents that need splitting
CREATE OR REPLACE TABLE document_db.s3_documents.large_document_registry (
  registry_id VARCHAR(150) PRIMARY KEY,
  original_file_path VARCHAR(1000) NOT NULL,
  original_file_name VARCHAR(500) NOT NULL,
  original_file_size NUMBER,
  estimated_pages NUMBER,
  split_required BOOLEAN DEFAULT FALSE,
  split_count NUMBER DEFAULT 0,
  split_status VARCHAR(500) DEFAULT 'pending',  -- Increased size for error messages
  error_message VARCHAR(2000),                   -- Separate column for detailed errors
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  processed_timestamp TIMESTAMP
)
COMMENT = 'Registry tracking large documents (>100MB or >500 pages) that require splitting';

-- Table to store split document parts
CREATE OR REPLACE TABLE document_db.s3_documents.document_split_parts (
  part_id VARCHAR(150) PRIMARY KEY,
  registry_id VARCHAR(150) NOT NULL,
  original_file_path VARCHAR(1000) NOT NULL,
  part_number NUMBER NOT NULL,
  total_parts NUMBER NOT NULL,
  part_stage_path VARCHAR(1000),
  part_size NUMBER,
  page_start NUMBER,
  page_end NUMBER,
  status VARCHAR(50) DEFAULT 'created',
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (registry_id) REFERENCES document_db.s3_documents.large_document_registry(registry_id)
)
COMMENT = 'Stores metadata about split document parts for large document processing';

-- parsed_documents - Stores AI_PARSE_DOCUMENT results
CREATE OR REPLACE TABLE document_db.s3_documents.parsed_documents (
  document_id VARCHAR(100) PRIMARY KEY,
  file_name VARCHAR(500) NOT NULL,
  file_path VARCHAR(1000) NOT NULL,
  file_size NUMBER,
  file_url VARCHAR(1000),
  document_type VARCHAR(50),
  parsed_content VARIANT,
  content_text STRING,
  is_split_part BOOLEAN DEFAULT FALSE,
  parent_registry_id VARCHAR(150),
  part_number NUMBER,
  parse_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  status VARCHAR(50) DEFAULT 'parsed'
)
COMMENT = 'Intermediate table storing parsed document content before classification';

-- document_classifications - Classification results from AI_CLASSIFY
CREATE OR REPLACE TABLE document_db.s3_documents.document_classifications (
  document_id VARCHAR(100) PRIMARY KEY,
  file_name VARCHAR(500) NOT NULL,
  file_path VARCHAR(1000) NOT NULL,
  file_size NUMBER,
  file_url VARCHAR(1000),
  document_type VARCHAR(50),
  parsed_content VARIANT,
  document_class VARCHAR(100),
  classification_confidence FLOAT,
  classification_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- document_extractions - Extracted attributes from AI_EXTRACT
CREATE OR REPLACE TABLE document_db.s3_documents.document_extractions (
  document_id VARCHAR(100),
  file_name VARCHAR(500),
  file_path VARCHAR(1000),
  document_class VARCHAR(100),
  attribute_name VARCHAR(200),
  attribute_value STRING,
  confidence_score FLOAT,
  extraction_json VARIANT,
  extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- extraction_prompts - Question templates for each document class
CREATE OR REPLACE TABLE document_db.s3_documents.extraction_prompts (
  document_class VARCHAR(100),
  attribute_name VARCHAR(200),
  question_text STRING,
  PRIMARY KEY (document_class, attribute_name)
);

-- document_chunks - Chunked text for Cortex Search
CREATE OR REPLACE TABLE document_db.s3_documents.document_chunks (
  chunk_id VARCHAR(150) PRIMARY KEY,
  document_id VARCHAR(100) NOT NULL,
  file_name VARCHAR(500),
  file_path VARCHAR(1000),
  document_class VARCHAR(100),
  chunk_index INTEGER,
  chunk_text STRING,
  chunk_size INTEGER,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (document_id) REFERENCES document_db.s3_documents.parsed_documents(document_id)
)
COMMENT = 'Table storing chunked document content for semantic search and RAG applications';

-- =============================
-- EXTRACTION PROMPTS - 10 attributes per document class
-- =============================
-- Seed all extraction prompts (79 total attributes across 9 document classes)
-- Updated to match demo_docs folder structure with comprehensive business-relevant attributes
INSERT INTO document_db.s3_documents.extraction_prompts

-- ======================
-- W2 TAX FORM (10 attributes)
-- ======================
SELECT 'w2' AS document_class, 'employee_name' AS attribute_name, 'What is the employee''s full legal name as shown on the W-2?' AS question_text UNION ALL
SELECT 'w2', 'employer_name', 'What is the employer company name on the W-2?' UNION ALL
SELECT 'w2', 'employer_ein', 'What is the Employer Identification Number (EIN)?' UNION ALL
SELECT 'w2', 'tax_year', 'What tax year is this W-2 for?' UNION ALL
SELECT 'w2', 'wages_tips_compensation', 'What is the amount in Box 1 - Wages, tips, other compensation?' UNION ALL
SELECT 'w2', 'federal_income_tax_withheld', 'What is the amount in Box 2 - Federal income tax withheld?' UNION ALL
SELECT 'w2', 'social_security_wages', 'What is the amount in Box 3 - Social security wages?' UNION ALL
SELECT 'w2', 'social_security_tax_withheld', 'What is the amount in Box 4 - Social security tax withheld?' UNION ALL
SELECT 'w2', 'medicare_wages', 'What is the amount in Box 5 - Medicare wages and tips?' UNION ALL
SELECT 'w2', 'state', 'What state is listed for state income tax on the W-2?' UNION ALL

-- ======================
-- VENDOR/SERVICE CONTRACT (10 attributes)
-- ======================
SELECT 'vendor_contract', 'vendor_name', 'What is the name of the vendor or service provider?' UNION ALL
SELECT 'vendor_contract', 'client_name', 'What is the name of the client or customer company?' UNION ALL
SELECT 'vendor_contract', 'contract_effective_date', 'When does the contract become effective?' UNION ALL
SELECT 'vendor_contract', 'contract_expiration_date', 'When does the contract expire or end?' UNION ALL
SELECT 'vendor_contract', 'contract_term_months', 'What is the length of the contract in months or years?' UNION ALL
SELECT 'vendor_contract', 'total_contract_value', 'What is the total value of the contract? Include currency.' UNION ALL
SELECT 'vendor_contract', 'payment_terms', 'What are the payment terms or payment schedule (e.g., Net 30, monthly, quarterly)?' UNION ALL
SELECT 'vendor_contract', 'services_description', 'What services or products are being provided? Provide a brief description.' UNION ALL
SELECT 'vendor_contract', 'auto_renewal_clause', 'Does the contract have an automatic renewal clause? (Yes/No)' UNION ALL
SELECT 'vendor_contract', 'termination_notice_period', 'How many days or months notice is required for termination?' UNION ALL

-- ======================
-- SALES REPORT/PERFORMANCE (8 attributes)
-- ======================
SELECT 'sales_report', 'reporting_period', 'What reporting period does this cover (e.g., Q4 2024, FY2025, December 2024)?' UNION ALL
SELECT 'sales_report', 'total_revenue', 'What is the total revenue for the reporting period? Include currency.' UNION ALL
SELECT 'sales_report', 'revenue_growth_percent', 'What is the revenue growth percentage (year-over-year or period-over-period)?' UNION ALL
SELECT 'sales_report', 'top_performing_region', 'What is the top performing sales region or territory?' UNION ALL
SELECT 'sales_report', 'new_customers_count', 'How many new customers were acquired during this period?' UNION ALL
SELECT 'sales_report', 'average_deal_size', 'What is the average deal size or average contract value?' UNION ALL
SELECT 'sales_report', 'sales_pipeline_value', 'What is the total value of the sales pipeline?' UNION ALL
SELECT 'sales_report', 'quota_attainment_percent', 'What percentage of sales quota was achieved?' UNION ALL

-- ======================
-- MARKETING CAMPAIGN REPORT (9 attributes)
-- ======================
SELECT 'marketing_report', 'campaign_name', 'What is the name of the marketing campaign?' UNION ALL
SELECT 'marketing_report', 'reporting_period', 'What time period does this campaign report cover?' UNION ALL
SELECT 'marketing_report', 'campaign_budget', 'What was the total budget allocated for this campaign? Include currency.' UNION ALL
SELECT 'marketing_report', 'total_impressions', 'How many total impressions or views were generated?' UNION ALL
SELECT 'marketing_report', 'total_clicks', 'How many total clicks were received?' UNION ALL
SELECT 'marketing_report', 'click_through_rate', 'What is the click-through rate (CTR) as a percentage?' UNION ALL
SELECT 'marketing_report', 'conversion_rate', 'What is the conversion rate as a percentage?' UNION ALL
SELECT 'marketing_report', 'cost_per_acquisition', 'What is the cost per acquisition (CPA) or cost per lead?' UNION ALL
SELECT 'marketing_report', 'roi_percent', 'What is the return on investment (ROI) as a percentage?' UNION ALL

-- ======================
-- HR POLICY/HANDBOOK (7 attributes)
-- ======================
SELECT 'hr_policy', 'document_title', 'What is the official title of this HR policy or handbook?' UNION ALL
SELECT 'hr_policy', 'effective_date', 'When does this policy become effective or was it last updated?' UNION ALL
SELECT 'hr_policy', 'version_number', 'What is the version number or revision number of this document?' UNION ALL
SELECT 'hr_policy', 'department', 'Which department is responsible for this policy (e.g., HR, Legal, Compliance)?' UNION ALL
SELECT 'hr_policy', 'policy_type', 'What type of policy is this (e.g., Employee Handbook, Performance Review Guidelines, Code of Conduct)?' UNION ALL
SELECT 'hr_policy', 'approval_authority', 'Who approved this policy (e.g., CEO, Board, HR Director)?' UNION ALL
SELECT 'hr_policy', 'last_review_date', 'When was this policy last reviewed?' UNION ALL

-- ======================
-- CORPORATE POLICY (8 attributes)
-- ======================
SELECT 'corporate_policy', 'policy_name', 'What is the name of this corporate policy?' UNION ALL
SELECT 'corporate_policy', 'policy_category', 'What category does this policy fall under (e.g., Expense Policy, Travel Policy, Vendor Management)?' UNION ALL
SELECT 'corporate_policy', 'effective_date', 'When does this policy take effect?' UNION ALL
SELECT 'corporate_policy', 'approval_date', 'When was this policy approved?' UNION ALL
SELECT 'corporate_policy', 'policy_owner', 'Which department or role owns this policy?' UNION ALL
SELECT 'corporate_policy', 'approval_levels_required', 'What approval levels or authorities are required under this policy?' UNION ALL
SELECT 'corporate_policy', 'spending_limits', 'What are the key spending limits or monetary thresholds mentioned?' UNION ALL
SELECT 'corporate_policy', 'review_frequency', 'How often is this policy reviewed (e.g., annually, quarterly)?' UNION ALL

-- ======================
-- FINANCIAL INFOGRAPHIC/EARNINGS REPORT (10 attributes)
-- ======================
SELECT 'financial_infographic', 'quarter', 'Which fiscal quarter is reported (e.g., Q1, Q2, Q3, Q4)?' UNION ALL
SELECT 'financial_infographic', 'fiscal_year', 'Which fiscal year is reported?' UNION ALL
SELECT 'financial_infographic', 'revenue', 'What is the total revenue for the period? Include currency and units.' UNION ALL
SELECT 'financial_infographic', 'revenue_growth_percent', 'What is the year-over-year revenue growth percentage?' UNION ALL
SELECT 'financial_infographic', 'operating_margin', 'What is the operating margin as a percentage?' UNION ALL
SELECT 'financial_infographic', 'net_income', 'What is the net income or profit for the period?' UNION ALL
SELECT 'financial_infographic', 'customers_count', 'How many total customers are reported?' UNION ALL
SELECT 'financial_infographic', 'customers_over_1m_count', 'How many customers have trailing 12-month revenue over $1M (enterprise customers)?' UNION ALL
SELECT 'financial_infographic', 'net_revenue_retention_rate', 'What is the net revenue retention rate (NRR) as a percentage?' UNION ALL
SELECT 'financial_infographic', 'gross_margin', 'What is the gross margin as a percentage?' UNION ALL

-- ======================
-- CASE STUDY/CUSTOMER SUCCESS STORY (7 attributes)
-- ======================
SELECT 'case_study', 'customer_name', 'What is the name of the featured customer or company?' UNION ALL
SELECT 'case_study', 'industry', 'What industry does the customer operate in?' UNION ALL
SELECT 'case_study', 'use_case', 'What was the primary use case or business problem being solved?' UNION ALL
SELECT 'case_study', 'business_impact', 'What was the key business impact or result achieved?' UNION ALL
SELECT 'case_study', 'metrics_improved', 'What specific metrics improved (e.g., 50% cost reduction, 2x faster processing)?' UNION ALL
SELECT 'case_study', 'implementation_duration', 'How long did the implementation take?' UNION ALL
SELECT 'case_study', 'testimonial_quote', 'What is the key testimonial or quote from the customer?' UNION ALL

-- ======================
-- STRATEGY DOCUMENT (8 attributes)
-- ======================
SELECT 'strategy_document', 'document_title', 'What is the title of this strategy document?' UNION ALL
SELECT 'strategy_document', 'planning_period', 'What time period does this strategy cover (e.g., 2025, FY2025, Q1-Q4 2025)?' UNION ALL
SELECT 'strategy_document', 'department', 'Which department or business unit does this strategy belong to (e.g., Marketing, Sales, Corporate)?' UNION ALL
SELECT 'strategy_document', 'strategic_goals', 'What are the main strategic goals or objectives?' UNION ALL
SELECT 'strategy_document', 'key_initiatives', 'What are the major initiatives or programs planned?' UNION ALL
SELECT 'strategy_document', 'budget_allocation', 'What is the total budget or key budget allocations mentioned?' UNION ALL
SELECT 'strategy_document', 'success_metrics', 'What are the key performance indicators (KPIs) or success metrics?' UNION ALL
SELECT 'strategy_document', 'document_date', 'When was this strategy document created or published?' UNION ALL

-- ======================
-- OTHER/FALLBACK (2 attributes - minimal extraction for unclassified docs)
-- ======================
SELECT 'other', 'document_title', 'What is the title of this document?' UNION ALL
SELECT 'other', 'document_date', 'What is the document''s date or most relevant date?';


-- =============================
-- SNOWPARK PYTHON PROCEDURE - SPLIT LARGE DOCUMENTS  
-- =============================
-- This procedure is part of the EVENT-DRIVEN pipeline:
-- 1. Queries large_document_registry for pending documents
-- 2. Reads each PDF using SnowflakeFile
-- 3. Checks page count and splits if >400 pages
-- 4. Uploads split parts back to the SAME external stage using session.file.put()
-- 5. The split parts trigger the stream and get processed normally!
--
-- This ensures ALL documents that land in S3 get processed, even large ones.

CREATE OR REPLACE PROCEDURE document_db.s3_documents.split_large_documents()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pypdf')
HANDLER = 'split_large_docs'
AS
$$
import io
import os
import tempfile
from snowflake.snowpark import Session
from snowflake.snowpark.files import SnowflakeFile
from pypdf import PdfReader, PdfWriter


def split_large_docs(session: Session) -> str:
    """
    Split large PDF documents and upload parts back to the external stage.
    The split parts will trigger the stream and be processed by the normal pipeline.
    
    This is part of the EVENT-DRIVEN pipeline - runs automatically via task.
    """
    
    MAX_PAGES_PER_PART = 200  # Stay under AI_PARSE_DOCUMENT 500 page limit
    MAX_SIZE_PER_PART_MB = 90  # Stay under 100MB limit
    STAGE_NAME = "document_db.s3_documents.document_stage"
    EXTERNAL_STAGE = f"@{STAGE_NAME}"
    SPLIT_FOLDER = "split_documents"
    
    processed_count = 0
    split_count = 0
    error_count = 0
    total_parts_created = 0
    
    try:
        # Query for pending large documents
        pending_query = """
        SELECT 
            registry_id,
            original_file_path,
            original_file_name,
            original_file_size
        FROM document_db.s3_documents.large_document_registry
        WHERE split_status IN ('pending', 'pending_split', 'pending_page_limit', 'requires_external_processing')
          AND split_required = TRUE
        ORDER BY created_timestamp ASC
        """
        
        pending_docs = session.sql(pending_query).collect()
        print("Pending documents:", pending_docs)
        if not pending_docs:
            return "No pending large documents to process"
        
        for doc in pending_docs:
            registry_id = doc['REGISTRY_ID']
            file_path = doc['ORIGINAL_FILE_PATH']
            file_name = doc['ORIGINAL_FILE_NAME']
            
            try:
                # Update status to splitting
                session.sql(f"""
                    UPDATE document_db.s3_documents.large_document_registry
                    SET split_status = 'splitting', processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                # Build the full stage path for SnowflakeFile
                # Format: @database.schema.stage/path/to/file.pdf
                stage_file_path = f"{EXTERNAL_STAGE}/{file_path}"
                
                # Read file using SnowflakeFile (works in stored procedures)
                with SnowflakeFile.open(stage_file_path, 'rb', require_scoped_url=False) as sf:
                    pdf_bytes = sf.read()
                
                # Read PDF and check page count
                pdf_stream = io.BytesIO(pdf_bytes)
                reader = PdfReader(pdf_stream)
                total_pages = len(reader.pages)
                size_mb = len(pdf_bytes) / (1024 * 1024)
                
                # Update estimated pages
                session.sql(f"""
                    UPDATE document_db.s3_documents.large_document_registry
                    SET estimated_pages = {total_pages}
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                # Check if split is actually needed
                if total_pages <= MAX_PAGES_PER_PART and size_mb <= MAX_SIZE_PER_PART_MB:
                    # No split needed - mark as such
                    session.sql(f"""
                        UPDATE document_db.s3_documents.large_document_registry
                        SET split_status = 'no_split_needed', 
                            split_count = 0,
                            processed_timestamp = CURRENT_TIMESTAMP()
                        WHERE registry_id = '{registry_id}'
                    """).collect()
                    processed_count += 1
                    continue
                
                # Split the PDF
                num_parts = (total_pages + MAX_PAGES_PER_PART - 1) // MAX_PAGES_PER_PART
                parts_created = 0
                
                # Use temp directory for intermediate files
                with tempfile.TemporaryDirectory() as tmp_dir:
                    for part_num in range(num_parts):
                        start_page = part_num * MAX_PAGES_PER_PART
                        end_page = min((part_num + 1) * MAX_PAGES_PER_PART, total_pages)
                        
                        # Create PDF part
                        writer = PdfWriter()
                        for page_idx in range(start_page, end_page):
                            writer.add_page(reader.pages[page_idx])
                        
                        # Write part to bytes
                        output_stream = io.BytesIO()
                        writer.write(output_stream)
                        part_bytes = output_stream.getvalue()
                        
                        # Generate part filename
                        base_name = file_name.rsplit('.', 1)[0]
                        part_name = f"{base_name}_part_{part_num + 1}_of_{num_parts}.pdf"
                        
                        # Write to temp file
                        part_path = os.path.join(tmp_dir, part_name)
                        with open(part_path, 'wb') as pf:
                            pf.write(part_bytes)
                        
                        # Upload to external stage using session.file.put()
                        # This works in Snowpark stored procedures
                        stage_target = f"{EXTERNAL_STAGE}/{SPLIT_FOLDER}"
                        put_result = session.file.put(
                            part_path, 
                            stage_target,
                            auto_compress=False,
                            overwrite=True
                        )
                        
                        # Track the part
                        part_id = f"{registry_id}_PART_{part_num + 1}"
                        escaped_path = f"{SPLIT_FOLDER}/{part_name}".replace("'", "''")
                        session.sql(f"""
                            INSERT INTO document_db.s3_documents.document_split_parts
                            (part_id, registry_id, original_file_path, part_number, total_parts,
                             part_stage_path, part_size, page_start, page_end, status)
                            VALUES (
                                '{part_id}', '{registry_id}', '{file_path.replace("'", "''")}',
                                {part_num + 1}, {num_parts},
                                '{escaped_path}', {len(part_bytes)},
                                {start_page + 1}, {end_page}, 'uploaded'
                            )
                        """).collect()
                        
                        parts_created += 1
                
                # Update registry as complete
                session.sql(f"""
                    UPDATE document_db.s3_documents.large_document_registry
                    SET split_status = 'split_complete', 
                        split_count = {num_parts},
                        processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                
                split_count += 1
                total_parts_created += parts_created
                    
            except Exception as e:
                # Store error in dedicated column with full details
                error_msg_short = str(e)[:100].replace("'", "''")
                error_msg_full = str(e)[:1900].replace("'", "''")
                session.sql(f"""
                    UPDATE document_db.s3_documents.large_document_registry
                    SET split_status = 'error',
                        error_message = '{error_msg_full}',
                        processed_timestamp = CURRENT_TIMESTAMP()
                    WHERE registry_id = '{registry_id}'
                """).collect()
                error_count += 1
            
            processed_count += 1
        
        return f"Processed:{processed_count} Split:{split_count} Parts:{total_parts_created} Errors:{error_count}"
        
    except Exception as e:
        return f"Error: {str(e)[:100]}"
$$;


-- =============================
-- HELPER FUNCTION - CHECK IF DOCUMENT IS LARGE
-- =============================
-- Returns TRUE if document exceeds size limits
CREATE OR REPLACE FUNCTION document_db.s3_documents.is_large_document(
  file_size NUMBER,
  file_path VARCHAR
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
  -- Check if file exceeds 100MB
  -- Page count check would require reading the file, so we use size as proxy
  -- Average PDF page is ~100KB, so 500 pages ≈ 50MB
  -- We use 100MB as the safe threshold
  file_size > 104857600  -- 100MB in bytes
$$;


-- =============================
-- PROCEDURES
-- =============================

-- Step 1: Parse new documents using AI_PARSE_DOCUMENT
-- Processes files from the stream and extracts text content using LAYOUT mode
-- Handles large documents by routing them to the large document handler
-- 
-- Size/Page Thresholds:
--   - PDFs > 100MB: Definitely too large
--   - PDFs > 50MB: Likely to have >500 pages (avg ~100KB/page)
--   - Other docs > 100MB: Too large
--   - Documents that fail parsing: Registered for notebook processing

CREATE OR REPLACE PROCEDURE document_db.s3_documents.parse_new_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  processed_count INTEGER := 0;
  large_doc_count INTEGER := 0;
  failed_parse_count INTEGER := 0;
  stream_count INTEGER := 0;
BEGIN
  
  -- =====================================================
  -- STEP 0: Capture stream data into temp table
  -- CRITICAL: Stream can only be read ONCE per transaction!
  -- =====================================================
  CREATE OR REPLACE TEMPORARY TABLE document_db.s3_documents.temp_stream_data AS
  SELECT 
    relative_path,
    size,
    file_url,
    METADATA$ACTION
  FROM document_db.s3_documents.new_documents_stream
  WHERE METADATA$ACTION = 'INSERT'
    AND relative_path IS NOT NULL
    AND relative_path != '';
  
  SELECT COUNT(*) INTO stream_count FROM document_db.s3_documents.temp_stream_data;
  
  -- Early exit if no new documents in stream
  IF (stream_count = 0) THEN
    DROP TABLE IF EXISTS document_db.s3_documents.temp_stream_data;
    RETURN 'No new documents in stream';
  END IF;
  
  -- =====================================================
  -- STEP 1: Register definitely large documents to the registry
  -- These will be processed by the split_large_documents procedure
  -- =====================================================
  -- Criteria for large document registration:
  --   1. Any document > 100MB (AI_PARSE_DOCUMENT hard limit)
  INSERT INTO document_db.s3_documents.large_document_registry
  (registry_id, original_file_path, original_file_name, original_file_size, split_required, split_status)
  SELECT
    CONCAT('REG_', ABS(HASH(relative_path)), '_', 
           REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':','')) as registry_id,
    relative_path as original_file_path,
    REGEXP_SUBSTR(relative_path, '[^/]+$') as original_file_name,
    size as original_file_size,
    TRUE as split_required,
    'pending' as split_status
  FROM document_db.s3_documents.temp_stream_data
  WHERE size > 104857600  -- 100MB threshold
    -- DEDUPLICATION: Skip files already registered
    AND relative_path NOT IN (
      SELECT original_file_path FROM document_db.s3_documents.large_document_registry WHERE original_file_path IS NOT NULL
    )
    -- Skip files already parsed
    AND relative_path NOT IN (
      SELECT file_path FROM document_db.s3_documents.parsed_documents WHERE file_path IS NOT NULL
    )
    AND (
      UPPER(relative_path) LIKE '%.PDF' 
      OR UPPER(relative_path) LIKE '%.DOCX'
      OR UPPER(relative_path) LIKE '%.PPTX'
    );
  
  large_doc_count := SQLROWCOUNT;
  
  -- =====================================================
  -- STEP 2: Process normal-sized documents in parallel
  -- Using set-based AI_PARSE_DOCUMENT for maximum parallelism
  -- =====================================================
  -- Only process documents that weren't registered as large
  INSERT INTO document_db.s3_documents.parsed_documents 
  (document_id, file_name, file_path, file_size, file_url, document_type, parsed_content, is_split_part)
  SELECT
    CONCAT('DOC_', REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':',''), '_', ABS(HASH(relative_path))) as document_id,
    REGEXP_SUBSTR(relative_path, '[^/]+$') as file_name,
    relative_path as file_path,
    size as file_size,
    file_url,
    CASE 
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) = '.PDF' THEN 'pdf'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) = '.DOCX' THEN 'docx'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) = '.PPTX' THEN 'pptx'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) IN ('.JPG', '.JPEG') THEN 'jpeg'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) = '.PNG' THEN 'png'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) IN ('.TIFF', '.TIF') THEN 'tiff'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) = '.HTML' THEN 'html'
      WHEN UPPER(REGEXP_SUBSTR(relative_path, '\.[^.]+$')) = '.TXT' THEN 'txt'
      ELSE 'unknown'
    END as document_type,
    AI_PARSE_DOCUMENT(
      TO_FILE('@document_db.s3_documents.document_stage', relative_path),
      {'mode': 'LAYOUT'}
    ) as parsed_content,
    -- Mark split parts appropriately
    CASE WHEN relative_path LIKE '%_part_%_of_%.pdf' THEN TRUE ELSE FALSE END as is_split_part
  FROM document_db.s3_documents.temp_stream_data
  WHERE size <= 104857600  -- Only docs under 100MB
    -- DEDUPLICATION: Skip files already parsed
    AND relative_path NOT IN (
      SELECT file_path FROM document_db.s3_documents.parsed_documents WHERE file_path IS NOT NULL
    )
    -- Skip files already registered as large (pending split)
    AND relative_path NOT IN (
      SELECT original_file_path FROM document_db.s3_documents.large_document_registry WHERE original_file_path IS NOT NULL
    )
    AND (
      UPPER(relative_path) LIKE '%.PDF' 
      OR UPPER(relative_path) LIKE '%.DOCX'
      OR UPPER(relative_path) LIKE '%.PPTX'
      OR UPPER(relative_path) LIKE '%.JPEG'
      OR UPPER(relative_path) LIKE '%.JPG' 
      OR UPPER(relative_path) LIKE '%.PNG'
      OR UPPER(relative_path) LIKE '%.TIFF'
      OR UPPER(relative_path) LIKE '%.TIF'
      OR UPPER(relative_path) LIKE '%.HTML'
      OR UPPER(relative_path) LIKE '%.TXT'
    );
  
  processed_count := SQLROWCOUNT;
  
  -- =====================================================
  -- STEP 3: Handle documents that failed due to page limit
  -- These have parsed_content with errorInformation about 500 pages
  -- Add them to large_document_registry for splitting
  -- =====================================================
  INSERT INTO document_db.s3_documents.large_document_registry
  (registry_id, original_file_path, original_file_name, original_file_size, split_required, split_status)
  SELECT
    CONCAT('REG_', ABS(HASH(file_path)), '_', 
           REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':','')) as registry_id,
    file_path as original_file_path,
    file_name as original_file_name,
    file_size as original_file_size,
    TRUE as split_required,
    'pending_page_limit' as split_status  -- Indicates 500 page limit exceeded
  FROM document_db.s3_documents.parsed_documents
  WHERE parsed_content:errorInformation::STRING LIKE '%500 pages%'
     OR parsed_content:errorInformation::STRING LIKE '%Maximum number of%pages exceeded%';
  
  failed_parse_count := SQLROWCOUNT;
  
  -- =====================================================
  -- STEP 4: Remove failed documents from parsed_documents
  -- They will be re-processed after splitting
  -- =====================================================
  /* * removing now for debugging purposes
  DELETE FROM document_db.s3_documents.parsed_documents
  WHERE parsed_content:errorInformation::STRING LIKE '%500 pages%'
     OR parsed_content:errorInformation::STRING LIKE '%Maximum number of%pages exceeded%';
     */
  
  -- Clean up temp table
  DROP TABLE IF EXISTS document_db.s3_documents.temp_stream_data;
  
  RETURN 'SUCCESS: Stream had ' || stream_count || ' files. Processed ' || processed_count || ' documents, ' || large_doc_count || ' large documents (by size), ' || failed_parse_count || ' documents failed parsing (page limit) - queued for splitting';
END;
$$;


-- Step 2: Classify parsed documents using AI_CLASSIFY
-- Categorizes documents into 9 business document types in parallel
-- All documents are processed in a single set-based operation
CREATE OR REPLACE PROCEDURE document_db.s3_documents.classify_parsed_documents()CREATE OR REPLACE PROCEDURE document_db.s3_documents.extract_attributes_for_classified_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  processed_count INTEGER := 0;
  error_count INTEGER := 0;
BEGIN
  
  -- Store extracted attributes using MERGE with set-based AI_EXTRACT
  -- Process all documents with their respective prompt objects in parallel
  MERGE INTO document_db.s3_documents.document_extractions t
  USING (
    SELECT 
      docs.document_id,
      docs.file_name,
      docs.file_path,
      docs.document_class,
      f.key::STRING AS attribute_name,
      f.value::STRING AS attribute_value,
      TRY_CAST(docs.extraction_result:output_details:scores[f.key]::STRING AS FLOAT) AS confidence_score,
      docs.extraction_result AS extraction_json
    FROM (
      SELECT 
        dc.document_id,
        dc.file_name,
        dc.file_path,
        dc.document_class,
        CASE 
          WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
          ELSE dc.document_class
        END AS document_class_norm,
        prompts.prompt_obj,
        AI_EXTRACT(
          pd.parsed_content:content::STRING,
          prompts.prompt_obj
        ) as extraction_result
      FROM document_db.s3_documents.document_classifications dc
      -- Join to parsed_documents to get the actual content
      INNER JOIN document_db.s3_documents.parsed_documents pd
        ON dc.document_id = pd.document_id
      INNER JOIN (
        SELECT 
          document_class,
          OBJECT_AGG(attribute_name, TO_VARIANT(question_text)) as prompt_obj
        FROM document_db.s3_documents.extraction_prompts
        GROUP BY document_class
      ) prompts
      ON LOWER(REPLACE(REPLACE(TRIM(prompts.document_class),' ','_'),'-','_')) = 
         LOWER(REPLACE(REPLACE(TRIM(
           CASE 
             WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
               TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
             ELSE dc.document_class
           END
         ),' ','_'),'-','_'))
      WHERE dc.file_path IS NOT NULL 
        AND dc.file_path != ''
        AND pd.parsed_content:content IS NOT NULL
        -- Only process documents not yet extracted
        AND dc.document_id NOT IN (SELECT DISTINCT document_id FROM document_db.s3_documents.document_extractions)
    ) docs,
    LATERAL FLATTEN(INPUT => docs.extraction_result) f
    WHERE docs.extraction_result IS NOT NULL
  ) s
  ON t.document_id = s.document_id AND t.attribute_name = s.attribute_name
  WHEN MATCHED THEN UPDATE SET
    t.attribute_value = s.attribute_value,
    t.confidence_score = s.confidence_score,
    t.extraction_json = s.extraction_json,
    t.extraction_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (document_id, file_name, file_path, document_class, attribute_name, attribute_value, confidence_score, extraction_json)
  VALUES (s.document_id, s.file_name, s.file_path, s.document_class, s.attribute_name, s.attribute_value, s.confidence_score, s.extraction_json);
  
  processed_count := SQLROWCOUNT;
  
  -- Count errors (classifications without successful extractions)
  SELECT COUNT(DISTINCT dc.document_id) INTO error_count
  FROM document_db.s3_documents.document_classifications dc
  LEFT JOIN document_db.s3_documents.document_extractions de 
    ON dc.document_id = de.document_id
  WHERE de.document_id IS NULL
    AND dc.file_path IS NOT NULL 
    AND dc.file_path != '';

  RETURN 'Extraction completed. Processed: ' || processed_count || ', Errors: ' || error_count;
END;
$$;
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  processed_count INTEGER;
  error_count INTEGER;
BEGIN
  
  -- Insert classifications directly using set-based AI_CLASSIFY
  -- This processes all unclassified documents in parallel
  -- AI_CLASSIFY returns an OBJECT with 'label' field, so we extract it
  INSERT INTO document_db.s3_documents.document_classifications 
  (document_id, file_name, file_path, file_size, file_url, document_type, 
   parsed_content, document_class, classification_confidence)
  SELECT 
    document_id,
    file_name,
    file_path,
    file_size,
    file_url,
    document_type,
    parsed_content,
    AI_CLASSIFY(
      parsed_content:content::STRING,
      ['w2', 'vendor_contract', 'sales_report', 'marketing_report', 'hr_policy', 
       'corporate_policy', 'financial_infographic', 'case_study', 'strategy_document', 'other']
    ):label::VARCHAR as document_class,
    NULL as classification_confidence
  FROM document_db.s3_documents.parsed_documents
  WHERE status = 'parsed'
    AND parsed_content IS NOT NULL
    AND LENGTH(TRIM(content_text)) > 0;
  
  processed_count := SQLROWCOUNT;
  
  -- Mark documents as classified to prevent reprocessing
  UPDATE document_db.s3_documents.parsed_documents 
  SET status = 'classified' 
  WHERE status = 'parsed'
    AND content_text IS NOT NULL
    AND LENGTH(TRIM(content_text)) > 0
    AND document_id IN (SELECT document_id FROM document_db.s3_documents.document_classifications);
  
  -- Count any errors (documents with NULL classification)
  SELECT COUNT(*) INTO error_count
  FROM document_db.s3_documents.document_classifications
  WHERE document_class IS NULL;
  
  RETURN 'Classification completed. Processed: ' || processed_count || ', Errors: ' || error_count;
END;
$$;


-- Step 3: Extract specific attributes using AI_EXTRACT
-- Uses document class to lookup relevant prompts and extract structured data
-- Extracts the 10 most common fields for each document type in parallel



-- Step 4: Chunk documents using SPLIT_TEXT_RECURSIVE_CHARACTER
-- Creates searchable chunks from ALL parsed documents for Cortex Search
-- NO dependency on classification - chunks everything that has been parsed
-- Processes all documents in parallel using set-based operations
CREATE OR REPLACE PROCEDURE document_db.s3_documents.chunk_parsed_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  processed_count INTEGER := 0;
  chunk_count INTEGER := 0;
  error_count INTEGER := 0;
BEGIN
  
  -- Insert chunks directly using set-based SPLIT_TEXT_RECURSIVE_CHARACTER
  -- Process ALL parsed documents in a single parallel operation
  -- Optionally joins to classifications if available for document_class
  INSERT INTO document_db.s3_documents.document_chunks
  (chunk_id, document_id, file_name, file_path, document_class, chunk_index, chunk_text, chunk_size)
  SELECT
    CONCAT(document_id, '_CHUNK_', chunk_index),
    document_id,
    file_name,
    file_path,
    document_class,
    chunk_index,
    chunk_text,
    LENGTH(chunk_text) as chunk_size
  FROM (
    SELECT 
      pd.document_id,
      pd.file_name,
      pd.file_path,
      -- Use classification if available, otherwise 'unclassified'
      COALESCE(
        CASE 
          WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
          ELSE dc.document_class
        END,
        'unclassified'
      ) as document_class,
      f.index as chunk_index,
      f.value::STRING as chunk_text
    FROM document_db.s3_documents.parsed_documents pd
    -- LEFT JOIN to get classification if available (not required)
    LEFT JOIN document_db.s3_documents.document_classifications dc
      ON pd.document_id = dc.document_id
    -- Exclude already chunked documents
    LEFT JOIN document_db.s3_documents.document_chunks existing_chunks
      ON pd.document_id = existing_chunks.document_id
    , LATERAL FLATTEN(
        INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
          pd.parsed_content:content::STRING,
          'none',  -- separators type (none, markdown, python, html, etc.)
          1000,    -- chunk_size
          200      -- chunk_overlap
        )
      ) f
    WHERE pd.parsed_content:content IS NOT NULL 
      AND LENGTH(TRIM(pd.parsed_content:content::STRING)) > 100
      AND existing_chunks.document_id IS NULL  -- Skip already chunked documents
      AND LENGTH(TRIM(f.value::STRING)) > 50   -- Only meaningful chunks
      -- Exclude documents with parsing errors
      AND pd.parsed_content:errorInformation IS NULL
  );
  
  chunk_count := SQLROWCOUNT;
  
  -- Count distinct documents processed
  SELECT COUNT(DISTINCT document_id) INTO processed_count
  FROM document_db.s3_documents.document_chunks;
  
  -- Count documents that failed to chunk (have content but no chunks)
  SELECT COUNT(DISTINCT pd.document_id) INTO error_count
  FROM document_db.s3_documents.parsed_documents pd
  LEFT JOIN document_db.s3_documents.document_chunks chunks
    ON pd.document_id = chunks.document_id
  WHERE pd.parsed_content:content IS NOT NULL 
    AND LENGTH(TRIM(pd.parsed_content:content::STRING)) > 100
    AND pd.parsed_content:errorInformation IS NULL
    AND chunks.document_id IS NULL;
  
  RETURN 'Chunking completed. Documents: ' || processed_count || ', Chunks: ' || chunk_count || ', Pending: ' || error_count;
END;
$$;

-- Keep old procedure name as alias for backward compatibility
CREATE OR REPLACE PROCEDURE document_db.s3_documents.chunk_classified_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  CALL document_db.s3_documents.chunk_parsed_documents();
  RETURN 'Redirected to chunk_parsed_documents()';
END;
$$;


-- =============================
-- CORTEX SEARCH SERVICE
-- =============================
-- Create Cortex Search Service for semantic search on document chunks
-- TARGET_LAG set to 2 minutes for near-real-time search updates
CREATE OR REPLACE CORTEX SEARCH SERVICE document_db.s3_documents.document_search_service
ON chunk_text
ATTRIBUTES document_id, file_name, file_path, document_class, chunk_index
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '2 minutes'
AS (
  SELECT 
    chunk_id,
    chunk_text,
    document_id,
    file_name,
    file_path,
    document_class,
    chunk_index
  FROM document_db.s3_documents.document_chunks
);


-- =============================
-- TASKS - Automated pipeline execution
-- =============================
-- 
-- PIPELINE FLOW:
--   1. parse_documents_task: Parses normal docs, registers large docs to registry
--   2. split_large_documents_task: Splits large PDFs, uploads parts back to stage
--      (Split parts trigger the stream again and get processed in the next run!)
--   3. classify_documents_task: Classifies all parsed documents
--   4. extract_documents_task: Extracts attributes based on classification
--   5. chunk_documents_task: Creates searchable chunks for Cortex Search
--
-- This ensures ALL documents get processed, even those >100MB or >500 pages!

-- Task 1: Triggered when new files appear in the stream
-- Parses documents using AI_PARSE_DOCUMENT, registers large docs for splitting
CREATE OR REPLACE TASK document_db.s3_documents.parse_documents_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  COMMENT = 'Parse new documents from S3 stage using AI_PARSE_DOCUMENT'
WHEN SYSTEM$STREAM_HAS_DATA('document_db.s3_documents.new_documents_stream')
AS
  CALL document_db.s3_documents.parse_new_documents();

ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- Task 1b: Splits large documents and uploads parts back to the SAME stage
-- The split parts will trigger the stream and be processed in the next pipeline run
-- This runs AFTER parsing to handle any large documents that were registered



-- Task 2: Runs after parsing and splitting complete
-- Classifies parsed documents using AI_CLASSIFY
CREATE OR REPLACE TASK document_db.s3_documents.classify_documents_task
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Classify parsed documents using AI_CLASSIFY'
  AFTER document_db.s3_documents.parse_documents_task
AS
  CALL document_db.s3_documents.classify_parsed_documents();

-- Task 3: Runs after classification completes
-- Extracts attributes using AI_EXTRACT based on document class
CREATE OR REPLACE TASK document_db.s3_documents.extract_documents_task
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Extract attributes using AI_EXTRACT after classification'
  AFTER document_db.s3_documents.classify_documents_task
AS
  CALL document_db.s3_documents.extract_attributes_for_classified_documents();

-- Task 4: Chunk documents for semantic search after extraction
-- Creates searchable chunks using SPLIT_TEXT_RECURSIVE_CHARACTER
CREATE OR REPLACE TASK document_db.s3_documents.chunk_documents_task
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Chunk ALL parsed documents for Cortex Search - no classification dependency'
  AFTER document_db.s3_documents.extract_documents_task
AS
  CALL document_db.s3_documents.chunk_parsed_documents();

CREATE OR REPLACE TASK document_db.s3_documents.split_large_documents_task
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Split large PDFs (>400 pages) and upload parts back to stage for processing'
  AFTER document_db.s3_documents.chunk_documents_task
AS
  CALL document_db.s3_documents.split_large_documents(); 

-- Resume all tasks in the correct order (leaf tasks first, then up to root)
-- Order: chunk → extract → classify → split_large → parse

ALTER TASK document_db.s3_documents.chunk_documents_task RESUME;
ALTER TASK document_db.s3_documents.extract_documents_task RESUME;
ALTER TASK document_db.s3_documents.classify_documents_task RESUME;
ALTER TASK document_db.s3_documents.split_large_documents_task RESUME;

ALTER TASK document_db.s3_documents.parse_documents_task RESUME;



-- =============================
-- FLATTENED DOCUMENT PROCESSING VIEW
-- =============================
-- Create a comprehensive flattened view that combines all document processing results
-- This view shows document_id, file_name, document_class, attribute_name, and attribute_value
-- Each row represents one document-attribute pair (e.g., customer_count: 12,062)

CREATE OR REPLACE VIEW document_db.s3_documents.document_processing_summary AS
WITH flattened_extractions AS (
    -- Handle regular attributes (non-JSON)
    SELECT 
        document_id,
        file_name,
        file_path,
        document_class,
        attribute_name,
        attribute_value,
        confidence_score,
        extraction_timestamp
    FROM document_db.s3_documents.document_extractions
    WHERE TRY_PARSE_JSON(attribute_value) IS NULL
    
    UNION ALL
    
    -- Handle JSON attributes - flatten them
    SELECT 
        de.document_id,
        de.file_name,
        de.file_path,
        de.document_class,
        f.key::STRING AS attribute_name,
        f.value::STRING AS attribute_value,
        de.confidence_score,
        de.extraction_timestamp
    FROM document_db.s3_documents.document_extractions de,
         LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(de.attribute_value)) f
    WHERE TRY_PARSE_JSON(de.attribute_value) IS NOT NULL
)
SELECT 
    dc.document_id,
    dc.file_name,
    dc.file_path,
    dc.document_type,
    -- Clean document classification (remove JSON formatting if present)
    CASE 
        WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
        ELSE dc.document_class
    END as document_classification,
    fe.attribute_name,
    fe.attribute_value,
    fe.confidence_score,
    dc.classification_timestamp,
    fe.extraction_timestamp
FROM document_db.s3_documents.document_classifications dc
LEFT JOIN flattened_extractions fe 
    ON dc.document_id = fe.document_id
ORDER BY dc.document_id, fe.attribute_name;


-- =============================
-- LARGE DOCUMENT STATUS VIEW
-- =============================
-- View to monitor large documents that need special handling
CREATE OR REPLACE VIEW document_db.s3_documents.large_document_status AS
SELECT 
    ldr.registry_id,
    ldr.original_file_path,
    ldr.original_file_name,
    ldr.original_file_size,
    ROUND(ldr.original_file_size / 1048576.0, 2) as size_mb,
    ldr.estimated_pages,
    ldr.split_required,
    ldr.split_count,
    ldr.split_status,
    ldr.created_timestamp,
    ldr.processed_timestamp,
    COUNT(dsp.part_id) as parts_created,
    SUM(CASE WHEN dsp.status = 'processed' THEN 1 ELSE 0 END) as parts_processed
FROM document_db.s3_documents.large_document_registry ldr
LEFT JOIN document_db.s3_documents.document_split_parts dsp 
    ON ldr.registry_id = dsp.registry_id
GROUP BY 
    ldr.registry_id,
    ldr.original_file_path,
    ldr.original_file_name,
    ldr.original_file_size,
    ldr.estimated_pages,
    ldr.split_required,
    ldr.split_count,
    ldr.split_status,
    ldr.created_timestamp,
    ldr.processed_timestamp
ORDER BY ldr.created_timestamp DESC;


-- =============================
-- PIPELINE STATUS VIEW
-- =============================
-- Comprehensive view of pipeline processing status
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
GROUP BY split_status;


-- =============================
-- START TASKS AND VALIDATION QUERIES
-- =============================

-- Verify task status
DESC TASK document_db.s3_documents.parse_documents_task;
DESC TASK document_db.s3_documents.classify_documents_task;
DESC TASK document_db.s3_documents.extract_documents_task;
DESC TASK document_db.s3_documents.chunk_documents_task;

-- Validation queries to check pipeline status
SELECT * FROM document_db.s3_documents.new_documents_stream;
SELECT * FROM document_db.s3_documents.parsed_documents;
SELECT * FROM document_db.s3_documents.document_classifications;
SELECT * FROM document_db.s3_documents.document_extractions;
SELECT * FROM document_db.s3_documents.document_chunks;

-- Check large document status
SELECT * FROM document_db.s3_documents.large_document_status;

-- Check overall pipeline status
SELECT * FROM document_db.s3_documents.pipeline_status;

-- Validation query for flattened document processing results
-- This shows each document with individual attribute-value pairs (one row per attribute)
SELECT * FROM document_db.s3_documents.document_processing_summary LIMIT 20;

-- Summary statistics for the flattened view
SELECT 
    COUNT(DISTINCT document_id) as total_documents,
    COUNT(DISTINCT document_classification) as unique_classifications,
    COUNT(DISTINCT attribute_name) as unique_attributes,
    COUNT(*) as total_rows_including_nulls
FROM document_db.s3_documents.document_processing_summary;


-- =============================
-- AUTO-REFRESH TESTING & MONITORING
-- =============================

-- Test auto-refresh functionality
-- After uploading a file to S3, these should automatically update:

-- 1. Directory table should show new files (within seconds)
SELECT relative_path, size, last_modified 
FROM DIRECTORY(@document_db.s3_documents.document_stage) 
ORDER BY last_modified DESC 
LIMIT 5;

-- 2. Stream should capture the new files (within seconds)
SELECT * FROM document_db.s3_documents.new_documents_stream;

-- 3. Tasks should automatically trigger
SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME ILIKE '%document%'
ORDER BY SCHEDULED_TIME DESC 
LIMIT 10;


-- =============================
-- CORTEX SEARCH SERVICE MONITORING
-- =============================

-- Check Cortex Search Service status
SHOW CORTEX SEARCH SERVICES IN SCHEMA document_db.s3_documents;

-- Test semantic search (after documents are processed)
-- SELECT * FROM TABLE(
--   document_db.s3_documents.document_search_service.SEARCH(
--     'contract renewal terms',
--     5
--   )
-- );


-- =============================
-- MANUAL PIPELINE EXECUTION (FOR TESTING)
-- =============================
-- Use these commands to manually run the pipeline steps:

-- CALL document_db.s3_documents.parse_new_documents();
-- CALL document_db.s3_documents.classify_parsed_documents();
-- CALL document_db.s3_documents.extract_attributes_for_classified_documents();
-- CALL document_db.s3_documents.chunk_classified_documents();


-- =============================
-- MONITORING AND TROUBLESHOOTING
-- =============================

-- Check auto-refresh status
-- DESC STAGE document_db.s3_documents.document_stage;

-- Monitor auto-refresh activity
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY(
--   DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
-- ));

-- View refresh history and any errors
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.STAGE_DIRECTORY_FILE_REGISTRATION_HISTORY(
--   'document_db.s3_documents.document_stage'
-- ));

-- Monitor costs (auto-refresh appears as Snowpipe usage)
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
--   DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
-- ));
select * FROM document_db.s3_documents.new_documents_stream;

ls @DOCUMENT_DB.S3_DOCUMENTS.DOCUMENT_STAGE;

DESC STREAM document_db.s3_documents.new_documents_stream;

DESC TASK document_db.s3_documents.parse_documents_task;

/* 
--troubleshooting commands
ALTER TASK document_db.s3_documents.parse_documents_task RESUME  ;
ALTER TASK document_db.s3_documents.split_large_documents_task RESUME;
ALTER TASK document_db.s3_documents.classify_documents_task RESUME;
ALTER TASK document_db.s3_documents.extract_documents_task RESUME;
ALTER TASK document_db.s3_documents.chunk_documents_task RESUME;
*/

--more troubleshooting commands
CALL document_db.s3_documents.parse_new_documents();
select * FROM large_document_registry;

select * FROM parsed_documents;

CALL split_large_documents();

CALL classify_parsed_documents();

call extract_attributes_for_classified_documents();
call chunk_parsed_documents();

SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE EXECUTION_STATUS = 'RUNNING'
ORDER BY START_TIME DESC;

-- Suspend the task first
ALTER TASK document_db.s3_documents.parse_documents_task SUSPEND;

-- Check what's in the stream (this will show data without consuming it)
SELECT COUNT(*) FROM document_db.s3_documents.new_documents_stream;

-- Resume the warehouse if needed
ALTER WAREHOUSE COMPUTE_WH RESUME;

-- Try running the procedure manually to see if it works
CALL document_db.s3_documents.parse_new_documents();

select * FROM document_db.s3_documents.parsed_documents
order by parse_timestamp desc;
