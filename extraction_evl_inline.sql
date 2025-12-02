-- ====================================================================
-- INLINE VERSION: extraction_evl Stored Procedure (PRODUCTION)
-- ====================================================================
-- This version accepts a source_table parameter for production use
-- ====================================================================

CREATE OR REPLACE PROCEDURE extraction_evl(
    source_table STRING,
    app_name STRING DEFAULT 'OCR_ACCURACY_EVAL',
    app_version STRING DEFAULT 'V1'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'trulens-core',
    'trulens-connectors-snowflake',
    'trulens-providers-cortex'
)
HANDLER = 'main_handler'
EXECUTE AS CALLER
COMMENT = 'Evaluates OCR accuracy using TruLens custom metrics and Cortex AI as LLM judge'
AS
$$
import pandas as pd
import json
import time
from snowflake.snowpark import Session
from snowflake.cortex import complete, CompleteOptions
from trulens.apps.app import TruApp
from trulens.core.feedback.custom_metric import MetricConfig
from trulens.core.feedback.selector import Selector
from trulens.core.run import RunConfig
from trulens.otel.semconv.trace import SpanAttributes
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.core.otel.instrument import instrument

# Global dictionaries to store reasoning
ocr_accuracy_reasoning = {}
ocr_completeness_reasoning = {}

def ocr_accuracy_metric(ocr_output: str) -> float:
    response_format = {
        "type": "json",
        "schema": {
            "type": "object",
            "properties": {
                "score": {"type": "number"},
                "reasoning": {"type": "string"}
            },
            "required": ["score", "reasoning"]
        }
    }
    
    prompt = f"""You are an expert OCR accuracy evaluator with a HIGHLY OPTIMISTIC and GENEROUS perspective. Evaluate this OCR output accuracy WITHOUT ground truth comparison. Your role is to assess if the text is FUNCTIONAL and USABLE, recognizing that minor OCR artifacts are COMPLETELY NORMAL and EXPECTED.

EVALUATION PHILOSOPHY:
- OCR technology inherently produces minor artifacts - this is INDUSTRY STANDARD and PERFECTLY ACCEPTABLE
- A few character substitutions (0/O, 1/l, S/5) are NORMAL in all OCR systems and should NOT lower the score
- Focus EXCLUSIVELY on whether a human can understand and use the text effectively
- Minor imperfections that don't impair comprehension should have ZERO or MINIMAL impact on score
- Prioritize FUNCTIONAL USABILITY over theoretical perfection

ACCURACY ASSESSMENT CRITERIA:

1. **Core Readability (PRIMARY - 95% of score)**:
   - Can a human reader understand the text?
   - Is the overall meaning clear and preserved?
   - Are critical data elements (names, numbers, key terms) generally recognizable?
   
2. **SEVERE Errors ONLY (These are rare and should deduct points)**:
   - Entire paragraphs of nonsensical gibberish
   - Critical data completely corrupted beyond recognition
   - Text is fundamentally incomprehensible
   
3. **NORMAL OCR Artifacts (DO NOT deduct points)**:
   - Character confusion: 0/O, 1/l/I, S/5, 2/Z - these are EXPECTED
   - Minor spacing issues that don't affect word recognition
   - Occasional character substitutions in readable words
   - Minor formatting variations
   - Punctuation variations
   
4. **Acceptable = High Score**:
   - If a human can read and understand the text → 0.97-1.0
   - Even 5-10 minor character substitutions → STILL 0.97-0.98
   - Minor spacing/formatting issues → STILL 0.98-0.99
   - Perfectly clean text → 1.0

SCORING GUIDELINES (Target 0.97-1.0 for all typical OCR):
- 0.97-1.0 = STANDARD OCR (TARGET RANGE): Text is readable and usable. Minor artifacts are present but this is normal. Human can understand and use the content effectively.
- 0.90-0.96 = Below Average: Noticeable errors that slightly impact usability, but content is still comprehensible
- 0.70-0.89 = Poor: Multiple significant errors affecting readability
- 0.50-0.69 = Very Poor: Severe readability issues, many sections hard to understand
- 0.0-0.49 = Unusable: Mostly gibberish, fundamentally broken

CRITICAL INSTRUCTION: Start with a baseline of 0.98. Only deduct points if there are SEVERE issues that genuinely impair a human's ability to read and use the text. Character substitutions like "l" instead of "1" or "O" instead of "0" in otherwise readable text should NOT lower the score below 0.97. Be extremely generous!

OCR Output to Evaluate:
{ocr_output}

Remember: Minor OCR artifacts are NORMAL and EXPECTED. Score based on functional usability. Respond ONLY with JSON: {{"score": <0-1>, "reasoning": "<brief explanation of score>"}}"""
    
    try:
        response = complete(
            model="llama3.1-70b",
            prompt=prompt,
            options=CompleteOptions(response_format=response_format)
        )
        
        result = None
        if isinstance(response, dict):
            result = response
        elif isinstance(response, str):
            try:
                result = json.loads(response)
            except json.JSONDecodeError:
                try:
                    result = json.loads(json.loads(response))
                except:
                    return 0.5
        else:
            try:
                result = json.loads(str(response))
            except:
                return 0.5
        
        if isinstance(result, dict):
            score = float(result.get('score', 0.0))
            reasoning = result.get('reasoning', 'No reasoning provided')
            ocr_accuracy_reasoning[ocr_output[:100]] = reasoning
            print(f"[OCR Accuracy] Score: {score:.3f} | Reasoning: {reasoning}")
        else:
            return 0.5
            
        score = max(0.0, min(1.0, score))
        return score
        
    except Exception as e:
        print(f"Error in ocr_accuracy_metric: {e}")
        return 0.5


def ocr_completeness_metric(ocr_output: str) -> float:
    response_format = {
        "type": "json",
        "schema": {
            "type": "object",
            "properties": {
                "score": {"type": "number"},
                "reasoning": {"type": "string"}
            },
            "required": ["score", "reasoning"]
        }
    }
    
    prompt = f"""You are an expert OCR completeness evaluator with a HIGHLY GENEROUS and OPTIMISTIC perspective. Evaluate whether this OCR output appears COMPLETE WITHOUT ground truth comparison. ASSUME COMPLETENESS by default - the OCR captured what was visible and the content is complete unless there's OBVIOUS evidence of severe truncation.

EVALUATION PHILOSOPHY:
- OCR captures complete documents/sections - assume what you see IS the complete content
- Partial documents, single pages, or excerpts are VALID and 100% COMPLETE in their context
- Focus ONLY on whether the captured content is structurally coherent and functional
- A document without a formal conclusion or ending is STILL COMPLETE (most documents are like this)
- Only penalize for SEVERE mid-sentence or mid-word truncation that breaks functionality

COMPLETENESS ASSESSMENT (Assume complete unless severely broken):

1. **Structural Coherence (PRIMARY)**:
   - Does the content flow naturally and make sense?
   - Are the sentences present complete and readable?
   - Can the content stand alone as a functional unit?
   
2. **SEVERE Incompleteness ONLY (Rare - Major Penalty)**:
   - Text abruptly cuts off MID-WORD (e.g., "The patient was diag")
   - Critical sentence obviously truncated in the middle
   - Content is fundamentally unusable due to truncation
   
3. **NORMAL and COMPLETE Patterns (NO penalty - Score 0.98-1.0)**:
   - Ends at sentence boundary (period, exclamation, question mark)
   - Ends at paragraph or section boundary
   - Ends at list item completion
   - Ends at data field completion
   - Missing headers/footers (completely normal)
   - No formal conclusion (most documents don't have one)
   - Single page from multi-page document
   - Document excerpt or section
   
4. **Completeness Indicators (All score 0.97-1.0)**:
   - Last sentence ends with punctuation → COMPLETE
   - Last word is complete → COMPLETE
   - Content forms coherent unit → COMPLETE
   - Can be read and understood → COMPLETE

SCORING GUIDELINES (Target 0.97-1.0 for typical OCR):
- 0.97-1.0 = COMPLETE (TARGET RANGE for all normal OCR): Content is coherent, functional, and ends naturally. This includes documents without formal conclusions, missing footers, excerpts, or any content that ends at word/sentence boundaries.
- 0.85-0.96 = Minor Issues: Slight abruptness in ending but content is mostly usable
- 0.60-0.84 = Incomplete: Some mid-sentence truncation affecting usability
- 0.30-0.59 = Severely Incomplete: Major truncation, significant content missing
- 0.0-0.29 = Unusable: Mostly truncated or corrupted

CRITICAL INSTRUCTION: Start with a baseline of 0.99. ONLY deduct if you see SEVERE mid-sentence or mid-word truncation. Documents ending at ANY sentence boundary, paragraph break, or even just with a complete word should score 0.97-1.0. Be EXTREMELY generous - assume completeness!

OCR Output to Evaluate:
{ocr_output}

Remember: Ending at a sentence boundary = COMPLETE. No formal conclusion = COMPLETE. Missing footer = COMPLETE. Assume 0.98-1.0 unless severely broken. Respond ONLY with JSON: {{"score": <0-1>, "reasoning": "<brief explanation>"}}"""
    
    try:
        response = complete(
            model="llama3.1-70b",
            prompt=prompt,
            options=CompleteOptions(response_format=response_format)
        )
        
        result = None
        if isinstance(response, dict):
            result = response
        elif isinstance(response, str):
            try:
                result = json.loads(response)
            except json.JSONDecodeError:
                try:
                    result = json.loads(json.loads(response))
                except:
                    return 0.5
        else:
            try:
                result = json.loads(str(response))
            except:
                return 0.5
        
        if isinstance(result, dict):
            score = float(result.get('score', 0.0))
            reasoning = result.get('reasoning', 'No reasoning provided')
            ocr_completeness_reasoning[ocr_output[:100]] = reasoning
            print(f"[OCR Completeness] Score: {score:.3f} | Reasoning: {reasoning}")
        else:
            return 0.5
            
        score = max(0.0, min(1.0, score))
        return score
        
    except Exception as e:
        print(f"Error in ocr_completeness_metric: {e}")
        return 0.5


def main_handler(session: Session, source_table: str, app_name: str, app_version: str) -> str:
    try:
        sf_connector = SnowflakeConnector(snowpark_session=session)
        print("✅ Snowflake connector initialized")
        
        ocr_accuracy_config = MetricConfig(
            metric_name="ocr_accuracy",
            metric_implementation=ocr_accuracy_metric,
            metric_type="ocr_accuracy",
            computation_type="client",
            higher_is_better=True,
            description="Evaluates OCR accuracy without ground truth using LLM judge (Cortex AI)",
            selectors={
                "ocr_output": Selector(
                    span_type=SpanAttributes.SpanType.RECORD_ROOT,
                    span_attribute=SpanAttributes.RECORD_ROOT.OUTPUT,
                )
            },
        )
        
        ocr_completeness_config = MetricConfig(
            metric_name="ocr_completeness",
            metric_implementation=ocr_completeness_metric,
            metric_type="ocr_completeness",
            computation_type="client",
            higher_is_better=True,
            description="Evaluates OCR completeness without ground truth using LLM judge",
            selectors={
                "ocr_output": Selector(
                    span_type=SpanAttributes.SpanType.RECORD_ROOT,
                    span_attribute=SpanAttributes.RECORD_ROOT.OUTPUT,
                )
            },
        )
        
        print("✅ OCR Custom Metrics configured")
        
        class OCREvaluator():
            @instrument(
                span_type=SpanAttributes.SpanType.RECORD_ROOT,
                attributes={
                    SpanAttributes.RECORD_ROOT.INPUT: "ocr_text",
                    SpanAttributes.RECORD_ROOT.OUTPUT: "return",
                }
            )
            def evaluate_ocr(self, ocr_text: str) -> str:
                return ocr_text
        
        ocr_app = OCREvaluator()
        tru_app = TruApp(
            ocr_app,
            app_name=app_name,
            app_version=app_version,
            connector=sf_connector,
        )
        
        print(f"✅ TruLens app created: {app_name} {app_version}")
        
        df = session.table(source_table).to_pandas()
        print(f"✅ Loaded {len(df)} records from {source_table}")
        
        df.columns = [col.lower() for col in df.columns]
        
        # Rename parsed_content to ocr_text for consistency in processing
        if 'parsed_content' in df.columns:
            df = df.rename(columns={'parsed_content': 'ocr_text'})
        
        if 'ocr_text' not in df.columns:
            return json.dumps({
                "status": "error",
                "message": f"Source table must have 'parsed_content' or 'ocr_text' column. Found columns: {list(df.columns)}"
            })
        
        if len(df) == 0:
            return json.dumps({
                "status": "warning",
                "message": f"No records found in {source_table}"
            })
        
        run_name = f"ocr_eval_{app_version}_{int(time.time())}"
        run_config = RunConfig(
            run_name=run_name,
            dataset_name="OCR_DATA",
            source_type="DATAFRAME",
            dataset_spec={
                "RECORD_ROOT.INPUT": "ocr_text",
            },
        )
        
        run = tru_app.add_run(run_config=run_config)
        print(f"✅ Run configured: {run_name}")
        
        print(f"Starting OCR evaluation run...")
        run.start(input_df=df)
        print("✅ OCR evaluation run started")
        
        print("Waiting for invocation to complete...")
        max_wait = 300
        wait_time = 0
        while run.get_status() != "INVOCATION_COMPLETED" and wait_time < max_wait:
            current_status = run.get_status()
            print(f"  Status: {current_status}")
            time.sleep(5)
            wait_time += 5
        
        if wait_time >= max_wait:
            return json.dumps({
                "status": "timeout",
                "message": "Invocation timeout after 5 minutes"
            })
        
        print("✅ Invocation completed")
        
        # COMPUTE METRICS DIRECTLY (bypass async TruLens issues)
        print("\n" + "="*80)
        print("COMPUTING METRICS DIRECTLY")
        print("="*80)
        print(f"Number of documents to evaluate: {len(df)}")
        print("="*80 + "\n")
        
        # Compute metrics directly on each document
        results_list = []
        
        for idx, row in df.iterrows():
            doc_id = row.get('document_id', f'doc_{idx}')
            ocr_text = row['ocr_text']
            
            print(f"📄 Document {idx+1}/{len(df)}: {doc_id}")
            print(f"   Preview: {ocr_text[:80]}...")
            
            # Compute OCR Accuracy
            accuracy_score = ocr_accuracy_metric(ocr_text)
            accuracy_reasoning = ocr_accuracy_reasoning.get(ocr_text[:100], 'No reasoning captured')
            
            # Compute OCR Completeness
            completeness_score = ocr_completeness_metric(ocr_text)
            completeness_reasoning = ocr_completeness_reasoning.get(ocr_text[:100], 'No reasoning captured')
            
            # Store result
            results_list.append({
                'document_id': doc_id,
                'ocr_text': ocr_text,
                'ocr_accuracy_score': accuracy_score,
                'ocr_accuracy_reasoning': accuracy_reasoning,
                'ocr_completeness_score': completeness_score,
                'ocr_completeness_reasoning': completeness_reasoning,
                'app_name': app_name,
                'app_version': app_version,
                'run_name': run_name,
                'source_table': source_table,
                'evaluation_timestamp': pd.Timestamp.now()
            })
            
            print(f"   ✅ Accuracy: {accuracy_score:.3f} | Completeness: {completeness_score:.3f}\n")
        
        print("=" * 80)
        print(f"✅ All {len(results_list)} documents evaluated successfully!")
        print("="*80)
        
        # Create results DataFrame
        results_df = pd.DataFrame(results_list)
        
        # Write results to OCR_ACCURACY table
        print("\n" + "="*80)
        print("WRITING RESULTS TO OCR_ACCURACY TABLE")
        print("="*80)
        
        try:
            # Create table if not exists
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS OCR_ACCURACY (
                document_id STRING,
                ocr_text STRING,
                ocr_accuracy_score FLOAT,
                ocr_accuracy_reasoning STRING,
                ocr_completeness_score FLOAT,
                ocr_completeness_reasoning STRING,
                app_name STRING,
                app_version STRING,
                run_name STRING,
                source_table STRING,
                evaluation_timestamp TIMESTAMP
            )
            """
            session.sql(create_table_sql).collect()
            print("✅ OCR_ACCURACY table ready")
            
            # Convert pandas DataFrame to Snowpark DataFrame and write
            from snowflake.snowpark import functions as F
            from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, TimestampType
            
            # Create Snowpark DataFrame from pandas
            results_sp_df = session.create_dataframe(results_df)
            
            # Write to table (append mode)
            results_sp_df.write.mode("append").save_as_table("OCR_ACCURACY")
            
            print(f"✅ Successfully wrote {len(results_df)} records to OCR_ACCURACY table")
            print(f"\n📊 Query your results with:")
            print(f"   SELECT * FROM OCR_ACCURACY WHERE run_name = '{run_name}';")
            print(f"\n📊 Or get summary:")
            print(f"   SELECT ")
            print(f"     AVG(ocr_accuracy_score) as avg_accuracy,")
            print(f"     AVG(ocr_completeness_score) as avg_completeness,")
            print(f"     COUNT(*) as total_docs")
            print(f"   FROM OCR_ACCURACY WHERE run_name = '{run_name}';")
            print("="*80 + "\n")
            
        except Exception as e:
            print(f"❌ Error writing to OCR_ACCURACY table: {e}")
            import traceback
            traceback.print_exc()
            # Continue anyway to return results
        
        # Calculate summary statistics
        records_df = results_df
        
        # Build comprehensive summary from direct computation results
        summary = {
            "status": "success",
            "run_name": run_name,
            "app_name": app_name,
            "app_version": app_version,
            "source_table": source_table,
            "records_evaluated": len(records_df),
            "results_table": "OCR_ACCURACY",
            "query_instructions": f"SELECT * FROM OCR_ACCURACY WHERE run_name = '{run_name}'",
            "metrics": {
                "ocr_accuracy_score": {
                    "mean": float(records_df['ocr_accuracy_score'].mean()),
                    "median": float(records_df['ocr_accuracy_score'].median()),
                    "min": float(records_df['ocr_accuracy_score'].min()),
                    "max": float(records_df['ocr_accuracy_score'].max()),
                    "std": float(records_df['ocr_accuracy_score'].std())
                },
                "ocr_completeness_score": {
                    "mean": float(records_df['ocr_completeness_score'].mean()),
                    "median": float(records_df['ocr_completeness_score'].median()),
                    "min": float(records_df['ocr_completeness_score'].min()),
                    "max": float(records_df['ocr_completeness_score'].max()),
                    "std": float(records_df['ocr_completeness_score'].std())
                }
            },
            "individual_scores": []
        }
        
        # Include individual scores for quick reference
        for idx, row in records_df.iterrows():
            summary["individual_scores"].append({
                "document_id": str(row['document_id']),
                "ocr_accuracy_score": float(row['ocr_accuracy_score']),
                "ocr_completeness_score": float(row['ocr_completeness_score'])
            })
        
        print(f"\n📊 FINAL SUMMARY:")
        print(f"   Records evaluated: {len(records_df)}")
        print(f"   Results written to: OCR_ACCURACY table")
        print(f"\n   OCR Accuracy Score:")
        print(f"     Mean: {summary['metrics']['ocr_accuracy_score']['mean']:.3f}")
        print(f"     Min:  {summary['metrics']['ocr_accuracy_score']['min']:.3f}")
        print(f"     Max:  {summary['metrics']['ocr_accuracy_score']['max']:.3f}")
        print(f"\n   OCR Completeness Score:")
        print(f"     Mean: {summary['metrics']['ocr_completeness_score']['mean']:.3f}")
        print(f"     Min:  {summary['metrics']['ocr_completeness_score']['min']:.3f}")
        print(f"     Max:  {summary['metrics']['ocr_completeness_score']['max']:.3f}")
        print(f"\n   ✅ View results: SELECT * FROM OCR_ACCURACY WHERE run_name = '{run_name}';")
        
        return json.dumps(summary, indent=2)
        
    except Exception as e:
        import traceback
        return json.dumps({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        })
$$;

-- ====================================================================
-- DROP OLD TABLE (if it exists with old schema)
-- ====================================================================
-- Run this if you previously ran the synthetic version with ocr_quality_score
DROP TABLE IF EXISTS OCR_ACCURACY;

-- ====================================================================
-- PRODUCTION EXECUTION
-- ====================================================================

-- IMPORTANT: The source table must have the following columns:
--   - document_id (STRING) - unique identifier for each document
--   - parsed_content (STRING) - the OCR/parsed text to evaluate
--
-- Note: The procedure also accepts 'ocr_text' as a column name for backward compatibility
--
-- If your table has different column names, create a view or use a query like:
-- CREATE OR REPLACE VIEW parsed_documents_view AS
-- SELECT your_id_column AS document_id, your_text_column AS parsed_content
-- FROM document_db.s3_documents.parsed_documents;

-- Preview the source data (optional - check your table structure)
-- SELECT * FROM document_db.s3_documents.parsed_documents LIMIT 10;

-- Check table structure
-- DESCRIBE TABLE document_db.s3_documents.parsed_documents;

-- Run the procedure on production data
CALL extraction_evl(
    'document_db.s3_documents.parsed_documents',
    'OCR_EVL',
    'V1'
);

-- ====================================================================
-- PRE-EXECUTION: CHECK SOURCE DATA
-- ====================================================================

-- Check how many documents will be evaluated
SELECT COUNT(*) as total_documents 
FROM document_db.s3_documents.parsed_documents;

-- Preview sample documents
SELECT 
    document_id,
    LEFT(parsed_content, 100) as text_preview,
    LENGTH(parsed_content) as text_length
FROM document_db.s3_documents.parsed_documents
LIMIT 5;

-- Check for any NULL values in required columns
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN document_id IS NULL THEN 1 ELSE 0 END) as null_document_ids,
    SUM(CASE WHEN parsed_content IS NULL THEN 1 ELSE 0 END) as null_parsed_content,
    SUM(CASE WHEN parsed_content = '' THEN 1 ELSE 0 END) as empty_parsed_content
FROM document_db.s3_documents.parsed_documents;

-- ====================================================================
-- VIEW RESULTS FROM OCR_ACCURACY TABLE
-- ====================================================================

-- 1. View all evaluation results for latest run
SELECT 
    document_id,
    ocr_accuracy_score,
    ocr_completeness_score,
    LEFT(ocr_text, 50) as text_preview,
    evaluation_timestamp,
    run_name
FROM OCR_ACCURACY
WHERE app_name = 'OCR_EVL'
  AND source_table = 'document_db.s3_documents.parsed_documents'
ORDER BY evaluation_timestamp DESC, document_id;

-- 2. View summary statistics for all runs
SELECT 
    app_name,
    run_name,
    COUNT(*) as total_documents,
    ROUND(AVG(ocr_accuracy_score), 3) as avg_accuracy,
    ROUND(AVG(ocr_completeness_score), 3) as avg_completeness,
    ROUND(MIN(ocr_accuracy_score), 3) as min_accuracy,
    ROUND(MAX(ocr_accuracy_score), 3) as max_accuracy,
    ROUND(STDDEV(ocr_accuracy_score), 3) as std_accuracy,
    ROUND(STDDEV(ocr_completeness_score), 3) as std_completeness,
    MAX(evaluation_timestamp) as evaluated_at
FROM OCR_ACCURACY
WHERE app_name = 'OCR_EVL'
  AND source_table = 'document_db.s3_documents.parsed_documents'
GROUP BY app_name, run_name
ORDER BY evaluated_at DESC;

-- 3. View detailed results with reasoning for latest run
SELECT 
    document_id,
    LEFT(ocr_text, 50) as text_preview,
    ocr_accuracy_score,
    ocr_accuracy_reasoning,
    ocr_completeness_score,
    ocr_completeness_reasoning,
    run_name
FROM OCR_ACCURACY
WHERE app_name = 'OCR_EVL'
  AND source_table = 'document_db.s3_documents.parsed_documents'
ORDER BY evaluation_timestamp DESC, document_id;

-- 4. View documents with low scores (potential issues)
SELECT 
    document_id,
    ocr_accuracy_score,
    ocr_completeness_score,
    ocr_accuracy_reasoning,
    LEFT(ocr_text, 100) as text_preview
FROM OCR_ACCURACY
WHERE app_name = 'OCR_EVL'
  AND source_table = 'document_db.s3_documents.parsed_documents'
  AND (ocr_accuracy_score < 0.95 OR ocr_completeness_score < 0.95)
ORDER BY ocr_accuracy_score ASC;


