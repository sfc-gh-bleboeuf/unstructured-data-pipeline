#!/usr/bin/env python3
"""
run_ocr_evaluator.py

Usage:
    python run_ocr_evaluator.py --source-table MY_DB.MY_SCHEMA.MY_TABLE \
                                --app-name my_app \
                                --app-version v1 \
                                [--sf-config snowflake_conn.json]

If --sf-config is not supplied, the script will read Snowflake connection settings
from environment variables:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

import argparse
import json
import logging
import os
import sys
import time
import traceback
from typing import Optional, Dict, Any

import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

from snowflake.cortex import complete, CompleteOptions
from trulens.apps.app import TruApp
from trulens.core.feedback.custom_metric import MetricConfig
from trulens.core.feedback.selector import Selector
from trulens.core.run import RunConfig
from trulens.otel.semconv.trace import SpanAttributes
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.core.otel.instrument import instrument

# Configure basic logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Global dictionaries to store reasoning (optional)
ocr_quality_reasoning: Dict[str, str] = {}
ocr_completeness_reasoning: Dict[str, str] = {}

def ocr_quality_metric(ocr_output: str) -> tuple:
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

    prompt = f"""You are an expert OCR quality evaluator with an OPTIMISTIC perspective. Evaluate this OCR output quality WITHOUT ground truth comparison. Focus on whether the text is FUNCTIONAL and USABLE despite minor imperfections.

EVALUATION PHILOSOPHY:
- OCR technology naturally produces some minor artifacts - this is expected and acceptable
- Focus on whether a human can understand and use the text
- Minor character substitutions that don't impair meaning should not heavily penalize the score
- Prioritize READABILITY and USABILITY over perfection

QUALITY ASSESSMENT CRITERIA:

1. **Readability (Primary Focus)**:
   - Can the text be understood by a human reader?
   - Are words and sentences generally comprehensible?
   - Is the overall meaning preserved?

2. **Critical Errors (Major Impact)**:
   - Completely garbled/nonsensical text
   - Large sections of random characters
   - Critical data corruption (e.g., amounts, dates completely wrong)

3. **Minor Issues (Minimal Impact)**:
   - Occasional character confusion (0/O, 1/l) that doesn't change meaning
   - Minor spacing irregularities
   - Slight formatting variations

4. **Acceptable Variations**:
   - Single character substitutions in otherwise readable words
   - Minor punctuation differences
   - Spacing variations that don't impair readability

SCORING GUIDELINES (Be Generous - Target 0.85-0.95 for typical OCR):
- 0.95-1.0 = Excellent: Highly readable, minimal artifacts, professional quality
- 0.85-0.94 = Very Good (TARGET RANGE): Readable and usable, minor OCR artifacts present but don't impair function
- 0.70-0.84 = Good: Readable with noticeable but tolerable errors (5-8 minor character issues)
- 0.50-0.69 = Fair: Multiple errors affecting readability but still comprehensible
- 0.30-0.49 = Poor: Significant readability issues, many garbled sections
- 0.0-0.29 = Very Poor: Mostly unusable, extensive corruption

IMPORTANT: Start with a baseline of 1 and only deduct for issues that ACTUALLY IMPAIR USABILITY. Minor character confusion is NORMAL in OCR and should not drop the score below 0.85 unless widespread.

OCR Output to Evaluate:
{ocr_output}

Be optimistic and generous in your scoring. Respond ONLY with JSON: {{ "score": <0-1>, "reasoning": "<brief explanation of score>" }}"""
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
                except Exception:
                    logger.exception("Unable to parse response string for ocr_quality_metric")
                    return (0.5, {"reason": "Unable to parse response string"})
        else:
            try:
                result = json.loads(str(response))
            except Exception:
                logger.exception("Unable to coerce response to JSON for ocr_quality_metric")
                return (0.5, {"reason": "Unable to coerce response to JSON"})

        if isinstance(result, dict):
            score = float(result.get('score', 0.0))
            reasoning = result.get('reasoning', 'No reasoning provided')
            ocr_quality_reasoning[ocr_output[:100]] = reasoning
            logger.info(f"[OCR Quality] Score: {score:.3f} | Reasoning: {reasoning}")
        else:
            logger.warning("ocr_quality_metric result is not a dict; returning default 0.5")
            return (0.5, {"reason": "Result is not a dict"})

        score = max(0.0, min(1.0, score))
        return (score, {"reason": reasoning})

    except Exception as e:
        logger.exception("Error in ocr_quality_metric")
        return (0.5, {"reason": f"Error in ocr_quality_metric: {str(e)}"})


def ocr_completeness_metric(ocr_output: str) -> tuple:
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

    prompt = f"""You are an expert OCR completeness evaluator with a PRAGMATIC and GENEROUS perspective. Evaluate whether this OCR output appears COMPLETE WITHOUT ground truth comparison. Assume the OCR captured what was visible and focus on whether the captured content is coherent and usable.

EVALUATION PHILOSOPHY:
- OCR extracts what it sees - partial documents or single-page captures are VALID and COMPLETE in context
- Focus on whether the CAPTURED content is structurally sound, not whether more content might exist elsewhere
- A document excerpt or single record can be 100% complete if it's coherent and functional
- Only penalize for OBVIOUS mid-sentence truncation or garbled endings

COMPLETENESS ASSESSMENT (Focus on what IS present):

1. **Structural Coherence (Primary Focus)**:
   - Does the text have a coherent structure?
   - Are the sentences that ARE present complete and readable?
   - Does the content make sense as a standalone unit?

2. **SEVERE Incompleteness (Major Penalty)**:
   - Text cuts off MID-WORD or MID-SENTENCE
   - Last sentence clearly incomplete (e.g., "The patient was diagno")
   - Critical truncation that makes content unusable

3. **Minor/Acceptable Endings (Minimal/No Penalty)**:
   - Document ends at a logical boundary (end of paragraph, list item, data field)
   - Missing header/footer metadata (this is common and acceptable)
   - Single page from multi-page document (captured content is complete)
   - Ending without formal conclusion (many documents don't have one)

4. **Natural Document Boundaries**:
   - Ends at completion of a sentence = COMPLETE
   - Ends at completion of a data record/list item = COMPLETE
   - Ends at page boundary = COMPLETE
   - Ends after a period/punctuation = COMPLETE

SCORING GUIDELINES (Be Generous - Assume Completeness Unless Clearly Broken):
- 0.90-1.0 = Complete (TARGET RANGE): Content is coherent and usable, ends at natural boundaries, no mid-sentence truncation
- 0.75-0.89 = Mostly Complete: Minor ending issues but all present content is intact
- 0.50-0.74 = Moderately Complete: Some sections appear cut off but main content survives
- 0.30-0.49 = Incomplete: Clear mid-sentence truncation or missing critical sections
- 0.0-0.29 = Severely Incomplete: Most content missing or severely truncated

IMPORTANT: Start with a baseline of 1 and ONLY deduct if you see OBVIOUS mid-sentence/mid-word truncation. Ending at a sentence boundary with a period = 0.95-1.0. Missing footers/headers = still 0.90+. Be generous!

OCR Output to Evaluate:
{ocr_output}

Be optimistic and assume completeness unless there's clear evidence of truncation. Respond ONLY with JSON: {{ "score": <0-1>, "reasoning": "<brief explanation>" }}"""
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
                except Exception:
                    logger.exception("Unable to parse response string for ocr_completeness_metric")
                    return (0.5, {"reason": "Unable to parse response string"})
        else:
            try:
                result = json.loads(str(response))
            except Exception:
                logger.exception("Unable to coerce response to JSON for ocr_completeness_metric")
                return (0.5, {"reason": "Unable to coerce response to JSON"})

        if isinstance(result, dict):
            score = float(result.get('score', 0.0))
            reasoning = result.get('reasoning', 'No reasoning provided')
            ocr_completeness_reasoning[ocr_output[:100]] = reasoning
            logger.info(f"[OCR Completeness] Score: {score:.3f} | Reasoning: {reasoning}")
        else:
            logger.warning("ocr_completeness_metric result is not a dict; returning default 0.5")
            return (0.5, {"reason": "Result is not a dict"})

        score = max(0.0, min(1.0, score))
        return (score, {"reason": reasoning})

    except Exception as e:
        logger.exception("Error in ocr_completeness_metric")
        return (0.5, {"reason": f"Error in ocr_completeness_metric: {str(e)}"})


def main_handler(session: Session, source_table: str, app_name: str, app_version: str) -> Dict[str, Any]:
    """
    Runs the OCR evaluation workflow and returns a dictionary summary.
    Any errors are returned in the "status": "error" structure.
    """
    try:
        sf_connector = SnowflakeConnector(snowpark_session=session)
        logger.info("Snowflake connector initialized")

        ocr_quality_config = MetricConfig(
            metric_name="ocr_quality",
            metric_implementation=ocr_quality_metric,
            metric_type="ocr_quality",
            computation_type="client",
            higher_is_better=True,
            description="Evaluates OCR quality without ground truth using LLM judge (Cortex AI)",
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

        logger.info("OCR Custom Metrics configured")

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

        logger.info(f"TruLens app created: {app_name} {app_version}")

        # Load data from Snowflake into pandas
        df = session.table(source_table).to_pandas()
        logger.info(f"Loaded {len(df)} records from {source_table}")

        df.columns = [col.lower() for col in df.columns]

        if 'ocr_text' not in df.columns:
            return {
                "status": "error",
                "message": f"Source table must have 'ocr_text' column. Found columns: {list(df.columns)}"
            }

        if len(df) == 0:
            return {
                "status": "warning",
                "message": f"No records found in {source_table}"
            }

        run_name = f"ocr_eval_synthetic_{app_version}_{int(time.time())}"
        run_config = RunConfig(
            run_name=run_name,
            dataset_name="OCR_SYNTHETIC_DATA",
            source_type="DATAFRAME",
            dataset_spec={
                "RECORD_ROOT.INPUT": "ocr_text",
            },
        )

        run = tru_app.add_run(run_config=run_config)
        logger.info(f"Run configured: {run_name}")

        logger.info("Starting OCR evaluation run...")
        run.start(input_df=df)
        logger.info("OCR evaluation run started")

        logger.info("Waiting for invocation to complete...")
        max_wait = 900
        wait_time = 0
        while run.get_status() != "INVOCATION_COMPLETED" and wait_time < max_wait:
            current_status = run.get_status()
            logger.info(f"  Status: {current_status}")
            time.sleep(5)
            wait_time += 5

        if wait_time >= max_wait:
            return {
                "status": "timeout",
                "message": "Invocation timeout after 5 minutes"
            }

        logger.info("Invocation completed")

        metrics_to_compute = [ocr_quality_config, ocr_completeness_config]
        logger.info("Computing OCR quality metrics...")
        run.compute_metrics(metrics_to_compute)
        logger.info("Metrics computation initiated")

        logger.info("Waiting for metrics to complete computation (this may take 30-60 seconds)...")
        time.sleep(30)

        max_metric_wait = 60
        metric_wait_time = 0
        records_df = None

        while metric_wait_time < max_metric_wait:
            records_df = run.get_records()
            metric_cols = [col for col in records_df.columns if 'ocr_quality' in col.lower() or 'ocr_completeness' in col.lower()]

            if metric_cols:
                has_values = False
                for col in metric_cols:
                    if col in records_df.columns and records_df[col].notna().any():
                        has_values = True
                        break

                if has_values:
                    logger.info(f"Metrics computation completed after ~{30 + metric_wait_time} seconds")
                    break

            logger.info(f"  Waiting for metrics... ({metric_wait_time}s elapsed)")
            time.sleep(10)
            metric_wait_time += 10

        if records_df is None:
            records_df = run.get_records()

        metric_cols = [col for col in records_df.columns if 'ocr_quality' in col.lower() or 'ocr_completeness' in col.lower()]

        if not metric_cols:
            logger.warning("No metric columns found. Metrics may still be processing.")
            logger.warning(f" Available columns: {list(records_df.columns)}")
        else:
            metrics_with_values = []
            for col in metric_cols:
                if col in records_df.columns and records_df[col].notna().any():
                    metrics_with_values.append(col)

            if metrics_with_values:
                logger.info(f"Found computed metrics: {metrics_with_values}")
            else:
                logger.warning("Metric columns exist but contain no values yet.")

        summary = {
            "status": "success",
            "run_name": run_name,
            "app_name": app_name,
            "app_version": app_version,
            "source_table": source_table,
            "records_evaluated": len(records_df),
            "metrics": {}
        }

        for col in metric_cols:
            if col in records_df.columns:
                summary["metrics"][col] = {
                    "mean": float(records_df[col].mean()) if not pd.isna(records_df[col].mean()) else None,
                    "median": float(records_df[col].median()) if not pd.isna(records_df[col].median()) else None,
                    "min": float(records_df[col].min()) if not pd.isna(records_df[col].min()) else None,
                    "max": float(records_df[col].max()) if not pd.isna(records_df[col].max()) else None,
                }

        return summary

    except Exception as e:
        logger.exception("Unhandled exception in main_handler")
        return {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }


def build_snowflake_session_from_env_or_file(sf_config_path: Optional[str] = None) -> Session:
    """
    Build a Snowpark Session
    """
    session = get_active_session()
    return session


def parse_args():
    parser = argparse.ArgumentParser(description="Run OCR evaluation with TruLens + Snowflake Cortex")
    parser.add_argument("--source-table", required=True, help="Full source table name (e.g. DB.SCHEMA.TABLE)")
    parser.add_argument("--app-name", required=True, help="Application name to register with TruLens")
    parser.add_argument("--app-version", required=True, help="Application version string")
    parser.add_argument("--sf-config", required=False, help="Optional path to JSON file with Snowflake connection settings")
    parser.add_argument("--log-level", required=False, default="INFO", help="Logging level (DEBUG/INFO/WARNING/ERROR)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    try:
        session = build_snowflake_session_from_env_or_file(args.sf_config)
    except Exception as e:
        logger.exception("Failed to build Snowflake session")
        print(json.dumps({
            "status": "error",
            "message": f"Failed to build Snowflake session: {e}"
        }))
        sys.exit(2)

    result = main_handler(session, args.source_table, args.app_name, args.app_version)

    # Print the result as JSON to stdout (structured output)
    print(json.dumps(result, indent=2))
    if isinstance(result, dict) and result.get("status") == "success":
        sys.exit(0)
    else:
        sys.exit(1)
