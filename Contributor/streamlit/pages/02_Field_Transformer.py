"""
AI-Powered Transformation App (Standalone)
=========================================
Loads saved field mappings and generates SQL transformations
This is the transformation phase separated from the mapping app for easier development
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# Configure page
st.set_page_config(
    page_title="Field Transformer",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 AI-Powered Field Transformations")
st.markdown("**Generate SQL transformations for mapped fields**")

# Initialize session state
if 'transformations' not in st.session_state:
    st.session_state.transformations = {}
if 'selected_mapping' not in st.session_state:
    st.session_state.selected_mapping = None
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []
if 'cortex_calls' not in st.session_state:
    st.session_state.cortex_calls = 0
if 'manual_edits' not in st.session_state:
    st.session_state.manual_edits = set()  # Track manually edited transformations
if 'selected_field' not in st.session_state:
    st.session_state.selected_field = None  # Track which field is selected for detail view

# ============= MECHANICAL PATTERNS LIBRARY =============
# Imported from Pattern Detection Test App

def currency_to_number_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check if source has currency format and target is numeric"""
    return (
        src_type in ["STRING", "VARCHAR", "TEXT"] and
        tgt_type in ["NUMBER", "DECIMAL", "FLOAT", "INTEGER", "NUMERIC"] and
        bool(re.match(r'^\$[\d,]+\.?\d*$', str(src_sample).strip()))
    )

def number_suffix_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check for K/M/B/T suffix patterns"""
    return (
        tgt_type in ["NUMBER", "INTEGER", "DECIMAL", "FLOAT", "NUMERIC"] and
        bool(re.match(r'^\d+\.?\d*[KMBT]$', str(src_sample).strip(), re.IGNORECASE))
    )

def dash_transformation_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check for dash addition/removal patterns"""
    src_clean = str(src_sample).replace('-', '').replace(' ', '')
    tgt_clean = str(tgt_sample).replace('-', '').replace(' ', '')
    return (
        len(src_clean) == len(tgt_clean) and
        (('-' in str(src_sample)) != ('-' in str(tgt_sample))) and
        src_clean.isalnum() and tgt_clean.isalnum()
    )

def case_transformation_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check if only case differs between source and target"""
    # Both should be string types
    if src_type not in ["STRING", "VARCHAR", "TEXT"] or tgt_type not in ["STRING", "VARCHAR", "TEXT"]:
        return False
    
    src_str = str(src_sample).strip()
    tgt_str = str(tgt_sample).strip()
    
    # They should be equal when case is ignored but different in actual case
    return src_str.upper() == tgt_str.upper() and src_str != tgt_str

def trim_whitespace_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check if only whitespace differs"""
    return (
        str(src_sample).strip() == str(tgt_sample) and
        len(str(src_sample)) != len(str(tgt_sample))
    )

def delimiter_replacement_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check for delimiter replacement patterns"""
    delimiters = ['_', '-', '.', '|', ',', ';', ':', '/']
    for d1 in delimiters:
        for d2 in delimiters + [' ']:
            if d1 != d2 and d1 in str(src_sample):
                if str(src_sample).replace(d1, d2) == str(tgt_sample):
                    return True
    return False

def delimiter_removal_test(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str) -> bool:
    """Check for delimiter removal patterns"""
    delimiters = ['_', '-', '.', '|', ',', ';', ':', '/', ' ']
    has_delim_src = any(d in str(src_sample) for d in delimiters)
    has_delim_tgt = any(d in str(tgt_sample) for d in delimiters[:-1])
    
    if has_delim_src and not has_delim_tgt:
        src_clean = re.sub(r'[_\-.;:/ ]+', '', str(src_sample))
        tgt_clean = re.sub(r'[_\-.;:/ ]+', '', str(tgt_sample))
        return src_clean == tgt_clean
    return False

# SQL generation functions for each pattern
def currency_to_number_sql(field: str) -> str:
    """Generate SQL to convert currency to number"""
    return f"TO_NUMBER(REPLACE(REPLACE({field}, '$', ''), ',', ''))"

def number_suffix_to_number_sql(field: str) -> str:
    """Generate SQL to convert K/M/B/T to full numbers"""
    return f"""CAST(REGEXP_SUBSTR({field}, '^[0-9.]+') AS NUMBER) *
    CASE UPPER(RIGHT(TRIM({field}), 1))
        WHEN 'K' THEN 1000
        WHEN 'M' THEN 1000000
        WHEN 'B' THEN 1000000000
        WHEN 'T' THEN 1000000000000
        ELSE 1
    END"""

def dash_transformation_sql(field: str, src_sample: Any, tgt_sample: Any) -> str:
    """Generate SQL for dash transformations"""
    if '-' in str(tgt_sample) and '-' not in str(src_sample):
        # Need to add dashes - this would require knowing the pattern
        # For now, just return the field
        return field
    else:
        # Remove dashes
        return f"REPLACE({field}, '-', '')"

def case_transformation_sql(field: str, src_sample: Any, tgt_sample: Any) -> str:
    """Generate SQL for case transformations"""
    tgt_str = str(tgt_sample).strip()
    if tgt_str.isupper():
        return f"UPPER(TRIM({field}))"
    elif tgt_str.islower():
        return f"LOWER(TRIM({field}))"
    elif tgt_str.istitle():
        return f"INITCAP(TRIM({field}))"
    else:
        return f"TRIM({field})"

def trim_whitespace_sql(field: str) -> str:
    """Generate SQL to trim whitespace"""
    return f"TRIM({field})"

def delimiter_replacement_sql(field: str, src_sample: Any, tgt_sample: Any) -> str:
    """Generate SQL for delimiter replacement"""
    delimiters = ['_', '-', '.', '|', ',', ';', ':', '/']
    for d1 in delimiters:
        for d2 in delimiters + [' ']:
            if d1 != d2 and d1 in str(src_sample) and str(src_sample).replace(d1, d2) == str(tgt_sample):
                return f"REPLACE({field}, '{d1}', '{d2}')"
    return field

def delimiter_removal_sql(field: str) -> str:
    """Generate SQL to remove delimiters"""
    return f"REGEXP_REPLACE({field}, '[_\\-.;:/ ]+', '')"

# Pattern configuration
MECHANICAL_PATTERNS = [
    {
        "name": "currency_to_number",
        "description": "Remove $ and convert to number",
        "test": currency_to_number_test,
        "sql": currency_to_number_sql,
        "requires_samples": False
    },
    {
        "name": "number_suffix_to_number",
        "description": "Convert K/M/B/T notation to full number",
        "test": number_suffix_test,
        "sql": number_suffix_to_number_sql,
        "requires_samples": False
    },
    {
        "name": "dash_transformation",
        "description": "Add or remove dashes",
        "test": dash_transformation_test,
        "sql": dash_transformation_sql,
        "requires_samples": True
    },
    {
        "name": "case_transformation",
        "description": "Change text case (UPPER/lower/Title)",
        "test": case_transformation_test,
        "sql": case_transformation_sql,
        "requires_samples": True
    },
    {
        "name": "trim_whitespace",
        "description": "Remove leading/trailing whitespace",
        "test": trim_whitespace_test,
        "sql": trim_whitespace_sql,
        "requires_samples": False
    },
    {
        "name": "delimiter_replacement",
        "description": "Replace one delimiter with another",
        "test": delimiter_replacement_test,
        "sql": delimiter_replacement_sql,
        "requires_samples": True
    },
    {
        "name": "delimiter_removal",
        "description": "Remove all delimiters",
        "test": delimiter_removal_test,
        "sql": delimiter_removal_sql,
        "requires_samples": False
    }
]

def test_mechanical_patterns(src_sample: Any, tgt_sample: Any, src_type: str, tgt_type: str, source_field: str) -> Optional[Dict[str, Any]]:
    """Test which mechanical patterns match this transformation"""
    for pattern in MECHANICAL_PATTERNS:
        try:
            if pattern["test"](src_sample, tgt_sample, src_type, tgt_type):
                # Generate SQL based on pattern requirements
                if pattern["requires_samples"]:
                    sql = pattern["sql"](source_field, src_sample, tgt_sample)
                else:
                    sql = pattern["sql"](source_field)
                
                return {
                    'sql': sql,
                    'confidence': 1.0,  # Mechanical patterns have 100% confidence
                    'explanation': f'Mechanical transformation: {pattern["description"]}',
                    'pattern': pattern["name"]
                }
        except Exception:
            # Pattern didn't match or error in SQL generation
            continue
    
    return None

# ============= COMPLEXITY DETECTION FUNCTIONS =============
# Stage 2: Add complexity detection for Claude vs Mistral routing

def is_hard_transformation(
    source_field: str,
    source_samples: List[Any],
    target_samples: List[Any]
) -> Tuple[bool, str]:
    """
    Determine if a transformation is complex enough to require Claude
    Returns (should_use_claude, reason)
    """
    # Convert samples to strings for analysis
    source_samples_str = [str(s).strip() for s in source_samples[:5] if s is not None and str(s).strip()]
    target_samples_str = [str(s).strip() for s in target_samples[:5] if s is not None and str(s).strip()]
    
    if not source_samples_str or not target_samples_str:
        return False, "No samples to analyze"
    
    # Check each source sample
    complexity_reasons = []
    
    for src in source_samples_str:
        # Check if this looks like a date transformation - if so, it's NOT complex
        # Common date patterns: MM-DD-YY, MM/DD/YY, MM-DD-YYYY, YYYY-MM-DD, etc.
        date_pattern = r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$"
        if re.match(date_pattern, src):
            # This is a date, not complex
            continue
        
        # 1. Pattern: WC-YYYY-XXX (e.g., WC-2024-001)
        if re.match(r"WC-\d{4}-\d+", src):
            complexity_reasons.append("WC-YYYY-XXX pattern transformation")
            break
        
        # 2. Pattern: value with parentheses like "Smith, John (TECH01)"
        if re.search(r"\(.+\)", src):
            complexity_reasons.append("Parentheses extraction/reordering")
            break
        
        # 3. Long, descriptive or unstructured text (like part descriptions)
        # UPDATED: Exclude colons from complexity check as they're usually just labels
        # Check for other punctuation that indicates complex text
        has_complex_punctuation = ("," in src or "." in src) and ":" not in src
        if len(src) > 30 and (has_complex_punctuation or re.search(r"\s\w{3,}\s", src)):
            # Check if it's not just being truncated simply
            if target_samples_str:
                tgt = target_samples_str[0]
                # Only consider it complex if the target is significantly different
                # (not just a substring or minor variation)
                if len(tgt) < len(src) * 0.5 and tgt not in src:
                    complexity_reasons.append("Complex text transformation")
                    break
        
        # 4. Multi-delimiter pattern: hyphenated with 2+ sections (e.g., VINs, codes)
        # BUT only if the transformation is complex (not just removing dashes)
        if src.count("-") >= 2 and re.search(r"\d", src):
            # Check if it's not just dash removal
            src_no_dash = src.replace("-", "")
            if target_samples_str:
                tgt = target_samples_str[0]
                tgt_no_dash = tgt.replace("-", "")
                if src_no_dash != tgt_no_dash:  # More than just dash removal
                    complexity_reasons.append("Multi-segment code transformation")
                    break
    
    # Determine if transformation is hard
    is_hard = len(complexity_reasons) > 0
    reason = "; ".join(complexity_reasons) if complexity_reasons else "Standard transformation"
    
    return is_hard, reason

# ============= HELPER FUNCTIONS =============

def log_message(message: str, level: str = "info", full_text: bool = False):
    """Add message to debug logs for troubleshooting"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.debug_logs.append({
        'time': timestamp,
        'level': level,
        'message': message,
        'full_text': full_text  # Flag to indicate this should not be truncated
    })

def call_cortex_complete(session: snowpark.Session, model: str, prompt: str) -> Optional[str]:
    """
    Call Snowflake Cortex COMPLETE function for AI-powered analysis
    Tracks number of calls for performance monitoring
    """
    # Log the full prompt being sent - with full_text flag
    log_message(f"Cortex Call #{st.session_state.cortex_calls + 1} to {model}", level="info")
    log_message(f"Prompt:\n{prompt}", level="debug", full_text=True)
    
    query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        '{model}',
        '{prompt.replace("'", "''")}'
    ) as response
    """
    
    try:
        st.session_state.cortex_calls += 1
        result = session.sql(query).collect()
        response = result[0]['RESPONSE'] if result else None
        
        # Log the response too - with full_text flag
        if response:
            log_message(f"Response:\n{response}", level="debug", full_text=True)
        
        return response
    except Exception as e:
        log_message(f"Cortex error: {str(e)}", level="error")
        return None

# ============= DATA ACCESS FUNCTIONS =============

def get_available_mappings(session: snowpark.Session) -> List[Dict]:
    """Get all available mapping configurations"""
    try:
        query = """
        SELECT DISTINCT
            MAPPING_ID,
            SNOWSPACE_ID,
            CONTRIBUTOR_TABLE,
            COUNT(*) as FIELD_COUNT,
            MAX(CREATED_AT) as LAST_UPDATED
        FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS
        WHERE IS_ACTIVE = TRUE
        GROUP BY MAPPING_ID, SNOWSPACE_ID, CONTRIBUTOR_TABLE
        ORDER BY LAST_UPDATED DESC
        """
        
        results = session.sql(query).collect()
        return [{
            'MAPPING_ID': row['MAPPING_ID'],
            'SNOWSPACE_ID': row['SNOWSPACE_ID'],
            'CONTRIBUTOR_TABLE': row['CONTRIBUTOR_TABLE'],
            'FIELD_COUNT': row['FIELD_COUNT'],
            'LAST_UPDATED': row['LAST_UPDATED']
        } for row in results]
        
    except Exception as e:
        log_message(f"Error fetching mappings: {str(e)}", level="error")
        return []

def load_mapping_details(session: snowpark.Session, mapping_id: str, snowspace_view_path: str) -> pd.DataFrame:
    """Load specific mapping configuration with field definitions from Snowspace view"""
    try:
        # Use cross-database join between contributor mappings and snowspace view
        query = f"""
        SELECT 
            m.*,
            f.FIELD_CATEGORY,
            f.DATA_TYPE,
            f.SAMPLE_VALUES,
            f.FIELD_DESCRIPTION as DESCRIPTION,
            f.SYNONYMS,
            f.ADDITIONAL_CONTEXT,
            f.IS_REQUIRED
        FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS m
        JOIN {snowspace_view_path} f
            ON m.TARGET_FIELD = f.FIELD_NAME
        WHERE m.MAPPING_ID = '{mapping_id}'
            AND m.IS_ACTIVE = TRUE
        ORDER BY m.TARGET_FIELD
        """
        return session.sql(query).to_pandas()
        
    except Exception as e:
        log_message(f"Error loading mapping details: {str(e)}", level="error")
        return pd.DataFrame()

def load_mapping_details_for_generation(session: snowpark.Session, mapping_id: str, snowspace_view_path: str) -> pd.DataFrame:
    """Load only mapping fields that need transformation generation"""
    try:
        # This query should exclude approved, skip, or manually edited fields
        # FIXED: Handle NULL values properly in boolean comparisons
        query = f"""
        SELECT 
            m.*,
            f.FIELD_CATEGORY,
            f.DATA_TYPE,
            f.SAMPLE_VALUES,
            f.FIELD_DESCRIPTION as DESCRIPTION,
            f.SYNONYMS,
            f.ADDITIONAL_CONTEXT,
            f.IS_REQUIRED
        FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS m
        JOIN {snowspace_view_path} f
            ON m.TARGET_FIELD = f.FIELD_NAME
        WHERE m.MAPPING_ID = '{mapping_id}'
            AND m.IS_ACTIVE = TRUE
            AND (m.IS_APPROVED IS NULL OR m.IS_APPROVED = FALSE)
            AND (m.SKIP_TRANSFORMATION IS NULL OR m.SKIP_TRANSFORMATION = FALSE)
            AND (m.IS_MANUALLY_EDITED IS NULL OR m.IS_MANUALLY_EDITED = FALSE)
        ORDER BY m.TARGET_FIELD
        """
        
        # Log the query for debugging
        log_message(f"Generation filter query: {query}")
        
        result = session.sql(query).to_pandas()
        log_message(f"Filtered query returned {len(result)} rows")
        
        return result
        
    except Exception as e:
        log_message(f"Error loading mapping details for generation: {str(e)}", level="error")
        return pd.DataFrame()

def get_sample_data(session: snowpark.Session, table_name: str, limit: int = 1000) -> pd.DataFrame:
    """Get sample data from a table for analysis"""
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return session.sql(query).to_pandas()
    except Exception as e:
        log_message(f"Error fetching sample data: {str(e)}", level="error")
        return pd.DataFrame()

def get_source_data_type(session: snowpark.Session, table_name: str, column_name: str) -> str:
    """Get the data type of a column from INFORMATION_SCHEMA"""
    try:
        parts = table_name.split('.')
        if len(parts) != 3:
            return "STRING"  # Default fallback
        
        db, schema, table = parts
        
        query = f"""
        SELECT DATA_TYPE
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{db}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
          AND COLUMN_NAME = '{column_name}'
        """
        
        result = session.sql(query).collect()
        if result:
            return result[0]['DATA_TYPE']
        return "STRING"
        
    except Exception as e:
        log_message(f"Error getting column type: {str(e)}", level="warning")
        return "STRING"

# ============= TRANSFORMATION FUNCTIONS =============

def should_skip_transformation_ai(
    source_field: str,
    source_samples: List[Any],
    target_field_info: pd.Series
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Check if we can skip AI analysis for obvious transformations
    Returns (should_skip, transformation_data_if_skipping)
    """
    source_field_lower = source_field.lower()
    target_field_lower = target_field_info.get('TARGET_FIELD', target_field_info.get('FIELD_NAME', '')).lower()
    
    # 1. Exact match - no transformation needed
    if source_field_lower == target_field_lower:
        return True, {
            'sql': source_field,
            'confidence': 1.0,
            'explanation': 'No transformation needed - exact field match'
        }
    
    # 2. Simple case differences only
    if source_field_lower.replace('_', '').replace('-', '') == target_field_lower.replace('_', '').replace('-', ''):
        # Check if sample data already matches expected format
        target_samples = str(target_field_info['SAMPLE_VALUES']).split(', ')
        source_samples_str = [str(s) for s in source_samples[:3]]
        
        # If samples look the same, no transformation needed
        if any(s in target_samples for s in source_samples_str):
            return True, {
                'sql': source_field,
                'confidence': 0.95,
                'explanation': 'Data format already matches target'
            }
    
    # 3. Common patterns that rarely need transformation
    if source_field_lower.endswith('_id') and target_field_lower.endswith('_id'):
        return True, {
            'sql': source_field,
            'confidence': 0.9,
            'explanation': 'ID fields typically need no transformation'
        }
    
    # 4. Numeric fields that are already numeric
    if pd.api.types.is_numeric_dtype(type(source_samples[0] if source_samples else 0)):
        if target_field_info.get('DATA_TYPE', '') in ['NUMBER', 'INTEGER', 'FLOAT']:
            return True, {
                'sql': source_field,
                'confidence': 0.9,
                'explanation': 'Numeric data types match'
            }
    
    return False, None

def build_transformation_prompt(
    source_field: str,
    source_data_type: str,
    source_samples: List[Any],
    target_field_info: pd.Series,
    user_context: str = ""
) -> str:
    """Build the improved prompt for transformation generation"""
    target_samples_raw = target_field_info['SAMPLE_VALUES']
    target_type = target_field_info['DATA_TYPE']
    target_description = target_field_info['DESCRIPTION']
    target_field = target_field_info.get('TARGET_FIELD', target_field_info.get('FIELD_NAME', ''))
    
    # Format samples with bullet points for clarity
    if isinstance(target_samples_raw, str) and ', ' in target_samples_raw:
        target_samples_list = [s.strip() for s in target_samples_raw.split(', ')]
        target_samples_formatted = '\n  - ' + '\n  - '.join(target_samples_list[:5])
    else:
        target_samples_formatted = f'\n  - {target_samples_raw}'
    
    source_samples_formatted = '\n  - ' + '\n  - '.join(str(s) for s in source_samples[:5])
    
    # Build the prompt using our improved structure
    prompt = f"""Generate a Snowflake SQL expression that converts the `{source_field}` field to match the format of the `{target_field}` field.

**Current State:**
The `{source_field}` field (type: `{source_data_type}`) contains values like:
{source_samples_formatted}

**Target State:**
The `{target_field}` field (type: `{target_type}`) should contain values like:
{target_samples_formatted}

**Target Field Description:**
`{target_description}`

## Transformation Rules:
- Focus on structural or formatting transformations only. Do **not** rely on matching example values.
- The examples above show **FORMAT PATTERNS**, not actual matched records. Your task is to match the structure or pattern of the target field, not the individual values.
- Use **Snowflake SQL functions only**.
- Use `TRIM()` and appropriate type casting **only if the source and target types differ**.
- Common transformations include:
  - Reordering parts (e.g., `'Last, First (ID)' → 'First Last'`)
  - Removing or replacing delimiters
  - Changing case
  - Date formatting
  - Extracting substrings using `SPLIT_PART`, `REGEXP_SUBSTR`, etc.

**Additional Context (if any):**
`{user_context if user_context else 'None provided'}`
*(These are explicit transformation rules from the user and take precedence.)*

## 🚫 What Not To Do:
- Never infer calculations based on example values (e.g., don't turn "5" into "500" just because of sample differences).
- Never hardcode example strings like names, IDs, or dates.

Return JSON:
```json
{{
  "transformation": "SQL expression using {source_field}",
  "confidence": 0.0-1.0,
  "explanation": "brief explanation of what transformation does"
}}
```"""
    
    return prompt

def generate_transformation_sql(
    session: snowpark.Session,
    source_field: str,
    source_samples: List[Any],
    target_field_info: pd.Series,
    sample_data: pd.DataFrame,
    user_context: str = "",
    cortex_model: str = 'snowflake-arctic',
    force_complex: bool = False
) -> Dict[str, Any]:
    """
    Generate SQL transformation logic by comparing source and target data patterns
    Uses mechanical patterns first, then AI if needed
    """
    # STAGE 1: Check mechanical patterns first
    if source_samples and target_field_info is not None:
        # Get source and target data types
        source_data_type = get_source_data_type(
            session,
            st.session_state.selected_mapping['CONTRIBUTOR_TABLE'],
            source_field
        )
        target_data_type = target_field_info.get('DATA_TYPE', 'STRING')
        
        # Parse target samples
        target_samples_str = str(target_field_info.get('SAMPLE_VALUES', ''))
        if ', ' in target_samples_str:
            target_samples = [s.strip() for s in target_samples_str.split(', ')]
        else:
            target_samples = [target_samples_str]
        
        # Test mechanical patterns
        for src_sample in source_samples[:1]:  # Just check first sample
            for tgt_sample in target_samples[:1]:
                mechanical_result = test_mechanical_patterns(
                    src_sample,
                    tgt_sample,
                    source_data_type,
                    target_data_type,
                    source_field
                )
                
                if mechanical_result:
                    log_message(f"✅ Mechanical pattern detected for {source_field}: {mechanical_result['pattern']}")
                    return mechanical_result
    
    # If no mechanical pattern found, continue with AI analysis
    # Check if we can skip AI analysis
    should_skip, quick_result = should_skip_transformation_ai(source_field, source_samples, target_field_info)
    if should_skip:
        log_message(f"⚡ Skipping AI for obvious transformation: {source_field}")
        return quick_result
    
    # STAGE 2: Determine AI model based on complexity
    # Parse target samples for complexity check
    target_samples_str = str(target_field_info.get('SAMPLE_VALUES', ''))
    if ', ' in target_samples_str:
        target_samples_list = [s.strip() for s in target_samples_str.split(', ')]
    else:
        target_samples_list = [target_samples_str]
    
    # Check if this is a complex transformation or if user forced complex model
    if force_complex:
        is_complex = True
        complexity_reason = "User requested complex model"
    else:
        # RESTORED: Always check complexity automatically
        is_complex, complexity_reason = is_hard_transformation(
            source_field,
            source_samples,
            target_samples_list
        )
    
    # Override cortex_model if complexity detected
    if is_complex:
        # Use Complex Model selection for complex transformations
        actual_model = st.session_state.get('complex_ai_model', 'claude-3-5-sonnet')
        log_message(f"🧠 Complex transformation detected for {source_field}: {complexity_reason}")
    else:
        # Use the default model for standard transformations
        actual_model = cortex_model
        log_message(f"🤖 Standard transformation for {source_field}: {complexity_reason}")
    
    # Get source data type for AI prompt
    source_data_type = get_source_data_type(
        session,
        st.session_state.selected_mapping['CONTRIBUTOR_TABLE'],
        source_field
    ) if st.session_state.selected_mapping else "STRING"
    
    # Build prompt using centralized function
    prompt = build_transformation_prompt(
        source_field,
        source_data_type,
        source_samples,
        target_field_info,
        user_context
    )
    
    response = call_cortex_complete(session, actual_model, prompt)
    
    if response:
        try:
            # Clean up common JSON issues
            response_cleaned = response.strip()
            
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response_cleaned, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                result = json.loads(json_str)
            else:
                # Try parsing the whole response
                result = json.loads(response_cleaned)
            
            # Replace placeholder with actual field name
            transformation = result.get('transformation', source_field)
            transformation = transformation.replace('source_field', source_field)
            
            return {
                'sql': transformation,
                'confidence': result.get('confidence', 0.5),
                'explanation': result.get('explanation', 'No transformation suggested'),
                'model_used': actual_model,
                'complexity': complexity_reason if is_complex else None
            }
            
        except json.JSONDecodeError as e:
            log_message(f"JSON parsing error: {str(e)}", level="warning")
            log_message(f"Raw response: {response[:500]}", level="debug")
            
            # Try to extract transformation manually as fallback
            if 'transformation' in response:
                try:
                    # Look for transformation value
                    trans_match = re.search(r'"transformation"\s*:\s*"([^"]+)"', response)
                    if trans_match:
                        transformation = trans_match.group(1).replace('source_field', source_field)
                        return {
                            'sql': transformation,
                            'confidence': 0.5,
                            'explanation': 'Extracted from response',
                            'model_used': actual_model,
                            'complexity': complexity_reason if is_complex else None
                        }
                except:
                    pass
        
        except Exception as e:
            log_message(f"Error parsing transformation response: {str(e)}", level="warning")
    
    # Fallback - no transformation
    return {
        'sql': source_field,
        'confidence': 0.5,
        'explanation': 'No transformation suggested',
        'model_used': actual_model,
        'complexity': None
    }

def load_existing_transformations(
    session: snowpark.Session,
    mapping_id: str
) -> Tuple[Dict[str, Dict], set, set, set, set]:
    """Load existing transformations from database"""
    try:
        # Load ALL fields, not just those with transformation SQL
        query = f"""
        SELECT
            SOURCE_FIELD,
            TARGET_FIELD,
            TRANSFORMATION_SQL,
            TRANSFORMATION_CONFIDENCE,
            IS_MANUALLY_EDITED,
            IS_APPROVED,
            SKIP_TRANSFORMATION,
            USE_COMPLEX_MODEL,
            MODEL_USED,
            EXPLANATION
        FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS
        WHERE MAPPING_ID = '{mapping_id}'
        """
        
        results = session.sql(query).collect()
        
        transformations = {}
        skip_transformations = set()
        complex_model_fields = set()
        approved_fields = set()
        manual_edits = set()
        
        for row in results:
            transform_key = f"{row['SOURCE_FIELD']}_to_{row['TARGET_FIELD']}"
            
            # Only add to transformations if there's actual SQL
            if row['TRANSFORMATION_SQL']:
                # Determine if it's mechanical based on confidence
                confidence = row['TRANSFORMATION_CONFIDENCE'] if row['TRANSFORMATION_CONFIDENCE'] is not None else 0.5
                is_mechanical = (confidence == 1.0 and not (row['IS_MANUALLY_EDITED'] if row['IS_MANUALLY_EDITED'] is not None else False))
                
                transformations[transform_key] = {
                    'sql': row['TRANSFORMATION_SQL'],
                    'confidence': confidence,
                    'explanation': row['EXPLANATION'] if row['EXPLANATION'] else ('Mechanical transformation' if is_mechanical else 'Loaded from database'),
                    'model_used': row['MODEL_USED'] if row['MODEL_USED'] else None,
                    'is_approved': row['IS_APPROVED'] if row['IS_APPROVED'] is not None else False,
                    'is_manually_edited': row['IS_MANUALLY_EDITED'] if row['IS_MANUALLY_EDITED'] is not None else False
                }
            
            # Track skip transformations
            if row['SKIP_TRANSFORMATION'] if row['SKIP_TRANSFORMATION'] is not None else False:
                skip_transformations.add(transform_key)
                if transform_key in transformations:
                    transformations[transform_key]['explanation'] = "Pass-through field (no transformation)"
            
            # Track complex model fields
            if row['USE_COMPLEX_MODEL'] if row['USE_COMPLEX_MODEL'] is not None else False:
                complex_model_fields.add(transform_key)
            
            # Track approved fields
            if row['IS_APPROVED'] if row['IS_APPROVED'] is not None else False:
                approved_fields.add(transform_key)
            
            # Track manual edits
            if row['IS_MANUALLY_EDITED'] if row['IS_MANUALLY_EDITED'] is not None else False:
                manual_edits.add(transform_key)
            
            log_message(f"Loaded {transform_key} - Approved: {row['IS_APPROVED'] if row['IS_APPROVED'] is not None else False}, "
                       f"Confidence: {row['TRANSFORMATION_CONFIDENCE'] if row['TRANSFORMATION_CONFIDENCE'] is not None else 'N/A'}, "
                       f"Skip: {row['SKIP_TRANSFORMATION'] if row['SKIP_TRANSFORMATION'] is not None else False}")
        
        return transformations, skip_transformations, complex_model_fields, approved_fields, manual_edits
        
    except Exception as e:
        log_message(f"Error loading existing transformations: {str(e)}", level="error")
        return {}, set(), set(), set(), set()

def generate_batch_transformations(
    session: snowpark.Session,
    mapping_details: pd.DataFrame,
    sample_data: pd.DataFrame,
    cortex_model: str = 'snowflake-arctic',
    progress_callback=None,
    status_callback=None,
    complex_model_fields: set = None  # Changed from Dict[str, bool]
) -> Dict[str, Dict]:
    """
    Generate transformations for multiple fields
    Process each field individually for better results
    """
    import time
    
    transformations = {}
    total_fields = len(mapping_details)
    total_time = 0
    complex_model_fields = complex_model_fields or set()
    
    log_message(f"Starting batch transformation for {total_fields} fields using model: {cortex_model}")
    batch_start = time.time()
    
    for idx, (_, row) in enumerate(mapping_details.iterrows()):
        field_start = time.time()
        
        source_field = row['SOURCE_FIELD']
        target_field = row['TARGET_FIELD']
        transform_key = f"{source_field}_to_{target_field}"
        
        # Update progress
        if progress_callback:
            progress_callback((idx + 1) / total_fields)
        
        # Update status - use total_fields not some other count
        if status_callback:
            status_callback(f"🤖 Analyzing: {source_field} → {target_field}", idx + 1, total_fields)
        
        # Get sample data
        source_samples = []
        if source_field in sample_data.columns:
            source_samples = sample_data[source_field].dropna().unique()[:5].tolist()
        
        # Include any existing context from the database
        existing_context = row.get('TRANSFORMATION_HINTS', '')
        
        # CHECK IF THIS FIELD SHOULD USE COMPLEX MODEL
        force_complex = transform_key in complex_model_fields  # Changed from dict lookup
        if force_complex:
            log_message(f"Using complex model for {transform_key} based on saved flag")
        
        # Generate transformation
        transform_result = generate_transformation_sql(
            session,
            source_field,
            source_samples,
            row,
            sample_data,
            user_context=existing_context,
            cortex_model=cortex_model,
            force_complex=force_complex  # PASS THE FLAG
        )
        
        transformations[transform_key] = transform_result
        
        # Calculate time for this field
        field_time = time.time() - field_start
        total_time += field_time
        avg_time = total_time / (idx + 1)
        
        log_message(f"Field {idx + 1}/{total_fields} took {field_time:.2f}s (avg: {avg_time:.2f}s/field)")
        
        # Show result
        if status_callback:
            confidence = transform_result.get('confidence', 0.5)
            if confidence >= 0.9:
                conf_emoji = "✅"
            elif confidence >= 0.7:
                conf_emoji = "⚠️"
            else:
                conf_emoji = "❌"
            
            # Initialize status_icon and model_info
            status_icon = conf_emoji
            model_info = ""
            
            # Check if it was mechanical
            if 'Mechanical transformation' in transform_result.get('explanation', ''):
                status_icon = "🔧"
                model_info = " (Mechanical)"
            else:
                # Show which AI model was used
                model_used = transform_result.get('model_used', cortex_model)
                if 'claude' in model_used.lower():
                    status_icon = "🧠"
                    model_info = " (Claude)"
                else:
                    model_info = f" ({model_used.split('-')[0].title()})"
            
            status_callback(f"{status_icon} Completed: {source_field} → {target_field} (Confidence: {confidence:.0%}){model_info}", idx + 1, total_fields, is_complete=True)
    
    batch_time = time.time() - batch_start
    log_message(f"Batch transformation completed in {batch_time:.2f}s for {total_fields} fields ({batch_time/total_fields:.2f}s per field)")
    
    return transformations

def preview_transformation(
    session: snowpark.Session,
    contributor_table: str,
    source_field: str,
    transformation_sql: str,
    limit: int = 5
) -> str:
    """
    Preview transformation results as a comma-separated string
    Returns format matching schema sample values
    """
    try:
        query = f"""
        SELECT {transformation_sql} as transformed
        FROM {contributor_table}
        WHERE {source_field} IS NOT NULL
        LIMIT {limit}
        """
        results = session.sql(query).collect()
        
        # Format as comma-separated list
        transformed_values = [str(row['TRANSFORMED']) for row in results]
        return ', '.join(transformed_values)
        
    except Exception as e:
        return f"Error: {str(e)}"

def save_field_attribute(
    session: snowpark.Session,
    mapping_id: str,
    source_field: str,
    target_field: str,
    attribute_name: str,
    value: Any
) -> bool:
    """Generic function to save any field attribute to database immediately"""
    try:
        # Handle different data types
        if isinstance(value, str):
            # Escape single quotes for SQL
            escaped_value = value.replace("'", "''")
            value_sql = f"'{escaped_value}'"
        elif isinstance(value, bool):
            value_sql = str(value).upper()  # TRUE/FALSE for SQL
        elif value is None:
            value_sql = 'NULL'
        else:
            value_sql = str(value)
        
        update_query = f"""
        UPDATE UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS
        SET {attribute_name} = {value_sql},
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE MAPPING_ID = '{mapping_id}'
          AND SOURCE_FIELD = '{source_field}'
          AND TARGET_FIELD = '{target_field}'
        """
        
        session.sql(update_query).collect()
        log_message(f"Auto-saved {attribute_name} for {source_field} -> {target_field}: {value}")
        return True
        
    except Exception as e:
        log_message(f"Error saving {attribute_name}: {str(e)}", level="error")
        return False

def save_transformations_to_db(
    session: snowpark.Session,
    mapping_id: str,
    transformations: Dict,
    manual_edits: set,
    approved_fields: set,
    skip_transformations: set = None
) -> bool:
    """Save all transformations to the database"""
    try:
        skip_transformations = skip_transformations or set()
        
        for transform_key, transform_data in transformations.items():
            # Parse the key to get source and target fields
            parts = transform_key.split('_to_')
            source_field = parts[0]
            target_field = '_to_'.join(parts[1:])  # Handle fields with underscores
            
            # Determine field states
            is_approved = transform_key in approved_fields
            is_manually_edited = transform_key in manual_edits
            skip_transformation = transform_key in skip_transformations
            
            # Extract confidence score and new fields
            confidence = transform_data.get('confidence', 0.5)
            model_used = transform_data.get('model_used') or ''
            explanation = transform_data.get('explanation') or ''
            
            # Update the database
            update_query = f"""
            UPDATE UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS
            SET TRANSFORMATION_SQL = '{transform_data['sql'].replace("'", "''")}',
                TRANSFORMATION_CONFIDENCE = {confidence},
                MODEL_USED = '{model_used.replace("'", "''")}',
                EXPLANATION = '{explanation.replace("'", "''")}',
                IS_MANUALLY_EDITED = {is_manually_edited},
                IS_APPROVED = {is_approved},
                SKIP_TRANSFORMATION = {skip_transformation},
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE MAPPING_ID = '{mapping_id}'
              AND SOURCE_FIELD = '{source_field}'
              AND TARGET_FIELD = '{target_field}'
            """
            
            session.sql(update_query).collect()
            log_message(f"Saved transformation for {source_field} -> {target_field} "
                       f"(confidence={confidence:.2f}, approved={is_approved})")
        
        return True
        
    except Exception as e:
        log_message(f"Error saving transformations: {str(e)}", level="error")
        return False

def calculate_transformation_stats(transformations: Dict[str, Dict], manual_edits: set, approved_fields: set, total_field_count: int = None) -> Dict[str, Any]:
    """Calculate statistics for transformations"""
    # If total_field_count is provided, use it; otherwise use transformation count
    total = total_field_count if total_field_count is not None else len(transformations)
    
    if total == 0:
        return {
            'total': 0,
            'high_confidence': 0,
            'medium_confidence': 0,
            'low_confidence': 0,
            'manual_edits': 0,
            'approved': 0,
            'needs_review': 0,
            'mechanical': 0,
            'skipped': 0
        }
    
    # Count only from actual transformations
    high_conf = sum(1 for t in transformations.values() if t.get('confidence', 0) >= 0.9)
    medium_conf = sum(1 for t in transformations.values() if 0.7 <= t.get('confidence', 0) < 0.9)
    low_conf = sum(1 for t in transformations.values() if t.get('confidence', 0) < 0.7)
    mechanical = sum(1 for t in transformations.values() if 'Mechanical transformation' in t.get('explanation', ''))
    
    # These can include fields without transformations
    manual = len(manual_edits)
    approved = len(approved_fields)
    needs_review = total - approved
    skipped = total - len(transformations)  # Fields without transformations
    
    return {
        'total': total,
        'high_confidence': high_conf,
        'medium_confidence': medium_conf,
        'low_confidence': low_conf,
        'manual_edits': manual,
        'approved': approved,
        'needs_review': needs_review,
        'mechanical': mechanical,
        'skipped': skipped
    }

# ============= SIDEBAR WITH CONNECTION AND MAPPING SELECTION =============
with st.sidebar:
    # Connect to Snowflake
    try:
        from snowflake.snowpark.context import get_active_session
        conn = get_active_session()
        st.success("✅ Connected to Snowflake")
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        st.stop()
    
    # Check if we're coming from the Mapper with a specific mapping to auto-load
    if 'selected_mapping_id' in st.session_state and st.session_state.selected_mapping_id and not st.session_state.get('selected_mapping'):
        # Auto-load the mapping
        auto_load_mapping_id = st.session_state.selected_mapping_id
        log_message(f"Auto-loading mapping from Mapper: {auto_load_mapping_id}")
        
        # Get snowspace view path from session state
        snowspace_view_path = st.session_state.get('selected_snowspace_path')
        if not snowspace_view_path:
            st.error("Missing Snowspace view path from Field Mapper")
            st.stop()
        
        # Get available mappings to find the one we need
        mappings = get_available_mappings(conn)
        
        # Look for the matching mapping
        mapping_found = False
        for mapping in mappings:
            if mapping['MAPPING_ID'] == auto_load_mapping_id:
                # Set it as selected and trigger auto-generation
                st.session_state.selected_mapping = mapping
                st.session_state.selected_snowspace_view = snowspace_view_path  # Store the view path
                st.session_state.transformations = {}
                st.session_state.manual_edits = set()
                st.session_state.selected_field = None
                st.session_state.approved_fields = set()
                st.session_state.auto_generate = True
                mapping_found = True
                log_message(f"Successfully auto-loaded mapping: {auto_load_mapping_id}")
                
                # Clear the navigation flag to prevent re-loading on refresh
                del st.session_state['selected_mapping_id']
                
                # Optional: Also clear the other navigation variables
                if 'selected_snowspace_id' in st.session_state:
                    del st.session_state['selected_snowspace_id']
                if 'contributor_table' in st.session_state:
                    del st.session_state['contributor_table']
                if 'selected_snowspace_path' in st.session_state:
                    del st.session_state['selected_snowspace_path']
                
                st.rerun()
                break
        
        if not mapping_found:
            log_message(f"Warning: Could not find mapping {auto_load_mapping_id}", level="warning")
            # Clear the invalid mapping ID
            del st.session_state['selected_mapping_id']
    
    # Manual Mapping Selection (kept for dev purposes)
    st.header("📋 Select Mapping Configuration")
    
    # Wrap in expander for dev mode
    with st.expander("🔧 Manual Mapping Selection (Dev Mode)", expanded=not st.session_state.get('selected_mapping')):
        mappings = get_available_mappings(conn)
        
        if mappings:
            # Create friendly display names
            mapping_options = [
                f"{m['MAPPING_ID']} ({m['FIELD_COUNT']} fields)"
                for m in mappings
            ]
            
            selected_index = st.selectbox(
                "Select a saved mapping configuration",
                range(len(mapping_options)),
                format_func=lambda x: mapping_options[x],
                help="Choose which field mapping to generate transformations for"
            )
            
            # Also need to input the Snowspace view path manually in dev mode
            snowspace_view_path = st.text_input(
                "Snowspace View Path",
                placeholder="e.g., SNOWSPACE_DB_TEST.SNOWSPACE.SNOWSPACE_TEST_VIEW",
                help="Full path to the Snowspace view containing field definitions"
            )
            
            if st.button("📥 Load Mapping", type="primary"):
                if not snowspace_view_path:
                    st.error("Please enter the Snowspace view path")
                else:
                    selected_mapping = mappings[selected_index]
                    st.session_state.selected_mapping = selected_mapping
                    st.session_state.selected_snowspace_view = snowspace_view_path  # Store the view path
                    st.session_state.transformations = {}  # Reset transformations
                    st.session_state.manual_edits = set()  # Reset manual edits tracking
                    st.session_state.selected_field = None  # Reset selected field
                    st.session_state.approved_fields = set()  # Reset approved fields
                    st.session_state.auto_generate = True  # Flag to auto-generate transformations
                    log_message("LOAD MAPPING CLICKED - auto_generate set to True")
                    st.rerun()
        else:
            st.warning("No saved mappings found. Please run the Field Mapper first.")
            st.stop()
    
    # Display mapping info if loaded (this part stays the same)
    if st.session_state.selected_mapping:
        mapping = st.session_state.selected_mapping
        
        # Get model selections
        default_model = st.session_state.get('default_ai_model', 'snowflake-arctic')
        complex_model = st.session_state.get('complex_ai_model', 'claude-3-5-sonnet')
        
        st.info(f"**Snowspace:** {mapping['SNOWSPACE_ID']} | **Table:** {mapping['CONTRIBUTOR_TABLE']} | **Fields:** {mapping['FIELD_COUNT']} | **Default Model:** {default_model} | **Complex Model:** {complex_model}")
    
    st.divider()
    
    # Model Configuration in Expander
    with st.expander("🤖 Model Configuration"):
        # Available models list
        available_models = [
            "claude-3-5-sonnet",
            "claude-3-7-sonnet", 
            "claude-4-opus",
            "claude-4-sonnet",
            "snowflake-arctic",
            "mistral-7b",
            "mistral-large",
            "llama2-70b-chat",
            "llama3-8b",
            "llama3-70b",
            "llama3.1-8b",
            "llama3.1-70b",
            "llama3.1-405b",
            "llama3.2-1b",
            "llama3.2-3b",
            "mixtral-8x7b",
            "mistral-7b-instruct-v0.1",
            "gemma-7b",
            "reka-flash",
            "jamba-1.5-mini",
            "jamba-1.5-large"
        ]
        
        # Default AI Model selector
        default_model = st.selectbox(
            "Default AI Model",
            available_models,
            index=6,  # Default to mistral-large
            help="Model used for standard transformations",
            key="default_ai_model"
        )
        
        # Complex Model selector
        complex_model = st.selectbox(
            "Complex Model",
            available_models,
            index=0,  # Default to claude-3-5-sonnet
            help="Model used when 'Use Complex Model' is checked or complexity is detected",
            key="complex_ai_model"
        )
    
    # Stats - Only keep Cortex Calls
    st.metric("Cortex Calls", st.session_state.cortex_calls)
    
    # Debug logs
    if 'debug_logs' in st.session_state and st.session_state.debug_logs:
        with st.expander("📜 Debug Logs", expanded=False):
            # Simple log display without nested expanders
            show_debug = st.checkbox("Show debug messages", value=False)
            
            for log in reversed(st.session_state.debug_logs[-50:]):  # Show last 50 logs
                if log['level'] == 'debug' and not show_debug:
                    continue  # Skip debug logs unless checkbox is checked
                
                if log['level'] == 'error':
                    st.error(f"[{log['time']}] {log['message']}")
                elif log['level'] == 'warning':
                    st.warning(f"[{log['time']}] {log['message']}")
                elif log['level'] == 'debug':
                    # Simple display for debug messages
                    if len(log['message']) > 100:
                        st.text(f"[{log['time']}] DEBUG: {log['message'][:100]}...")
                    else:
                        st.text(f"[{log['time']}] DEBUG: {log['message']}")
                else:
                    st.info(f"[{log['time']}] {log['message']}")
            
            if st.button("Clear Logs"):
                st.session_state.debug_logs = []
                st.session_state.cortex_calls = 0
                st.rerun()

# ============= MAIN UI =============

# Display mapping info if loaded
if st.session_state.selected_mapping:
    mapping = st.session_state.selected_mapping
    
    # Get the Snowspace view path from session state
    snowspace_view_path = st.session_state.get('selected_snowspace_view')
    if not snowspace_view_path:
        st.error("Missing Snowspace view path. Please reload the mapping.")
        st.stop()
    
    # Load mapping details - this loads ALL fields for display
    mapping_details = load_mapping_details(conn, mapping['MAPPING_ID'], snowspace_view_path)
    
    if mapping_details.empty:
        st.error("Failed to load mapping details")
        st.stop()
    
    # Load sample data
    sample_data = get_sample_data(conn, mapping['CONTRIBUTOR_TABLE'])
    
    # Transformation UI
    st.header("🔄 Field Transformations")
    
    log_message(f"CHECKING AUTO-GENERATE: {st.session_state.get('auto_generate', False)}")
    log_message(f"TRANSFORMATIONS EMPTY? {not st.session_state.transformations}")
    
    # Auto-generate transformations if flag is set OR if no transformations exist
    if st.session_state.get('auto_generate', False) or not st.session_state.transformations:
        if st.session_state.get('auto_generate', False):
            st.session_state.auto_generate = False  # Reset flag
        
        log_message("ENTERING AUTO-GENERATION BLOCK")
        
        # Initialize progress tracking
        progress_bar = st.progress(0)
        progress_text = st.empty()
        status_text = st.empty()
        
        # First, load any existing approved/manual transformations
        status_text.info("📥 Loading existing transformations...")
        existing_transformations, skip_transformations, complex_model_fields, approved_fields, manual_edits = load_existing_transformations(conn, mapping['MAPPING_ID'])
        
        # Update session state with existing transformations
        st.session_state.transformations = existing_transformations
        st.session_state.skip_transformations = skip_transformations
        st.session_state.complex_model_fields = complex_model_fields  # Changed from complex_model_flags
        st.session_state.approved_fields = approved_fields
        st.session_state.manual_edits = manual_edits  # Now being set from database
        
        # Log complex model fields for debugging
        if complex_model_fields:
            log_message(f"Complex model fields loaded: {list(complex_model_fields)}")
        
        # Track which fields were loaded
        if existing_transformations:
            # Already tracked in load_existing_transformations, no need to duplicate
            pass
        
        loaded_count = len(existing_transformations)
        status_text.success(f"✅ Loaded {loaded_count} existing transformations")
        time.sleep(1)
        
        # FIXED: Use filtered query to get only fields needing generation
        fields_needing_generation = load_mapping_details_for_generation(conn, mapping['MAPPING_ID'], snowspace_view_path)
        
        # Calculate counts for display
        total_fields = len(mapping_details)  # All fields for GUI
        fields_to_generate_count = len(fields_needing_generation)  # Fields needing generation
        skipped_count = total_fields - fields_to_generate_count
        
        # Initial status
        status_text.info(f"🔍 Found {total_fields} total fields, {skipped_count} already processed, {fields_to_generate_count} need generation")
        time.sleep(1)
        
        if fields_to_generate_count > 0:
            # Status callback function that shows correct count
            def update_status(message, current, total, is_complete=False):
                # Override total with actual fields to generate
                progress_text.text(f"Processing field {current} of {fields_to_generate_count}")
                if is_complete:
                    status_text.success(message)
                else:
                    status_text.info(message)
            
            # Generate only missing transformations
            with st.spinner(f"Generating {fields_to_generate_count} new transformations..."):
                # Use the filtered dataframe directly
                new_transformations = generate_batch_transformations(
                    conn,
                    fields_needing_generation,  # This now contains only fields that need generation
                    sample_data,
                    cortex_model=st.session_state.get('default_ai_model', 'mistral-large'),
                    progress_callback=lambda p: progress_bar.progress(p),
                    status_callback=update_status,
                    complex_model_fields=complex_model_fields  # Changed from complex_model_flags
                )
                
                # Merge new transformations with existing ones
                st.session_state.transformations.update(new_transformations)
                
                # AUTO-SAVE NEW TRANSFORMATIONS TO DATABASE
                status_text.info("💾 Auto-saving new transformations...")
                if save_transformations_to_db(
                    conn,
                    mapping['MAPPING_ID'],
                    new_transformations,  # Only save the new ones
                    set(),  # No manual edits for auto-generated
                    set(),  # No approvals yet for new ones
                    set()   # No skip transformations
                ):
                    status_text.success(f"✅ Auto-saved {len(new_transformations)} new transformations")
                else:
                    status_text.warning("⚠️ Failed to auto-save transformations")
        
        else:
            status_text.success("✅ All fields already have transformations!")
            time.sleep(1)
        
        # Clear progress indicators
        progress_bar.empty()
        progress_text.empty()
        status_text.empty()
        
        # Show summary
        all_transformations = st.session_state.transformations
        stats = calculate_transformation_stats(
            all_transformations,
            st.session_state.manual_edits,
            st.session_state.approved_fields,
            total_field_count=len(mapping_details)  # Pass total fields from mapping
        )
        
        st.success(f"""
        ✅ Transformation generation complete!
        - Total transformations: {len(all_transformations)}
        - Loaded from database: {len(existing_transformations)}
        - Newly generated: {fields_to_generate_count}
        
        Breakdown:
        - 🔧 Mechanical: {stats['mechanical']}
        - 🟢 High confidence: {stats['high_confidence'] - stats['mechanical']}
        - 🟡 Medium confidence: {stats['medium_confidence']}
        - 🔴 Low confidence: {stats['low_confidence']}
        """)
        
        time.sleep(2)  # Pause to show summary
        st.rerun()
    
    # IMPORTANT: Skip the rest if we just did auto-generation
    elif st.session_state.get('auto_generate', False):
        # This should not happen, but just in case
        st.session_state.auto_generate = False
    
    # Add summary statistics
    if st.session_state.transformations:
        stats = calculate_transformation_stats(
            st.session_state.transformations,
            st.session_state.manual_edits,
            st.session_state.approved_fields,
            total_field_count=len(mapping_details)  # Pass total fields from mapping
        )
        
        st.subheader("📊 Transformation Summary")
        
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
        
        with col1:
            st.metric("Total Fields", stats['total'])
        with col2:
            st.metric("Skipped", stats['skipped'], 
                      help="Fields without transformations")
        with col3:
            st.metric("Mechanical", stats['mechanical'], 
                      help="No AI needed")
        with col4:
            st.metric("High Confidence", stats['high_confidence'] - stats['mechanical'],
                      help="90%+ confidence")
        with col5:
            st.metric("Medium Confidence", stats['medium_confidence'], 
                      help="70-89% confidence")
        with col6:
            st.metric("Approved", stats['approved'], 
                      help="Fields approved for final SQL")
        with col7:
            st.metric("Needs Review", stats['needs_review'], 
                      help="Fields not yet approved")
        with col8:
            st.metric("Manual Edits", stats['manual_edits'], 
                      help="Transformations you've edited")
        
        st.divider()
        
        st.info("Review your transformations below. Click on any row to edit details and approve transformations.")
    
    # Side-by-side layout
    if st.session_state.transformations:
        # Create left and right columns (50/50 split) with gap
        left_col, gap, right_col = st.columns([2.5, 0.1, 2.5])
        
        with left_col:
            st.subheader("Transformation Overview")
            
            # Simplified table headers
            header_container = st.container()
            with header_container:
                cols = st.columns([3, 1.5, 1])
                with cols[0]:
                    st.markdown("**Field Mapping**")
                with cols[1]:
                    st.markdown("**Status**")
                with cols[2]:
                    st.markdown("**Approved**")
            
            # Scrollable container for rows - increased height
            table_container = st.container(height=600)
            
            with table_container:
                # Display each transformation row - iterate through ALL mapping details
                for _, row in mapping_details.iterrows():
                    source_field = row['SOURCE_FIELD']
                    target_field = row['TARGET_FIELD']
                    transform_key = f"{source_field}_to_{target_field}"
                    
                    # Check if we have a transformation for this field
                    confidence = 0.0
                    conf_icon = "⚪"
                    is_mechanical = False
                    
                    if transform_key in st.session_state.transformations:
                        transform = st.session_state.transformations[transform_key]
                        confidence = transform.get('confidence', 0.5)
                        
                        # Check if mechanical
                        is_mechanical = 'Mechanical transformation' in transform.get('explanation', '')
                        
                        # Confidence indicator - ALWAYS show color
                        if confidence >= 0.9:
                            conf_icon = "🟢"
                        elif confidence >= 0.7:
                            conf_icon = "🟡"
                        else:
                            conf_icon = "🔴"
                    
                    # Check status flags (these can exist even without transformation)
                    is_manual = transform_key in st.session_state.get('manual_edits', set())
                    is_approved = transform_key in st.session_state.get('approved_fields', set())
                    is_skipped = transform_key in st.session_state.get('skip_transformations', set())
                    use_complex = transform_key in st.session_state.get('complex_model_fields', set())
                    
                    # Row columns
                    cols = st.columns([3, 1.5, 1])
                    
                    with cols[0]:
                        # Clickable field mapping
                        if st.button(
                            f"{source_field} → {target_field}",
                            key=f"select_{transform_key}",
                            use_container_width=True
                        ):
                            st.session_state.selected_field = transform_key
                    
                    with cols[1]:
                        # Status with method indicator
                        if transform_key in st.session_state.transformations:
                            transform = st.session_state.transformations[transform_key]
                            confidence = transform.get('confidence', 0.5)
                            
                            # Check if mechanical
                            is_mechanical = 'Mechanical transformation' in transform.get('explanation', '')
                            
                            # Confidence indicator - always based on score, not type
                            if confidence >= 0.9:
                                conf_icon = "🟢"
                            elif confidence >= 0.7:
                                conf_icon = "🟡"
                            else:
                                conf_icon = "🔴"
                            
                            # Determine method
                            if is_skipped:
                                method_icon = "🚫"
                            elif is_manual:
                                method_icon = "📝"
                            elif is_mechanical:
                                method_icon = "🔧"
                            elif use_complex:
                                method_icon = "🧠"
                            else:
                                method_icon = "🤖"
                            
                            # Format percentage with fixed width for alignment
                            pct_str = f"{confidence*100:3.0f}%"
                            st.write(f"{conf_icon} {pct_str} {method_icon}")
                        else:
                            # No transformation yet
                            st.write("⚪   0% ❓")
                    
                    with cols[2]:
                        # Approval status
                        if is_approved:
                            st.write("✅ Yes")
                        else:
                            st.write("❌ No")
            
            # Caption and legend below the scrollable area
            st.caption("Click on any row to view and edit details • 🔧 Mechanical | 🤖 AI Model | 🧠 Complex Model | 📝 Manual Edit")
        
        with right_col:
            if st.session_state.selected_field:
                # Find the selected field details
                selected_parts = st.session_state.selected_field.split('_to_')
                source_field = selected_parts[0]
                target_field = '_to_'.join(selected_parts[1:])  # Handle fields with underscores
                
                # Put the field mapping in the subheader with smaller font for the mapping part
                st.markdown(f"### Edit Details: <span style='font-size: 0.8em;'>{source_field} → {target_field}</span>", unsafe_allow_html=True)
                
                # Get the row details
                selected_row = mapping_details[
                    (mapping_details['SOURCE_FIELD'] == source_field) & 
                    (mapping_details['TARGET_FIELD'] == target_field)
                ].iloc[0]
                
                # Get current transformation (if it exists)
                if st.session_state.selected_field in st.session_state.transformations:
                    current_transform = st.session_state.transformations[st.session_state.selected_field]
                else:
                    # No transformation exists yet - create a placeholder
                    current_transform = {
                        'sql': source_field,  # Default to just the field name
                        'confidence': 0.0,
                        'explanation': 'No transformation generated yet',
                        'model_used': None
                    }
                
                # Source, target, and transformed output formats as tables
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.write("**Source Format:**")
                    if source_field in sample_data.columns:
                        source_samples = sample_data[source_field].dropna().unique()[:5].tolist()
                        # Create a DataFrame for table display
                        source_df = pd.DataFrame({'Sample Values': source_samples})
                        st.dataframe(source_df, hide_index=True, height=180)
                    else:
                        st.info("No samples available")
                
                with col2:
                    st.write("**Target Format:**")
                    # Parse target samples into list format
                    target_samples = selected_row['SAMPLE_VALUES'].split(', ') if ', ' in selected_row['SAMPLE_VALUES'] else [selected_row['SAMPLE_VALUES']]
                    # Create a DataFrame for table display
                    target_df = pd.DataFrame({'Expected Values': target_samples[:5]})
                    st.dataframe(target_df, hide_index=True, height=180)
                
                with col3:
                    st.write("**Transformed Output:**")
                    try:
                        preview = preview_transformation(
                            conn,
                            mapping['CONTRIBUTOR_TABLE'],
                            source_field,
                            current_transform['sql']
                        )
                        # Show preview in a nice formatted box
                        preview_values = preview.split(', ')
                        preview_df = pd.DataFrame({'Result': preview_values})
                        st.dataframe(preview_df, hide_index=True, height=180)
                    except Exception as e:
                        st.error(f"Error: {str(e)[:50]}...")
                
                # Transformation hints with complex model checkbox
                st.write("**Transformation Hints:**")
                hint_container = st.container()
                with hint_container:
                    # Create unique keys using selected field to avoid duplicates
                    hint_col, check_col = st.columns([4.5, 1.5])
                    
                    with hint_col:
                        context = st.text_area(
                            "Hints",
                            placeholder="e.g., Remove $ symbol, convert to uppercase, change date format",
                            value=selected_row.get('TRANSFORMATION_HINTS', ''),
                            key=f"hint_area_{st.session_state.selected_field}",
                            label_visibility="collapsed",
                            height=100,
                            on_change=lambda: save_field_attribute(
                                conn,
                                mapping['MAPPING_ID'],
                                source_field,
                                target_field,
                                "TRANSFORMATION_HINTS",
                                st.session_state[f"hint_area_{st.session_state.selected_field}"]
                            )
                        )
                    
                    with check_col:
                        st.write("")  # Spacing to align with textarea
                        # Get saved complex model flag if it exists
                        saved_complex_flag = selected_row.get('USE_COMPLEX_MODEL', False)
                        use_complex = st.checkbox(
                            "Use Complex Model",
                            value=saved_complex_flag,
                            key=f"complex_{st.session_state.selected_field}",
                            help="Force Claude for this transformation",
                            on_change=lambda: save_field_attribute(
                                conn,
                                mapping['MAPPING_ID'],
                                source_field,
                                target_field,
                                "USE_COMPLEX_MODEL",
                                st.session_state[f"complex_{st.session_state.selected_field}"]
                            )
                        )
                    
                    if st.button("🔄 Regenerate", key=f"regen_{st.session_state.selected_field}"):
                        # Check if approved or skip - warn user
                        is_approved = selected_row.get('IS_APPROVED', False)
                        is_skipped = selected_row.get('SKIP_TRANSFORMATION', False)
                        
                        if is_approved:
                            st.warning("This field is approved. Uncheck 'Approve' before regenerating.")
                        elif is_skipped:
                            st.warning("This field is set to skip transformation. Uncheck 'Do not transform' before regenerating.")
                        else:
                            with st.spinner("Regenerating..."):
                                transform_result = generate_transformation_sql(
                                    conn,
                                    source_field,
                                    sample_data[source_field].dropna().unique()[:5].tolist() if source_field in sample_data.columns else [],
                                    selected_row,
                                    sample_data,
                                    user_context=context,
                                    cortex_model=st.session_state.get('default_ai_model', 'snowflake-arctic'),
                                    force_complex=use_complex
                                )
                                
                                # Add to transformations if it doesn't exist
                                st.session_state.transformations[st.session_state.selected_field] = transform_result
                                st.session_state.manual_edits.discard(st.session_state.selected_field)

                                # Also clear the manual edit flag in the database
                                save_field_attribute(
                                    conn,
                                    mapping['MAPPING_ID'],
                                    source_field,
                                    target_field,
                                    "IS_MANUALLY_EDITED",
                                    False
                                )

                                
                                # AUTO-SAVE the regenerated transformation
                                save_field_attribute(
                                    conn,
                                    mapping['MAPPING_ID'],
                                    source_field,
                                    target_field,
                                    "TRANSFORMATION_SQL",
                                    transform_result['sql']
                                )
                                save_field_attribute(
                                    conn,
                                    mapping['MAPPING_ID'],
                                    source_field,
                                    target_field,
                                    "TRANSFORMATION_CONFIDENCE",
                                    transform_result.get('confidence', 0.5)
                                )
                                save_field_attribute(
                                conn,
                                mapping['MAPPING_ID'],
                                source_field,
                                target_field,
                                "MODEL_USED",
                                transform_result.get('model_used', '')
                                )
                                save_field_attribute(
                                conn,
                                mapping['MAPPING_ID'],
                                source_field,
                                target_field,
                                "EXPLANATION",
                                transform_result.get('explanation', '')
                                )

                                
                                st.rerun()
                
                # SQL Transformation
                st.write("**SQL Transformation:**")
                
                # Track edit mode in session state
                edit_mode_key = f"edit_mode_{st.session_state.selected_field}"
                
                # Show either code display or editor based on mode
                if st.session_state.get(edit_mode_key, False):
                    # Edit mode - show text area with save button
                    new_sql = st.text_area(
                        "Edit SQL",
                        value=current_transform['sql'],
                        height=150,
                        key=f"sql_edit_{st.session_state.selected_field}",
                        label_visibility="collapsed",
                        help="Edit the SQL transformation logic"
                    )
                    
                    # Update if changed
                    if new_sql != current_transform['sql']:
                        # Ensure field exists in transformations
                        if st.session_state.selected_field not in st.session_state.transformations:
                            st.session_state.transformations[st.session_state.selected_field] = {}
                        
                        st.session_state.transformations[st.session_state.selected_field]['sql'] = new_sql
                        st.session_state.transformations[st.session_state.selected_field]['confidence'] = 1.0
                        st.session_state.manual_edits.add(st.session_state.selected_field)

                        # Auto-save the manual edit
                        save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "TRANSFORMATION_SQL", new_sql)
                        save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "IS_MANUALLY_EDITED", True)
                        save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "TRANSFORMATION_CONFIDENCE", 1.0)
                        
                        # Force refresh the preview
                        st.rerun()
                    
                    # Save button to exit edit mode
                    if st.button("💾 Save", type="primary", key=f"save_sql_{st.session_state.selected_field}"):
                        st.session_state[edit_mode_key] = False
                        st.rerun()
                
                else:
                    # View mode - show syntax highlighted code with edit button inline
                    code_container = st.container()
                    with code_container:
                        # Use columns to put edit button next to the code
                        code_col, btn_col = st.columns([5.5, 0.5])
                        
                        with code_col:
                            st.code(
                                current_transform['sql'],
                                language='sql'
                            )
                        
                        with btn_col:
                            # Add padding to align properly
                            st.write("")  # Spacing
                            if st.button("✏️", key=f"edit_btn_{st.session_state.selected_field}", help="Edit SQL"):
                                st.session_state[edit_mode_key] = True
                                st.rerun()
                
                # AI Reasoning
                if current_transform.get('explanation'):
                    with st.expander("ℹ️ AI Reasoning"):
                        st.info(current_transform['explanation'])
                        if current_transform.get('model_used'):
                            st.caption(f"Model used: {current_transform['model_used']}")
                        if current_transform.get('complexity'):
                            st.caption(f"Complexity: {current_transform['complexity']}")
                
                # Approval checkbox at the bottom
                st.divider()
                
                # Create two columns for checkboxes
                check_col1, check_col2 = st.columns(2)
                
                with check_col1:
                    is_approved = selected_row.get('IS_APPROVED', False)
                    new_approval_state = st.checkbox(
                        "✅ **Approve this transformation**",
                        value=is_approved,
                        key=f"approve_{st.session_state.selected_field}"
                    )
                    
                    # Update approval state if changed
                    if new_approval_state != is_approved:
                        if new_approval_state:
                            st.session_state.approved_fields.add(st.session_state.selected_field)
                        else:
                            st.session_state.approved_fields.discard(st.session_state.selected_field)
                        
                        # Save to database immediately
                        save_field_attribute(
                            conn,
                            mapping['MAPPING_ID'],
                            source_field,
                            target_field,
                            "IS_APPROVED",
                            new_approval_state
                        )
                        st.rerun()  # Force refresh to update the overview table
                
                with check_col2:
                    # Initialize skip transformations set if not exists
                    if 'skip_transformations' not in st.session_state:
                        st.session_state.skip_transformations = set()
                    
                    is_skipped = selected_row.get('SKIP_TRANSFORMATION', False)
                    new_skip_state = st.checkbox(
                        "🚫 **Do not transform (pass through as-is)**",
                        value=is_skipped,
                        key=f"skip_{st.session_state.selected_field}",
                        help="Source field will be passed through without any transformation"
                    )
                    
                    # Update skip state if changed
                    if new_skip_state != is_skipped:
                        if new_skip_state:
                            st.session_state.skip_transformations.add(st.session_state.selected_field)
                            # Clear manual edit flag when setting to skip
                            st.session_state.manual_edits.discard(st.session_state.selected_field)
                            
                            # Update transformation to be pass-through
                            if st.session_state.selected_field not in st.session_state.transformations:
                                st.session_state.transformations[st.session_state.selected_field] = {}
                            st.session_state.transformations[st.session_state.selected_field]['sql'] = source_field
                            st.session_state.transformations[st.session_state.selected_field]['confidence'] = 1.0
                            st.session_state.transformations[st.session_state.selected_field]['explanation'] = "Pass-through field (no transformation)"
                        else:
                            st.session_state.skip_transformations.discard(st.session_state.selected_field)
                            # Clear manual edit flag when unchecking skip
                            st.session_state.manual_edits.discard(st.session_state.selected_field)
                            # Regenerate transformation when unchecking skip
                            # Could optionally regenerate the transformation here
                        
                        # Save to database immediately
                        save_field_attribute(
                            conn,
                            mapping['MAPPING_ID'],
                            source_field,
                            target_field,
                            "SKIP_TRANSFORMATION",
                            new_skip_state
                        )
                        
                        # Also clear the manual edit flag in the database
                        save_field_attribute(
                            conn,
                            mapping['MAPPING_ID'],
                            source_field,
                            target_field,
                            "IS_MANUALLY_EDITED",
                            False
                        )

                        # Auto-save the transformation changes
                        if new_skip_state:
                            # Save pass-through transformation
                            save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "TRANSFORMATION_SQL", source_field)
                            save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "TRANSFORMATION_CONFIDENCE", 1.0)
                            save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "EXPLANATION", "Pass-through field (no transformation)")
                            save_field_attribute(conn, mapping['MAPPING_ID'], source_field, target_field, "MODEL_USED", "")
        
                        st.rerun()
            
            else:
                # No field selected
                st.subheader("Edit Details")
                st.info("Select a field from the table to view and edit transformation details")
    
    # Save transformations button and bottom buttons
    st.divider()
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        # Regenerate all button (moved from top)
        if st.session_state.transformations:
            if st.button("🔄 Regenerate All Transformations", help="Regenerate all transformations from scratch", use_container_width=True):
                st.session_state.transformations = {}
                st.session_state.manual_edits = set()
                st.session_state.approved_fields = set()
                st.session_state.auto_generate = True
                st.rerun()
    
    with col2:
        # Count approved fields
        approved_count = len(st.session_state.approved_fields)
        total_count = len(st.session_state.transformations)
        
        # Save button is ALWAYS enabled
        if st.button(
            f"💾 Save Transformations ({total_count} fields, {approved_count} approved)",
            type="primary",  # Always primary style
            use_container_width=True
        ):
            with st.spinner("Saving transformations to database..."):
                if save_transformations_to_db(
                    conn,
                    mapping['MAPPING_ID'],
                    st.session_state.transformations,
                    st.session_state.manual_edits,
                    st.session_state.approved_fields,
                    st.session_state.get('skip_transformations', set())
                ):
                    st.success(f"✅ Saved all {total_count} transformations! ({approved_count} approved)")
                    time.sleep(2)
                else:
                    st.error("Failed to save transformations. Check debug logs.")
    
    with col3:
        # Generate SQL button - only enabled when ALL fields are approved
        total_fields = len(mapping_details)
        approved_count = len(st.session_state.approved_fields)
        all_approved = approved_count == total_fields and total_fields > 0
        
        if st.button(
            "📝 Generate Final SQL →",
            type="primary" if all_approved else "secondary",
            use_container_width=True,
            disabled=not all_approved,
            help=f"All {total_fields} fields must be approved" if not all_approved else "Generate SQL for all fields"
        ):
            # Navigate to page 03
            st.switch_page("pages/03_FinalSQL_Sharing.py")