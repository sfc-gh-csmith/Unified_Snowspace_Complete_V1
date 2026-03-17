"""
AI Field Builder Page - Analyzes target table and generates field definitions
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import time


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle numpy types"""
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


# Configure page
st.set_page_config(
    page_title="AI Field Builder - Unified Snowspace",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []
if 'cortex_calls' not in st.session_state:
    st.session_state.cortex_calls = 0
if 'current_snowspace_id' not in st.session_state:
    st.error("No Snowspace selected. Please go back to the main page.")
    st.stop()
if 'analysis_started' not in st.session_state:
    st.session_state.analysis_started = False

# Connect to Snowflake
try:
    conn = snowpark.Session.builder.create()
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")
    st.stop()

# --- Helper Functions ---
def log_message(message: str, level: str = "info"):
    """Add message to debug logs"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.debug_logs.append({
        'time': timestamp,
        'level': level,
        'message': message
    })

def get_snowspace_info(snowspace_id: str) -> Dict[str, Any]:
    """Get snowspace configuration from database"""
    try:
        result = conn.sql(f"""
            SELECT snowspace_id, snowspace_name, description, target_table
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES
            WHERE snowspace_id = '{snowspace_id}'
        """).collect()
        
        if result:
            row = result[0]
            return {
                'snowspace_id': row['SNOWSPACE_ID'],
                'snowspace_name': row['SNOWSPACE_NAME'],
                'description': row['DESCRIPTION'],
                'target_table': row['TARGET_TABLE']
            }
    except Exception as e:
        log_message(f"Error loading snowspace: {str(e)}", level="error")
    return None

def call_cortex_complete(session: snowpark.Session, model: str, prompt: str) -> Optional[str]:
    """
    Call Snowflake Cortex COMPLETE function
    Tracks number of calls for performance monitoring
    """
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
        return response
    except Exception as e:
        log_message(f"Cortex error: {str(e)}", level="error")
        return None

def get_example_values(column_data: pd.Series, max_examples: int = 5) -> str:
    """Get representative example values from a column"""
    unique_values = column_data.dropna().unique()
    
    if len(unique_values) <= max_examples:
        examples = unique_values.tolist()
    else:
        # Get diverse examples
        examples = []
        try:
            sorted_values = sorted(unique_values)
            examples.append(sorted_values[0])  # Min
            examples.append(sorted_values[-1])  # Max
            # Add some middle values
            middle_indices = np.linspace(1, len(sorted_values)-2, 
                                       min(3, max_examples-2), dtype=int)
            for idx in middle_indices:
                if len(examples) < max_examples:
                    examples.append(sorted_values[idx])
        except:
            # If not sortable, just take first N
            examples = unique_values[:max_examples].tolist()
    
    return ', '.join([str(v)[:50] for v in examples])

def save_field_to_database(
    session: snowpark.Session,
    snowspace_id: str,
    field_name: str,
    updates: Dict[str, Any]
) -> bool:
    """Save or update a single field in the FIELD_DEFINITIONS table"""
    try:
        # Build UPDATE statement
        set_clauses = []
        for key, value in updates.items():
            if key == 'is_required':
                set_clauses.append(f"{key} = {value}")
            elif key == 'confidence_score':
                set_clauses.append(f"{key} = {value}")
            else:
                # Escape single quotes in string values
                escaped_value = str(value).replace("'", "''")
                set_clauses.append(f"{key} = '{escaped_value}'")
        
        set_clauses.append("UPDATED_AT = CURRENT_TIMESTAMP()")
        
        update_query = f"""
        UPDATE UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS 
        SET {', '.join(set_clauses)}
        WHERE SNOWSPACE_ID = '{snowspace_id}' 
        AND FIELD_NAME = '{field_name}'
        """
        
        session.sql(update_query).collect()
        return True
    except Exception as e:
        log_message(f"Error updating field {field_name}: {str(e)}", level="error")
        return False

def save_analysis_to_database(
    session: snowpark.Session,
    snowspace_id: str,
    results: Dict[str, Any]
) -> bool:
    """Save the complete analysis results to FIELD_DEFINITIONS table"""
    try:
        # First, delete any existing records for this snowspace
        session.sql(f"DELETE FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS WHERE SNOWSPACE_ID = '{snowspace_id}'").collect()
        
        # Prepare data for insertion
        rows = []
        current_timestamp = datetime.now()
        
        for field in results['field_definitions']:
            rows.append({
                'SNOWSPACE_ID': snowspace_id,
                'FIELD_NAME': field['field_name'],
                'FIELD_CATEGORY': field['field_category'],
                'DATA_TYPE': field['data_type'],
                'SAMPLE_VALUES': field['sample_values'],
                'DESCRIPTION': field['description'],
                'SYNONYMS': field['synonyms'],
                'ADDITIONAL_CONTEXT': field.get('additional_context', ''),
                'IS_REQUIRED': field['is_required'],
                'CONFIDENCE_SCORE': field.get('confidence_score', 0.9),
                'CREATED_AT': current_timestamp,
                'UPDATED_AT': current_timestamp
            })
        
        # Create DataFrame and insert
        df = session.create_dataframe(rows)
        df.write.mode("append").save_as_table("UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS")
        
        log_message(f"Saved {len(rows)} field definitions to database")
        return True
    except Exception as e:
        log_message(f"Error saving to database: {str(e)}", level="error")
        return False

def generate_basic_synonyms(column_name: str) -> List[str]:
    """Generate basic synonyms from column name - used as fallback"""
    synonyms = [column_name.lower()]
    
    # Add variations
    if '_' in column_name:
        # Camel case version
        parts = column_name.lower().split('_')
        camel = parts[0] + ''.join(p.capitalize() for p in parts[1:])
        synonyms.append(camel)
        # Space separated
        synonyms.append(' '.join(parts))
    
    # Add uppercase
    synonyms.append(column_name.upper())
    
    # Common abbreviations
    replacements = {
        'number': ['num', 'no', 'nbr'],
        'quantity': ['qty', 'quant'],
        'amount': ['amt'],
        'description': ['desc', 'descr'],
        'category': ['cat', 'categ'],
        'customer': ['cust', 'client'],
        'product': ['prod', 'item'],
        'date': ['dt'],
        'identifier': ['id'],
        'code': ['cd'],
        'type': ['typ'],
        'status': ['stat', 'sts'],
        'transaction': ['trans', 'txn', 'trx']
    }
    
    name_lower = column_name.lower()
    for full, abbrevs in replacements.items():
        if full in name_lower:
            for abbr in abbrevs:
                synonyms.append(name_lower.replace(full, abbr))
        else:
            # Check if abbreviation is used, suggest full word
            for abbr in abbrevs:
                if abbr in name_lower and len(abbr) > 2:  # Avoid false matches on short abbreviations
                    synonyms.append(name_lower.replace(abbr, full))
    
    return list(set(synonyms))[:10]  # Return up to 10 unique synonyms

def map_to_snowflake_type(pandas_dtype: str) -> str:
    """Map pandas dtype to Snowflake data type"""
    dtype_str = str(pandas_dtype).lower()
    
    if 'int' in dtype_str:
        return 'NUMBER'
    elif 'float' in dtype_str:
        return 'NUMBER'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP_NTZ'
    elif 'date' in dtype_str:
        return 'DATE'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        return 'STRING'

def classify_column_rules_only(
    column_name: str,
    column_data: pd.Series
) -> Tuple[str, float]:
    """
    Rule-based classification only - no Cortex calls
    Returns (field_category, confidence)
    """
    # Get basic statistics
    dtype = str(column_data.dtype)
    unique_count = column_data.nunique()
    total_count = len(column_data)
    unique_ratio = unique_count / total_count if total_count > 0 else 0
    
    # 1. Time dimensions are straightforward
    if pd.api.types.is_datetime64_any_dtype(column_data):
        return 'TIME_DIMENSION', 0.95
    elif any(pattern in column_name.lower() for pattern in ['date', 'time', '_at', '_on']):
        # Check if it's a date string
        sample_val = str(column_data.dropna().iloc[0]) if len(column_data.dropna()) > 0 else ""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}'   # MM-DD-YYYY
        ]
        if any(re.match(pattern, sample_val) for pattern in date_patterns):
            return 'TIME_DIMENSION', 0.95
    
    # 2. Numeric columns
    if pd.api.types.is_numeric_dtype(column_data):
        # Check if it's likely a metric/fact
        fact_keywords = ['amount', 'total', 'sum', 'count', 'quantity', 'revenue',
                        'cost', 'price', 'sales', 'units', 'value', 'rate', 'percent',
                        'qty', 'amt', 'val', 'hours', 'minutes', 'days']
        if any(keyword in column_name.lower() for keyword in fact_keywords):
            return 'FACT', 0.95
        # Low cardinality numeric might be dimensional
        elif unique_count < 20:
            return 'DIMENSION', 0.90
        else:
            # Ambiguous numeric - default to FACT but lower confidence
            return 'FACT', 0.70
    
    # 3. Everything else is a dimension
    return 'DIMENSION', 0.90

def should_skip_ai_analysis(column_name: str, column_data: pd.Series) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Check if we can skip AI analysis for obvious columns
    Returns (should_skip, semantic_data_if_skipping)
    """
    name_lower = column_name.lower()
    sample_values = column_data.dropna().unique()[:5]
    unique_count = column_data.nunique()
    
    # Check for obvious patterns
    
    # 1. Year columns (generic pattern)
    if 'year' in name_lower or (pd.api.types.is_numeric_dtype(column_data) and 
                                all(1900 <= v <= 2100 for v in sample_values if pd.notna(v))):
        return True, {
            'description': 'Year value',
            'synonyms': ['year', 'yr', name_lower.replace('_year', '_yr')],
            'additional_context': ''
        }
    
    # 2. Simple ID/Number columns
    if name_lower.endswith(('_id', '_number', '_num', '_no', '_code')):
        entity = name_lower.replace('_id', '').replace('_number', '').replace('_num', '').replace('_no', '').replace('_code', '').replace('_', ' ').strip()
        return True, {
            'description': f'Unique identifier for {entity}',
            'synonyms': generate_basic_synonyms(column_name)[:5],
            'additional_context': f'Reference field for {entity} lookup'
        }
    
    # 3. Common categorical fields
    common_categoricals = {
        'state': 'Geographic state or territory',
        'status': 'Current status or state of the record',
        'type': 'Classification or category type',
        'category': 'Item category or classification',
        'country': 'Country name or code',
        'city': 'City name'
    }
    
    for key, description_template in common_categoricals.items():
        if key == name_lower or name_lower.endswith(f'_{key}'):
            return True, {
                'description': description_template,
                'synonyms': generate_basic_synonyms(column_name)[:5],
                'additional_context': ''
            }
    
    return False, None

def analyze_column_semantics_with_cortex(
    session: snowpark.Session,
    column_name: str,
    column_data: pd.Series,
    field_category: str,
    data_type: str,
    cortex_model: str = 'snowflake-arctic',
    user_context: str = None
) -> Dict[str, Any]:
    """
    Progressive semantic analysis: analyze data first, then build description
    """
    # Get sample values and data characteristics
    sample_values = column_data.dropna().unique()[:10].tolist()
    sample_str = ', '.join([f'"{str(v)[:50]}"' for v in sample_values[:5]])
    unique_count = column_data.nunique()
    total_count = len(column_data)
    
    # Build progressive prompt that analyzes data THEN creates description
    context_section = ""
    if user_context:
        context_section = f"\nAdditional context from user: {user_context}\n"
    
    prompt = f"""Analyze this database column based on its name AND actual data values:

Column Name: {column_name}
Data Type: {field_category}
Sample Values: {sample_str}
Statistics: {unique_count} unique values out of {total_count} rows
{context_section}
CRITICAL RULES FOR DESCRIPTION:
1. NEVER start with "The [field_name] field/column contains/represents..."
2. Start DIRECTLY with what it is (e.g., "Unique identifier for records" NOT "The ID field contains...")
3. NO statistics in the description (no counts, no "50 unique values", no averages)
4. NO speculation ("likely", "probably", "such as USD or EUR")
5. Be concise and definitive about what the field IS

Return JSON:
{{
    "description": "Direct, concise description starting with WHAT it is, not 'The field...'",
    "synonyms": ["3-5 alternative COLUMN NAMES with varied formatting (underscores, spaces, camelCase)"]
}}"""
    
    response = call_cortex_complete(session, cortex_model, prompt)
    
    if response:
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = json.loads(response)
            
            # Get synonyms
            synonyms = result.get('synonyms', [])
            if len(synonyms) < 3:
                synonyms.extend(generate_basic_synonyms(column_name))
                
            return {
                'description': result.get('description', f'{field_category} field'),
                'synonyms': list(set(synonyms))[:7],  # Up to 7 synonyms
                'additional_context': ''
            }
        except Exception as e:
            log_message(f"Error parsing semantic analysis: {str(e)}", level="warning")
    
    # Fallback response
    return {
        'description': f'{field_category} field',
        'synonyms': generate_basic_synonyms(column_name)[:5],
        'additional_context': ''
    }

def get_nullability(column_data: pd.Series) -> bool:
    """Simple check if field has any null values"""
    return column_data.isna().sum() > 0

def analyze_column_complete(
    session: snowpark.Session,
    column_name: str,
    column_data: pd.Series,
    cortex_model: str = 'snowflake-arctic',
    user_context: str = None
) -> Dict[str, Any]:
    """
    Complete column analysis with simplified approach:
    1. Rule-based classification (no Cortex)
    2. Check if we can skip AI for obvious columns
    3. AI-powered semantic analysis only when needed
    4. Return clean, simple metadata
    """
    start_time = time.time()
    
    # Step 1: Rule-based classification
    field_category, confidence = classify_column_rules_only(column_name, column_data)
    log_message(f"Classified {column_name} as {field_category} (confidence: {confidence})")
    
    # Step 2: Data type mapping
    data_type = map_to_snowflake_type(column_data.dtype)
    
    # Step 3: Check if we can skip AI
    should_skip, semantic_data = should_skip_ai_analysis(column_name, column_data)
    
    if should_skip:
        log_message(f"⚡ Skipping AI for obvious column: {column_name}")
        semantic_analysis = semantic_data
    else:
        # Use Cortex for semantic analysis with progressive approach
        log_message(f"🤖 Using AI for column: {column_name}")
        semantic_analysis = analyze_column_semantics_with_cortex(
            session, column_name, column_data, field_category, data_type, cortex_model, user_context
        )
    
    # Step 4: Simple required check
    is_required = not get_nullability(column_data)
    
    elapsed = time.time() - start_time
    log_message(f"Analyzed {column_name} in {elapsed:.2f}s")
    
    # Return clean, simple analysis
    return {
        'field_name': column_name,
        'field_category': field_category,
        'data_type': data_type,
        'sample_values': get_example_values(column_data),
        'description': semantic_analysis['description'],
        'synonyms': ', '.join(semantic_analysis['synonyms']),
        'additional_context': semantic_analysis.get('additional_context', ''),
        'is_required': is_required,
        'confidence_score': confidence,  # Add confidence score
        'used_ai': not should_skip
    }

def analyze_table_with_cortex(
    session: snowpark.Session,
    database: str,
    schema: str,
    table_name: str,
    sample_size: int = 10000,
    cortex_model: str = 'snowflake-arctic'
) -> Dict[str, Any]:
    """
    Main function to analyze a table
    Uses simplified approach focused on semantic understanding
    """
    results = {
        'schema_metadata': {},
        'field_definitions': [],
        'analysis_summary': {}
    }
    
    # Reset Cortex call counter
    st.session_state.cortex_calls = 0
    
    try:
        log_message(f"Starting analysis of {database}.{schema}.{table_name}")
        overall_start = time.time()
        
        # Get total row count
        count_query = f'SELECT COUNT(*) as cnt FROM "{database}"."{schema}"."{table_name}"'
        total_rows = session.sql(count_query).collect()[0]['CNT']
        
        # Determine sampling
        if total_rows <= sample_size:
            sample_query = f'SELECT * FROM "{database}"."{schema}"."{table_name}"'
            actual_sample_size = total_rows
        else:
            sample_query = f'SELECT * FROM "{database}"."{schema}"."{table_name}" SAMPLE ({sample_size} ROWS)'
            actual_sample_size = sample_size
        
        # Load sample data
        df = session.sql(sample_query).to_pandas()
        log_message(f"Loaded {len(df):,} rows for analysis (total: {total_rows:,})")
        
        # Store dataframe in session state for re-analysis
        st.session_state.original_df = df
        st.session_state.cortex_model = cortex_model
        
        # Analyze each column
        progress = st.progress(0)
        total_cols = len(df.columns)
        
        for i, col in enumerate(df.columns):
            progress.progress((i + 1) / total_cols)
            
            field_def = analyze_column_complete(
                session, col, df[col], cortex_model
            )
            results['field_definitions'].append(field_def)
        
        progress.empty()
        
        # Generate metadata
        results['schema_metadata'] = {
            'source_database': database,
            'source_schema': schema,
            'source_table': table_name,
            'row_count': total_rows,
            'analysis_sample_size': actual_sample_size,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # Summary
        results['analysis_summary'] = {
            'total_columns': len(df.columns),
            'dimensions': sum(1 for f in results['field_definitions'] if f['field_category'] == 'DIMENSION'),
            'time_dimensions': sum(1 for f in results['field_definitions'] if f['field_category'] == 'TIME_DIMENSION'),
            'facts': sum(1 for f in results['field_definitions'] if f['field_category'] == 'FACT'),
            'cortex_calls': st.session_state.cortex_calls,
            'columns_using_ai': sum(1 for f in results['field_definitions'] if f.get('used_ai', False))
        }
        
        elapsed = time.time() - overall_start
        log_message(f"✅ Analysis complete in {elapsed:.1f} seconds with {st.session_state.cortex_calls} Cortex calls")
        
        return results
        
    except Exception as e:
        log_message(f"Error analyzing table: {str(e)}", level="error")
        raise

# --- Main UI ---

# Load snowspace info
snowspace_info = get_snowspace_info(st.session_state.current_snowspace_id)
if not snowspace_info:
    st.error("Failed to load Snowspace information")
    st.stop()

# Parse target table
target_parts = snowspace_info['target_table'].split('.')
if len(target_parts) != 3:
    st.error(f"Invalid target table format: {snowspace_info['target_table']}")
    st.stop()

database, schema, table = target_parts

# Sidebar with analysis options
with st.sidebar:
    # Snowspace Info in a compact expander
    with st.expander("📊 Snowspace Details", expanded=False):
        st.markdown(f"**Snowspace:** {snowspace_info['snowspace_name']} (`{snowspace_info['snowspace_id']}`)")
        st.markdown(f"**Target Table:** `{snowspace_info['target_table']}`")
        
        # Performance Stats (only show if analysis has been run)
        if st.session_state.analysis_results:
            st.divider()
            summary = st.session_state.analysis_results['analysis_summary']
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Cortex Calls", summary['cortex_calls'])
            with col2:
                st.metric("AI Analyzed", f"{summary['columns_using_ai']}/{summary['total_columns']}")
            st.caption("⚡ = Rule-based\n🤖 = AI-powered")
    
    with st.expander("⚙️ Analysis Options", expanded=False):
        sample_size = st.number_input(
            "Sample Size (rows)",
            min_value=1000,
            max_value=100000,
            value=10000,
            step=1000,
            help="Number of rows to analyze. Larger samples give better results but take longer."
        )
        
        cortex_model = st.selectbox(
            "Cortex Model",
            [
                "snowflake-arctic",
                "mistral-large",
                "mixtral-8x7b",
                "mistral-7b",
                "llama3.1-70b",
                "llama3.1-8b",
                "llama2-70b-chat",
                "reka-core",
                "reka-flash",
                "gemma-7b"
            ],
            help="Arctic and Mistral Large are recommended for best results"
        )
        
        if st.button("🔄 Re-analyze All Fields", type="primary", 
                     disabled=not st.session_state.analysis_results):
            st.session_state.analysis_started = False
            st.rerun()

# Sidebar with debug logs
with st.sidebar:
    if st.session_state.debug_logs:
        with st.expander("📜 Debug Logs", expanded=False):
            for log in reversed(st.session_state.debug_logs[-20:]):
                if log['level'] == 'error':
                    st.error(f"[{log['time']}] {log['message']}")
                elif log['level'] == 'warning':
                    st.warning(f"[{log['time']}] {log['message']}")
                else:
                    st.info(f"[{log['time']}] {log['message']}")
    
    if st.button("Clear Logs"):
        st.session_state.debug_logs = []
        st.session_state.cortex_calls = 0
        st.rerun()

# Header with breadcrumb
st.markdown("""
<div style="margin-bottom: 2rem;">
    <p style="color: #666; margin: 0;">Create Snowspace ✓ → <strong>Generate Field Definitions</strong> → Contributors → Publish</p>
</div>
""", unsafe_allow_html=True)

st.title("🤖 Generate Field Definitions")

# Auto-analyze on first load
if not st.session_state.analysis_started:
    st.session_state.analysis_started = True
    
    with st.spinner(f"Analyzing {table} with Cortex AI..."):
        try:
            results = analyze_table_with_cortex(
                conn,
                database,
                schema,
                table,
                sample_size,
                cortex_model
            )
            st.session_state.analysis_results = results
            
            # Save to database
            if save_analysis_to_database(conn, st.session_state.current_snowspace_id, results):
                st.success(f"✅ Analysis complete! Used {results['analysis_summary']['cortex_calls']} Cortex calls.")
            else:
                st.warning("Analysis complete but failed to save to database")
        except Exception as e:
            st.error(f"Analysis failed: {str(e)}")
            st.session_state.analysis_started = False

# Display results
if st.session_state.analysis_results:
    results = st.session_state.analysis_results
    
    # Summary metrics
    st.subheader("📊 Summary")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    summary = results['analysis_summary']
    col1.metric("Total Fields", summary['total_columns'])
    col2.metric("Dimensions", summary['dimensions'])
    col3.metric("Time Dimensions", summary['time_dimensions'])
    col4.metric("Facts", summary['facts'])
    col5.metric("AI Analyzed", summary['columns_using_ai'])
    
    # Field definitions
    st.subheader("📋 Generated Field Definitions")
    st.info("💡 Review generated metadata and make any necessary adjustments. Click on any field to expand and edit.")
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["Table View", "Manually Edit"])
    
    with tab1:
        # Summary table
        field_df = pd.DataFrame(results['field_definitions'])
        display_df = pd.DataFrame({
            'Field Name': field_df['field_name'],
            'Category': field_df['field_category'],
            'Type': field_df['data_type'],
            'Required': field_df['is_required'].map({True: '✓', False: ''}),
            'Confidence': field_df.apply(lambda x: f"{x.get('confidence_score', 0.9):.0%}", axis=1),
            'Description': field_df['description'],
            'Synonyms': field_df['synonyms'],
            'AI': field_df.get('used_ai', pd.Series([True]*len(field_df))).map({True: '🤖', False: '⚡'})
        })
        st.dataframe(display_df, use_container_width=True)
    
    with tab2:
        # Detailed field-by-field view with re-analysis capability
        for i, field in enumerate(results['field_definitions']):
            with st.expander(f"**{field['field_name']}** - {field['field_category']}"):
                # Add context input and re-analyze button at the top
                context_container = st.container()
                with context_container:
                    col_context, col_reanalyze = st.columns([4, 1])
                    
                    with col_context:
                        field_context = st.text_input(
                            "💡 Add context to improve AI analysis",
                            key=f"context_{field['field_name']}",
                            value=field.get('additional_context', ''),
                            placeholder=f"e.g., 'This field contains warranty claim codes from our ERP system'",
                            help="Provide additional context about this field to generate better description and synonyms",
                            on_change=lambda fn=field['field_name']: save_field_to_database(
                                conn, 
                                st.session_state.current_snowspace_id,
                                fn,
                                {'ADDITIONAL_CONTEXT': st.session_state[f"context_{fn}"], 'CONFIDENCE_SCORE': 1.0}
                            )
                        )
                    
                    with col_reanalyze:
                        if st.button("🔄 Re-analyze", key=f"reanalyze_{field['field_name']}", 
                                   disabled=not field_context,
                                   help="Re-analyze this field with the provided context"):
                            # Re-analyze just this field with context
                            with st.spinner(f"Re-analyzing {field['field_name']} with context..."):
                                try:
                                    # Get the column data from the original dataframe
                                    if 'original_df' in st.session_state:
                                        column_data = st.session_state.original_df[field['field_name']]
                                        
                                        # Re-run semantic analysis with context
                                        updated_analysis = analyze_column_semantics_with_cortex(
                                            conn,
                                            field['field_name'],
                                            column_data,
                                            field['field_category'],
                                            field['data_type'],
                                            st.session_state.get('cortex_model', 'snowflake-arctic'),
                                            user_context=field_context
                                        )
                                        
                                        # Update the field definition
                                        field['description'] = updated_analysis['description']
                                        field['synonyms'] = ', '.join(updated_analysis['synonyms'])
                                        field['additional_context'] = field_context
                                        
                                        # Save to database
                                        save_field_to_database(
                                            conn,
                                            st.session_state.current_snowspace_id,
                                            field['field_name'],
                                            {
                                                'DESCRIPTION': field['description'],
                                                'SYNONYMS': field['synonyms'],
                                                'ADDITIONAL_CONTEXT': field_context,
                                                'CONFIDENCE_SCORE': 1.0
                                            }
                                        )
                                        
                                        # Increment Cortex call counter
                                        st.session_state.cortex_calls += 1
                                        
                                        st.success("✅ Field re-analyzed and saved!")
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error("Original data not found. Please re-run the full analysis.")
                                except Exception as e:
                                    st.error(f"Re-analysis failed: {str(e)}")
                
                    st.divider()
                
                # Rest of the field display - now editable
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Editable description
                    description = st.text_area(
                        "**Description:**",
                        value=field['description'],
                        key=f"desc_{field['field_name']}",
                        height=100,
                        on_change=lambda fn=field['field_name']: save_field_to_database(
                            conn,
                            st.session_state.current_snowspace_id,
                            fn,
                            {'DESCRIPTION': st.session_state[f"desc_{fn}"], 'CONFIDENCE_SCORE': 1.0}
                        )
                    )
                    
                    # Editable synonyms
                    synonyms = st.text_input(
                        "**Synonyms:**",
                        value=field['synonyms'],
                        key=f"syn_{field['field_name']}",
                        help="Comma-separated list of alternative names",
                        on_change=lambda fn=field['field_name']: save_field_to_database(
                            conn,
                            st.session_state.current_snowspace_id,
                            fn,
                            {'SYNONYMS': st.session_state[f"syn_{fn}"], 'CONFIDENCE_SCORE': 1.0}
                        )
                    )
                
                with col2:
                    st.write("**Metadata:**")
                    
                    # Editable field category
                    category = st.selectbox(
                        "Category:",
                        options=['DIMENSION', 'FACT', 'TIME_DIMENSION'],
                        index=['DIMENSION', 'FACT', 'TIME_DIMENSION'].index(field['field_category']),
                        key=f"cat_{field['field_name']}",
                        on_change=lambda fn=field['field_name']: save_field_to_database(
                            conn,
                            st.session_state.current_snowspace_id,
                            fn,
                            {'FIELD_CATEGORY': st.session_state[f"cat_{fn}"], 'CONFIDENCE_SCORE': 1.0}
                        )
                    )
                    
                    # Editable required flag
                    is_required = st.checkbox(
                        "Required",
                        value=field['is_required'],
                        key=f"req_{field['field_name']}",
                        on_change=lambda fn=field['field_name']: save_field_to_database(
                            conn,
                            st.session_state.current_snowspace_id,
                            fn,
                            {'IS_REQUIRED': st.session_state[f"req_{fn}"]}
                        )
                    )
                    
                    st.write(f"Type: {field['data_type']}")
                    
                    # Check if field was manually edited
                    current_confidence = field.get('confidence_score', 0.9)
                    if field.get('additional_context') and current_confidence < 1.0:
                        field['confidence_score'] = 1.0
                    
                    st.write(f"Confidence: {field.get('confidence_score', 0.9):.0%}")
                    st.write(f"AI Used: {'Yes 🤖' if field.get('used_ai') else 'No ⚡'}")
                    
                    st.write("**Sample Values:**")
                    st.code(field['sample_values'])
    
    # Navigation buttons
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("⬅️ Back to Snowspace Config", type="secondary"):
            st.switch_page("pages/0_Homepage.py")
    
    with col3:
        if st.button("Continue to Contributors ➡️", type="primary"):
            st.switch_page("pages/2_Contributors_Fields.py")