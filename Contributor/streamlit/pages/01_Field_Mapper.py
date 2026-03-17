"""
AI-Powered Field Mapping
========================
Contributor-side application for mapping raw data to predefined schemas
with AI-assisted field suggestions

This application provides intelligent field mapping from contributor tables
to OEM-defined schemas, preparing data for transformation in the next phase.
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
    page_title="Field Mapper",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 AI-Powered Field Mapping")

# Check if we have the required session state from Connect page
if not st.session_state.get('selected_schema_id') or not st.session_state.get('contributor_table'):
    st.error("❌ No schema or table selected. Please go back to Connect Snowspaces.")
    if st.button("← Back to Connect Snowspaces"):
        st.switch_page("pages/00_Connect_Snowspaces.py")
    st.stop()

# Also need the snowspace path to query field definitions
if not st.session_state.get('selected_snowspace_path'):
    st.error("❌ No Snowspace view path found. Please go back to Connect Snowspaces.")
    if st.button("← Back to Connect Snowspaces"):
        st.switch_page("pages/00_Connect_Snowspaces.py")
    st.stop()

# Initialize session state - using the EXACT same structure as original
if 'mapping_config' not in st.session_state:
    st.session_state.mapping_config = {
        'snowspace_id': st.session_state.get('selected_schema_id'),  # Using snowspace_id now
        'contributor_table': st.session_state.get('contributor_table'),
        'snowspace_view': st.session_state.get('selected_snowspace_path'),  # Store the view path
        'field_mappings': {},
        'current_phase': 'mapping',  # Start directly at mapping phase since selection is done
        'existing_mapping_loaded': False
    }

if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []

if 'cortex_calls' not in st.session_state:
    st.session_state.cortex_calls = 0

# ============= HELPER FUNCTIONS =============

def log_message(message: str, level: str = "info"):
    """Add message to debug logs for troubleshooting"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.debug_logs.append({
        'time': timestamp,
        'level': level,
        'message': message
    })

def call_cortex_complete(session: snowpark.Session, model: str, prompt: str) -> Optional[str]:
    """
    Call Snowflake Cortex COMPLETE function for AI-powered analysis
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

# ============= DATA ACCESS FUNCTIONS =============

def get_schema_fields(session: snowpark.Session, snowspace_view: str) -> pd.DataFrame:
    """Get all field definitions from the Snowspace view"""
    try:
        # Query the Snowspace view directly for field definitions
        query = f"""
        SELECT DISTINCT
            FIELD_NAME,
            DATA_TYPE,
            IS_REQUIRED,
            FIELD_DESCRIPTION as DESCRIPTION,
            SAMPLE_VALUES,
            SYNONYMS,
            ADDITIONAL_CONTEXT
        FROM {snowspace_view}
        ORDER BY FIELD_NAME
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        log_message(f"Error fetching schema fields from {snowspace_view}: {str(e)}", level="error")
        return pd.DataFrame()

def get_table_columns(session: snowpark.Session, table_name: str) -> List[Dict]:
    """Get column information for a table using INFORMATION_SCHEMA"""
    try:
        parts = table_name.split('.')
        if len(parts) != 3:
            log_message(f"Invalid table name format: {table_name}. Expected: DATABASE.SCHEMA.TABLE", level="error")
            return []
        
        db, schema, table = parts
        
        query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{db}'
        AND TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        
        results = session.sql(query).collect()
        return [{'COLUMN_NAME': row['COLUMN_NAME'],
                 'DATA_TYPE': row['DATA_TYPE'],
                 'IS_NULLABLE': row['IS_NULLABLE']} for row in results]
    except Exception as e:
        log_message(f"Error fetching columns: {str(e)}", level="error")
        return []

def get_sample_data(session: snowpark.Session, table_name: str, limit: int = 1000) -> pd.DataFrame:
    """Get sample data from a table for analysis"""
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return session.sql(query).to_pandas()
    except Exception as e:
        log_message(f"Error fetching sample data: {str(e)}", level="error")
        return pd.DataFrame()

# ============= FIELD MAPPING FUNCTIONS =============

def suggest_field_mapping(
    session: snowpark.Session,
    schema_fields: pd.DataFrame,
    contributor_columns: List[Dict],
    contributor_sample_data: pd.DataFrame,
    cortex_model: str = 'snowflake-arctic',
    progress_bar = None,
    progress_text = None
) -> Dict[str, Dict]:
    """
    AI-powered field mapping with multi-stage matching:
    1. Exact name matching (100% confidence)
    2. Normalized matching - ignore case/underscores (95% confidence)
    3. Synonym matching from schema (90% confidence)
    4. Partial matching with context (50-65% confidence)
    5. AI-powered semantic matching for remaining fields
    
    Returns mapping suggestions with confidence scores and match types
    """
    mappings = {}
    assigned_contributor_fields = set()
    
    # Initialize all target fields
    for _, schema_field in schema_fields.iterrows():
        target_name = schema_field['FIELD_NAME']
        mappings[target_name] = {
            'suggested_source': None,
            'confidence': 0.0,
            'reason': 'No match found',
            'match_type': ''
        }
    
    # Build all possible matches with scores
    all_matches = []
    total_fields = len(schema_fields)
    
    # Score all possible field combinations
    for idx, (_, schema_field) in enumerate(schema_fields.iterrows()):
        target_name = schema_field['FIELD_NAME']
        
        if progress_bar:
            progress_bar.progress((idx + 1) / total_fields)
        if progress_text:
            progress_text.text(f"Analyzing field {idx + 1}/{total_fields}: {target_name}")
        
        target_synonyms = schema_field['SYNONYMS'].split(',') if pd.notna(schema_field['SYNONYMS']) else []
        target_synonyms = [s.strip().lower() for s in target_synonyms]
        
        log_message(f"Matching target field: {target_name}")
        
        for contributor_col in contributor_columns:
            col_name = contributor_col['COLUMN_NAME']
            col_type = contributor_col['DATA_TYPE']
            
            # Get sample data if available
            sample_values = []
            if col_name in contributor_sample_data.columns:
                sample_values = contributor_sample_data[col_name].dropna().unique()[:5].tolist()
            
            # Initialize scoring
            score = 0.0
            reason = ""
            match_type = ""
            
            # 1. Exact name match (case-insensitive)
            if col_name.lower() == target_name.lower():
                score = 1.0
                reason = "Exact name match"
                match_type = "="
            
            # 2. Normalized name match (remove underscores, hyphens, spaces)
            elif col_name.lower().replace('_', '').replace('-', '').replace(' ', '') == \
                 target_name.lower().replace('_', '').replace('-', '').replace(' ', ''):
                score = 0.95
                reason = "Normalized name match"
                match_type = "≈"
            
            # 3. Synonym match
            elif col_name.lower() in target_synonyms:
                score = 0.9
                reason = "Synonym match"
                match_type = "≈"
            
            # 4. Partial match scoring
            else:
                # Split into component words
                target_parts = target_name.lower().replace('_', ' ').replace('-', ' ').split()
                col_parts = col_name.lower().replace('_', ' ').replace('-', ' ').split()
                
                # Find common parts
                common_parts = set(target_parts).intersection(set(col_parts))
                
                # All parts match
                if len(target_parts) > 1 and all(part in col_parts for part in target_parts):
                    score = 0.65
                    reason = f"All parts match: {', '.join(target_parts)}"
                    match_type = "~"
                elif common_parts:
                    # Filter out short/common words
                    important_parts = [p for p in common_parts if len(p) > 3]
                    
                    if important_parts:
                        # Special case: weak match for single common words
                        if len(important_parts) == 1 and important_parts[0] in ['date', 'time', 'name', 'code', 'type', 'status']:
                            score = 0.5
                            reason = f"Weak match: only '{important_parts[0]}' in common"
                            match_type = "~"
                        else:
                            part_score = len(important_parts) / max(len(target_parts), len(col_parts))
                            if part_score > 0.5:
                                score = 0.55 + (part_score * 0.1)  # 0.55-0.65 range
                                reason = f"Partial match: {', '.join(important_parts)}"
                                match_type = "~"
                
                # Limited semantic equivalents (only very generic cases)
                semantic_equivalents = {
                    'customer': ['client', 'cust'],
                    'states': ['state', 'province']
                }
                
                # Check semantic matches with required context
                for target_part in target_parts:
                    for col_part in col_parts:
                        if target_part in semantic_equivalents:
                            if col_part in semantic_equivalents[target_part]:
                                # Require additional context for semantic match
                                other_target_parts = [p for p in target_parts if p != target_part]
                                if any(tp in col_parts for tp in other_target_parts):
                                    score = max(score, 0.7)
                                    reason = f"Semantic match with context: {target_part} ≈ {col_part}"
                                    match_type = "≈"
                                    break
            
            # Store all potential matches
            if score > 0:
                # Apply a small penalty for system/technical-looking fields
                # This helps prefer business fields over technical fields when scores are similar
                technical_indicators = ['sys', 'system', 'import', 'export', 'etl', 'load', 'extract']
                for indicator in technical_indicators:
                    if indicator in col_name.lower() and indicator not in target_name.lower():
                        score = score * 0.9  # 10% penalty
                        break
                
                all_matches.append({
                    'target': target_name,
                    'source': col_name,
                    'score': score,
                    'reason': reason,
                    'match_type': match_type
                })
                
                log_message(f"  Potential match: {col_name} (score: {score:.2f}, reason: {reason})")
    
    # Sort matches by score (highest first) and assign optimally
    all_matches.sort(key=lambda x: x['score'], reverse=True)
    
    # Assign matches ensuring each contributor field is used only once
    for match in all_matches:
        target = match['target']
        source = match['source']
        
        # Skip if target already has a better match or source is already used
        if mappings[target]['confidence'] >= match['score']:
            continue
        if source in assigned_contributor_fields:
            continue
        
        # Assign this match
        mappings[target] = {
            'suggested_source': source,
            'confidence': match['score'],
            'reason': match['reason'],
            'match_type': match['match_type']
        }
        assigned_contributor_fields.add(source)
        
        log_message(f"  ✓ Assigned: {source} → {target} ({match['score']:.0%})")
    
    # AI phase for fields with confidence < 95%
    unmatched_targets = [k for k, v in mappings.items() if v['confidence'] < 0.95]
    all_contributor_fields = [col['COLUMN_NAME'] for col in contributor_columns]
    
    if unmatched_targets:
        log_message(f"Using AI for {len(unmatched_targets)} fields with low confidence")
        
        if progress_text:
            progress_text.text(f"🤖 Using AI to improve {len(unmatched_targets)} low-confidence matches...")
        
        # Process in batches for efficiency
        for i in range(0, len(unmatched_targets), 5):
            batch_targets = unmatched_targets[i:i+5]
            batch_num = (i // 5) + 1
            total_batches = (len(unmatched_targets) + 4) // 5
            
            if progress_text:
                progress_text.text(f"🤖 AI analysis batch {batch_num}/{total_batches}...")
            
            # Build target info with current matches
            target_info_list = []
            for target_name in batch_targets:
                target_info = schema_fields[schema_fields['FIELD_NAME'] == target_name].iloc[0]
                current_match = mappings[target_name].get('suggested_source', 'None')
                current_conf = mappings[target_name].get('confidence', 0)
                
                target_info_list.append(
                    f"- {target_name}: {target_info['DESCRIPTION']} "
                    f"(type: {target_info['DATA_TYPE']}, samples: {target_info['SAMPLE_VALUES'][:50]}, "
                    f"current match: {current_match} at {current_conf:.0%})"
                )
            
            # Build comprehensive prompt
            prompt = f"""Match these target fields to the best contributor fields. Consider field names, descriptions, and sample values.
You can suggest a different match than the current one if you find a better option.

Target Fields:
{chr(10).join(target_info_list)}

Available Contributor Fields (with sample data):
"""
            
            # Add contributor field info with samples and current assignments
            contributor_field_info = []
            for contrib_field in all_contributor_fields[:40]:
                if contrib_field in contributor_sample_data.columns:
                    samples = contributor_sample_data[contrib_field].dropna().unique()[:3]
                    sample_str = ', '.join([str(s)[:20] for s in samples])
                    if contrib_field in assigned_contributor_fields:
                        # Find which field this is assigned to
                        assigned_to_list = [k for k, v in mappings.items() if v.get('suggested_source') == contrib_field]
                        if assigned_to_list:
                            assigned_to = assigned_to_list[0]
                            contributor_field_info.append(f"- {contrib_field}: [{sample_str}] (currently assigned to {assigned_to})")
                        else:
                            contributor_field_info.append(f"- {contrib_field}: [{sample_str}]")
                    else:
                        contributor_field_info.append(f"- {contrib_field}: [{sample_str}]")
                else:
                    contributor_field_info.append(f"- {contrib_field}")
            
            prompt += chr(10).join(contributor_field_info)
            
            prompt += """

Consider the following when matching:
- Field names and their semantic meaning
- Data types and formats
- Sample values and patterns
- Current assignments (avoid breaking good matches unless you find significantly better ones)

You can suggest reassigning a field if you find a much better match (confidence must be significantly higher than current).

Return JSON array only:
[
  {"target": "field1", "match": "contributor_field_or_null", "confidence": 0.0-1.0, "reason": "explanation"},
  ...
]"""
            
            response = call_cortex_complete(session, cortex_model, prompt)
            
            if response:
                try:
                    # Parse AI response
                    json_match = re.search(r'\[.*\]', response, re.DOTALL)
                    if json_match:
                        results = json.loads(json_match.group())
                    else:
                        results = json.loads(response)
                    
                    for result in results:
                        target = result.get('target')
                        suggested = result.get('match')
                        confidence = result.get('confidence', 0)
                        
                        if target in mappings and suggested and suggested != 'null':
                            current_confidence = mappings[target].get('confidence', 0)
                            
                            # Handle reassignments
                            if suggested in assigned_contributor_fields:
                                # Require only slightly better confidence to steal assignment
                                if confidence > current_confidence + 0.05:  # Lowered from 0.1 to 0.05
                                    # Unassign from previous target
                                    for k, v in mappings.items():
                                        if v.get('suggested_source') == suggested and k != target:
                                            v['suggested_source'] = None
                                            v['confidence'] = 0.0
                                            v['reason'] = 'Reassigned to better match'
                                            break
                                    
                                    # Assign to new target
                                    mappings[target] = {
                                        'suggested_source': suggested,
                                        'confidence': confidence,
                                        'reason': result.get('reason', 'AI suggestion'),
                                        'match_type': '🤖'
                                    }
                                    log_message(f"  🤖 AI reassigned: {suggested} → {target} ({confidence:.0%})")
                            else:
                                # Field is available
                                if confidence > current_confidence:
                                    mappings[target] = {
                                        'suggested_source': suggested,
                                        'confidence': confidence,
                                        'reason': result.get('reason', 'AI suggestion'),
                                        'match_type': '🤖'
                                    }
                                    assigned_contributor_fields.add(suggested)
                                    log_message(f"  🤖 AI match: {suggested} → {target} ({confidence:.0%})")
                
                except Exception as e:
                    log_message(f"Error parsing AI response: {str(e)}", level="warning")
    
    if progress_bar:
        progress_bar.empty()
    if progress_text:
        progress_text.empty()
    
    return mappings

def load_existing_mappings(
    session: snowpark.Session,
    mapping_id: str
) -> Dict[str, Dict]:
    """Load existing mappings from database"""
    try:
        query = f"""
        SELECT 
            SOURCE_FIELD,
            TARGET_FIELD,
            MAPPING_CONFIDENCE,
            MAPPING_TYPE,
            MAPPING_REASON
        FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS
        WHERE MAPPING_ID = '{mapping_id}'
        AND IS_ACTIVE = TRUE
        """
        results = session.sql(query).collect()
        
        mappings = {}
        for row in results:
            mappings[row['TARGET_FIELD']] = {
                'suggested_source': row['SOURCE_FIELD'],
                'confidence': row['MAPPING_CONFIDENCE'],
                'reason': row['MAPPING_REASON'],
                'match_type': row['MAPPING_TYPE']
            }
        
        return mappings
    except Exception as e:
        log_message(f"Error loading existing mappings: {str(e)}", level="error")
        return {}

def save_mappings_to_table(
    session: snowpark.Session,
    mapping_id: str,
    snowspace_id: str,
    contributor_table: str,
    field_mappings: Dict
) -> bool:
    """Save all field mappings to the CONTRIBUTOR_FIELD_MAPPINGS table"""
    try:
        # Get current user
        current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
        
        # IMPORTANT: Only update mapping-related fields, NEVER touch transformation fields
        for target_field, mapping_info in field_mappings.items():
            source_field = mapping_info.get('selected_source')
            if source_field and source_field != '-- Not Mapped --':  # Only save mapped fields
                # Update ONLY the mapping fields, preserve all transformation data
                update_query = f"""
                UPDATE UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS
                SET SOURCE_FIELD = '{source_field}',
                    MAPPING_CONFIDENCE = {float(mapping_info.get('confidence', 0))},
                    MAPPING_TYPE = '{mapping_info.get('match_type', 'manual')}',
                    MAPPING_REASON = '{mapping_info.get('reason', 'User selected').replace("'", "''")}',
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE MAPPING_ID = '{mapping_id}' 
                AND TARGET_FIELD = '{target_field}'
                """
                
                # Check if record exists
                exists_check = session.sql(f"""
                    SELECT COUNT(*) as cnt 
                    FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS 
                    WHERE MAPPING_ID = '{mapping_id}' 
                    AND TARGET_FIELD = '{target_field}'
                """).collect()[0]['CNT']
                
                if exists_check > 0:
                    # Update existing record
                    session.sql(update_query).collect()
                else:
                    # Insert new record only if it doesn't exist
                    insert_query = f"""
                    INSERT INTO UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS (
                        MAPPING_ID, SNOWSPACE_ID, CONTRIBUTOR_TABLE,
                        SOURCE_FIELD, TARGET_FIELD, MAPPING_CONFIDENCE,
                        MAPPING_TYPE, MAPPING_REASON, IS_ACTIVE,
                        CREATED_BY, CREATED_AT, UPDATED_AT
                    ) VALUES (
                        '{mapping_id}', '{snowspace_id}', '{contributor_table}',
                        '{source_field}', '{target_field}', {float(mapping_info.get('confidence', 0))},
                        '{mapping_info.get('match_type', 'manual')}', 
                        '{mapping_info.get('reason', 'User selected').replace("'", "''")}',
                        TRUE, '{current_user}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    )
                    """
                    session.sql(insert_query).collect()
        
        log_message(f"Updated field mappings for {mapping_id} - transformation data preserved")
        return True
            
    except Exception as e:
        log_message(f"Error saving mappings: {str(e)}", level="error")
        return False

# ============= MAIN UI =============

# Connect to Snowflake
try:
    from snowflake.snowpark.context import get_active_session
    conn = get_active_session()
    st.session_state.connection = conn
    log_message("Connected to Snowflake successfully")
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")
    st.stop()

# Check for existing mapping
mapping_id = f"{st.session_state.mapping_config['snowspace_id']}-{st.session_state.mapping_config['contributor_table']}"
existing_mappings = load_existing_mappings(conn, mapping_id)

if existing_mappings:
    st.session_state.mapping_config['existing_mapping_loaded'] = True

# Auto-run analysis on page load if not already done
if 'auto_analyzed' not in st.session_state:
    st.session_state.auto_analyzed = True
    # Get values from session state
    sample_size = st.session_state.get('sample_size', 1000)
    cortex_model = st.session_state.get('cortex_model', 'snowflake-arctic')
    
    with st.spinner("Analyzing fields and suggesting mappings..."):
        import time
        start_time = time.time()
        # Get schema fields from the Snowspace view
        schema_start = time.time()
        schema_fields = get_schema_fields(conn, st.session_state.mapping_config['snowspace_view'])
        log_message(f"Schema fields loaded in {time.time() - schema_start:.2f}s")
        
        # Get contributor columns and sample data
        contrib_start = time.time()
        contributor_cols = get_table_columns(conn, st.session_state.mapping_config['contributor_table'])
        log_message(f"Contributor columns loaded in {time.time() - contrib_start:.2f}s")
        
        sample_start = time.time()
        sample_data = get_sample_data(conn, st.session_state.mapping_config['contributor_table'], sample_size)
        log_message(f"Sample data loaded in {time.time() - sample_start:.2f}s")
        
        # Add progress bar for mapping
        progress_container = st.container()
        with progress_container:
            progress_bar = st.progress(0)
            progress_text = st.empty()
        
        # Get AI suggestions
        mapping_start = time.time()
        mappings = suggest_field_mapping(
            conn,
            schema_fields,
            contributor_cols,
            sample_data,
            cortex_model,
            progress_bar,
            progress_text
        )
        log_message(f"Field mapping completed in {time.time() - mapping_start:.2f}s")
        
        # Clear progress indicators
        progress_bar.empty()
        progress_text.empty()
        
        # If existing mappings were found, merge with AI suggestions
        if st.session_state.mapping_config['existing_mapping_loaded'] and existing_mappings:
            # Preserve existing mappings where they exist
            for target_field, existing_mapping in existing_mappings.items():
                if target_field in mappings:
                    mappings[target_field] = existing_mapping
            log_message("Loaded existing mappings and merged with new analysis")
        
        # Store results
        st.session_state.mapping_config['field_mappings'] = mappings
        st.session_state.mapping_config['schema_fields'] = schema_fields
        st.session_state.mapping_config['contributor_columns'] = contributor_cols
        st.session_state.mapping_config['contributor_sample_data'] = sample_data
        st.session_state.mapping_config['mapping_id'] = mapping_id
        
        # Count successful mappings
        mapped_count = sum(1 for m in mappings.values() if m.get('suggested_source'))
        high_confidence = sum(1 for m in mappings.values() if m.get('confidence', 0) > 0.8)
        
        total_time = time.time() - start_time
        log_message(f"Total analysis time: {total_time:.2f}s")
        log_message(f"Mapped {mapped_count}/{len(mappings)} fields ({high_confidence} with high confidence)")

# Display mapping interface if we have results
if st.session_state.mapping_config.get('field_mappings'):
    # Get data from session state
    mappings = st.session_state.mapping_config.get('field_mappings', {})
    schema_fields = st.session_state.mapping_config.get('schema_fields', pd.DataFrame())
    contributor_cols = st.session_state.mapping_config.get('contributor_columns', [])
    
    # Create contributor field options
    contributor_field_names = ['-- Not Mapped --'] + [col['COLUMN_NAME'] for col in contributor_cols]
    
    # Track mapped fields to prevent duplicates
    mapped_contributor_fields = set()
    
    # Column headers
    col1, col2, col3 = st.columns([2.5, 2.5, 3])
    col1.markdown("**Target Field**")
    col2.markdown("**Contributor Field**")
    col3.markdown("**Match Info**")
    st.divider()
    
    # Display mapping interface
    for _, target_field in schema_fields.iterrows():
        target_name = target_field['FIELD_NAME']
        is_required = target_field['IS_REQUIRED']
        
        # Get mapping info
        mapping_info = mappings.get(target_name, {})
        suggested_source = mapping_info.get('suggested_source')
        confidence = mapping_info.get('confidence', 0)
        
        col1, col2, col3 = st.columns([2.5, 2.5, 3])
        
        with col1:
            # Display target field with description
            display_name = f"{target_name} *" if is_required else target_name
            desc = target_field['DESCRIPTION'] if target_field['DESCRIPTION'] else ""
            st.markdown(f"**{display_name}:** <span style='color: gray; font-size: 0.9em;'>{desc}</span>", unsafe_allow_html=True)
        
        with col2:
            # Contributor field dropdown
            default_index = 0
            if suggested_source and suggested_source in contributor_field_names:
                default_index = contributor_field_names.index(suggested_source)
            
            current_selection = st.selectbox(
                "Source",
                options=contributor_field_names,
                index=default_index,
                key=f"source_{target_name}",
                label_visibility="collapsed"
            )
            
            # Update mapping
            if current_selection != '-- Not Mapped --':
                # Check for duplicate mappings
                already_mapped_to = None
                for other_target, other_mapping in mappings.items():
                    if other_target != target_name and other_mapping.get('selected_source') == current_selection:
                        already_mapped_to = other_target
                        break
                
                if already_mapped_to:
                    st.warning(f"⚠️ {current_selection} is already mapped to {already_mapped_to}")
                else:
                    mappings[target_name]['selected_source'] = current_selection
                    mapped_contributor_fields.add(current_selection)
            else:
                mappings[target_name]['selected_source'] = None
        
        with col3:
            # Match info display
            match_type = mapping_info.get('match_type', '')
            
            # Confidence indicator
            if confidence >= 0.9:
                conf_emoji = "🟢"
            elif confidence >= 0.7:
                conf_emoji = "🟡"
            else:
                conf_emoji = "🔴"
            
            # Display match info
            reason = mapping_info.get('reason', '')
            if match_type and reason:
                st.markdown(f"{match_type} {conf_emoji} **{confidence:.0%}** - {reason}")
            elif match_type:
                st.markdown(f"{match_type} {conf_emoji} **{confidence:.0%}**")
            else:
                st.markdown(f"{conf_emoji} **{confidence:.0%}**")
    
    # Validation checks
    unmapped_required = [
        field['FIELD_NAME'] for _, field in schema_fields.iterrows()
        if field['IS_REQUIRED'] and not mappings.get(field['FIELD_NAME'], {}).get('selected_source')
    ]
    
    if unmapped_required:
        st.warning(f"⚠️ Required fields not mapped: {', '.join(unmapped_required)}")
    
    # Check for duplicate mappings
    all_selections = [m.get('selected_source') for m in mappings.values() if m.get('selected_source')]
    duplicates = [x for x in set(all_selections) if all_selections.count(x) > 1]
    
    if duplicates:
        st.error(f"❌ Duplicate mappings found: {', '.join(duplicates)}")
    
    # Navigation and Save buttons
    st.divider()
    col1, col2, col3 = st.columns([1, 1, 1])
    
    # Back button
    with col1:
        if st.button("← Back to Connect", type="secondary", use_container_width=True):
            st.switch_page("pages/00_Connect_Snowspaces.py")
    
    # Save button
    with col2:
        if st.button("💾 Save Mappings", type="secondary", use_container_width=True):
            if save_mappings_to_table(
                conn,
                mapping_id,
                st.session_state.mapping_config['snowspace_id'],
                st.session_state.mapping_config['contributor_table'],
                mappings
            ):
                # Show appropriate success message based on whether it's new or updated
                if st.session_state.mapping_config.get('existing_mapping_loaded'):
                    st.info("ℹ️ Updated existing mapping configuration - transformation settings preserved")
                else:
                    st.success("✅ Created new mapping configuration!")
            else:
                st.error("❌ Failed to save mappings")
    
    # Continue button
    with col3:
        # Disable navigation if there are validation errors
        nav_disabled = bool(duplicates) or bool(unmapped_required)
        
        if st.button(
            "Save & Continue →", 
            type="primary", 
            use_container_width=True, 
            disabled=nav_disabled,
            help="All required fields must be mapped and no duplicates allowed" if nav_disabled else "Continue to transformation phase"
        ):
            # For new mappings, proceed directly
            if not st.session_state.mapping_config.get('existing_mapping_loaded'):
                # Proceed with save
                if save_mappings_to_table(
                    conn,
                    mapping_id,
                    st.session_state.mapping_config['snowspace_id'],
                    st.session_state.mapping_config['contributor_table'],
                    mappings
                ):
                    st.success("✅ Created new mapping configuration!")
                    
                    # Set session state for transformer
                    st.session_state['selected_mapping_id'] = mapping_id
                    st.session_state['selected_schema_id'] = st.session_state.mapping_config['snowspace_id']
                    st.session_state['contributor_table'] = st.session_state.mapping_config['contributor_table']
                    
                    # Give user a moment to see the success message
                    import time
                    time.sleep(1.5)
                    
                    # Navigate to transformer app
                    st.switch_page("pages/02_Field_Transformer.py")
                else:
                    st.error("❌ Failed to save mappings. Please try again.")
            else:
                # For existing mappings, show confirmation
                st.session_state['show_overwrite_confirm'] = True
    
    # Show confirmation dialog if needed
    if st.session_state.get('show_overwrite_confirm', False):
        st.warning("⚠️ You are about to overwrite existing mappings. Transformation settings will be preserved.")
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            if st.button("Yes, Continue", type="primary", key="confirm_yes"):
                # Create a progress container
                progress_container = st.container()
                with progress_container:
                    with st.spinner("Saving mappings..."):
                        # Proceed with save
                        save_success = save_mappings_to_table(
                            conn,
                            mapping_id,
                            st.session_state.mapping_config['snowspace_id'],
                            st.session_state.mapping_config['contributor_table'],
                            mappings
                        )
                    
                    if save_success:
                        st.success("✅ Updated existing mappings! Transformation settings preserved.")
                        
                        # Set session state for transformer
                        st.session_state['selected_mapping_id'] = mapping_id
                        st.session_state['selected_schema_id'] = st.session_state.mapping_config['snowspace_id']
                        st.session_state['contributor_table'] = st.session_state.mapping_config['contributor_table']
                        
                        # Clear the flag
                        del st.session_state['show_overwrite_confirm']
                        
                        # Show loading message
                        with st.spinner("Loading Field Transformer... This may take a moment as we prepare your transformations."):
                            time.sleep(1)  # Brief pause to ensure message is visible
                        
                        # Navigate to transformer app
                        st.switch_page("pages/02_Field_Transformer.py")
                    else:
                        st.error("❌ Failed to save mappings. Please try again.")
        with subcol2:
            if st.button("Cancel", key="confirm_cancel"):
                del st.session_state['show_overwrite_confirm']
                st.rerun()

# ============= SIDEBAR DEBUG CONSOLE =============
with st.sidebar:
    # Analyze button at the top
    if st.button("🤖 Analyze & Map Fields", type="primary", use_container_width=True):
        with st.spinner("Analyzing fields and suggesting mappings..."):
            import time
            start_time = time.time()
            # Get values from session state
            sample_size = st.session_state.get('sample_size', 1000)
            cortex_model = st.session_state.get('cortex_model', 'snowflake-arctic')
            
            # Get schema fields
            schema_start = time.time()
            schema_fields = get_schema_fields(conn, st.session_state.mapping_config['snowspace_view'])
            log_message(f"Schema fields loaded in {time.time() - schema_start:.2f}s")
            
            # Get contributor columns and sample data
            contrib_start = time.time()
            contributor_cols = get_table_columns(conn, st.session_state.mapping_config['contributor_table'])
            log_message(f"Contributor columns loaded in {time.time() - contrib_start:.2f}s")
            
            sample_start = time.time()
            sample_data = get_sample_data(conn, st.session_state.mapping_config['contributor_table'], sample_size)
            log_message(f"Sample data loaded in {time.time() - sample_start:.2f}s")
            
            # Add progress bar for mapping
            progress_container = st.container()
            with progress_container:
                progress_bar = st.progress(0)
                progress_text = st.empty()
            
            # Get AI suggestions
            mapping_start = time.time()
            mappings = suggest_field_mapping(
                conn,
                schema_fields,
                contributor_cols,
                sample_data,
                cortex_model,
                progress_bar,
                progress_text
            )
            log_message(f"Field mapping completed in {time.time() - mapping_start:.2f}s")
            
            # Clear progress indicators
            progress_bar.empty()
            progress_text.empty()
            
            # If existing mappings were found, merge with AI suggestions
            if st.session_state.mapping_config['existing_mapping_loaded'] and existing_mappings:
                # Preserve existing mappings where they exist
                for target_field, existing_mapping in existing_mappings.items():
                    if target_field in mappings:
                        mappings[target_field] = existing_mapping
                log_message("Loaded existing mappings and merged with new analysis")
            
            # Store results
            st.session_state.mapping_config['field_mappings'] = mappings
            st.session_state.mapping_config['schema_fields'] = schema_fields
            st.session_state.mapping_config['contributor_columns'] = contributor_cols
            st.session_state.mapping_config['contributor_sample_data'] = sample_data
            st.session_state.mapping_config['mapping_id'] = mapping_id
            
            # Count successful mappings
            mapped_count = sum(1 for m in mappings.values() if m.get('suggested_source'))
            high_confidence = sum(1 for m in mappings.values() if m.get('confidence', 0) > 0.8)
            
            total_time = time.time() - start_time
            log_message(f"Total analysis time: {total_time:.2f}s")
            log_message(f"Mapped {mapped_count}/{len(mappings)} fields ({high_confidence} with high confidence)")
            st.rerun()
    
    st.divider()
    
    # Session Details (collapsed by default)
    with st.expander("📋 Session Details", expanded=False):
        st.info(f"**Snowspace ID:** {st.session_state.mapping_config['snowspace_id']}")
        st.info(f"**Snowspace View:** {st.session_state.mapping_config['snowspace_view']}")
        st.info(f"**Table:** {st.session_state.mapping_config['contributor_table']}")
        
        # Existing mapping status
        if st.session_state.mapping_config.get('existing_mapping_loaded'):
            st.warning("⚠️ Found existing mapping configuration")
        
        # Stats
        if st.session_state.mapping_config.get('field_mappings'):
            mappings = st.session_state.mapping_config['field_mappings']
            total_fields = len(mappings)
            mapped = sum(1 for m in mappings.values() if m.get('selected_source'))
            st.metric("Fields Mapped", f"{mapped}/{total_fields}")
        
        st.metric("Cortex Calls", st.session_state.cortex_calls)
    
    # Analysis options (collapsed by default)
    with st.expander("⚙️ Analysis Options", expanded=False):
        st.session_state['sample_size'] = st.number_input(
            "Sample Size (rows)",
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="Number of rows to analyze. Larger samples give better results but take longer.",
            key="sidebar_sample_size"
        )
        
        st.session_state['cortex_model'] = st.selectbox(
            "Cortex Model",
            ["snowflake-arctic", "llama2-70b-chat", "mistral-7b"],
            help="Arctic is recommended for best results",
            key="sidebar_cortex_model"
        )
    
    # Debug logs
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