"""
Final SQL Generation and Data Sharing
=====================================
Generates final SQL from approved transformations and enables data sharing
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure page
st.set_page_config(
    page_title="Final SQL & Sharing",
    page_icon="📝",
    layout="wide"
)

st.title("📝 Final SQL Generation")
st.markdown("**Review and execute your transformation SQL**")

# Initialize session state if needed
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []

# ============= HELPER FUNCTIONS =============

def log_message(message: str, level: str = "info"):
    """Add message to debug logs for troubleshooting"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.debug_logs.append({
        'time': timestamp,
        'level': level,
        'message': message
    })

def load_approved_transformations(
    session: snowpark.Session,
    mapping_id: str
) -> pd.DataFrame:
    """Load only approved transformations from database"""
    try:
        query = f"""
        SELECT
            m.SOURCE_FIELD,
            m.TARGET_FIELD,
            m.TRANSFORMATION_SQL,
            m.TRANSFORMATION_CONFIDENCE,
            m.IS_MANUALLY_EDITED,
            m.MODEL_USED,
            m.EXPLANATION,
            f.FIELD_CATEGORY,
            f.DATA_TYPE,
            f.IS_REQUIRED
        FROM UNIFIEDSNOWSPACE_CONTRIBUTOR.SNOWSPACE.CONTRIBUTOR_FIELD_MAPPINGS m
        JOIN {st.session_state.selected_snowspace_view} f
            ON m.TARGET_FIELD = f.FIELD_NAME
        WHERE m.MAPPING_ID = '{mapping_id}'
            AND m.IS_ACTIVE = TRUE
            AND m.IS_APPROVED = TRUE
        ORDER BY m.TARGET_FIELD
        """
        
        return session.sql(query).to_pandas()
        
    except Exception as e:
        log_message(f"Error loading approved transformations: {str(e)}", level="error")
        return pd.DataFrame()

def generate_final_sql(
    snowspace_id: str,
    contributor_table: str,
    approved_transformations: pd.DataFrame
) -> str:
    """Generate complete SQL SELECT statement for creating transformed view"""
    if approved_transformations.empty:
        return "-- No approved transformations found"
    
    select_expressions = []
    
    for _, row in approved_transformations.iterrows():
        source_field = row['SOURCE_FIELD']
        target_field = row['TARGET_FIELD']
        transformation_sql = row['TRANSFORMATION_SQL']
        
        # Build expression
        if transformation_sql and transformation_sql != source_field:
            expression = f"    {transformation_sql} AS {target_field}"
        else:
            expression = f"    {source_field} AS {target_field}"
        
        select_expressions.append(expression)
    
    if not select_expressions:
        return "-- No fields to include in the view"
    
    # Build complete SQL
    select_clause = ',\n'.join(select_expressions)
    
    sql = f"""CREATE OR REPLACE VIEW {snowspace_id}_TRANSFORMED AS
SELECT
{select_clause}
FROM {contributor_table}"""
    
    return sql

def preview_sql_results(
    session: snowpark.Session,
    sql: str,
    limit: int = 20
) -> pd.DataFrame:
    """Preview the results of the generated SQL"""
    try:
        # Extract just the SELECT statement from the CREATE VIEW
        select_start = sql.find("SELECT")
        if select_start == -1:
            return pd.DataFrame()
        
        select_sql = sql[select_start:]
        
        # Add LIMIT for preview
        preview_sql = f"{select_sql} LIMIT {limit}"
        
        # Run the query
        return session.sql(preview_sql).to_pandas()
        
    except Exception as e:
        log_message(f"Error previewing SQL: {str(e)}", level="error")
        return pd.DataFrame()

# ============= MAIN UI =============

# Check if we have the required session state
if not st.session_state.get('selected_mapping'):
    st.error("❌ No mapping configuration found")
    st.info("Please select a mapping configuration from the Field Transformer page first.")
    if st.button("← Go to Field Transformer", type="primary"):
        st.switch_page("pages/02_Field_Transformer.py")
    st.stop()

# Connect to Snowflake
try:
    from snowflake.snowpark.context import get_active_session
    conn = get_active_session()
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")
    st.stop()

# Get mapping details from session state
mapping = st.session_state.selected_mapping
snowspace_view_path = st.session_state.get('selected_snowspace_view')

if not snowspace_view_path:
    st.error("Missing Snowspace view path. Please go back to Field Transformer.")
    if st.button("← Go to Field Transformer", type="primary"):
        st.switch_page("pages/02_Field_Transformer.py")
    st.stop()

# Load approved transformations
with st.spinner("Loading approved transformations..."):
    approved_transformations = load_approved_transformations(conn, mapping['MAPPING_ID'])

if approved_transformations.empty:
    st.warning("⚠️ No approved transformations found")
    st.info("Please go back to the Field Transformer and approve at least one transformation.")
    if st.button("← Go to Field Transformer", type="primary"):
        st.switch_page("pages/02_Field_Transformer.py")
    st.stop()

# Display mapping information
st.info(f"""
**Mapping Configuration**
- Snowspace: `{mapping['SNOWSPACE_ID']}`
- Source Table: `{mapping['CONTRIBUTOR_TABLE']}`
- Approved Fields: `{len(approved_transformations)}`
""")

# Generate SQL
with st.spinner("Generating SQL..."):
    final_sql = generate_final_sql(
        mapping['SNOWSPACE_ID'],
        mapping['CONTRIBUTOR_TABLE'],
        approved_transformations
    )

# Display SQL
st.subheader("Generated SQL")
st.code(final_sql, language='sql')

# Action buttons
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📋 Copy SQL", use_container_width=True):
        # Using a workaround since st.clipboard isn't available
        st.info("SQL copied! (Note: Use Ctrl+A and Ctrl+C to copy from the code block above)")

with col2:
    if st.button("← Back to Transformer", use_container_width=True):
        st.switch_page("pages/02_Field_Transformer.py")

with col3:
    preview_button = st.button("🔍 Preview Results", type="primary", use_container_width=True)

# Preview section
if preview_button:
    st.divider()
    st.subheader("Preview Results")
    
    with st.spinner("Running query..."):
        preview_df = preview_sql_results(conn, final_sql)
        
        if not preview_df.empty:
            st.success(f"✅ Query executed successfully! Showing first {len(preview_df)} rows.")
            
            # Display dataframe with some formatting
            st.dataframe(
                preview_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Show some stats
            st.caption(f"Preview shows {len(preview_df)} rows × {len(preview_df.columns)} columns")
        else:
            st.error("Failed to execute query. Check the SQL syntax or permissions.")

# Future: Data Sharing Controls will go here
st.divider()
st.subheader("🔒 Data Sharing")
st.info("Data sharing controls will be available in a future release. For now, you can manually create a Secure Share using the generated SQL view.")

# Sidebar with summary stats
with st.sidebar:
    st.header("📊 Transformation Summary")
    
    if not approved_transformations.empty:
        # Count by confidence level
        high_conf = len(approved_transformations[approved_transformations['TRANSFORMATION_CONFIDENCE'] >= 0.9])
        medium_conf = len(approved_transformations[
            (approved_transformations['TRANSFORMATION_CONFIDENCE'] >= 0.7) & 
            (approved_transformations['TRANSFORMATION_CONFIDENCE'] < 0.9)
        ])
        low_conf = len(approved_transformations[approved_transformations['TRANSFORMATION_CONFIDENCE'] < 0.7])
        
        # Count by type
        manual_edits = len(approved_transformations[approved_transformations['IS_MANUALLY_EDITED'] == True])
        
        st.metric("Total Approved Fields", len(approved_transformations))
        st.metric("High Confidence", high_conf, help="90%+ confidence")
        st.metric("Medium Confidence", medium_conf, help="70-89% confidence")
        st.metric("Low Confidence", low_conf, help="Below 70% confidence")
        st.metric("Manual Edits", manual_edits, help="Manually edited transformations")
    
    # Debug logs (minimal version)
    if st.session_state.debug_logs:
        with st.expander("📜 Debug Logs", expanded=False):
            for log in reversed(st.session_state.debug_logs[-20:]):  # Last 20 logs
                if log['level'] == 'error':
                    st.error(f"[{log['time']}] {log['message']}")
                elif log['level'] == 'warning':
                    st.warning(f"[{log['time']}] {log['message']}")
                else:
                    st.info(f"[{log['time']}] {log['message']}")