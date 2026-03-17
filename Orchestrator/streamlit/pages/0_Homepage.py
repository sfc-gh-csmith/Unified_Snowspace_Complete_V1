"""
Dashboard Page - View and manage Snowspaces
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime
import time
import json

# Configure page
st.set_page_config(
    page_title="Dashboard - Unified Snowspace",
    page_icon="🌌",
    layout="wide"
)

# Custom CSS for better styling - including the blue header and beveled metrics
st.markdown("""
<style>
.stMetric {
    background: white;
    padding: 1rem;
    border-radius: 10px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}
.stButton > button {
    border-radius: 5px;
    border: none;
    padding: 0.5rem 1rem;
    font-weight: 500;
}
.stButton > button[data-baseweb="button"][kind="primary"] {
    background: linear-gradient(90deg, #1f4e79 0%, #2d7dd2 100%);
}
</style>
""", unsafe_allow_html=True)

# --- Session State Initialization ---
if "current_step" not in st.session_state:
    st.session_state.current_step = "dashboard"
if "current_snowspace_id" not in st.session_state:
    st.session_state.current_snowspace_id = None
if "current_page" not in st.session_state:
    st.session_state.current_page = 0
if "debug_log" not in st.session_state:
    st.session_state.debug_log = []

# --- Connect to Snowflake ---
try:
    conn = snowpark.Session.builder.create()
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")
    st.stop()

# --- Helper Functions ---
def log_debug(category, message, data=None):
    """Centralized debug logging"""
    if "debug_log" not in st.session_state:
        st.session_state.debug_log = []
    
    timestamp = datetime.now().strftime('%H:%M:%S')
    log_entry = {
        "time": timestamp,
        "category": category,
        "message": message,
        "data": data
    }
    st.session_state.debug_log.append(log_entry)
    
    # Keep only last 30 entries
    if len(st.session_state.debug_log) > 30:
        st.session_state.debug_log = st.session_state.debug_log[-30:]

def get_current_account():
    """Get current Snowflake account identifier"""
    try:
        result = conn.sql("SELECT CURRENT_ACCOUNT()").collect()
        return result[0][0] if result else "UNKNOWN_ACCOUNT"
    except:
        return "UNKNOWN_ACCOUNT"

def get_accessible_databases():
    """Get databases accessible to the current user"""
    try:
        # Use the SNOWFLAKE account database INFORMATION_SCHEMA which shows all databases
        query = """
        SELECT DISTINCT DATABASE_NAME 
        FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES
        WHERE DATABASE_NAME NOT IN ('SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA')
        ORDER BY DATABASE_NAME
        """
        result = conn.sql(query).collect()
        databases = [row['DATABASE_NAME'] for row in result]
        return databases
    except Exception as e:
        # Fallback to the original method if the above fails
        try:
            query = """
                SELECT DISTINCT DATABASE_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
                ORDER BY DATABASE_NAME
            """
            databases = conn.sql(query).collect()
            db_names = [row[0] for row in databases]
            return sorted(db_names)
        except Exception as e2:
            log_debug("DB", f"Error: {e} / Fallback error: {e2}")
            return []

def get_schemas_in_database(database_name):
    """Get schemas in a specific database"""
    try:
        query = f"""
            SELECT SCHEMA_NAME 
            FROM "{database_name}".INFORMATION_SCHEMA.SCHEMATA 
            WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
            ORDER BY SCHEMA_NAME
        """
        schemas = conn.sql(query).collect()
        return [row['SCHEMA_NAME'] for row in schemas]
    except Exception as e:
        log_debug("SCHEMA", f"Error getting schemas: {e}")
        return []

def get_tables_in_schema(database_name, schema_name):
    """Get tables in a specific schema"""
    try:
        query = f"""
            SELECT TABLE_NAME 
            FROM "{database_name}".INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        tables = conn.sql(query).collect()
        return [row['TABLE_NAME'] for row in tables]
    except Exception as e:
        log_debug("TABLE", f"Error getting tables: {e}")
        return []

def save_snowspace_config(snowspace_id, snowspace_name, description, orchestrator_account, 
                         recommended_lag, target_table, contributor_accounts=None):
    """Save snowspace configuration to database"""
    try:
        # Delete existing if exists
        conn.sql("""
            DELETE FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES WHERE snowspace_id = ?
        """, params=[snowspace_id]).collect()
        
        # Convert contributor accounts to JSON
        contributor_json = None
        if contributor_accounts:
            contributor_json = json.dumps(contributor_accounts)
        
        # Insert new config with target_table
        if contributor_json:
            conn.sql("""
                INSERT INTO UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
                (snowspace_id, snowspace_name, description, orchestrator_account, 
                 recommended_target_lag, target_table, contributor_accounts, status)
                SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?), 'DRAFT'
            """, params=[snowspace_id, snowspace_name, description, orchestrator_account, 
                        recommended_lag, target_table, contributor_json]).collect()
        else:
            conn.sql("""
                INSERT INTO UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
                (snowspace_id, snowspace_name, description, orchestrator_account, 
                 recommended_target_lag, target_table, status)
                VALUES (?, ?, ?, ?, ?, ?, 'DRAFT')
            """, params=[snowspace_id, snowspace_name, description, orchestrator_account, 
                        recommended_lag, target_table]).collect()
        
        log_debug("SAVE", f"Snowspace config saved: {snowspace_id}")
        return True
        
    except Exception as e:
        log_debug("SAVE", f"Error saving Snowspace config: {e}")
        return False

def get_existing_snowspaces():
    """Get list of existing snowspaces"""
    try:
        result = conn.sql("""
            SELECT snowspace_id, snowspace_name, description, orchestrator_account, 
                   recommended_target_lag, target_table, status, created_at
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES
            ORDER BY created_at DESC
        """).collect()
        
        return [row.as_dict() for row in result]
        
    except Exception as e:
        log_debug("LOAD", f"Error loading existing snowspaces: {e}")
        return []

def get_dashboard_metrics():
    """Get dashboard metrics"""
    try:
        # Get all metrics in a single query
        result = conn.sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'PUBLISHED' THEN 1 ELSE 0 END) as published,
                SUM(CASE WHEN status = 'DRAFT' THEN 1 ELSE 0 END) as draft
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES
        """).collect()
        
        if result:
            row = result[0]
            total = row['TOTAL']
            published = row['PUBLISHED']
            draft = row['DRAFT']
        else:
            total = published = draft = 0
        
        # Get active contributors (for now, use total published as proxy)
        active_contributors = published
        
        return {
            'total_snowspaces': total,
            'published_snowspaces': published,
            'draft_snowspaces': draft,
            'active_contributors': active_contributors
        }
    except Exception as e:
        log_debug("METRICS", f"Error getting dashboard metrics: {e}")
        return {
            'total_snowspaces': 0,
            'published_snowspaces': 0,
            'draft_snowspaces': 0,
            'active_contributors': 0
        }

def render_status_badge(status):
    """Render a colored status badge"""
    if status == 'PUBLISHED':
        return "🟢 Published"
    elif status == 'DRAFT':
        return "🟡 Draft"
    elif status == 'ARCHIVED':
        return "📁 Archived"
    else:
        return "🔴 Needs Review"

def handle_clone_action(snowspace_id):
    """Handle cloning a Snowspace"""
    try:
        # Get original snowspace config
        result = conn.sql("""
            SELECT snowspace_name, description, orchestrator_account, 
                   recommended_target_lag, target_table
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
            WHERE snowspace_id = ?
        """, params=[snowspace_id]).collect()
        
        if result:
            original = result[0].as_dict()
            
            # Generate new ID with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            new_snowspace_id = f"{snowspace_id}_clone_{timestamp}"
            new_snowspace_name = f"{original['SNOWSPACE_NAME']} (Clone)"
            
            # Save cloned config
            if save_snowspace_config(
                new_snowspace_id, 
                new_snowspace_name,
                original.get('DESCRIPTION', ''),
                original['ORCHESTRATOR_ACCOUNT'], 
                original['RECOMMENDED_TARGET_LAG'],
                original.get('TARGET_TABLE', '')
            ):
                # Clone field definitions
                fields_result = conn.sql("""
                    SELECT field_name, field_category, data_type, description, 
                           is_required, confidence_score, sample_values, synonyms, additional_context
                    FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS
                    WHERE snowspace_id = ?
                """, params=[snowspace_id]).collect()
                
                if fields_result:
                    # Insert cloned fields
                    for field in fields_result:
                        conn.sql("""
                            INSERT INTO UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS
                            (snowspace_id, field_name, field_category, data_type, description, 
                             is_required, confidence_score, sample_values, synonyms, additional_context)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, params=[
                            new_snowspace_id,
                            field['FIELD_NAME'],
                            field['FIELD_CATEGORY'],
                            field['DATA_TYPE'],
                            field['DESCRIPTION'],
                            field['IS_REQUIRED'],
                            field['CONFIDENCE_SCORE'],
                            field['SAMPLE_VALUES'],
                            field['SYNONYMS'],
                            field['ADDITIONAL_CONTEXT']
                        ]).collect()
                
                st.success(f"🎉 Snowspace cloned successfully! New ID: {new_snowspace_id}")
                time.sleep(1)
                st.rerun()
            else:
                st.error("❌ Error cloning Snowspace configuration")
        else:
            st.error("❌ Original Snowspace not found")
            
    except Exception as e:
        log_debug("CLONE", f"Error cloning Snowspace: {e}")
        st.error("❌ Error cloning Snowspace")

def handle_share_link_action(snowspace_id):
    """Handle generating a share link for a Snowspace"""
    try:
        # Get current account for share link
        current_account = get_current_account()
        
        # Generate share link (simplified for MVP)
        share_link = f"https://{current_account}.snowflakecomputing.com/snowspace/{snowspace_id}"
        
        # Display share information
        st.info(f"📋 **Share Link Generated**")
        st.code(share_link, language=None)
        st.markdown("*Contributors can use this link to access the Snowspace configuration*")
        
        # Option to copy to clipboard (informational)
        st.caption("💡 Copy the link above to share with potential contributors")
        
        log_debug("SHARE", f"Share link generated for {snowspace_id}")
        
    except Exception as e:
        log_debug("SHARE", f"Error generating share link: {e}")
        st.error("❌ Error generating share link")

def handle_archive_action(snowspace_id):
    """Handle archiving a Snowspace"""
    try:
        # Show confirmation dialog
        st.warning(f"⚠️ **Archive Snowspace: {snowspace_id}**")
        st.markdown("Archiving will change the status to 'ARCHIVED' and stop new contributor connections.")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("✅ Confirm Archive", type="primary", key=f"confirm_archive_{snowspace_id}"):
                # Update status to ARCHIVED
                conn.sql("""
                    UPDATE UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
                    SET status = 'ARCHIVED' 
                    WHERE snowspace_id = ?
                """, params=[snowspace_id]).collect()
                
                st.success(f"📁 Snowspace {snowspace_id} archived successfully!")
                log_debug("ARCHIVE", f"Snowspace archived: {snowspace_id}")
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("❌ Cancel", key=f"cancel_archive_{snowspace_id}"):
                st.rerun()
                
    except Exception as e:
        log_debug("ARCHIVE", f"Error archiving Snowspace: {e}")
        st.error("❌ Error archiving Snowspace")

def show_dashboard():
    """Dashboard view with metrics and Snowspace management"""
    
    # Header with blue gradient background
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0; background: linear-gradient(90deg, #1f4e79 0%, #2d7dd2 100%); color: white; border-radius: 10px; margin-bottom: 2rem;">
        <h1 style="margin: 0; font-size: 2.5rem;">🌌 Manage Your Snowspaces</h1>
        <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem; opacity: 0.9;">Standardize data. Share instantly. Connect your ecosystem.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Get metrics
    metrics = get_dashboard_metrics()
    
    # Metrics cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🌟 Total Snowspaces",
            value=metrics['total_snowspaces'],
            help="Total number of data spaces configured"
        )
    
    with col2:
        st.metric(
            label="🟢 Active Spaces",
            value=metrics['published_snowspaces'],
            help="Published and actively sharing data"
        )
    
    with col3:
        st.metric(
            label="📝 Draft Spaces",
            value=metrics['draft_snowspaces'],
            help="Spaces in development"
        )
    
    with col4:
        st.metric(
            label="🤝 Active Contributors",
            value=metrics['active_contributors'],
            help="Suppliers actively contributing data"
        )
    
    st.markdown("---")
    
    # Existing Snowspaces section with search and create button
    st.subheader("🌟 Existing Snowspaces")
    
    existing_snowspaces = get_existing_snowspaces()
    
    if existing_snowspaces:
        # Search bar and create button in same row
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_term = st.text_input(
                "🔍 Search Snowspaces",
                placeholder="Search by name or ID...",
                key="search_input"
            )
        
        with col2:
            st.markdown("<div style='height: 32px'></div>", unsafe_allow_html=True)  # Spacer to align with search bar
            if st.button("🆕 Create New Snowspace", type="primary", use_container_width=True):
                st.session_state.current_step = "create"
                st.rerun()
        
        # Filter snowspaces based on search
        filtered_snowspaces = existing_snowspaces
        if search_term:
            filtered_snowspaces = [
                snowspace for snowspace in existing_snowspaces
                if search_term.lower() in snowspace['SNOWSPACE_NAME'].lower() or 
                   search_term.lower() in snowspace['SNOWSPACE_ID'].lower()
            ]
        
        # Pagination
        total_items = len(filtered_snowspaces)
        page_size = 10  # Hardcoded value instead of session state
        total_pages = (total_items + page_size - 1) // page_size
        
        if total_items > 0:
            # Pagination controls
            col1, col2, col3 = st.columns([2, 1, 2])
            
            with col1:
                if st.session_state.current_page > 0:
                    if st.button("⬅️ Previous"):
                        st.session_state.current_page -= 1
                        st.rerun()
            
            with col2:
                st.write(f"Page {st.session_state.current_page + 1} of {total_pages}")
            
            with col3:
                if st.session_state.current_page < total_pages - 1:
                    if st.button("Next ➡️"):
                        st.session_state.current_page += 1
                        st.rerun()
            
            # Calculate items for current page
            start_idx = st.session_state.current_page * page_size
            end_idx = min(start_idx + page_size, total_items)
            page_snowspaces = filtered_snowspaces[start_idx:end_idx]
            
            # Display snowspaces in a clean table format
            for snowspace in page_snowspaces:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        st.markdown(f"**{snowspace['SNOWSPACE_NAME']}**")
                        st.caption(f"ID: {snowspace['SNOWSPACE_ID']}")
                        if snowspace.get('TARGET_TABLE'):
                            st.caption(f"Target: {snowspace['TARGET_TABLE']}")
                    
                    with col2:
                        st.text(render_status_badge(snowspace['STATUS']))
                    
                    with col3:
                        created_date = snowspace['CREATED_AT']
                        if hasattr(created_date, 'strftime'):
                            st.caption(f"Created: {created_date.strftime('%Y-%m-%d')}")
                        else:
                            st.caption(f"Created: {created_date}")
                    
                    with col4:
                        # Quick actions dropdown
                        action = st.selectbox(
                            "Actions",
                            ["Select Action", "✏️ Edit", "📋 Clone", "🔗 Share Link", "📁 Archive"],
                            key=f"action_{snowspace['SNOWSPACE_ID']}",
                            label_visibility="collapsed"
                        )
                        
                        # Handle selected action
                        if action == "✏️ Edit":
                            # For edit, we'll navigate to the AI Field Builder page
                            st.session_state.current_snowspace_id = snowspace['SNOWSPACE_ID']
                            st.switch_page("pages/1_AI_Field_Builder.py")
                        elif action == "📋 Clone":
                            handle_clone_action(snowspace['SNOWSPACE_ID'])
                        elif action == "🔗 Share Link":
                            handle_share_link_action(snowspace['SNOWSPACE_ID'])
                        elif action == "📁 Archive":
                            handle_archive_action(snowspace['SNOWSPACE_ID'])
                    
                    st.markdown("---")
        else:
            if search_term:
                st.info(f"No Snowspaces found matching '{search_term}'")
            else:
                st.info("No existing Snowspaces found")
    else:
        # When no snowspaces exist, show a centered call-to-action
        st.info("No Snowspaces created yet.")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🆕 Create Your First Snowspace", type="primary", use_container_width=True):
                st.session_state.current_step = "create"
                st.rerun()

def show_create_snowspace():
    """Create New Snowspace with DB/Schema/Table selection"""
    
    # Header with blue gradient background
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0; background: linear-gradient(90deg, #1f4e79 0%, #2d7dd2 100%); color: white; border-radius: 10px; margin-bottom: 2rem;">
        <h1 style="margin: 0; font-size: 2.5rem;">🌟 Create New Snowspace</h1>
        <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem; opacity: 0.9;">Define your data space for contributor mapping</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Breadcrumb navigation
    st.markdown("**Configuration:** Select target table and define your Snowspace")
    
    # Target Table Selection FIRST
    st.subheader("🎯 Target Table Selection")
    st.info("Select the table that defines the schema contributors will map their data to.")
    
    # Database selection
    databases = get_accessible_databases()
    
    if databases:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            selected_db = st.selectbox(
                "Database*",
                ["Select a database..."] + databases,
                help="Select the database containing your target table"
            )
        
        with col2:
            # Schema selection (only show if database selected)
            if selected_db and selected_db != "Select a database...":
                schemas = get_schemas_in_database(selected_db)
                
                if schemas:
                    selected_schema = st.selectbox(
                        "Schema*",
                        ["Select a schema..."] + schemas,
                        help="Select the schema containing your target table"
                    )
                else:
                    st.warning("No schemas found")
                    selected_schema = None
            else:
                selected_schema = st.selectbox(
                    "Schema*",
                    ["Select a database first..."],
                    disabled=True
                )
        
        with col3:
            # Table selection (only show if schema selected)
            if (selected_db and selected_db != "Select a database..." and 
                selected_schema and selected_schema != "Select a schema..." and 
                selected_schema != "Select a database first..."):
                tables = get_tables_in_schema(selected_db, selected_schema)
                
                if tables:
                    selected_table = st.selectbox(
                        "Table*",
                        ["Select a table..."] + tables,
                        help="Select the target table for this Snowspace"
                    )
                else:
                    st.warning("No tables found")
                    selected_table = None
            else:
                selected_table = st.selectbox(
                    "Table*",
                    ["Select a schema first..."],
                    disabled=True
                )
    else:
        st.error("No accessible databases found. Please check permissions.")
        selected_db = None
        selected_schema = None
        selected_table = None
    
    # Show selected table path and generate default ID
    if (selected_db and selected_db != "Select a database..." and
        selected_schema and selected_schema != "Select a schema..." and
        selected_table and selected_table != "Select a table..."):
        st.success(f"✅ Selected table: `{selected_db}.{selected_schema}.{selected_table}`")
        
        # Generate default snowspace ID from table selection with dots as separators
        timestamp = datetime.now().strftime("%Y%m%d")
        default_snowspace_id = f"{selected_db}.{selected_schema}.{selected_table}.{timestamp}".upper()
        default_snowspace_name = f"{selected_table.replace('_', ' ').title()} Snowspace"
    else:
        default_snowspace_id = ""
        default_snowspace_name = ""
    
    st.markdown("---")
    
    # Snowspace Configuration
    st.subheader("📋 Snowspace Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        snowspace_id = st.text_input(
            "Snowspace ID*",
            value=default_snowspace_id,
            placeholder="e.g., automotive_quality_v2",
            help="Unique identifier for this Snowspace (auto-generated from table selection)"
        )
        
        snowspace_name = st.text_input(
            "Snowspace Name*",
            value=default_snowspace_name,
            placeholder="e.g., Automotive Quality Metrics",
            help="Human-readable name for this Snowspace"
        )
    
    with col2:
        # Auto-populate with current account
        current_account = get_current_account()
        orchestrator_account = st.text_input(
            "Orchestrator Account Locator*",
            value=current_account,
            help="Snowflake account identifier for the Orchestrator (auto-populated)"
        )
        
        # Target lag selection
        lag_options = ['1 MINUTE', '5 MINUTES', '15 MINUTES', '1 HOUR', '4 HOURS', '1 DAY']
        recommended_lag = st.selectbox(
            "Recommended Refresh Frequency*",
            lag_options,
            index=1,  # Default to 5 minutes
            help="How frequently should contributors refresh this data?"
        )
    
    # Description field
    description = st.text_area(
        "Description",
        placeholder="Describe the purpose and scope of this Snowspace...",
        help="Optional description to help contributors understand this data space",
        height=100
    )
    
    # Buttons
    st.markdown("---")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        if st.button("⬅️ Back to Dashboard", type="secondary"):
            st.session_state.current_step = "dashboard"
            st.rerun()
    
    with col2:
        if st.button("💾 Save and Continue", type="primary"):
            # Validate all required fields
            if not all([snowspace_id, snowspace_name, orchestrator_account, recommended_lag]):
                st.error("Please fill in all required Snowspace configuration fields")
            elif not selected_db or selected_db == "Select a database...":
                st.error("Please select a database")
            elif not selected_schema or selected_schema == "Select a schema...":
                st.error("Please select a schema")
            elif not selected_table or selected_table == "Select a table...":
                st.error("Please select a table")
            else:
                # Construct full table path
                target_table = f"{selected_db}.{selected_schema}.{selected_table}"
                
                with st.spinner("Saving Snowspace configuration..."):
                    time.sleep(1)  # Brief pause for UX
                    
                    # Save snowspace config with target table
                    if save_snowspace_config(
                        snowspace_id, 
                        snowspace_name, 
                        description, 
                        orchestrator_account, 
                        recommended_lag, 
                        target_table
                    ):
                        st.success("🎉 Snowspace saved as draft!")
                        time.sleep(1)
                        st.session_state.current_snowspace_id = snowspace_id
                        # Navigate to AI Field Builder page
                        st.switch_page("pages/1_AI_Field_Builder.py")
                    else:
                        st.error("Error saving Snowspace configuration")

# --- Main Content ---
# Show appropriate step
current_step = st.session_state.current_step

if current_step == "dashboard":
    show_dashboard()
elif current_step == "create":
    show_create_snowspace()
else:
    # Fallback to dashboard
    st.session_state.current_step = "dashboard"
    st.rerun()