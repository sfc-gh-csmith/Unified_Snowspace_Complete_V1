import streamlit as st
import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException
import snowflake.snowpark as snowpark
import base64

# Page config
st.set_page_config(
    page_title="Connect to Snowspace",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize Snowflake session
@st.cache_resource
def init_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to get active Snowflake session: {str(e)}")
        st.stop()

session = init_session()

# Styling
CONTAINER_STYLE = """
<style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
        margin: 0 auto;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stButton button {width: 100%;}
    div[data-testid="stSidebar"] {display: none;}
    h1, h2, h3, h4, h5, h6 { text-align: center; }
</style>
"""
st.markdown(CONTAINER_STYLE, unsafe_allow_html=True)

# Removed banner per user's request

# Session state defaults
for key in ["selected_snowspace", "selected_source_db", "selected_source_schema", "selected_source_table"]:
    if key not in st.session_state:
        st.session_state[key] = None

st.markdown("# 🔗 Connect to Snowspace")
st.markdown("---")

# Helper functions with cache to improve responsiveness
@st.cache_data(ttl=60)
def get_snowspace_views():
    try:
        db_query = """
        SELECT DISTINCT DATABASE_NAME 
        FROM INFORMATION_SCHEMA.DATABASES
        WHERE DATABASE_NAME LIKE 'SNOWSPACE_%'
        ORDER BY DATABASE_NAME
        """
        databases = session.sql(db_query).collect()
        snowspace_views = []
        for db_row in databases:
            db_name = db_row['DATABASE_NAME']
            schema_query = f"""
            SELECT SCHEMA_NAME 
            FROM {db_name}.INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME = 'SNOWSPACE'
            """
            try:
                if session.sql(schema_query).collect():
                    view_query = f"""
                    SELECT TABLE_NAME 
                    FROM {db_name}.INFORMATION_SCHEMA.VIEWS
                    WHERE TABLE_SCHEMA = 'SNOWSPACE'
                    AND LOWER(TABLE_NAME) LIKE 'snowspace_%'
                    AND LOWER(TABLE_NAME) LIKE '%_view'
                    """
                    views = session.sql(view_query).collect()
                    for v in views:
                        snowspace_views.append({
                            'display_name': db_name,
                            'full_path': f"{db_name}.SNOWSPACE.{v['TABLE_NAME']}",
                            'database': db_name,
                            'view_name': v['TABLE_NAME']
                        })
            except:
                continue
        return snowspace_views
    except Exception as e:
        st.error(f"Error discovering Snowspace views: {str(e)}")
        return []

@st.cache_data(ttl=60)
def get_databases():
    try:
        result = session.sql("""
        SELECT DISTINCT DATABASE_NAME 
        FROM INFORMATION_SCHEMA.DATABASES
        WHERE DATABASE_NAME NOT LIKE 'SNOWSPACE_%'
        AND DATABASE_NAME NOT IN ('SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA')
        ORDER BY DATABASE_NAME
        """).collect()
        return [r['DATABASE_NAME'] for r in result]
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

@st.cache_data(ttl=60)
def get_schemas(db):
    try:
        result = session.sql(f"""
        SELECT SCHEMA_NAME 
        FROM {db}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
        ORDER BY SCHEMA_NAME
        """).collect()
        return [r['SCHEMA_NAME'] for r in result]
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

@st.cache_data(ttl=60)
def get_tables(db, schema):
    try:
        result = session.sql(f"""
        SELECT TABLE_NAME 
        FROM {db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """).collect()
        return [r['TABLE_NAME'] for r in result]
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

# Main content
col1, col2, col3 = st.columns([1, 3, 1])

with col2:
    # Step 1: Select Snowspace
    st.markdown("## Step 1: Select Target Snowspace")
    st.info("Select the Snowspace view that defines your target schema")
    
    snowspace_views = get_snowspace_views()
    
    if not snowspace_views:
        st.warning("No Snowspace views found. Please ensure the Orchestrator has shared a Snowspace with your account.")
    else:
        # Create selection dropdown
        view_options = {view['display_name']: view for view in snowspace_views}
        
        selected_display = st.selectbox(
            "Available Snowspaces",
            options=list(view_options.keys()),
            index=0 if not st.session_state.selected_snowspace else 
                  list(view_options.keys()).index(
                      next((k for k, v in view_options.items() 
                            if v['full_path'] == st.session_state.selected_snowspace), 
                           list(view_options.keys())[0])
                  ),
            help="These are the Snowspace schemas shared by your Orchestrator"
        )
        
        if selected_display:
            st.session_state.selected_snowspace = view_options[selected_display]['full_path']
            
            # Show selected Snowspace info
            with st.expander("View Snowspace Details", expanded=True):
                selected_view = view_options[selected_display]
                
                # Query the view to get metadata
                try:
                    metadata_query = f"""
                    SELECT DISTINCT
                        SNOWSPACE_ID,
                        SNOWSPACE_NAME,
                        SNOWSPACE_DESCRIPTION,
                        ORCHESTRATOR_ACCOUNT,
                        RECOMMENDED_TARGET_LAG
                    FROM {selected_view['full_path']}
                    LIMIT 1
                    """
                    metadata = session.sql(metadata_query).collect()
                    
                    if metadata:
                        meta = metadata[0]
                        
                        # Display metadata in a nice format
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**📋 Snowspace Information**")
                            st.markdown(f"**Name:** {meta['SNOWSPACE_NAME']}")
                            st.markdown(f"**ID:** `{meta['SNOWSPACE_ID']}`")
                            if meta['SNOWSPACE_DESCRIPTION']:
                                st.markdown(f"**Description:** {meta['SNOWSPACE_DESCRIPTION']}")
                        
                        with col2:
                            st.markdown("**🔧 Technical Details**")
                            st.markdown(f"**Database:** `{selected_view['database']}`")
                            st.markdown(f"**Orchestrator:** `{meta['ORCHESTRATOR_ACCOUNT']}`")
                            if meta['RECOMMENDED_TARGET_LAG']:
                                st.markdown(f"**Target Lag:** {meta['RECOMMENDED_TARGET_LAG']}")
                        
                        # Get field count and categories
                        field_query = f"""
                        SELECT 
                            COUNT(DISTINCT FIELD_NAME) as field_count,
                            COUNT(DISTINCT FIELD_CATEGORY) as category_count,
                            SUM(CASE WHEN IS_REQUIRED THEN 1 ELSE 0 END) as required_count
                        FROM {selected_view['full_path']}
                        """
                        field_stats = session.sql(field_query).collect()[0]
                        
                        st.markdown("---")
                        
                        # Display field statistics
                        stat1, stat2, stat3 = st.columns(3)
                        with stat1:
                            st.metric("Total Fields", field_stats['FIELD_COUNT'])
                        with stat2:
                            st.metric("Required Fields", field_stats['REQUIRED_COUNT'])
                        with stat3:
                            st.metric("Categories", field_stats['CATEGORY_COUNT'])
                        
                    else:
                        st.warning("Could not retrieve Snowspace metadata")
                        
                except Exception as e:
                    st.error(f"Error loading Snowspace details: {str(e)}")
                    # Fall back to basic info
                    st.markdown(f"**Database:** `{selected_view['database']}`")
                    st.markdown(f"**Schema:** `SNOWSPACE`")
                    st.markdown(f"**View:** `{selected_view['view_name']}`")
                    st.markdown(f"**Full Path:** `{selected_view['full_path']}`")
    
    st.markdown("---")
    
    # Step 2: Select Source Table
    st.markdown("## Step 2: Select Source Table")
    st.info("Select the table containing your data to map to the Snowspace schema")
    
    # Database selector
    databases = get_databases()
    if databases:
        db_index = 0
        if st.session_state.selected_source_db and st.session_state.selected_source_db in databases:
            db_index = databases.index(st.session_state.selected_source_db)
            
        selected_db = st.selectbox(
            "Database",
            options=databases,
            index=db_index,
            help="Select the database containing your source data"
        )
        st.session_state.selected_source_db = selected_db
        
        # Schema selector (only show if database selected)
        if selected_db:
            schemas = get_schemas(selected_db)
            if schemas:
                schema_index = 0
                if st.session_state.selected_source_schema and st.session_state.selected_source_schema in schemas:
                    schema_index = schemas.index(st.session_state.selected_source_schema)
                    
                selected_schema = st.selectbox(
                    "Schema",
                    options=schemas,
                    index=schema_index,
                    help="Select the schema containing your source table"
                )
                st.session_state.selected_source_schema = selected_schema
                
                # Table selector (only show if schema selected)
                if selected_schema:
                    tables = get_tables(selected_db, selected_schema)
                    if tables:
                        table_index = 0
                        if st.session_state.selected_source_table and st.session_state.selected_source_table in tables:
                            table_index = tables.index(st.session_state.selected_source_table)
                            
                        selected_table = st.selectbox(
                            "Table",
                            options=tables,
                            index=table_index,
                            help="Select the table to map to the Snowspace schema"
                        )
                        st.session_state.selected_source_table = selected_table
                        
                        # Show full source path
                        if selected_table:
                            source_path = f"{selected_db}.{selected_schema}.{selected_table}"
                            st.success(f"**Selected Source:** `{source_path}`")
                    else:
                        st.warning(f"No tables found in {selected_db}.{selected_schema}")
                else:
                    st.info("Select a schema to view available tables")
            else:
                st.warning(f"No schemas found in {selected_db}")
        else:
            st.info("Select a database to continue")
    else:
        st.error("No accessible databases found")
    
    st.markdown("---")
    
    # Navigation buttons
    col_prev, col_next = st.columns(2)
    
    with col_prev:
        if st.button("← Back to Home", use_container_width=True):
            st.switch_page("streamlit_app.py")
    
    with col_next:
        # Enable next button only if both selections are made
        if st.session_state.selected_snowspace and st.session_state.selected_source_table:
            if st.button("Continue to Field Mapping →", type="primary", use_container_width=True):
                # Extract schema_id from the selected snowspace
                # We need to query the view to get the SNOWSPACE_ID
                try:
                    schema_query = f"""
                    SELECT DISTINCT SNOWSPACE_ID 
                    FROM {st.session_state.selected_snowspace}
                    LIMIT 1
                    """
                    result = session.sql(schema_query).collect()
                    if result:
                        schema_id = result[0]['SNOWSPACE_ID']
                        
                        # Set the required session state variables for Field Mapper
                        st.session_state['selected_schema_id'] = schema_id
                        st.session_state['contributor_table'] = f"{st.session_state.selected_source_db}.{st.session_state.selected_source_schema}.{st.session_state.selected_source_table}"
                        
                        # Also store the snowspace path for reference
                        st.session_state['selected_snowspace_path'] = st.session_state.selected_snowspace
                        
                        # Clear any previous auto-analysis flag so Field Mapper runs fresh
                        if 'auto_analyzed' in st.session_state:
                            del st.session_state['auto_analyzed']
                        
                        st.switch_page("pages/01_Field_Mapper.py")
                    else:
                        st.error("Could not retrieve schema ID from the selected Snowspace")
                except Exception as e:
                    st.error(f"Error retrieving schema information: {str(e)}")
        else:
            st.button("Continue to Field Mapping →", 
                     type="primary", 
                     use_container_width=True, 
                     disabled=True,
                     help="Please select both a Snowspace and source table to continue")