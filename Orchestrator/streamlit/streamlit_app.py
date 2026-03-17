import streamlit as st
import snowflake.snowpark as snowpark
from datetime import datetime
import time
import base64
#from assets import LOGO_BASE64

# --- Configuration ---
DEVELOPMENT_MODE = True  # Set to False for production Native App

# --- Session State Initialization ---
if "setup_complete" not in st.session_state:
    st.session_state.setup_complete = False
if "debug_log" not in st.session_state:
    st.session_state.debug_log = []

# Configure page - hide sidebar
st.set_page_config(
    page_title="Unified Snowspace - Orchestrator",
    page_icon="🌉",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Hide the sidebar
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# --- Connect to Snowflake ---
try:
    conn = snowpark.Session.builder.create()
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")
    st.stop()

# --- Helper Functions ---
def log_debug(category, message, data=None):
    """Centralized debug logging"""
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

def get_accessible_databases():
    """Get databases accessible to the current user"""
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
    except Exception as e:
        log_debug("DB", f"Error: {e}")
        return []

def verify_orchestrator_permissions():
    """Verify the app has required permissions"""
    try:
        # Check 1: Can we create shares?
        test_share_name = "SNOWSPACE_PERMISSION_TEST_SHARE"
        try:
            conn.sql(f"CREATE SHARE IF NOT EXISTS {test_share_name}").collect()
            conn.sql(f"DROP SHARE IF EXISTS {test_share_name}").collect()
        except Exception as e:
            return False, f"Missing CREATE SHARE permission: {str(e)}"
        
        # Check 2: Can we access databases?
        databases = get_accessible_databases()
        if not databases:
            return False, "No accessible databases found"
        
        # Check 3: Can we create tables and views? (test in first accessible database)
        if databases:
            test_db = databases[0]
            try:
                # Get schemas in the database
                schemas = conn.sql(f"""
                    SELECT SCHEMA_NAME 
                    FROM "{test_db}".INFORMATION_SCHEMA.SCHEMATA 
                    WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA') 
                    LIMIT 1
                """).collect()
                
                if schemas:
                    test_schema = schemas[0]['SCHEMA_NAME']
                    # Try to create a test view
                    test_view_name = f'"{test_db}"."{test_schema}".SNOWSPACE_PERMISSION_TEST_VIEW'
                    conn.sql(f"CREATE OR REPLACE VIEW {test_view_name} AS SELECT 1 as test").collect()
                    conn.sql(f"DROP VIEW IF EXISTS {test_view_name}").collect()
            except Exception as e:
                return False, f"Missing CREATE VIEW permission: {str(e)}"
        
        return True, "All permissions verified!"
        
    except Exception as e:
        return False, f"Permission check failed: {str(e)}"

def display_app_banner():
    """Display the application logo banner - Streamlit-friendly version"""
    col1, col2, col3 = st.columns([1, 4, 1])
    with col2:
        st.markdown("""
        <div style='
            position: relative;
            width: 100%;
            height: 160px;
            background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 50%, #16213e 100%);
            border-radius: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 2rem;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        '>
            <div style='
                font-size: 2.2em;
                font-weight: bold;
                color: #FFD700;
                text-shadow: 2px 2px 8px rgba(0,0,0,0.8);
                letter-spacing: 0.08em;
                text-align: center;
            '>
                🪐 UNIFIED SNOWSPACE ❄️
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)

# --- Permission Wizard ---
def show_permission_wizard():
    """Simple permission wizard for Native App mode"""
    display_app_banner()
    
    st.title("🔐 Permission Setup Required")
    
    st.info("""
    ### Required Permissions
    
    This application needs the following permissions to function:
    
    1. **Database Access**: Read/write access to store configuration and field mappings
    2. **Share Creation**: Ability to create shares for distributing schema definitions to contributors
    3. **Table/View Creation**: Ability to create secure views for sharing configurations
    
    Please grant the following permissions to the application:
    """)
    
    app_name = "UNIFIED_SNOWSPACE_ORCHESTRATOR"
    
    # Display required grants
    st.code(f"""
-- Grant share creation privilege
GRANT CREATE SHARE ON ACCOUNT TO APPLICATION {app_name};

-- Grant database access (replace with your database/schema)
GRANT USAGE ON DATABASE <your_database> TO APPLICATION {app_name};
GRANT USAGE ON SCHEMA <your_database>.<your_schema> TO APPLICATION {app_name};
GRANT CREATE TABLE ON SCHEMA <your_database>.<your_schema> TO APPLICATION {app_name};
GRANT CREATE VIEW ON SCHEMA <your_database>.<your_schema> TO APPLICATION {app_name};
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA <your_database>.<your_schema> TO APPLICATION {app_name};
    """, language='sql')
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("✅ I've granted the permissions", type="primary"):
            with st.spinner("Verifying permissions..."):
                verified, message = verify_orchestrator_permissions()
                
                if verified:
                    st.success(f"🎉 {message}")
                    time.sleep(1)
                    st.session_state.setup_complete = True
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
                    st.info("Please ensure all required permissions are granted and try again.")
    
    with col2:
        if st.button("🔍 Check Permissions Only"):
            with st.spinner("Checking permissions..."):
                verified, message = verify_orchestrator_permissions()
                
                if verified:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")

# --- Home Screen ---
def show_home_screen():
    """Main home screen with overview and navigation"""
    # Display logo banner
    display_app_banner()
    
    # Main heading
    st.markdown(
        """
        <h1 style='text-align: center; color: #1e3d59; margin-bottom: 0;'>
            Orchestrator Portal
        </h1>
        <p style='text-align: center; color: #3e5c76; font-size: 1.3em; margin-top: 0;'>
            Define Standards. Enable Contributors. Unify Your Data Ecosystem.
        </p>
        """,
        unsafe_allow_html=True
    )
    
    # Add some space
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Main content area with constrained width
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col2:
        # Process overview
        st.markdown("### How It Works")
        
        step1, step2 = st.columns(2)
        
        with step1:
            st.info(
                """
                **🎯 Step 1: Define Your Standard**
                
                Create a Snowspace by selecting your target table schema that contributors will map to.
                
                • Choose your golden schema  
                • AI analyzes field patterns  
                • Set refresh requirements
                """
            )
        
        with step2:
            st.success(
                """
                **🚀 Step 2: Enable Contributors**
                
                Share your Snowspace with suppliers and partners who can instantly map their data.
                
                • One-click sharing  
                • Automated field mapping  
                • Real-time data flow
                """
            )
        
        # Benefits section
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Metrics
        met1, met2, met3 = st.columns(3)
        
        with met1:
            st.markdown(
                """
                <div style='text-align: center; background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem;'>
                    <h3 style='color: #1e3d59; margin: 0;'>Minutes</h3>
                    <p style='margin: 0; color: #666;'>Not months</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with met2:
            st.markdown(
                """
                <div style='text-align: center; background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem;'>
                    <h3 style='color: #1e3d59; margin: 0;'>1000s</h3>
                    <p style='margin: 0; color: #666;'>Of contributors</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with met3:
            st.markdown(
                """
                <div style='text-align: center; background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem;'>
                    <h3 style='color: #1e3d59; margin: 0;'>Zero</h3>
                    <p style='margin: 0; color: #666;'>Manual pipelines</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        # Call to action
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        # Action buttons
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Analytics", type="primary", help="View analytics and insights"):
                #st.info("Analytics dashboard coming soon!")
                st.switch_page("pages/4_Analytics_Dashboard.py")
                # Chris added this change on 8/6/2025
        
        with col2:
            if st.button("🔧 Manage Snowspaces", type="secondary", help="Create and manage Snowspaces"):
                st.switch_page("pages/0_Homepage.py")
    
    # Footer
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #888; font-size: 0.9em;'>
            <p>🔒 Enterprise-grade security with Snowflake Native Apps</p>
            <p>📊 Part of the Unified Snowspace ecosystem</p>
        </div>
        """,
        unsafe_allow_html=True
    )

# --- Main Application ---

# Check setup - use different wizards based on mode
if not st.session_state.setup_complete:
    if DEVELOPMENT_MODE:
        # In development mode, just mark as complete and continue
        st.session_state.setup_complete = True
    else:
        # In production mode, use the permission wizard
        show_permission_wizard()

# Only show the main app if setup is complete
if st.session_state.setup_complete:
    # Show home screen
    show_home_screen()