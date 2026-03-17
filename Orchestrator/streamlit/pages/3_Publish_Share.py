"""
Publish & Share Page - Review configuration and publish Snowspace
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
import json
import time
from datetime import datetime

# Configure page
st.set_page_config(
    page_title="Publish - Unified Snowspace",
    page_icon="🚀",
    layout="wide"
)

# Initialize session state
if "current_snowspace_id" not in st.session_state:
    st.error("No Snowspace selected. Please go back to the main page.")
    st.stop()
if "publish_complete" not in st.session_state:
    st.session_state.publish_complete = False

# Connect to Snowflake
try:
    conn = snowpark.Session.builder.create()
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")
    st.stop()

# Helper functions
def get_snowspace_info(snowspace_id: str):
    """Get snowspace configuration from database"""
    try:
        result = conn.sql(f"""
            SELECT snowspace_id, snowspace_name, description, target_table, status, 
                   orchestrator_account, recommended_target_lag, contributor_accounts
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES
            WHERE snowspace_id = '{snowspace_id}'
        """).collect()
        
        if result:
            row = result[0]
            contributor_accounts = []
            if row['CONTRIBUTOR_ACCOUNTS']:
                try:
                    contributor_accounts = json.loads(str(row['CONTRIBUTOR_ACCOUNTS']))
                except:
                    pass
                    
            return {
                'snowspace_id': row['SNOWSPACE_ID'],
                'snowspace_name': row['SNOWSPACE_NAME'],
                'description': row['DESCRIPTION'],
                'target_table': row['TARGET_TABLE'],
                'status': row['STATUS'],
                'orchestrator_account': row['ORCHESTRATOR_ACCOUNT'],
                'recommended_target_lag': row['RECOMMENDED_TARGET_LAG'],
                'contributor_accounts': contributor_accounts
            }
    except Exception as e:
        st.error(f"Error loading snowspace: {str(e)}")
    return None

def get_field_definitions(snowspace_id: str):
    """Get field definitions for the snowspace"""
    try:
        result = conn.sql(f"""
            SELECT field_name, field_category, data_type, description, 
                   is_required, confidence_score
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS
            WHERE snowspace_id = '{snowspace_id}'
            ORDER BY field_name
        """).collect()
        
        return [row.as_dict() for row in result]
    except Exception as e:
        st.error(f"Error loading field definitions: {str(e)}")
        return []

def publish_snowspace(snowspace_id: str):
    """Publish the snowspace (change status to PUBLISHED)"""
    try:
        conn.sql("""
            UPDATE UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
            SET status = 'PUBLISHED',
                updated_at = CURRENT_TIMESTAMP()
            WHERE snowspace_id = ?
        """, params=[snowspace_id]).collect()
        
        return True
    except Exception as e:
        st.error(f"Error publishing Snowspace: {str(e)}")
        return False

def create_snowspace_view(snowspace_id: str, debug_mode: bool = True):
    """Create a secure view for the snowspace configuration"""
    # Clean up the snowspace_id to create valid view name
    clean_id = snowspace_id.replace('.', '_').replace('-', '_')
    
    view_name = f"SNOWSPACE_{clean_id}_VIEW"
    results = {
        'view_name': view_name,
        'success': False,
        'errors': [],
        'debug_info': []
    }
    
    def log_debug(message: str, sql: str = None):
        """Log debug information"""
        results['debug_info'].append(message)
        if debug_mode:
            st.write(f"🔍 {message}")
            if sql:
                st.code(sql, language='sql')
    
    log_debug(f"Original snowspace_id: {snowspace_id}")
    log_debug(f"Cleaned ID for view: {clean_id}")
    
    try:
        # Create unified SECURE view
        log_debug("Creating unified secure view...")
        
        # Create SECURE unified view joining both tables
        create_view_sql = f"""
CREATE OR REPLACE SECURE VIEW UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{view_name} AS 
SELECT
    s.SNOWSPACE_ID,
    s.SNOWSPACE_NAME,
    s.DESCRIPTION AS SNOWSPACE_DESCRIPTION,
    s.ORCHESTRATOR_ACCOUNT,
    s.RECOMMENDED_TARGET_LAG,
    f.FIELD_NAME,
    f.FIELD_CATEGORY,
    f.DATA_TYPE,
    f.SAMPLE_VALUES,
    f.DESCRIPTION AS FIELD_DESCRIPTION,
    f.SYNONYMS,
    f.ADDITIONAL_CONTEXT,
    f.IS_REQUIRED
FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES s
JOIN UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS f
    ON s.SNOWSPACE_ID = f.SNOWSPACE_ID
WHERE s.SNOWSPACE_ID = '{snowspace_id}'"""
        
        log_debug(f"Creating view: {view_name}", create_view_sql)
        conn.sql(create_view_sql).collect()
        log_debug(f"✅ Created unified view: {view_name}")
        results['view_full_name'] = f"UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{view_name}"
        
        # Verify the view was created
        verify_sql = f"SELECT COUNT(*) as cnt FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{view_name}"
        log_debug("Verifying unified view...", verify_sql)
        verify_result = conn.sql(verify_sql).collect()
        log_debug(f"✅ Unified view verified: {verify_result[0]['CNT']} rows")
        
        results['success'] = True
        log_debug("🎉 View creation completed successfully!")
        
    except Exception as e:
        error_msg = f"Failed to create unified view: {str(e)}"
        log_debug(f"❌ {error_msg}")
        results['errors'].append(error_msg)
        results['success'] = False
    
    return results

# Load snowspace info
snowspace_id = st.session_state.current_snowspace_id
snowspace_info = get_snowspace_info(snowspace_id)

if not snowspace_info:
    st.error("Failed to load Snowspace information")
    st.stop()

# Check if already published
if not st.session_state.publish_complete:
    # REVIEW MODE
    
    # Header with breadcrumb
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <p style="color: #666; margin: 0;">Create Snowspace ✓ → AI Field Analysis ✓ → Contributors ✓ → <strong>Review & Publish</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    st.title("🚀 Review & Publish Snowspace")
    st.markdown("Review your configuration before publishing. Once published, you'll need to create a private listing to share with contributors.")
    
    # Warning if already published
    if snowspace_info['status'] == 'PUBLISHED':
        st.warning("⚠️ This Snowspace is already published. Publishing again will recreate the view.")
    
    # Configuration Summary
    st.subheader("📋 Snowspace Configuration")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Name:** {snowspace_info['snowspace_name']}")
        st.markdown(f"**ID:** `{snowspace_info['snowspace_id']}`")
        st.markdown(f"**Target Table:** `{snowspace_info['target_table']}`")
    
    with col2:
        st.markdown(f"**Orchestrator:** {snowspace_info['orchestrator_account']}")
        st.markdown(f"**Refresh Frequency:** {snowspace_info['recommended_target_lag']}")
        st.markdown(f"**Status:** {snowspace_info['status']}")
    
    if snowspace_info['description']:
        st.markdown(f"**Description:** {snowspace_info['description']}")
    
    # Field Definitions Summary
    st.subheader("📊 Field Definitions")
    fields = get_field_definitions(snowspace_id)
    
    if fields:
        col1, col2, col3, col4 = st.columns(4)
        
        total_fields = len(fields)
        dimensions = sum(1 for f in fields if f['FIELD_CATEGORY'] == 'DIMENSION')
        time_dims = sum(1 for f in fields if f['FIELD_CATEGORY'] == 'TIME_DIMENSION')
        facts = sum(1 for f in fields if f['FIELD_CATEGORY'] == 'FACT')
        
        col1.metric("Total Fields", total_fields)
        col2.metric("Dimensions", dimensions)
        col3.metric("Time Dimensions", time_dims)
        col4.metric("Facts", facts)
        
        # Show a sample of fields
        with st.expander("View Field Details", expanded=False):
            field_df = pd.DataFrame(fields)
            display_df = pd.DataFrame({
                'Field': field_df['FIELD_NAME'],
                'Category': field_df['FIELD_CATEGORY'],
                'Type': field_df['DATA_TYPE'],
                'Required': field_df['IS_REQUIRED'].map({True: '✓', False: ''}),
                'Description': field_df['DESCRIPTION'],
                'Confidence': field_df['CONFIDENCE_SCORE'].apply(lambda x: f"{x:.0%}")
            })
            st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("No field definitions found!")
    
    # Contributors Summary
    st.subheader("👥 Contributors")
    
    if snowspace_info['contributor_accounts']:
        st.info(f"This Snowspace will be shared with **{len(snowspace_info['contributor_accounts'])} contributor accounts**")
        
        with st.expander("View Contributor Accounts", expanded=False):
            for account in snowspace_info['contributor_accounts']:
                st.markdown(f"• `{account}`")
    else:
        st.warning("No contributor accounts configured. The Snowspace will be published but not shared.")
    
    # What will be created
    st.subheader("🔨 What Will Be Created")
    
    # Clean the snowspace_id for valid object names
    clean_id = snowspace_id.replace('.', '_').replace('-', '_')
    view_name = f"SNOWSPACE_{clean_id}_VIEW"
    
    st.markdown("**Secure View:**")
    st.code(f"UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{view_name}")
    
    st.info(f"""
    📌 **Manual Step Required**: After publishing, you'll need to create a private listing in Snowsight to share this view with contributors.
    
    Note: The snowspace_id '{snowspace_id}' contains special characters. The view name will use '{clean_id}' instead.
    """)
    
    # Action buttons
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("⬅️ Back to Contributors", type="secondary"):
            st.switch_page("pages/2_Contributors_Fields.py")
    
    with col3:
        if st.button("🚀 Publish Snowspace", type="primary", disabled=len(fields) == 0):
            st.info("Starting publish process...")
            
            # Update status to PUBLISHED first
            if publish_snowspace(snowspace_id):
                st.success("✅ Updated Snowspace status to PUBLISHED")
                
                # Create the secure view
                st.info("Creating secure view...")
                view_results = create_snowspace_view(
                    snowspace_id, 
                    debug_mode=True  # Enable debug output
                )
                
                # Store results
                st.session_state.view_results = view_results
                st.session_state.publish_complete = True
                
                # Show final status
                if view_results['success']:
                    st.success("✅ Publishing complete! Secure view created successfully.")
                else:
                    st.error("❌ Failed to create secure view. Check the errors above.")
                
                # Add a button to continue to success view
                if view_results['success']:
                    if st.button("Continue to Next Steps"):
                        st.rerun()
            else:
                st.error("❌ Failed to update Snowspace status")

else:
    # SUCCESS MODE - After publishing
    
    # Header with breadcrumb
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <p style="color: #666; margin: 0;">Create Snowspace ✓ → AI Field Analysis ✓ → Contributors ✓ → Review & Publish ✓ → <strong>Create Listing</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Success banner
    st.markdown("""
    <div style="text-align: center; padding: 2rem; background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%); color: white; border-radius: 15px; margin: 2rem 0;">
        <h1 style="margin: 0; font-size: 2.5rem;">✅ Snowspace Published!</h1>
        <p style="margin: 1rem 0 0 0; font-size: 1.3rem;">Secure view created. Now create a private listing to share with contributors.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Get view results
    view_results = st.session_state.get('view_results', {})
    
    # Manual steps section
    st.markdown("---")
    st.subheader("📋 Next Steps: Create Private Listing")
    
    # Clean the snowspace_id for the view name
    clean_id = snowspace_id.replace('.', '_').replace('-', '_')
    view_name = f"SNOWSPACE_{clean_id}_VIEW"
    full_view_name = f"UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{view_name}"
    
    st.markdown("Follow these steps to share your Snowspace with contributors:")
    
    # Step-by-step instructions
    with st.expander("📝 Step-by-Step Instructions", expanded=True):
        st.markdown("""
        ### 1️⃣ Navigate to Private Sharing
        - In Snowsight, go to **Data** → **Private Sharing**
        - Click **"+ Share"** button
        
        ### 2️⃣ Select Data to Share
        - Choose **"Share Data"**
        - Navigate to: `UNIFIEDSNOWSPACE_ORCHESTRATOR` → `SNOWSPACE`
        - Select the view:
        """)
        st.code(view_name)
        
        st.markdown("""
        ### 3️⃣ Configure the Listing
        - **Listing Title:**
        """)
        st.code(f"Snowspace: {snowspace_info['snowspace_name']}")
        
        st.markdown("- **Description:**")
        description = snowspace_info.get('description', f"Unified data space configuration for {snowspace_info['snowspace_name']}")
        st.code(f"{description}\n\nContains field definitions and metadata for the {snowspace_info['snowspace_name']} snowspace.")
        
        st.markdown("""
        ### 4️⃣ Add Consumer Accounts
        Add the following accounts:
        """)
        
        if snowspace_info['contributor_accounts']:
            # Create a text area with all accounts for easy copying
            accounts_text = '\n'.join(snowspace_info['contributor_accounts'])
            # Calculate height with minimum of 68 pixels (Streamlit requirement)
            text_height = max(68, min(len(snowspace_info['contributor_accounts']) * 25, 300))
            st.text_area("Copy these account identifiers:", value=accounts_text, height=text_height)
        else:
            st.warning("No contributor accounts configured.")
        
        st.markdown("""
        ### 5️⃣ Publish the Listing
        - Review your configuration
        - Click **"Publish"**
        - The listing will be available to all specified accounts immediately
        """)
    
    # Quick reference box
    with st.expander("⚡ Quick Copy Reference", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**View to Share:**")
            st.code(full_view_name)
            
            st.markdown("**Listing Title:**")
            st.code(f"Snowspace: {snowspace_info['snowspace_name']}")
        
        with col2:
            st.markdown("**Snowspace ID:**")
            st.code(snowspace_id)
            
            if snowspace_info['contributor_accounts']:
                st.markdown(f"**Number of Accounts:** {len(snowspace_info['contributor_accounts'])}")
    
    # What happens next
    st.markdown("---")
    st.subheader("🔮 What Happens Next")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### For Contributors:
        1. They'll see the listing in their Private Sharing area
        2. They can "Get" the listing to access the view
        3. Install the Unified Snowspace Native App
        4. Select this Snowspace using the ID
        5. Map their data fields using AI assistance
        """)
    
    with col2:
        st.markdown("""
        ### For You (OEM):
        - Monitor which contributors have accessed the listing
        - Track data quality metrics as contributors map fields
        - View unified data as contributors enable sharing
        - Manage schema updates and versioning
        """)
    
    # Benefits callout
    st.info("""
    💡 **Why Private Listings?**
    - Works across all cloud providers (AWS, Azure, GCP)
    - No complex replication setup needed
    - Contributors get instant access
    - Centralized access management
    """)
    
    # Action buttons
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🆕 Create Another Snowspace", type="primary"):
            # Clear session state
            st.session_state.current_step = "create"
            st.session_state.current_snowspace_id = None
            st.session_state.parsed_fields = []
            st.session_state.contributor_accounts = []
            st.session_state.view_results = None
            st.session_state.publish_complete = False
            st.info("Use the sidebar to navigate to the home page")
    
    with col2:
        if st.button("📊 View Dashboard", type="secondary"):
            st.session_state.current_step = "dashboard"
            st.session_state.publish_complete = False
            st.info("Use the sidebar to navigate to the dashboard")
    
    with col3:
        if st.button("🏠 Home"):
            st.session_state.publish_complete = False
            st.switch_page("pages/0_Homepage.py")
    
    # Timestamp
    st.markdown("---")
    st.caption(f"Published at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")