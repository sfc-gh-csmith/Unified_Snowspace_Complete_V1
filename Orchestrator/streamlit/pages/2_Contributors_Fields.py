"""
Contributors & Field Review Page - Manage contributor accounts and review fields
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
import json
from datetime import datetime
import time

# Configure page
st.set_page_config(
    page_title="Contributors - Unified Snowspace",
    page_icon="🤝",
    layout="wide"
)

# Initialize session state
if "contributor_accounts" not in st.session_state:
    st.session_state.contributor_accounts = []
if "current_snowspace_id" not in st.session_state:
    st.error("No Snowspace selected. Please go back to the main page.")
    st.stop()
if "parsed_fields" not in st.session_state:
    st.session_state.parsed_fields = []
if "share_results" not in st.session_state:
    st.session_state.share_results = None

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
            SELECT snowspace_id, snowspace_name, description, target_table, contributor_accounts
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES
            WHERE snowspace_id = '{snowspace_id}'
        """).collect()
        
        if result:
            row = result[0]
            return {
                'snowspace_id': row['SNOWSPACE_ID'],
                'snowspace_name': row['SNOWSPACE_NAME'],
                'description': row['DESCRIPTION'],
                'target_table': row['TARGET_TABLE'],
                'contributor_accounts': row['CONTRIBUTOR_ACCOUNTS']
            }
    except Exception as e:
        st.error(f"Error loading snowspace: {str(e)}")
    return None

def load_field_definitions(snowspace_id: str):
    """Load field definitions for the snowspace"""
    try:
        result = conn.sql(f"""
            SELECT field_name, field_category, data_type, sample_values,
                   description, synonyms, additional_context, is_required, confidence_score
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS
            WHERE snowspace_id = '{snowspace_id}'
            ORDER BY field_name
        """).collect()
        
        fields = []
        for row in result:
            fields.append({
                'field_name': row['FIELD_NAME'],
                'field_category': row['FIELD_CATEGORY'],
                'data_type': row['DATA_TYPE'],
                'sample_values': row['SAMPLE_VALUES'] or '',
                'description': row['DESCRIPTION'] or '',
                'synonyms': row['SYNONYMS'] or '',
                'additional_context': row['ADDITIONAL_CONTEXT'] or '',
                'is_required': row['IS_REQUIRED'],
                'confidence_score': row['CONFIDENCE_SCORE'] or 0.9
            })
        return fields
    except Exception as e:
        st.error(f"Error loading field definitions: {str(e)}")
        return []

def save_contributor_accounts(snowspace_id: str, contributor_accounts: list):
    """Save contributor accounts to database"""
    try:
        json_str = json.dumps(contributor_accounts) if contributor_accounts else None
        
        conn.sql("""
            UPDATE UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
            SET contributor_accounts = PARSE_JSON(?)
            WHERE snowspace_id = ?
        """, params=[json_str, snowspace_id]).collect()
        
        return True
    except Exception as e:
        st.error(f"Error saving contributor accounts: {str(e)}")
        return False

def publish_snowspace(snowspace_id: str):
    """Publish the snowspace (change status to PUBLISHED)"""
    try:
        conn.sql("""
            UPDATE UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
            SET status = 'PUBLISHED' 
            WHERE snowspace_id = ?
        """, params=[snowspace_id]).collect()
        
        return True
    except Exception as e:
        st.error(f"Error publishing Snowspace: {str(e)}")
        return False

def create_snowspace_shares(snowspace_id: str, contributor_accounts: list):
    """Create shares for all contributor accounts"""
    share_name = f"SNOWSPACE_{snowspace_id}_SHARE"
    results = {
        'success_accounts': [],
        'failed_accounts': [],
        'errors': []
    }
    
    try:
        # Step 1: Drop existing share if it exists
        try:
            conn.sql(f"DROP SHARE IF EXISTS {share_name}").collect()
        except Exception as e:
            pass  # It's ok if share doesn't exist
        
        # Step 2: Create filtered SECURE views for this specific Snowspace
        config_view_name = f"SNOWSPACE_{snowspace_id}_CONFIG"
        fields_view_name = f"SNOWSPACE_{snowspace_id}_FIELDS"
        
        # Create SECURE config view
        conn.sql(f"""
            CREATE OR REPLACE SECURE VIEW UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{config_view_name} AS 
            SELECT * FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES 
            WHERE snowspace_id = '{snowspace_id}'
        """).collect()
        
        # Create SECURE fields view
        conn.sql(f"""
            CREATE OR REPLACE SECURE VIEW UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{fields_view_name} AS 
            SELECT * FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.FIELD_DEFINITIONS 
            WHERE snowspace_id = '{snowspace_id}'
        """).collect()
        
        # Step 3: Create new share
        conn.sql(f"CREATE OR REPLACE SHARE {share_name}").collect()
        
        # Step 4: Grant usage on database and schema
        conn.sql(f"""
            GRANT USAGE ON DATABASE UNIFIEDSNOWSPACE_ORCHESTRATOR TO SHARE {share_name}
        """).collect()
        
        conn.sql(f"""
            GRANT USAGE ON SCHEMA UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE TO SHARE {share_name}
        """).collect()
        
        # Step 5: Grant SELECT on the SECURE views
        conn.sql(f"""
            GRANT SELECT ON VIEW UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{config_view_name} TO SHARE {share_name}
        """).collect()
        
        conn.sql(f"""
            GRANT SELECT ON VIEW UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.{fields_view_name} TO SHARE {share_name}
        """).collect()
        
        # Step 6: Add contributor accounts one by one
        for account in contributor_accounts:
            try:
                conn.sql(f"""
                    ALTER SHARE {share_name} ADD ACCOUNTS = {account}
                """).collect()
                results['success_accounts'].append(account)
            except Exception as e:
                error_msg = f"Failed to add {account}: {str(e)}"
                results['failed_accounts'].append(account)
                results['errors'].append(error_msg)
        
        return True, results
        
    except Exception as e:
        error_msg = f"Share creation failed: {str(e)}"
        results['errors'].append(error_msg)
        return False, results

# Load snowspace info
snowspace_info = get_snowspace_info(st.session_state.current_snowspace_id)
if not snowspace_info:
    st.error("Failed to load Snowspace information")
    st.stop()

# Load existing contributor accounts
if not st.session_state.contributor_accounts and snowspace_info['contributor_accounts']:
    try:
        existing_accounts = json.loads(str(snowspace_info['contributor_accounts']))
        st.session_state.contributor_accounts = existing_accounts if existing_accounts else []
    except:
        st.session_state.contributor_accounts = []

# Load field definitions
if not st.session_state.parsed_fields:
    st.session_state.parsed_fields = load_field_definitions(st.session_state.current_snowspace_id)

# Header with breadcrumb
st.markdown("""
<div style="margin-bottom: 2rem;">
    <p style="color: #666; margin: 0;">Create Snowspace ✓ → AI Field Analysis ✓ → <strong>Contributors & Review</strong> → Publish</p>
</div>
""", unsafe_allow_html=True)

st.title("🤝 Configure Contributors & Review")
st.markdown(f"**Snowspace:** {snowspace_info['snowspace_name']} (`{snowspace_info['snowspace_id']}`)")

# Create tabs
tab1, tab2 = st.tabs(["👥 Contributor Accounts", "📋 Field Summary"])

with tab1:
    st.markdown("""
    Configure which Snowflake accounts should have access to this Snowspace. 
    Contributors will be able to install the Native App and map their data to your schema.
    """)
    
    # Input methods section
    st.subheader("📝 Add Contributor Accounts")
    
    # Method selection
    input_method = st.radio(
        "Choose input method:",
        ["➕ Add Individual Account", "📄 Paste Multiple Accounts", "📁 Upload CSV File"],
        horizontal=True
    )
    
    if input_method == "➕ Add Individual Account":
        col1, col2 = st.columns([3, 1])
        
        with col1:
            new_account = st.text_input(
                "Account Locator",
                placeholder="e.g., ABC12345",
                help="Enter the Snowflake account locator (e.g., ABC12345)"
            )
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)  # Spacing
            if st.button("➕ Add Account", type="primary"):
                if new_account:
                    new_account = new_account.strip().upper()
                    if new_account not in st.session_state.contributor_accounts:
                        st.session_state.contributor_accounts.append(new_account)
                        st.success(f"✅ Added account: {new_account}")
                        st.rerun()
                    else:
                        st.warning(f"⚠️ Account {new_account} already added")
                else:
                    st.error("Please enter an account locator")
    
    elif input_method == "📄 Paste Multiple Accounts":
        bulk_accounts = st.text_area(
            "Paste account locators (one per line)",
            placeholder="ABC12345\nDEF67890\nGHI11111",
            height=150,
            help="Enter one account locator per line"
        )
        
        if st.button("📥 Add All Accounts", type="primary"):
            if bulk_accounts:
                lines = [line.strip().upper() for line in bulk_accounts.split('\n') if line.strip()]
                added_count = 0
                for account in lines:
                    if account and account not in st.session_state.contributor_accounts:
                        st.session_state.contributor_accounts.append(account)
                        added_count += 1
                
                if added_count > 0:
                    st.success(f"✅ Added {added_count} accounts")
                    st.rerun()
                else:
                    st.warning("⚠️ No new accounts to add")
            else:
                st.error("Please paste account locators")
    
    elif input_method == "📁 Upload CSV File":
        uploaded_file = st.file_uploader(
            "Upload CSV file with account locators",
            type=['csv'],
            help="CSV should have a column named 'account_locator' or 'account_id'"
        )
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file)
                
                # Try to find account column
                account_col = None
                for col in df.columns:
                    if 'account' in col.lower() or 'locator' in col.lower():
                        account_col = col
                        break
                
                if account_col:
                    accounts = df[account_col].dropna().astype(str).str.strip().str.upper().tolist()
                    
                    if st.button("📥 Import from CSV", type="primary"):
                        added_count = 0
                        for account in accounts:
                            if account and account not in st.session_state.contributor_accounts:
                                st.session_state.contributor_accounts.append(account)
                                added_count += 1
                        
                        if added_count > 0:
                            st.success(f"✅ Imported {added_count} accounts from CSV")
                            st.rerun()
                        else:
                            st.warning("⚠️ No new accounts to import")
                else:
                    st.error("CSV must contain a column with 'account' or 'locator' in the name")
            except Exception as e:
                st.error(f"Error processing CSV: {str(e)}")
    
    # Current contributor accounts section
    if st.session_state.contributor_accounts:
        st.markdown("---")
        st.subheader(f"👥 Configured Contributors ({len(st.session_state.contributor_accounts)})")
        
        # Display accounts in a clean table format
        for i, account in enumerate(st.session_state.contributor_accounts):
            col1, col2 = st.columns([4, 1])
            
            with col1:
                st.markdown(f"**{account}**")
                st.caption("Snowflake Account Locator")
            
            with col2:
                if st.button("🗑️ Remove", key=f"remove_{account}_{i}"):
                    st.session_state.contributor_accounts.remove(account)
                    st.success(f"✅ Removed {account}")
                    st.rerun()
            
            if i < len(st.session_state.contributor_accounts) - 1:
                st.markdown("---")
    else:
        st.info("📝 No contributor accounts configured yet. Add accounts using one of the methods above.")

with tab2:
    st.markdown("Review the field definitions that will be shared with contributors.")
    
    if st.session_state.parsed_fields:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_fields = len(st.session_state.parsed_fields)
        dimensions = sum(1 for f in st.session_state.parsed_fields if f['field_category'] == 'DIMENSION')
        time_dims = sum(1 for f in st.session_state.parsed_fields if f['field_category'] == 'TIME_DIMENSION')
        facts = sum(1 for f in st.session_state.parsed_fields if f['field_category'] == 'FACT')
        
        col1.metric("Total Fields", total_fields)
        col2.metric("Dimensions", dimensions)
        col3.metric("Time Dimensions", time_dims)
        col4.metric("Facts", facts)
        
        # Field table
        st.markdown("---")
        
        field_df = pd.DataFrame(st.session_state.parsed_fields)
        display_df = pd.DataFrame({
            'Field Name': field_df['field_name'],
            'Category': field_df['field_category'],
            'Type': field_df['data_type'],
            'Required': field_df['is_required'].map({True: '✓', False: ''}),
            'Description': field_df['description'],
            'Confidence': field_df['confidence_score'].apply(lambda x: f"{x:.0%}")
        })
        
        st.dataframe(display_df, use_container_width=True, height=400)
    else:
        st.warning("No field definitions found. Please complete the AI Field Analysis step.")

# Navigation and action buttons
st.markdown("---")
col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

with col1:
    if st.button("⬅️ Back to Field Analysis", type="secondary"):
        # Save contributor accounts before navigating
        save_contributor_accounts(st.session_state.current_snowspace_id, st.session_state.contributor_accounts)
        st.switch_page("pages/1_AI_Field_Builder.py")

with col2:
    if st.button("💾 Save Contributors"):
        if save_contributor_accounts(st.session_state.current_snowspace_id, st.session_state.contributor_accounts):
            st.success("✅ Contributor accounts saved!")
            time.sleep(1)

with col4:
    ready_to_publish = len(st.session_state.parsed_fields) > 0
    
    if st.button(
        "Review & Publish ➡️", 
        type="primary",
        disabled=not ready_to_publish,
        help="Review configuration before publishing" if ready_to_publish else "Complete field analysis before publishing"
    ):
        # Auto-save contributors before proceeding
        save_contributor_accounts(st.session_state.current_snowspace_id, st.session_state.contributor_accounts)
        
        # Navigate to publish review page
        st.switch_page("pages/3_Publish_Share.py")