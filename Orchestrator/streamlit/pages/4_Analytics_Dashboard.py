"""
Analytics Dashboard - Monitor Snowspace shares and data health
"""

import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import re

# Configure page
st.set_page_config(
    page_title="Analytics Dashboard - Unified Snowspace",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for better styling - matching other pages
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
.health-healthy { background-color: #d4edda; padding: 0.5rem; border-radius: 5px; color: #155724; }
.health-warning { background-color: #fff3cd; padding: 0.5rem; border-radius: 5px; color: #856404; }
.health-critical { background-color: #f8d7da; padding: 0.5rem; border-radius: 5px; color: #721c24; }
.health-unknown { background-color: #e2e3e5; padding: 0.5rem; border-radius: 5px; color: #383d41; }
.snowspace-header { background: linear-gradient(90deg, #f8f9fa 0%, #e9ecef 100%); padding: 1rem; border-radius: 8px; margin: 1rem 0; }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "debug_log" not in st.session_state:
    st.session_state.debug_log = []
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = None

# Connect to Snowflake - use same pattern as other pages
try:
    from snowflake.snowpark.context import get_active_session
    conn = get_active_session()
except Exception as e:
    # Fallback to builder if no active session
    try:
        import snowflake.snowpark as snowpark
        conn = snowpark.Session.builder.create()
    except Exception as e2:
        st.error(f"Failed to connect to Snowflake: {str(e)} / Fallback error: {str(e2)}")
        st.stop()

# Helper Functions
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
    
    # Keep only last 50 entries
    if len(st.session_state.debug_log) > 50:
        st.session_state.debug_log = st.session_state.debug_log[-50:]

def parse_lag_to_timedelta(lag_string):
    """Parse lag strings like '5 MINUTES', '1 HOUR', '1 DAY' to timedelta"""
    if not lag_string:
        return timedelta(days=1)  # Default to 1 day
    
    try:
        parts = lag_string.upper().split()
        if len(parts) != 2:
            return timedelta(days=1)
        
        amount = int(parts[0])
        unit = parts[1]
        
        if 'SECOND' in unit:
            return timedelta(seconds=amount)
        elif 'MINUTE' in unit:
            return timedelta(minutes=amount)
        elif 'HOUR' in unit:
            return timedelta(hours=amount)
        elif 'DAY' in unit:
            return timedelta(days=amount)
        else:
            return timedelta(days=1)
    except:
        return timedelta(days=1)

def calculate_health_status(last_update, recommended_lag, buffer_percent=20):
    """Calculate health status with buffer"""
    if not last_update or not recommended_lag:
        return 'unknown', 'No update time or lag specified'
    
    target_lag = parse_lag_to_timedelta(recommended_lag)
    warning_threshold = target_lag * (1 + buffer_percent / 100)  # 20% buffer
    
    time_since_update = datetime.now() - last_update.replace(tzinfo=None)
    
    if time_since_update <= target_lag:
        return 'healthy', f"Fresh (updated {format_time_ago(time_since_update)})"
    elif time_since_update <= warning_threshold:
        return 'warning', f"Slightly stale (updated {format_time_ago(time_since_update)})"
    else:
        return 'critical', f"Stale (updated {format_time_ago(time_since_update)})"

def format_time_ago(td):
    """Format timedelta as human readable"""
    total_seconds = int(td.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds}s ago"
    elif total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    elif total_seconds < 86400:
        return f"{total_seconds // 3600}h ago"
    else:
        return f"{total_seconds // 86400}d ago"

def match_database_to_snowspace(database_name, snowspaces):
    """Match an imported database to its source Snowspace"""
    database_upper = database_name.upper()
    
    # Try direct matching patterns
    for snowspace in snowspaces:
        snowspace_id = snowspace['snowspace_id'].upper()
        
        # Pattern 1: Database name contains Snowspace ID parts
        snowspace_parts = re.split(r'[._-]', snowspace_id)
        if len(snowspace_parts) >= 2:
            # Check if major parts of snowspace ID are in database name
            match_count = sum(1 for part in snowspace_parts[:3] if part in database_upper)
            if match_count >= 2:  # At least 2 parts match
                return snowspace
        
        # Pattern 2: Look for SNOWSPACE_*_SHARE pattern variations
        clean_id = re.sub(r'[^A-Z0-9]', '_', snowspace_id)
        expected_patterns = [
            f"SNOWSPACE_{clean_id}_SHARE",
            f"{clean_id}_SHARE",
            clean_id
        ]
        
        for pattern in expected_patterns:
            if pattern in database_upper or database_upper in pattern:
                return snowspace
    
    return None

def get_existing_snowspaces():
    """Get all created Snowspaces from the orchestrator database"""
    try:
        result = conn.sql("""
            SELECT 
                snowspace_id, 
                snowspace_name, 
                description, 
                orchestrator_account,
                recommended_target_lag,
                target_table,
                status,
                created_at,
                contributor_accounts
            FROM UNIFIEDSNOWSPACE_ORCHESTRATOR.SNOWSPACE.SNOWSPACES
            ORDER BY created_at DESC
        """).collect()
        
        snowspaces = []
        for row in result:
            contributor_accounts = []
            if row['CONTRIBUTOR_ACCOUNTS']:
                try:
                    contributor_accounts = json.loads(str(row['CONTRIBUTOR_ACCOUNTS']))
                except:
                    pass
            
            snowspaces.append({
                'snowspace_id': row['SNOWSPACE_ID'],
                'snowspace_name': row['SNOWSPACE_NAME'],
                'description': row['DESCRIPTION'],
                'orchestrator_account': row['ORCHESTRATOR_ACCOUNT'],
                'recommended_target_lag': row['RECOMMENDED_TARGET_LAG'],
                'target_table': row['TARGET_TABLE'],
                'status': row['STATUS'],
                'created_at': row['CREATED_AT'],
                'contributor_accounts': contributor_accounts
            })
        
        log_debug("SNOWSPACES", f"Found {len(snowspaces)} Snowspaces")
        return snowspaces
        
    except Exception as e:
        log_debug("SNOWSPACES", f"Error loading Snowspaces: {e}")
        return []

def get_imported_databases():
    """Get imported databases (inbound shares) using TYPE column"""
    try:
        result = conn.sql("""
            SELECT 
                database_name,
                database_owner,
                created,
                last_altered,
                comment,
                type,
                owner_role_type
            FROM INFORMATION_SCHEMA.DATABASES
            WHERE type = 'IMPORTED DATABASE'
            ORDER BY created DESC
        """).collect()
        
        databases = []
        for row in result:
            databases.append({
                'database_name': row['DATABASE_NAME'],
                'database_owner': row['DATABASE_OWNER'],
                'created': row['CREATED'],
                'last_altered': row['LAST_ALTERED'],
                'comment': row['COMMENT'],
                'type': row['TYPE'],
                'owner_role_type': row['OWNER_ROLE_TYPE']
            })
        
        log_debug("IMPORTED_DBS", f"Found {len(databases)} imported databases")
        return databases
        
    except Exception as e:
        log_debug("IMPORTED_DBS", f"Error loading imported databases: {e}")
        return []

def get_outbound_shares():
    """Simplified - return empty since we can't reliably get outbound shares"""
    log_debug("OUTBOUND", "Outbound shares detection disabled - SHOW SHARES not available")
    return []

def get_inbound_shares():
    """Get inbound shares by querying imported databases"""
    try:
        imported_dbs = get_imported_databases()
        
        shares = []
        for db in imported_dbs:
            shares.append({
                'name': f"Share for {db['database_name']}", 
                'kind': 'INBOUND',
                'owner': db['database_owner'],
                'comment': db['comment'],
                'created_on': db['created'],
                'database_name': db['database_name'],
                'origin': db['database_owner']
            })
        
        log_debug("INBOUND", f"Found {len(shares)} inbound shares from imported databases")
        return shares
        
    except Exception as e:
        log_debug("INBOUND", f"Error loading inbound shares: {e}")
        return []

def get_share_databases_with_snowspace_mapping():
    """Get share databases with Snowspace associations"""
    try:
        imported_dbs = get_imported_databases()
        snowspaces = get_existing_snowspaces()
        
        databases = []
        for db in imported_dbs:
            # Try to match to Snowspace
            matched_snowspace = match_database_to_snowspace(db['database_name'], snowspaces)
            
            databases.append({
                'database_name': db['database_name'],
                'origin': db['database_owner'],
                'share_name': f"Share for {db['database_name']}",
                'created': db['created'],
                'last_altered': db['last_altered'],
                'comment': db['comment'],
                'owner': db['database_owner'],
                'matched_snowspace': matched_snowspace,
                'snowspace_id': matched_snowspace['snowspace_id'] if matched_snowspace else None,
                'snowspace_name': matched_snowspace['snowspace_name'] if matched_snowspace else None,
                'recommended_lag': matched_snowspace['recommended_target_lag'] if matched_snowspace else None
            })
        
        log_debug("SHARE_DBS", f"Found {len(databases)} share databases, {sum(1 for d in databases if d['matched_snowspace'])} matched to Snowspaces")
        return databases
        
    except Exception as e:
        log_debug("SHARE_DBS", f"Error loading share databases: {e}")
        return []

def check_share_health_enhanced(database_name, recommended_lag=None, snowspace_name=None):
    """Enhanced health check with Snowspace-specific thresholds"""
    try:
        # Get basic table info
        tables_query = f"""
        SELECT 
            table_name,
            table_type,
            row_count,
            created,
            last_altered
        FROM {database_name}.INFORMATION_SCHEMA.TABLES
        WHERE table_type = 'BASE TABLE'
        ORDER BY last_altered DESC NULLS LAST
        """
        
        tables = conn.sql(tables_query).collect()
        
        if not tables:
            return {
                'status': 'unknown',
                'table_count': 0,
                'total_rows': 0,
                'last_update': None,
                'staleness': 'unknown',
                'health_message': 'No tables found',
                'snowspace_name': snowspace_name
            }
        
        table_count = len(tables)
        total_rows = sum(row['ROW_COUNT'] if row['ROW_COUNT'] else 0 for row in tables)
        last_update = max((row['LAST_ALTERED'] for row in tables if row['LAST_ALTERED']), default=None)
        
        # Calculate staleness with enhanced logic
        staleness = 'unknown'
        health_message = 'Status unknown'
        
        if last_update and recommended_lag:
            staleness, health_message = calculate_health_status(last_update, recommended_lag)
        elif last_update:
            # Fallback to 1-day rule
            staleness, health_message = calculate_health_status(last_update, "1 DAY")
        
        return {
            'status': 'active',
            'table_count': table_count,
            'total_rows': total_rows,
            'last_update': last_update,
            'staleness': staleness,
            'health_message': health_message,
            'snowspace_name': snowspace_name,
            'recommended_lag': recommended_lag
        }
        
    except Exception as e:
        log_debug("HEALTH", f"Error checking health for {database_name}: {e}")
        return {
            'status': 'error',
            'table_count': 0,
            'total_rows': 0,
            'last_update': None,
            'staleness': 'error',
            'health_message': f'Health check failed: {str(e)}',
            'snowspace_name': snowspace_name,
            'recommended_lag': recommended_lag
        }

def render_health_badge(staleness, health_message=""):
    """Render enhanced health status badge"""
    if staleness == 'healthy':
        return f"🟢 Healthy", "health-healthy"
    elif staleness == 'warning':
        return f"🟡 Warning", "health-warning"
    elif staleness == 'critical':
        return f"🔴 Critical", "health-critical"
    elif staleness == 'error':
        return f"❌ Error", "health-critical"
    else:
        return f"⚪ Unknown", "health-unknown"

def calculate_dashboard_metrics(snowspaces, outbound_shares, inbound_shares, share_databases):
    """Calculate enhanced dashboard metrics"""
    # Basic counts
    total_snowspaces = len(snowspaces)
    published_snowspaces = len([s for s in snowspaces if s['status'] == 'PUBLISHED'])
    total_inbound = len(inbound_shares)
    total_imported_dbs = len(share_databases)
    
    # Snowspace associations
    matched_databases = len([db for db in share_databases if db['matched_snowspace']])
    
    # Recent activity (last 30 days) - only for inbound
    thirty_days_ago = datetime.now() - timedelta(days=30)
    recent_inbound = len([s for s in inbound_shares 
                         if s['created_on'] and s['created_on'].replace(tzinfo=None) > thirty_days_ago])
    
    # Health status with enhanced checking
    health_stats = {'healthy': 0, 'warning': 0, 'critical': 0, 'error': 0, 'unknown': 0}
    
    for db in share_databases:
        health = check_share_health_enhanced(
            db['database_name'], 
            db['recommended_lag'],
            db['snowspace_name']
        )
        health_stats[health['staleness']] += 1
    
    return {
        'total_snowspaces': total_snowspaces,
        'published_snowspaces': published_snowspaces,
        'total_inbound': total_inbound,
        'total_imported_dbs': total_imported_dbs,
        'matched_databases': matched_databases,
        'recent_inbound': recent_inbound,
        'health_stats': health_stats
    }

# Header with blue gradient background
st.markdown("""
<div style="text-align: center; padding: 2rem 0; background: linear-gradient(90deg, #1f4e79 0%, #2d7dd2 100%); color: white; border-radius: 10px; margin-bottom: 2rem;">
    <h1 style="margin: 0; font-size: 2.5rem;">📊 Analytics Dashboard</h1>
    <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem; opacity: 0.9;">Monitor Snowspace shares and data health with enhanced insights</p>
</div>
""", unsafe_allow_html=True)

# Load data with caching
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_dashboard_data():
    """Load all dashboard data with caching"""
    snowspaces = get_existing_snowspaces()
    outbound_shares = get_outbound_shares()
    inbound_shares = get_inbound_shares()
    share_databases = get_share_databases_with_snowspace_mapping()
    
    return snowspaces, outbound_shares, inbound_shares, share_databases

# Load data
with st.spinner("Loading enhanced analytics data..."):
    snowspaces, outbound_shares, inbound_shares, share_databases = load_dashboard_data()
    st.session_state.last_refresh = datetime.now()

# Calculate metrics
metrics = calculate_dashboard_metrics(snowspaces, outbound_shares, inbound_shares, share_databases)

# Enhanced metrics row
col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

with col1:
    st.metric(
        label="🌟 Snowspaces",
        value=metrics['total_snowspaces'],
        delta=f"{metrics['published_snowspaces']} published"
    )

with col2:
    st.metric(
        label="📥 Inbound Shares", 
        value=metrics['total_inbound'],
        delta=f"{metrics['recent_inbound']} recent"
    )

with col3:
    st.metric(
        label="💾 Imported DBs",
        value=metrics['total_imported_dbs'],
        help="Databases created from inbound shares"
    )

with col4:
    st.metric(
        label="🔗 Matched",
        value=f"{metrics['matched_databases']}/{metrics['total_imported_dbs']}",
        help="Databases matched to Snowspaces"
    )

with col5:
    health_issues = metrics['health_stats']['critical'] + metrics['health_stats']['error']
    st.metric(
        label="🟢 Healthy",
        value=metrics['health_stats']['healthy'],
        delta=f"{health_issues} need attention" if health_issues > 0 else "all good"
    )

with col6:
    # Show current account info
    try:
        current_account = conn.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
        st.metric(
            label="🏢 Account",
            value=current_account[:8] + "..." if len(current_account) > 8 else current_account
        )
    except:
        st.metric("🏢 Account", "Unknown")

with col7:
    if st.button("🔄 Refresh", type="secondary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# Main content tabs - enhanced with Snowspace mapping
tab1, tab2, tab3, tab4, tab5 = st.tabs(["🌟 Snowspaces", "📥 Imported Databases", "🔗 Database Mapping", "💾 Share Details", "🏥 Health Monitor"])

with tab1:
    st.subheader("📋 Snowspace Overview")
    
    if snowspaces:
        # Enhanced Snowspace table
        snowspace_data = []
        for snowspace in snowspaces:
            contributor_count = len(snowspace.get('contributor_accounts', []))
            
            # Count associated databases
            associated_dbs = len([db for db in share_databases if db['snowspace_id'] == snowspace['snowspace_id']])
            
            snowspace_data.append({
                'Name': snowspace['snowspace_name'],
                'ID': snowspace['snowspace_id'],
                'Status': snowspace['status'],
                'Contributors': contributor_count,
                'Associated DBs': associated_dbs,
                'Target Lag': snowspace.get('recommended_target_lag', 'N/A'),
                'Created': snowspace['created_at'].strftime('%Y-%m-%d') if snowspace['created_at'] else 'N/A'
            })
        
        df = pd.DataFrame(snowspace_data)
        
        # Add search/filter
        search = st.text_input("🔍 Search Snowspaces", placeholder="Search by name or ID...")
        if search:
            df = df[df.apply(lambda row: search.lower() in row.astype(str).str.lower().values, axis=1)]
        
        # Display table
        st.dataframe(df, use_container_width=True, hide_index=True)
        
    else:
        st.info("No Snowspaces found. Create your first Snowspace to get started!")

with tab2:
    st.subheader("📥 Imported Databases (Enhanced)")
    st.info("Databases from inbound shares with Snowspace matching and health status.")
    
    if share_databases:
        # Enhanced table with Snowspace associations
        db_data = []
        for db in share_databases:
            # Get enhanced health status
            health = check_share_health_enhanced(
                db['database_name'], 
                db['recommended_lag'],
                db['snowspace_name']
            )
            health_status, health_class = render_health_badge(health['staleness'], health['health_message'])
            
            # Snowspace association
            snowspace_info = db['snowspace_name'] if db['snowspace_name'] else "❓ Unmatched"
            
            db_data.append({
                'Database Name': db['database_name'],
                'Snowspace': snowspace_info,
                'Owner': db['owner'],
                'Health Status': health_status,
                'Health Details': health['health_message'],
                'Target Lag': db.get('recommended_lag', 'Default') or 'Default',
                'Created': db['created'].strftime('%Y-%m-%d %H:%M') if db['created'] else 'N/A'
            })
        
        df_imported = pd.DataFrame(db_data)
        st.dataframe(df_imported, use_container_width=True, hide_index=True)
        
    else:
        st.info("No imported databases found.")

with tab3:
    st.subheader("🔗 Database-to-Snowspace Mapping")
    st.info("Shows how imported databases are matched to their source Snowspaces.")
    
    if share_databases:
        # Group by Snowspace
        snowspace_groups = {}
        unmatched_dbs = []
        
        for db in share_databases:
            if db['matched_snowspace']:
                snowspace_id = db['snowspace_id']
                if snowspace_id not in snowspace_groups:
                    snowspace_groups[snowspace_id] = {
                        'snowspace': db['matched_snowspace'],
                        'databases': []
                    }
                snowspace_groups[snowspace_id]['databases'].append(db)
            else:
                unmatched_dbs.append(db)
        
        # Display matched Snowspaces
        for snowspace_id, group in snowspace_groups.items():
            snowspace = group['snowspace']
            databases = group['databases']
            
            st.markdown(f"""
            <div class="snowspace-header">
                <h4>🌟 {snowspace['snowspace_name']}</h4>
                <p><strong>ID:</strong> {snowspace_id} | <strong>Target Lag:</strong> {snowspace['recommended_target_lag']} | <strong>Databases:</strong> {len(databases)}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Show associated databases
            for db in databases:
                health = check_share_health_enhanced(
                    db['database_name'], 
                    db['recommended_lag'],
                    db['snowspace_name']
                )
                health_status, health_class = render_health_badge(health['staleness'])
                
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    st.markdown(f"**📊 {db['database_name']}**")
                    st.caption(f"Owner: {db['owner']}")
                with col2:
                    st.markdown(f"<div class='{health_class}'>{health_status}</div>", unsafe_allow_html=True)
                with col3:
                    st.caption(health['health_message'])
        
        # Show unmatched databases
        if unmatched_dbs:
            st.markdown("---")
            st.markdown("### ❓ Unmatched Databases")
            st.warning(f"Found {len(unmatched_dbs)} databases that couldn't be matched to Snowspaces:")
            
            for db in unmatched_dbs:
                st.markdown(f"• **{db['database_name']}** (Owner: {db['owner']})")
            
            st.info("💡 Tip: Ensure share names follow the pattern `SNOWSPACE_{id}_SHARE` for automatic matching.")
    else:
        st.info("No databases to map.")

with tab4:
    st.subheader("💾 Share Database Details")
    st.info("Detailed analysis of all imported databases.")
    
    if share_databases:
        # Show databases in expandable format with enhanced details
        for db in share_databases:
            # Get enhanced health info
            health = check_share_health_enhanced(
                db['database_name'], 
                db['recommended_lag'],
                db['snowspace_name']
            )
            health_status, health_class = render_health_badge(health['staleness'])
            
            snowspace_indicator = "🌟" if db['matched_snowspace'] else "❓"
            
            with st.expander(f"{snowspace_indicator} {db['database_name']} - {health_status}", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Database Information:**")
                    st.markdown(f"- **Name:** `{db['database_name']}`")
                    st.markdown(f"- **Origin:** `{db['origin']}`")
                    st.markdown(f"- **Created:** {db['created'].strftime('%Y-%m-%d %H:%M') if db['created'] else 'N/A'}")
                    if db['comment']:
                        st.markdown(f"- **Comment:** {db['comment']}")
                    
                    if db['matched_snowspace']:
                        st.markdown("**Snowspace Association:**")
                        st.markdown(f"- **Snowspace:** {db['snowspace_name']}")
                        st.markdown(f"- **ID:** `{db['snowspace_id']}`")
                        st.markdown(f"- **Target Lag:** {db['recommended_lag']}")
                
                with col2:
                    st.markdown("**Enhanced Health Check:**")
                    
                    col2a, col2b = st.columns(2)
                    with col2a:
                        st.metric("Tables", health['table_count'])
                        st.metric("Rows", f"{health['total_rows']:,}")
                    
                    with col2b:
                        st.markdown(f"<div class='{health_class}'>{health_status}</div>", unsafe_allow_html=True)
                        st.caption(health['health_message'])
                    
                    if health['last_update']:
                        st.markdown(f"**Last Update:** {health['last_update'].strftime('%Y-%m-%d %H:%M')}")
                    
                    if health.get('recommended_lag'):
                        st.markdown(f"**Target Frequency:** {health['recommended_lag']}")
    else:
        st.info("No share databases detected.")

with tab5:
    st.subheader("🏥 Enhanced Health Monitoring")
    
    # Health overview with Snowspace grouping
    if share_databases:
        # Health summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        
        health_stats = metrics['health_stats']
        col1.metric("🟢 Healthy", health_stats['healthy'])
        col2.metric("🟡 Warning", health_stats['warning'])
        col3.metric("🔴 Critical", health_stats['critical'])
        col4.metric("❌ Error", health_stats['error'])
        col5.metric("⚪ Unknown", health_stats['unknown'])
        
        st.markdown("---")
        
        # Group health by Snowspace
        st.markdown("### 🌟 Health by Snowspace")
        
        # Get health data grouped by Snowspace
        snowspace_health = {}
        unmatched_health = []
        
        for db in share_databases:
            health = check_share_health_enhanced(
                db['database_name'], 
                db['recommended_lag'],
                db['snowspace_name']
            )
            
            if db['matched_snowspace']:
                snowspace_id = db['snowspace_id']
                if snowspace_id not in snowspace_health:
                    snowspace_health[snowspace_id] = {
                        'name': db['snowspace_name'],
                        'databases': []
                    }
                snowspace_health[snowspace_id]['databases'].append({
                    'db': db,
                    'health': health
                })
            else:
                unmatched_health.append({
                    'db': db,
                    'health': health
                })
        
        # Display Snowspace health groups
        for snowspace_id, group in snowspace_health.items():
            databases = group['databases']
            
            # Calculate group health stats
            group_stats = {'healthy': 0, 'warning': 0, 'critical': 0, 'error': 0, 'unknown': 0}
            for db_health in databases:
                group_stats[db_health['health']['staleness']] += 1
            
            # Health indicator for the group
            if group_stats['critical'] > 0 or group_stats['error'] > 0:
                group_indicator = "🔴"
                group_status = "Issues Found"
            elif group_stats['warning'] > 0:
                group_indicator = "🟡"  
                group_status = "Warnings"
            elif group_stats['healthy'] > 0:
                group_indicator = "🟢"
                group_status = "Healthy"
            else:
                group_indicator = "⚪"
                group_status = "Unknown"
            
            with st.expander(f"{group_indicator} {group['name']} - {group_status} ({len(databases)} databases)"):
                # Show group summary
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("Healthy", group_stats['healthy'])
                col2.metric("Warning", group_stats['warning'])
                col3.metric("Critical", group_stats['critical'])
                col4.metric("Error", group_stats['error'])
                col5.metric("Unknown", group_stats['unknown'])
                
                # Show individual database health
                st.markdown("**Database Details:**")
                for db_health in databases:
                    db = db_health['db']
                    health = db_health['health']
                    health_status, health_class = render_health_badge(health['staleness'])
                    
                    col1, col2, col3 = st.columns([2, 1, 2])
                    with col1:
                        st.markdown(f"**{db['database_name']}**")
                    with col2:
                        st.markdown(f"<div class='{health_class}' style='text-align: center;'>{health_status}</div>", unsafe_allow_html=True)
                    with col3:
                        st.caption(health['health_message'])
        
        # Show unmatched databases health
        if unmatched_health:
            with st.expander(f"❓ Unmatched Databases ({len(unmatched_health)} databases)"):
                st.warning("These databases couldn't be matched to Snowspaces, using default 1-day lag threshold:")
                
                for db_health in unmatched_health:
                    db = db_health['db']
                    health = db_health['health']
                    health_status, health_class = render_health_badge(health['staleness'])
                    
                    col1, col2, col3 = st.columns([2, 1, 2])
                    with col1:
                        st.markdown(f"**{db['database_name']}**")
                    with col2:
                        st.markdown(f"<div class='{health_class}' style='text-align: center;'>{health_status}</div>", unsafe_allow_html=True)
                    with col3:
                        st.caption(health['health_message'])
        
        # Critical issues alert
        critical_issues = [db for db in share_databases 
                          if check_share_health_enhanced(db['database_name'], db['recommended_lag'], db['snowspace_name'])['staleness'] in ['critical', 'error']]
        
        if critical_issues:
            st.error(f"🚨 {len(critical_issues)} databases need immediate attention!")
            
            for db in critical_issues:
                health = check_share_health_enhanced(db['database_name'], db['recommended_lag'], db['snowspace_name'])
                snowspace_info = f" ({db['snowspace_name']})" if db['snowspace_name'] else ""
                st.markdown(f"**{db['database_name']}{snowspace_info}**: {health['health_message']}")
        else:
            st.success("✅ All databases are in good health!")
        
    else:
        st.info("No shared databases to monitor.")

# Enhanced sidebar with better controls and debug info
with st.sidebar:
    st.header("🔧 Enhanced Dashboard Controls")
    
    # Refresh info
    if st.session_state.last_refresh:
        st.info(f"Last refreshed: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
    
    # Quick metrics
    with st.expander("📊 Quick Stats"):
        if share_databases:
            matched_pct = int((metrics['matched_databases'] / metrics['total_imported_dbs']) * 100) if metrics['total_imported_dbs'] > 0 else 0
            st.metric("Match Rate", f"{matched_pct}%")
            
            healthy_pct = int((metrics['health_stats']['healthy'] / metrics['total_imported_dbs']) * 100) if metrics['total_imported_dbs'] > 0 else 0
            st.metric("Health Rate", f"{healthy_pct}%")
    
    # System info
    with st.expander("ℹ️ System Info"):
        try:
            current_account = conn.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
            st.markdown(f"**Account:** `{current_account}`")
        except:
            st.markdown("**Account:** Unknown")
            
        st.markdown(f"**Snowspaces:** {len(snowspaces)}")
        st.markdown(f"**Imported DBs:** {len(share_databases)}")
        st.markdown(f"**Matched:** {metrics['matched_databases']}")
        st.markdown(f"**Health Issues:** {metrics['health_stats']['critical'] + metrics['health_stats']['error']}")
    
    # Debug logs
    if st.session_state.debug_log:
        with st.expander("📜 Debug Logs", expanded=False):
            for log in reversed(st.session_state.debug_log[-20:]):
                st.info(f"[{log['time']}] {log['category']}: {log['message']}")
        
        if st.button("Clear Logs"):
            st.session_state.debug_log = []
            st.rerun()

# Footer
st.markdown("---")
st.caption("💡 Enhanced with Snowspace-specific health thresholds and intelligent database matching. Health status uses 20% buffer on target lag times.")