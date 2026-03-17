import streamlit as st
import base64
from io import BytesIO

# Configure page - hide sidebar
st.set_page_config(
    page_title="Unified Snowspace - Contributor",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

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


# Call it right after st.set_page_config()
display_app_banner()

# Hide the sidebar
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Main content - Improved design
st.markdown(
    """
    <h1 style='text-align: center; color: #1e3d59; margin-bottom: 0;'>
        Contributor Portal
    </h1>
    <p style='text-align: center; color: #3e5c76; font-size: 1.3em; margin-top: 0;'>
        AI-Powered Data Standardization in Minutes
    </p>
    """,
    unsafe_allow_html=True
)

# Add some space
st.markdown("<br>", unsafe_allow_html=True)

# Main content area with constrained width for better readability
col1, col2, col3 = st.columns([1, 3, 1])

with col2:
    # Show status if returning user (moved up for better visibility)
    if 'selected_mapping_id' in st.session_state:
        st.success(f"✅ Welcome back! You have a mapping in progress: `{st.session_state['selected_mapping_id']}`")
        if st.button("Continue to Transformations →", type="secondary", use_container_width=True):
            st.switch_page("pages/02_Field_Transformer.py")
        st.markdown("---")
        st.markdown("##### Or start a new mapping:")
    
    # Process cards side by side
    st.markdown("### How It Works")
    
    step1, step2 = st.columns(2)
    
    with step1:
        st.info(
            """
            **📊 Step 1: Field Mapping**
            
            Our AI analyzes your data structure and intelligently maps your fields to the target schema.
            
            • Auto-detection of field matches  
            • Confidence scoring  
            • Manual override when needed
            """
        )
    
    with step2:
        st.success(
            """
            **🔄 Step 2: Transformations**
            
            Automatically generate SQL to transform your data formats to match requirements.
            
            • Smart format conversion  
            • Data type handling  
            • Live preview before applying
            """
        )
    
    # Benefits section
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Metrics in a subtle way
    met1, met2, met3 = st.columns(3)
    
    with met1:
        st.markdown(
            """
            <div style='text-align: center; background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem;'>
                <h3 style='color: #1e3d59; margin: 0;'>5 mins</h3>
                <p style='margin: 0; color: #666;'>Average time</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with met2:
        st.markdown(
            """
            <div style='text-align: center; background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem;'>
                <h3 style='color: #1e3d59; margin: 0;'>95%+</h3>
                <p style='margin: 0; color: #666;'>Accuracy</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with met3:
        st.markdown(
            """
            <div style='text-align: center; background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem;'>
                <h3 style='color: #1e3d59; margin: 0;'>No SQL</h3>
                <p style='margin: 0; color: #666;'>Required</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    # Call to action
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # Center the button
    if st.button("🚀 Start Field Mapping", type="primary", use_container_width=True, help="Begin connecting your data"):
            # Clear any existing mapping state to start fresh
            if 'mapping_config' in st.session_state:
                del st.session_state['mapping_config']
            if 'auto_analyzed' in st.session_state:
                del st.session_state['auto_analyzed']
            st.switch_page("pages/00_Connect_Snowspaces.py")

# Footer
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #888; font-size: 0.9em;'>
        <p>🔒 All data processing happens securely within your Snowflake environment</p>
    </div>
    """,
    unsafe_allow_html=True
)