 

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from s3_analysis import S3Analyzer
import time

# Page config
st.set_page_config(
    page_title="S3 Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px;
    }
    .stProgress > div > div > div > div {
        background-color: #1f77b4;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'analyzer' not in st.session_state:
    st.session_state.analyzer = S3Analyzer('config.yaml')
if 'last_analysis' not in st.session_state:
    st.session_state.last_analysis = None

# Sidebar
with st.sidebar:
    st.title("S3 Analytics Dashboard")
    st.markdown("---")
    
    # Bucket selection
    bucket_name = st.text_input("Bucket Name", value=st.session_state.analyzer.config['bucket'])
    prefix = st.text_input("Prefix", value=st.session_state.analyzer.config['prefix'])
    
    # Analysis controls
    if st.button("Run Analysis", type="primary"):
        with st.spinner('Analyzing bucket...'):
            start_time = time.time()
            results = st.session_state.analyzer.analyze_bucket(bucket_name, prefix)
            st.session_state.last_analysis = {
                'results': results,
                'timestamp': datetime.now(),
                'duration': time.time() - start_time
            }

# Main content
st.title("S3 Bucket Analysis Dashboard")

if st.session_state.last_analysis:
    results = st.session_state.last_analysis['results']
    
    # Metrics cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Size", f"{results['total_size_mb']} MB")
    with col2:
        st.metric("Total Objects", results['total_objects'])
    with col3:
        st.metric("Average Size", f"{results['average_size_mb']} MB")
    with col4:
        st.metric("Analysis Time", f"{st.session_state.last_analysis['duration']:.2f}s")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Size distribution pie chart
        st.subheader("Size Distribution")
        size_df = pd.DataFrame({
            'Category': list(results['size_distribution'].keys()),
            'Count': list(results['size_distribution'].values())
        })
        fig = px.pie(size_df, values='Count', names='Category', 
                    title='Object Size Distribution')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Storage class distribution
        st.subheader("Storage Class Distribution")
        storage_df = pd.DataFrame({
            'Class': list(results['storage_class_distribution'].keys()),
            'Count': list(results['storage_class_distribution'].values())
        })
        fig = px.bar(storage_df, x='Class', y='Count',
                    title='Storage Class Distribution')
        st.plotly_chart(fig, use_container_width=True)
    
    # Objects table with filters
    st.subheader("Objects")
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_size = st.number_input("Minimum Size (MB)", min_value=0.0, 
                                 value=0.0, step=0.1)
    with col2:
        date_range = st.date_input("Date Range", 
                                 value=(datetime.now() - timedelta(days=30), 
                                       datetime.now()))
    
    # Filter objects
    df = pd.DataFrame(results['objects'])
    df['Last Modified'] = pd.to_datetime(df['Last Modified'])
    filtered_df = df[
        (df['Size (MB)'] >= min_size) &
        (df['Last Modified'].dt.date >= date_range[0]) &
        (df['Last Modified'].dt.date <= date_range[1])
    ]
    
    # Display filtered table
    st.dataframe(
        filtered_df,
        column_config={
            "Size (MB)": st.column_config.NumberColumn(
                format="%.2f MB"
            ),
            "Last Modified": st.column_config.DatetimeColumn(
                format="YYYY-MM-DD HH:mm:ss"
            )
        },
        use_container_width=True
    )
    
    # Export options
    st.subheader("Export Data")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Export to CSV"):
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="s3_analysis.csv",
                mime="text/csv"
            )
    with col2:
        if st.button("Export to JSON"):
            json_data = filtered_df.to_json(orient='records')
            st.download_button(
                label="Download JSON",
                data=json_data,
                file_name="s3_analysis.json",
                mime="application/json"
            )

else:
    st.info("Click 'Run Analysis' in the sidebar to start analyzing your S3 bucket.")
