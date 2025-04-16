#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from s3_analysis import S3Analyzer # Make sure s3_analysis.py exists and is correct
import time
import yaml # Added import for yaml

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

# Function to load config safely
def load_config(config_path='config.yaml'):
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        st.error(f"Configuration file '{config_path}' not found.")
        return None
    except Exception as e:
        st.error(f"Error loading configuration: {e}")
        return None

# Load configuration
config = load_config()

# Initialize session state only if config loaded successfully
if config:
    if 'analyzer' not in st.session_state:
        try:
            st.session_state.analyzer = S3Analyzer('config.yaml')
        except Exception as e:
            st.error(f"Failed to initialize S3 Analyzer: {e}")
            # Stop the app if analyzer can't be initialized
            st.stop()
    if 'last_analysis' not in st.session_state:
        st.session_state.last_analysis = None
else:
    # Stop the app if config loading failed
    st.stop()

# Sidebar
with st.sidebar:
    st.title("S3 Analytics Dashboard")
    st.markdown("---")

    # Get default values from config, handle potential KeyError
    default_bucket = config.get('bucket', '') # Use .get for safe access
    default_prefix = config.get('prefix', '') # Use .get for safe access

    # Bucket selection
    bucket_name_input = st.text_input("Bucket Name", value=default_bucket)
    prefix_input = st.text_input("Prefix", value=default_prefix)

    # Analysis controls
    if st.button("Run Analysis", type="primary"):
        # Strip whitespace from inputs
        bucket_name = bucket_name_input.strip()
        prefix = prefix_input.strip()

        if not bucket_name:
            st.error("Bucket name cannot be empty.")
        else:
            with st.spinner('Analyzing bucket...'):
                start_time = time.time()
                # Ensure analyzer is available before calling analyze_bucket
                if 'analyzer' in st.session_state:
                    try:
                        results = st.session_state.analyzer.analyze_bucket(bucket_name, prefix)
                        if results:
                            st.session_state.last_analysis = {
                                'results': results,
                                'timestamp': datetime.now(),
                                'duration': time.time() - start_time
                            }
                        else:
                            # Error is already displayed by S3Analyzer
                            pass # st.error("Failed to analyze bucket. Check logs for details.")
                    except Exception as e:
                         st.error(f"An unexpected error occurred during analysis: {e}")
                         st.session_state.last_analysis = None # Clear previous results on error
                else:
                    st.error("S3 Analyzer not initialized. Please check configuration.")


# Main content
st.title("S3 Bucket Analysis Dashboard")

# Display results only if analysis was successful and results exist
if st.session_state.get('last_analysis') and st.session_state.last_analysis.get('results'):
    results = st.session_state.last_analysis['results']

    try:
        # --- Metrics Cards ---
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Size", f"{results.get('total_size_mb', 0):.2f} MB") # Added formatting
        with col2:
            st.metric("Total Objects", f"{results.get('total_objects', 0):,}") # Added comma formatting
        with col3:
            st.metric("Average Size", f"{results.get('average_size_mb', 0):.2f} MB") # Added formatting
        with col4:
            st.metric("Analysis Time", f"{st.session_state.last_analysis.get('duration', 0):.2f}s")

        st.markdown("---") # Separator

        # --- Charts ---
        charts_col1, charts_col2 = st.columns(2)

        with charts_col1:
            st.subheader("Object Size Distribution")
            size_dist_data = results.get('size_distribution')
            if size_dist_data and sum(size_dist_data.values()) > 0:
                size_df = pd.DataFrame({
                    'Category': list(size_dist_data.keys()),
                    'Count': list(size_dist_data.values())
                })
                # Define order for categories
                size_order = ["Small (<1MB)", "Medium (1-10MB)", "Large (10-100MB)", "Very Large (>100MB)"]
                size_df['Category'] = pd.Categorical(size_df['Category'], categories=size_order, ordered=True)
                size_df = size_df.sort_values('Category')

                fig_size = px.pie(size_df, values='Count', names='Category',
                                  title='Object Count by Size Category',
                                  hole=0.3) # Donut chart
                fig_size.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_size, use_container_width=True)
            else:
                st.info("No size distribution data available.")

        with charts_col2:
            st.subheader("Storage Class Distribution")
            storage_dist_data = results.get('storage_class_distribution')
            if storage_dist_data and sum(storage_dist_data.values()) > 0:
                storage_df = pd.DataFrame({
                    'Class': list(storage_dist_data.keys()),
                    'Count': list(storage_dist_data.values())
                }).sort_values('Count', ascending=False) # Sort by count

                fig_storage = px.bar(storage_df, x='Class', y='Count',
                                     title='Object Count by Storage Class',
                                     text_auto=True) # Show values on bars
                fig_storage.update_layout(xaxis_title="Storage Class", yaxis_title="Number of Objects")
                st.plotly_chart(fig_storage, use_container_width=True)
            else:
                st.info("No storage class data available.")

        st.markdown("---") # Separator

        # --- Objects Table with Filters ---
        st.subheader("Object Details")
        objects_data = results.get('objects')

        if objects_data:
            df = pd.DataFrame(objects_data)
            # Convert Last Modified safely, coercing errors
            df['Last Modified'] = pd.to_datetime(df['Last Modified'], errors='coerce')
            df = df.dropna(subset=['Last Modified']) # Remove rows where date conversion failed

            # --- Filters ---
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            with filter_col1:
                # Use slider for size filtering
                min_size_filter, max_size_filter = st.slider(
                    "Filter by Size (MB)",
                    min_value=0.0,
                    max_value=float(df['Size (MB)'].max()) if not df.empty else 100.0, # Dynamic max or default
                    value=(0.0, float(df['Size (MB)'].max()) if not df.empty else 100.0),
                    step=0.1
                )
            with filter_col2:
                 # Use min/max dates from data if available, otherwise default
                min_date = df['Last Modified'].min().date() if not df.empty else (datetime.now() - timedelta(days=30)).date()
                max_date = df['Last Modified'].max().date() if not df.empty else datetime.now().date()

                start_date, end_date = st.date_input(
                    "Filter by Last Modified Date",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                 )
            with filter_col3:
                # Filter by storage class
                available_classes = ['All'] + sorted(df['Storage Class'].unique())
                selected_class = st.selectbox("Filter by Storage Class", options=available_classes)

            # Apply filters
            filtered_df = df[
                (df['Size (MB)'] >= min_size_filter) &
                (df['Size (MB)'] <= max_size_filter) &
                (df['Last Modified'].dt.date >= start_date) &
                (df['Last Modified'].dt.date <= end_date)
            ]
            if selected_class != 'All':
                filtered_df = filtered_df[filtered_df['Storage Class'] == selected_class]

            # Display filtered table
            st.dataframe(
                filtered_df,
                column_config={
                    "Key": st.column_config.TextColumn(label="Object Key"),
                    "Size (MB)": st.column_config.NumberColumn(
                        label="Size (MB)",
                        format="%.2f MB"
                    ),
                    "Last Modified": st.column_config.DatetimeColumn(
                        label="Last Modified",
                        format="YYYY-MM-DD HH:mm:ss"
                    ),
                    "Storage Class": st.column_config.TextColumn(label="Storage Class"),
                },
                use_container_width=True,
                hide_index=True # Hide the default index column
            )

            st.caption(f"Showing {len(filtered_df):,} of {len(df):,} objects")

            # --- Export Options ---
            st.subheader("Export Filtered Data")
            export_col1, export_col2 = st.columns(2)
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            json_data = filtered_df.to_json(orient='records', date_format='iso').encode('utf-8')

            with export_col1:
                st.download_button(
                    label="💾 Download as CSV",
                    data=csv,
                    file_name=f"{bucket_name}_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key='csv_download' # Added key for unique widget
                )
            with export_col2:
                st.download_button(
                    label="💾 Download as JSON",
                    data=json_data,
                    file_name=f"{bucket_name}_analysis_{datetime.now().strftime('%Y%m%d')}.json",
                    mime="application/json",
                    key='json_download' # Added key for unique widget
                )
        else:
            st.info("No object data available to display or filter.")

    except KeyError as e:
        st.error(f"Error accessing results data: Missing key {e}. Please ensure S3 analysis ran correctly.")
    except Exception as e:
        st.error(f"An unexpected error occurred while displaying results: {e}")
        st.exception(e) # Show traceback for debugging

elif st.session_state.get('last_analysis') is not None and st.session_state.last_analysis.get('results') is None:
     # Case where analysis ran but failed (error already shown)
     st.warning("Analysis failed. Please check the error message above and your configuration.")
else:
    # Initial state before analysis is run
    st.info("👈 Enter bucket details in the sidebar and click 'Run Analysis' to begin.")
