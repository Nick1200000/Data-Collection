#!/usr/bin/env python3

import boto3
import yaml
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from io import BytesIO
import json

class S3Analyzer:
    def __init__(self, config_path):
        self.config = self._load_config(config_path)
        self.s3_client = self._initialize_s3_client()
        self.cache = {}
        
    def _load_config(self, config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _initialize_s3_client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.config['aws']['access_key_id'],
            aws_secret_access_key=self.config['aws']['secret_access_key'],
            region_name=self.config['aws']['region']
        )

    def get_bucket_metrics(self, bucket_name, prefix):
        """Get detailed metrics for a bucket/prefix"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            total_size = 0
            total_objects = 0
            objects = []
            size_distribution = {}
            last_modified_dates = []
            storage_classes = {}
            
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        size_mb = obj['Size'] / (1024 * 1024)
                        total_size += size_mb
                        total_objects += 1
                        last_modified = obj['LastModified']
                        last_modified_dates.append(last_modified)
                        storage_class = obj.get('StorageClass', 'STANDARD')
                        
                        # Update storage class distribution
                        storage_classes[storage_class] = storage_classes.get(storage_class, 0) + 1
                        
                        # Categorize by size
                        size_category = self._get_size_category(size_mb)
                        size_distribution[size_category] = size_distribution.get(size_category, 0) + 1
                        
                        objects.append({
                            'Key': obj['Key'],
                            'Size (MB)': round(size_mb, 2),
                            'Last Modified': last_modified.strftime('%Y-%m-%d %H:%M:%S'),
                            'Storage Class': storage_class
                        })

            return {
                'total_size_mb': round(total_size, 2),
                'total_objects': total_objects,
                'objects': objects,
                'size_distribution': size_distribution,
                'storage_class_distribution': storage_classes,
                'last_modified_dates': last_modified_dates,
                'average_size_mb': round(total_size / total_objects, 2) if total_objects > 0 else 0
            }
        except Exception as e:
            st.error(f"Error analyzing bucket: {str(e)}")
            return None

    def _get_size_category(self, size_mb):
        """Categorize objects by size"""
        if size_mb < 1:
            return "Small (<1MB)"
        elif size_mb < 10:
            return "Medium (1-10MB)"
        elif size_mb < 100:
            return "Large (10-100MB)"
        else:
            return "Very Large (>100MB)"

    def analyze_bucket(self, bucket_name, prefix):
        """Analyze bucket with caching"""
        cache_key = f"{bucket_name}:{prefix}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        results = self.get_bucket_metrics(bucket_name, prefix)
        if results:
            self.cache[cache_key] = results
        return results
