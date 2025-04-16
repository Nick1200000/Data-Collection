#!/usr/bin/env python3

import boto3
import yaml
import streamlit as st # Keep Streamlit import for st.error
import time
import logging
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError, ProfileNotFound

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class S3Analyzer:
    """
    Analyzes S3 bucket contents providing metrics like size, object count,
    size distribution, and storage class distribution.
    """
    def __init__(self, config_path='config.yaml'):
        """
        Initializes the S3Analyzer.

        Args:
            config_path (str): Path to the configuration YAML file.
        """
        self.config = self._load_config(config_path)
        self.s3_client = self._initialize_s3_client()
        self.cache = {} # Simple in-memory cache

        # Stop initialization if config or client failed
        if not self.config or not self.s3_client:
            raise ValueError("Failed to initialize S3Analyzer due to configuration or S3 client error.")

    def _load_config(self, config_path):
        """Loads configuration from YAML file safely."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logging.info(f"Configuration loaded successfully from {config_path}")
                 # Basic validation
                if 'aws' not in config or not all(k in config['aws'] for k in ['region', 'access_key_id', 'secret_access_key']):
                    logging.error("AWS configuration section is missing or incomplete in config file.")
                    st.error("AWS configuration section is missing or incomplete in config file.")
                    return None
                return config
        except FileNotFoundError:
            logging.error(f"Configuration file not found at {config_path}")
            st.error(f"Configuration file '{config_path}' not found.")
            return None
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file {config_path}: {e}")
            st.error(f"Error parsing configuration file: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred loading configuration: {e}")
            st.error(f"An unexpected error occurred loading configuration: {e}")
            return None

    def _initialize_s3_client(self):
        """Initializes and returns the Boto3 S3 client."""
        if not self.config: # Ensure config loaded
             return None
        try:
            logging.info(f"Initializing Boto3 S3 client for region {self.config['aws']['region']}")
            # Ensure credentials are strings and not empty
            access_key = self.config['aws'].get('access_key_id')
            secret_key = self.config['aws'].get('secret_access_key')
            region = self.config['aws'].get('region')

            if not all([access_key, secret_key, region]):
                 logging.error("AWS credentials or region missing in configuration.")
                 st.error("AWS Access Key ID, Secret Access Key, or Region missing in configuration.")
                 return None

            client = boto3.client(
                's3',
                aws_access_key_id=str(access_key),
                aws_secret_access_key=str(secret_key),
                region_name=str(region)
            )
            # Test credentials line removed:
            # client.list_buckets() # <-- THIS LINE IS REMOVED
            logging.info("Boto3 S3 client initialized successfully.")
            return client
        except (NoCredentialsError, PartialCredentialsError):
            logging.error("AWS credentials not found or incomplete.")
            st.error("AWS credentials not found, incomplete, or invalid. Please check config.yaml.")
            return None
        except ProfileNotFound as e:
             logging.error(f"AWS profile specified in config not found: {e}")
             st.error(f"AWS profile specified in config not found: {e}")
             return None
        except ClientError as e:
            # Handle specific client errors like invalid credentials
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'InvalidClientTokenId' or error_code == 'SignatureDoesNotMatch':
                 logging.error(f"Invalid AWS credentials provided: {error_code}")
                 st.error("Invalid AWS credentials provided. Please check config.yaml.")
            elif error_code == 'AccessDenied':
                 logging.error(f"Initial Access Denied checking S3 client: {e}")
                 st.error(f"Access Denied during S3 client initialization. Check basic S3 permissions for the user.")
            else:
                 logging.error(f"Failed to initialize S3 client due to ClientError: {e}")
                 st.error(f"Failed to initialize S3 client: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred initializing S3 client: {e}")
            st.error(f"An unexpected error occurred initializing S3 client: {e}")
            return None

    def _get_size_category(self, size_mb):
        """Categorizes objects by size in MB."""
        if size_mb < 1:
            return "Small (<1MB)"
        elif size_mb < 10:
            return "Medium (1-10MB)"
        elif size_mb < 100:
            return "Large (10-100MB)"
        else:
            return "Very Large (>100MB)"

    def get_bucket_metrics(self, bucket_name, prefix):
        """
        Retrieves and calculates detailed metrics for objects within a
        specified S3 bucket and prefix.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix (folder path) within the bucket.

        Returns:
            dict: A dictionary containing calculated metrics, or None if an error occurs.
                  Keys include: 'total_size_mb', 'total_objects', 'objects' (list),
                  'size_distribution', 'storage_class_distribution', 'average_size_mb'.
        """
        if not self.s3_client:
            st.error("S3 client not initialized. Cannot analyze bucket.")
            return None

        logging.info(f"Starting analysis for bucket: '{bucket_name}', prefix: '{prefix}'")
        start_time = time.time()
        total_size_bytes = 0
        total_objects = 0
        objects_list = []
        size_distribution = {}
        storage_class_distribution = {}

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            processed_pages = 0
            for page in pages:
                processed_pages += 1
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Size'] == 0 and obj['Key'].endswith('/'):
                            continue # Skip potential folder placeholders

                        size_bytes = obj['Size']
                        size_mb = size_bytes / (1024 * 1024)
                        total_size_bytes += size_bytes
                        total_objects += 1
                        storage_class = obj.get('StorageClass', 'STANDARD')

                        storage_class_distribution[storage_class] = storage_class_distribution.get(storage_class, 0) + 1
                        size_category = self._get_size_category(size_mb)
                        size_distribution[size_category] = size_distribution.get(size_category, 0) + 1

                        last_modified_str = "N/A"
                        if 'LastModified' in obj:
                            try:
                                # Ensure LastModified is a datetime object before formatting
                                if isinstance(obj['LastModified'], datetime):
                                     last_modified_str = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                                else:
                                     logging.warning(f"LastModified for key {obj['Key']} is not a datetime object: {type(obj['LastModified'])}")
                            except AttributeError:
                                logging.warning(f"Could not format LastModified for key: {obj['Key']}")
                            except Exception as dt_err:
                                logging.warning(f"Unexpected error formatting LastModified for key {obj['Key']}: {dt_err}")


                        objects_list.append({
                            'Key': obj['Key'],
                            'Size (MB)': round(size_mb, 2),
                            'Last Modified': last_modified_str,
                            'Storage Class': storage_class
                        })
                if processed_pages % 50 == 0:
                     logging.info(f"Processed {processed_pages} pages for {bucket_name}/{prefix}...")

            total_size_mb = total_size_bytes / (1024 * 1024)
            average_size_mb = (total_size_mb / total_objects) if total_objects > 0 else 0
            analysis_duration = time.time() - start_time
            logging.info(f"Analysis complete for {bucket_name}/{prefix}. "
                         f"Objects: {total_objects}, Total Size: {total_size_mb:.2f} MB. "
                         f"Duration: {analysis_duration:.2f}s")

            return {
                'total_size_mb': round(total_size_mb, 2),
                'total_objects': total_objects,
                'average_size_mb': round(average_size_mb, 2),
                'size_distribution': size_distribution,
                'storage_class_distribution': storage_class_distribution,
                'objects': objects_list,
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            logging.error(f"AWS ClientError analyzing bucket '{bucket_name}/{prefix}': {error_code} - {error_message}")
            if error_code == 'NoSuchBucket':
                 st.error(f"Error: Bucket '{bucket_name}' not found or you don't have permission to access it.")
            elif error_code == 'AccessDenied':
                 st.error(f"Error: Access Denied. Check IAM permissions for s3:ListBucket on '{bucket_name}'.")
            elif error_code == 'InvalidBucketName':
                 st.error(f"Error: Invalid bucket name '{bucket_name}'. Please check the name format.")
            else:
                 st.error(f"An AWS error occurred during analysis: {error_code}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred during bucket analysis for '{bucket_name}/{prefix}': {e}", exc_info=True)
            st.error(f"An unexpected error occurred during analysis: {e}")
            return None

    def analyze_bucket(self, bucket_name, prefix, use_cache=True):
        """
        Analyzes the specified bucket and prefix, optionally using an in-memory cache.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix (folder path) within the bucket.
            use_cache (bool): Whether to use the cache (default: True).

        Returns:
            dict: A dictionary containing analysis results, or None if analysis failed.
        """
        if not isinstance(bucket_name, str) or not bucket_name:
             st.error("Invalid bucket name provided.")
             return None
        if not isinstance(prefix, str):
             st.warning("Prefix is not a string, using empty prefix.")
             prefix = ""

        cache_key = f"{bucket_name}::{prefix}"

        if use_cache and cache_key in self.cache:
            logging.info(f"Returning cached results for {cache_key}")
            return self.cache[cache_key]

        results = self.get_bucket_metrics(bucket_name, prefix)

        if results and use_cache:
            logging.info(f"Caching results for {cache_key}")
            self.cache[cache_key] = results

        return results

# --- Example usage section remains the same ---
# if __name__ == "__main__":
#     # Ensure config.yaml is present for direct testing
#     try:
#         analyzer = S3Analyzer('config.yaml')
#         if analyzer and analyzer.s3_client:
#             test_bucket = analyzer.config.get('bucket', 'YOUR_TEST_BUCKET')
#             test_prefix = analyzer.config.get('prefix', 'YOUR_TEST_PREFIX/')
#             print(f"--- Running Test Analysis ---")
#             print(f"Bucket: {test_bucket}, Prefix: {test_prefix}")
#             test_results = analyzer.analyze_bucket(test_bucket, test_prefix, use_cache=False)
#             if test_results:
#                 print("--- Analysis Results ---")
#                 print(f"Total Size (MB): {test_results['total_size_mb']:.2f}")
#                 print(f"Total Objects: {test_results['total_objects']:,}")
#                 print(f"Average Size (MB): {test_results['average_size_mb']:.2f}")
#                 print("\nSize Distribution:")
#                 for category, count in test_results['size_distribution'].items():
#                     print(f"  {category}: {count:,}")
#                 print("\nStorage Class Distribution:")
#                 for storage_class, count in test_results['storage_class_distribution'].items():
#                     print(f"  {storage_class}: {count:,}")
#                 print(f"\nFirst 5 Objects:")
#                 for obj in test_results['objects'][:5]:
#                     print(f"  - {obj['Key']} ({obj['Size (MB)']:.2f} MB, {obj['Storage Class']}, {obj['Last Modified']})")
#             else:
#                 print("--- Analysis Failed ---")
#         else:
#              print("Analyzer could not be initialized.")
#     except ValueError as e:
#          print(f"Initialization Error: {e}")
#     except Exception as e:
#          print(f"An unexpected error occurred during test run: {e}")
