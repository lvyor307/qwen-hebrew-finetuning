#!/usr/bin/env python3
"""
Simple word counter for JSONL.GZ files in S3 bucket
"""

import boto3
import gzip
import json
import argparse
from typing import Dict, Any


def count_words_in_jsonl_gz(bucket: str, prefix: str) -> int:
    """
    Count all words in JSONL.GZ files in the specified S3 path
    
    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        prefix: S3 prefix/path
        
    Returns:
        Total word count across all files
    """
    total_words = 0
    s3_client = boto3.client('s3')
    # List all objects with the prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            
            # Only process .jsonl.gz files
            if not key.endswith('.jsonl.gz'):
                continue
                
            try:
                # Download and process the file
                response = s3_client.get_object(Bucket=bucket, Key=key)
                with gzip.open(response['Body'], 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            # Extract text from common field names
                            text = ""
                            if isinstance(data, dict):
                                # Try common text field names
                                for field in ['text', 'content', 'body', 'message', 'data']:
                                    if field in data and isinstance(data[field], str):
                                        text = data[field]
                                        break
                                # If no specific field found, use the entire dict as string
                                if not text:
                                    text = str(data)
                            else:
                                text = str(data)
                            
                            # Count words (split by whitespace)
                            words = text.split()
                            total_words += len(words)
                            
                        except json.JSONDecodeError:
                            # Skip invalid JSON lines
                            continue
                            
            except Exception as e:
                print(f"Error processing {key}: {e}")
                continue
    
    return total_words


def main():
    bucket = 'gepeta-datasets'
    prefix = 'processed_cleaned_filtered/run_5_files'
    total_words = count_words_in_jsonl_gz(bucket, prefix)
    # Print final result
    print(f"Total words: {total_words:,}")

if __name__ == "__main__":
    main()
