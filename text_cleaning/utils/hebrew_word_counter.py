#!/usr/bin/env python3
import boto3
import pandas as pd
import re
import io

def count_hebrew_words(bucket, path):
    """Count Hebrew words in all CSV files in the path"""
    s3_client = boto3.client('s3')
    
    # List all files in the path
    paginator = s3_client.get_paginator('list_objects_v2')
    csv_files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=path):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.csv'):
                    csv_files.append(obj['Key'])
    
    print(f"Found {len(csv_files)} CSV files")
    
    total_hebrew_words = 0
    total_n_words = 0
    file_count = 0
    
    # Process each CSV file
    for csv_file in csv_files:
        # Read file from S3
        response = s3_client.get_object(Bucket=bucket, Key=csv_file)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        
        file_count += 1
        print(f"Processing file {file_count}/{len(csv_files)}: {csv_file}")
        
        # Concatenate all text from 'text' column
        all_text = ' '.join(df['text'].dropna().astype(str))
        
        # Count Hebrew words in the concatenated text
        hebrew_words = re.findall(r'[\u0590-\u05FF]+', all_text)
        hebrew_words_in_file = len(hebrew_words)
        
        # Sum n_words column
        n_words_in_file = df['n_words'].sum()
        
        total_hebrew_words += hebrew_words_in_file
        total_n_words += n_words_in_file
        
        # Calculate percentage
        percentage = (hebrew_words_in_file / n_words_in_file * 100) if n_words_in_file > 0 else 0
        
        print(f"  Hebrew words: {hebrew_words_in_file}")
        print(f"  Total words: {n_words_in_file}")
        print(f"  Hebrew percentage: {percentage:.2f}%")
    
    # Final summary
    total_percentage = (total_hebrew_words / total_n_words * 100) if total_n_words > 0 else 0
    print(f"\nTotal Hebrew words across all files: {total_hebrew_words:,}")
    print(f"Total words across all files: {total_n_words:,}")
    print(f"Overall Hebrew percentage: {total_percentage:.2f}%")

if __name__ == "__main__":
    bucket = 'israllm-datasets'
    path = 'raw-datasets/Yifat4+5/csv_output/'
    count_hebrew_words(bucket, path)
