import pandas as pd
import hashlib
import time
import psutil
import os

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def main():
    start_time = time.time()
    initial_memory = get_memory_usage()

    # Read CSV file
    print("Reading CSV file...")
    df = pd.read_csv("./files/sample_ads_data.csv")
    
    # Create ID column
    print("Creating ID column...")
    df['id'] = range(1, len(df) + 1)
    
    # Create hash column
    print("Creating hash column...")
    df['hashAccount'] = df['account_id'].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
    
    # Group by and aggregate
    print("Performing groupby operation...")
    grouped_df = df.groupby(['account_id', 'campaign_id'])['clicks'].sum().reset_index()
    
    # Save to parquet
    print("Saving to parquet...")
    grouped_df.to_parquet("./files/pandas_output.parquet")
    
    end_time = time.time()
    peak_memory = get_memory_usage()
    
    print(f"\nPerformance Metrics:")
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(f"Initial memory: {initial_memory:.2f} MB")
    print(f"Peak memory: {peak_memory:.2f} MB")
    print(f"Memory difference: {peak_memory - initial_memory:.2f} MB")

if __name__ == "__main__":
    main()
