import duckdb
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

    # Initialize DuckDB
    print("Initializing DuckDB...")
    con = duckdb.connect(database=':memory:')
    
    # Read CSV file and perform operations in a single SQL query
    print("Executing query...")
    query = """
    WITH numbered_data AS (
        SELECT 
            *,
            ROW_NUMBER() OVER () as id,
            md5(account_id::VARCHAR) as hashAccount
        FROM read_csv('./files/sample_ads_data.csv', auto_detect=true)
    )
    SELECT 
        account_id,
        campaign_id,
        CAST(SUM(clicks) AS INTEGER) as clicks
    FROM numbered_data
    GROUP BY account_id, campaign_id
    """
    
    # Execute query and save results
    print("Saving to parquet...")
    con.execute(f"""
    COPY ({query}) TO './files/duckdb_output.parquet' (FORMAT PARQUET)
    """)
    
    # Close connection
    con.close()
    
    end_time = time.time()
    peak_memory = get_memory_usage()
    
    print(f"\nPerformance Metrics:")
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(f"Initial memory: {initial_memory:.2f} MB")
    print(f"Peak memory: {peak_memory:.2f} MB")
    print(f"Memory difference: {peak_memory - initial_memory:.2f} MB")

if __name__ == "__main__":
    main()
