import polars as pl
import time
import psutil
import os


def get_memory_usage() -> float:
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def main(input_csv: str = "./files/sample_ads_data.csv", output_parquet: str = "./files/polars_output.parquet") -> None:
    """
    Process CSV data with Polars, aggregate, and save as Parquet.
    Measures time and memory usage.
    """
    start_time = time.time()
    initial_memory = get_memory_usage()

    print("Building lazy Polars pipeline...")
    lazy_df = (
        pl.scan_csv(input_csv)
        .with_row_index("id", offset=1)
        .with_columns([
            pl.col("account_id").hash(seed=0).cast(pl.Utf8).alias("hashAccount")
        ])
        .group_by(["account_id", "campaign_id"])
        .agg([
            pl.col("clicks").sum()
        ])
    )

    print("Executing pipeline and saving to parquet...")
    result = lazy_df.collect()
    result.write_parquet(output_parquet)

    end_time = time.time()
    peak_memory = get_memory_usage()

    print(f"\nPerformance Metrics:")
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(f"Initial memory: {initial_memory:.2f} MB")
    print(f"Peak memory: {peak_memory:.2f} MB")
    print(f"Memory difference: {peak_memory - initial_memory:.2f} MB")


if __name__ == "__main__":
    main()
