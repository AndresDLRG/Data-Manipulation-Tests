import pandas as pd
from faker import Faker
import random
import os
import glob
import pyarrow.parquet as pq
import pyarrow as pa

# Settings
num_rows = 5_000_000
chunk_size = 100_000  # Write 100k rows at a time
output_path = './files/sample_ads_data.csv'
parquet_path = './files/sample_ads_data.parquet'

# Initialize Faker
fake = Faker()

# Set seed for reproducibility
SEED = 42
random.seed(SEED)
fake.seed_instance(SEED)

# Generate fake campaign/account/ad IDs
campaign_ids = [f"CAMP{str(i).zfill(5)}" for i in range(1, 101)]
ad_ids = [f"AD{str(i).zfill(6)}" for i in range(1, 1001)]
account_ids = [f"ACC{str(i).zfill(4)}" for i in range(1, 21)]

# Write header first
columns = [
    'date', 'account_id', 'campaign_id', 'ad_id', 'clicks', 'impressions', 'cost',
    'ad_group_id', 'ad_type', 'device', 'region', 'conversion', 'revenue', 'cpc', 'cpm', 'ctr', 'campaign_name', 'ad_name', 'account_name'
]
# Ensure the output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Remove existing CSV file if present (to avoid appending to old data)
if os.path.exists(output_path):
    os.remove(output_path)

# Write header first, create the file if it does not exist
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(','.join(columns) + '\n')

# Remove existing parquet file if present (to avoid appending to old data)
if os.path.exists(parquet_path):
    os.remove(parquet_path)

ad_types = ['Banner', 'Video', 'Native', 'Search', 'Social']
devices = ['Mobile', 'Desktop', 'Tablet']
regions = ['North America', 'Europe', 'Asia', 'South America', 'Africa', 'Oceania']

parquet_chunk_paths = []

# Generate and write data in chunks
for start in range(0, num_rows, chunk_size):
    rows = []
    for _ in range(min(chunk_size, num_rows - start)):
        campaign = random.choice(campaign_ids)
        ad = random.choice(ad_ids)
        account = random.choice(account_ids)
        ad_group_id = f"AG{random.randint(1, 5000):05d}"
        ad_type = random.choice(ad_types)
        device = random.choice(devices)
        region = random.choice(regions)
        date = fake.date_between(start_date='-2y', end_date='today')
        clicks = random.randint(0, 100)
        impressions = random.randint(clicks, clicks + random.randint(100, 1000))
        cost = round(random.uniform(0.1, 5.0) * clicks, 2)
        conversion = random.randint(0, clicks)
        revenue = round(conversion * random.uniform(1.0, 10.0), 2)
        cpc = round(cost / clicks, 4) if clicks > 0 else 0
        cpm = round(cost / impressions * 1000, 4) if impressions > 0 else 0
        ctr = round(clicks / impressions, 4) if impressions > 0 else 0
        campaign_name = fake.bs().title()
        ad_name = fake.catch_phrase()
        account_name = fake.company()
        rows.append({
            'date': date,
            'account_id': account,
            'campaign_id': campaign,
            'ad_id': ad,
            'clicks': clicks,
            'impressions': impressions,
            'cost': cost,
            'ad_group_id': ad_group_id,
            'ad_type': ad_type,
            'device': device,
            'region': region,
            'conversion': conversion,
            'revenue': revenue,
            'cpc': cpc,
            'cpm': cpm,
            'ctr': ctr,
            'campaign_name': campaign_name,
            'ad_name': ad_name,
            'account_name': account_name
        })
    df = pd.DataFrame(rows)
    df.to_csv(output_path, mode='a', header=False, index=False)
    # Save each chunk as a separate parquet file
    chunk_parquet_path = f'./files/sample_ads_data_chunk_{start//chunk_size + 1}.parquet'
    df.to_parquet(chunk_parquet_path, engine='pyarrow', index=False, compression='snappy')
    parquet_chunk_paths.append(chunk_parquet_path)
    print(f"Wrote rows {start+1} to {start+len(rows)}")

# Merge all chunk parquet files into one big file
table_list = [pq.read_table(path) for path in parquet_chunk_paths]
if table_list:
    big_table = pa.concat_tables(table_list)
    pq.write_table(big_table, parquet_path, compression='snappy')
    # Remove chunk files
    for path in parquet_chunk_paths:
        os.remove(path)

print(f'Sample advertising CSV file created at {output_path}')
print(f'Sample advertising Parquet file created at {parquet_path}')