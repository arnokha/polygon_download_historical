## BTC/USD minute data downloader
import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta
import time

# Get API key from environment variable
api_key = os.environ.get("POLYGON_API_KEY")
if not api_key:
    raise ValueError("POLYGON_API_KEY environment variable not set")

client = RESTClient(api_key)

# Function to download data in chunks by date
def download_btc_data(start_date, end_date):
    # Convert string dates to datetime objects
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_data = []
    current_dt = start_dt
    
    # Process data in chunks of 7 days to avoid hitting limits
    while current_dt < end_dt:
        # Calculate chunk end date (either 7 days later or end_date, whichever comes first)
        chunk_end_dt = min(current_dt + timedelta(days=7), end_dt)
        
        # Format dates for API call
        current_date_str = current_dt.strftime("%Y-%m-%d")
        chunk_end_date_str = chunk_end_dt.strftime("%Y-%m-%d")
        
        print(f"Downloading data from {current_date_str} to {chunk_end_date_str}...")
        
        # Download chunk with pagination
        chunk_data = []
        next_page_token = None
        
        while True:
            try:
                # Make API call with pagination
                kwargs = {
                    "ticker": "X:BTCUSD",
                    "multiplier": 1,
                    "timespan": "minute",
                    "from_": current_date_str,
                    "to": chunk_end_date_str,
                    "adjusted": "true",
                    "sort": "asc",
                    "limit": 50000
                }
                
                if next_page_token:
                    kwargs["page_token"] = next_page_token
                
                aggs_response = client.list_aggs(**kwargs)
                
                # Get the data
                for a in aggs_response:
                    chunk_data.append(a)
                
                # Check if there's more data to fetch
                if hasattr(aggs_response, 'next_page_token') and aggs_response.next_page_token:
                    next_page_token = aggs_response.next_page_token
                    # Add a small delay to avoid hitting rate limits
                    time.sleep(0.2)
                else:
                    break
                    
            except Exception as e:
                print(f"Error fetching data: {e}")
                # If we encounter an error, wait a bit and try again
                time.sleep(5)
                continue
        
        print(f"Downloaded {len(chunk_data)} records for period {current_date_str} to {chunk_end_date_str}")
        all_data.extend(chunk_data)
        
        # Move to next chunk
        current_dt = chunk_end_dt
        # Add a small delay between chunks to avoid hitting rate limits
        time.sleep(1)
    
    return all_data

# Download BTC/USD minute data from 2025-01-01 to 2025-03-01
start_date = "2025-01-01"
end_date = "2025-03-01"
aggs = download_btc_data(start_date, end_date)

# Convert to DataFrame and save to CSV
if aggs:
    df = pd.DataFrame([{
        'timestamp': datetime.fromtimestamp(a.timestamp/1000),
        'open': a.open,
        'high': a.high,
        'low': a.low,
        'close': a.close,
        'volume': a.volume,
        'vwap': a.vwap,
        'transactions': a.transactions
    } for a in aggs])
    
    # Save to CSV
    output_file = f"btcusd_minute_{start_date.replace('-', '')}_to_{end_date.replace('-', '')}.csv"
    df.to_csv(output_file, index=False)
    print(f"Downloaded {len(aggs)} total records. Saved to {output_file}")
else:
    print("No data retrieved")

