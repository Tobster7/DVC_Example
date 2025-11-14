import os
import yfinance as yf
import pandas as pd
import subprocess
from datetime import datetime, timedelta

def download_update_stock_data(ticker: str, save_path: str = "data/raw/"):
    """
    Downloads or updates historical stock price data and saves it as a Parquet file.
    Then, it is versioned using DVC.
    """
    # Ensure the save path exists
    os.makedirs(save_path, exist_ok=True)
    
    # File path for storing data
    file_path = os.path.join(save_path, f"{ticker}.parquet")
    
    # Determine last recorded date
    if os.path.exists(file_path):
        existing_data = pd.read_parquet(file_path)
        last_date = existing_data.index.max().strftime("%Y-%m-%d")
        print(f"Last recorded date for {ticker}: {last_date}")
    else:
        last_date = "2015-01-01"  # Default start date if no previous data exists
    
    # Determine the end date as today
    end_date = datetime.now().strftime("%Y-%m-%d")

    if last_date >= end_date:
        print(f"Data for {ticker} is already up to date.")
        return
    
    # Fetch new data from Yahoo Finance
    print(f"Downloading new data for {ticker} from {last_date} to {end_date}...")
    new_data = yf.download(ticker, start=last_date, end=end_date, interval="1d")
    
    if new_data.empty:
        print("Error: No new data received. Check the ticker symbol!")
        return
    
    # Append new data to existing dataset if available
    if os.path.exists(file_path):
        updated_data = pd.concat([existing_data, new_data])
        updated_data = updated_data[~updated_data.index.duplicated(keep='last')]
    else:
        updated_data = new_data
    
    subprocess.run(["dvc", "unprotect", file_path])

    # Save updated dataset as Parquet
    updated_data.to_parquet(file_path, engine='pyarrow')
    print(f"Updated data saved at: {file_path}")
    
    # Version data with DVC
    subprocess.run(["dvc", "add", file_path])
    subprocess.run(["git", "add", f"{file_path}.dvc", ".gitignore"])
    subprocess.run(["git", "commit", "-m", f"Update stock data for {ticker} to {end_date} in DVC"])
    
    print(f"{ticker} data successfully updated and versioned with DVC!")

if __name__ == "__main__":
    ticker = "NVDA"  # Change the symbol for other stocks
    download_update_stock_data(ticker)