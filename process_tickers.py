#!/usr/bin/env python3
"""
Batch Stock Data Processor for a list of predefined tickers

This script processes a list of specified ticker symbols to fetch and store
all available financial data from Yahoo Finance in both MongoDB and SQLite.
"""

import os
import sys
from batch_process import BatchProcessor

# List of tickers to process
TICKERS = [
    "HD", "AMZN", "AAPL", "MSFT", "GOOGL", "NVDA", "IBM", "ZM", "SHOP", "PLTR",
    "FTNT", "AI", "DOCN", "FSLY", "TSLA", "NEE", "BEP", "FSLR", "ENPH", "SEDG",
    "PLUG", "CHPT", "HASI", "AES", "MDT", "SYK", "BSX", "ABT", "ISRG", "EW",
    "BDX", "COST", "WMT", "TGT", "KR", "ACI", "BBY", "BJ", "LOW", "PLD",
    "O", "SPG", "AVB", "PSA", "DLR", "AMT", "WELL", "BXP", "EQR"
]

def main():
    """Main function to process all tickers"""
    # Get MongoDB connection string from environment
    mongodb_uri = os.environ.get('MONGODB_URI')
    
    print(f"Starting batch processing of {len(TICKERS)} tickers...")
    print(f"MongoDB connection: {'Available' if mongodb_uri else 'Not available'}")
    
    # Initialize batch processor
    processor = BatchProcessor(mongodb_uri=mongodb_uri, verbose=True)
    
    # Process all tickers
    force_refresh = True  # Set to True to refresh all data, False to use cache if available
    results = processor.process_tickers(TICKERS, force_refresh=force_refresh)
    
    # Print results
    print("\nProcessing complete!")
    print(f"Successfully processed {results['success']} out of {len(TICKERS)} tickers.")
    if results['failed']:
        print(f"Failed to process {results['failed']} tickers.")
        print("Failed tickers:", results['failed_tickers'])
    
    print("\nData categories processed:")
    for category, count in results['categories'].items():
        print(f"  - {category}: {count} items")

if __name__ == "__main__":
    main()