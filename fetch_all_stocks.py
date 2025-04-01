"""
Fetch All Stock Data 

This script fetches all available data for a list of ticker symbols, 
stores it in both SQLite and MongoDB databases, and exports it to Excel files.
"""

from batch_process import BatchProcessor

# MongoDB connection string
MONGODB_URI = "mongodb+srv://thatguy14066:TZsgoq2OzEUbmmlq@swipecloud1.loenl1s.mongodb.net/?retryWrites=true&w=majority&appName=SwipeCloud1"

# List of tickers from the user
tickers = [
    "HD", "AMZN", "AAPL", "MSFT", "GOOGL", "NVDA", "IBM", "ZM", "SHOP", "PLTR",
    "FTNT", "AI", "DOCN", "FSLY", "TSLA", "NEE", "BEP", "FSLR", "ENPH", "SEDG",
    "PLUG", "CHPT", "HASI", "AES", "MDT", "SYK", "BSX", "ABT", "ISRG", "EW",
    "BDX", "COST", "WMT", "TGT", "KR", "ACI", "BBY", "BJ", "LOW", "PLD",
    "O", "SPG", "AVB", "PSA", "DLR", "AMT", "WELL", "BXP", "EQR"
]

def main():
    print("Starting stock data collection process...")
    print(f"Processing {len(tickers)} tickers...")
    
    # Initialize the batch processor
    processor = BatchProcessor(mongodb_uri=MONGODB_URI, verbose=True)
    
    # Process all tickers
    results = processor.process_tickers(tickers, force_refresh=True)
    
    # Print summary
    print("\n=== Processing Summary ===")
    print(f"Total tickers processed: {results['total_tickers']}")
    print(f"Successfully processed: {results['successful_tickers']}")
    print(f"Failed to process: {results['failed_tickers']}")
    print(f"Data points stored: {results['data_points_stored']}")
    print(f"Data points exported: {results['data_points_exported']}")
    print(f"Processing time: {results['processing_time_formatted']}")
    print()
    print("Data has been stored in:")
    print("1. SQLite database: stock_data.db")
    print("2. MongoDB database: stock_data (in your MongoDB Atlas cluster)")
    print("3. Excel files: in the exports/ directory")

if __name__ == "__main__":
    main()