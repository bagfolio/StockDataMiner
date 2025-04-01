"""
Batch Stock Data Processor

This script processes a list of ticker symbols to fetch and store
all available financial data from Yahoo Finance in both MongoDB and SQLite.
It also exports the data to Excel files.
"""

import pandas as pd
import yfinance as yf
import os
import datetime
import time
from pathlib import Path
import requests_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from database import DatabaseManager
from mongodb_manager import MongoDBManager, HAS_PYMONGO

# MongoDB connection string - update this to your connection string
MONGODB_URI = "mongodb+srv://thatguy14066:TZsgoq2OzEUbmmlq@swipecloud1.loenl1s.mongodb.net/?retryWrites=true&w=majority&appName=SwipeCloud1"

# Create exports directory if it doesn't exist
EXPORTS_DIR = Path("exports")
EXPORTS_DIR.mkdir(exist_ok=True)

# Set up caching for faster repeated requests
session = requests_cache.CachedSession('yfinance_cache')

class BatchProcessor:
    def __init__(self, mongodb_uri=None, verbose=True):
        """
        Initialize the batch processor
        
        Parameters:
        mongodb_uri (str): MongoDB connection string
        verbose (bool): Whether to print progress messages
        """
        self.verbose = verbose
        
        # Connect to SQLite database
        try:
            self.db_manager = DatabaseManager("stock_data.db")
            self.db_manager.initialize_database()
            
            # Test connection with a simple query
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            self.db_manager.close()
            
            if self.verbose:
                print("Connected to SQLite database successfully")
        except Exception as e:
            if self.verbose:
                print(f"Error connecting to SQLite database: {e}")
                print("Will only export to Excel files")
            self.db_manager = None
        
        # Connect to MongoDB if available
        self.mongodb_manager = None
        if HAS_PYMONGO and mongodb_uri:
            try:
                self.mongodb_manager = MongoDBManager(mongodb_uri)
                if self.verbose:
                    print("Connected to MongoDB successfully")
            except Exception as e:
                if self.verbose:
                    print(f"Error connecting to MongoDB: {e}")
                    print("Data will not be stored in MongoDB")
        elif not HAS_PYMONGO:
            if self.verbose:
                print("pymongo not installed or not properly configured - data will not be stored in MongoDB")
        
        # Define categories and info types to process
        self.categories_to_process = {
            "General Information": ["Basic Info", "Fast Info", "News"],
            "Historical Data": ["Price History", "Dividends", "Splits", "Actions"],
            "Financial Statements": ["Income Statement", "Balance Sheet", "Cash Flow", "Earnings"],
            "Analysis & Holdings": [
                "Recommendations", "Analyst Price Target", "Upgrades Downgrades",
                "Earnings Estimates", "Revenue Estimates", "EPS Trend", "Growth Estimates",
                "Major Holders", "Institutional Holders", "Mutual Fund Holders", 
                "Insider Transactions"
            ]
        }
    
    def process_tickers(self, tickers, force_refresh=False):
        """
        Process a list of tickers, storing data in both databases and exporting to Excel
        
        Parameters:
        tickers (list): List of ticker symbols to process
        force_refresh (bool): Whether to force refresh data from Yahoo Finance
        
        Returns:
        dict: Results summary with counts of processed data
        """
        results = {
            "total_tickers": len(tickers),
            "successful_tickers": 0,
            "failed_tickers": 0,
            "data_points_stored": 0,
            "data_points_exported": 0
        }
        
        # Process ticker batches efficiently using yfinance's built-in batch functionality
        start_time = time.time()
        
        if self.verbose:
            print(f"Processing {len(tickers)} tickers...")
        
        # Get historical data for all tickers at once (optimization)
        try:
            # Process in batches of 20 to avoid overwhelming the API
            batch_size = 20
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i+batch_size]
                if self.verbose:
                    print(f"Fetching historical data for batch {i//batch_size + 1}/{(len(tickers)+batch_size-1)//batch_size}")
                
                # Get 5 years of daily data for the current batch
                hist_data = yf.download(
                    tickers=batch, 
                    period="5y",
                    group_by='ticker', 
                    auto_adjust=True, 
                    threads=True, 
                    repair=True,
                    session=session
                )
                
                # Process the historical data for each ticker
                for ticker in batch:
                    try:
                        # For single ticker, the data structure is different
                        if len(batch) == 1:
                            ticker_hist = hist_data.copy()
                        else:
                            ticker_hist = hist_data[ticker].copy()
                        
                        if not ticker_hist.empty:
                            # Store historical data
                            ticker_hist.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                            ticker_hist.attrs['data_timestamp'] = ticker_hist.index.max().isoformat()
                            ticker_hist.attrs['source'] = 'Yahoo Finance'
                            
                            # Store in SQLite if database is available
                            if self.db_manager:
                                try:
                                    self.db_manager.store_data(ticker, "Historical Data", "Price History", 
                                                        ticker_hist, 
                                                        data_timestamp=ticker_hist.attrs['data_timestamp'], 
                                                        source=ticker_hist.attrs['source'])
                                except Exception as e:
                                    if self.verbose:
                                        print(f"Error storing data in SQLite: {e}")
                            
                            # Store in MongoDB if available
                            if self.mongodb_manager:
                                try:
                                    self.mongodb_manager.store_data(ticker, "Historical Data", "Price History", 
                                                                ticker_hist, 
                                                                data_timestamp=ticker_hist.attrs['data_timestamp'], 
                                                                source=ticker_hist.attrs['source'])
                                except Exception as e:
                                    if self.verbose:
                                        print(f"Error storing data in MongoDB: {e}")
                            
                            # Export to Excel
                            self._export_to_excel(ticker, "Historical Data", "Price History", ticker_hist)
                            
                            results["data_points_stored"] += 1
                            results["data_points_exported"] += 1
                    except Exception as e:
                        if self.verbose:
                            print(f"Error processing historical data for {ticker}: {e}")
        except Exception as e:
            if self.verbose:
                print(f"Error in batch historical data download: {e}")
        
        # Process each ticker individually for other data
        for ticker in tickers:
            try:
                if self.verbose:
                    print(f"Processing {ticker}...")
                
                # Create a Ticker object
                try:
                    yf_ticker = yf.Ticker(ticker, session=session)
                    results["successful_tickers"] += 1
                except Exception as e:
                    if self.verbose:
                        print(f"Error creating Ticker object for {ticker}: {e}")
                    results["failed_tickers"] += 1
                    continue
                
                # Process each category and info type
                for category, info_types in self.categories_to_process.items():
                    for info_type in info_types:
                        # Skip Price History as we've already processed it in batch
                        if category == "Historical Data" and info_type == "Price History":
                            continue
                            
                        try:
                            data = self._get_data(yf_ticker, ticker, category, info_type)
                            
                            if data is not None and not (isinstance(data, pd.DataFrame) and data.empty):
                                # Store in SQLite if database is available
                                source = data.attrs.get('source', 'Yahoo Finance') if hasattr(data, 'attrs') else 'Yahoo Finance'
                                data_timestamp = data.attrs.get('data_timestamp') if hasattr(data, 'attrs') else None
                                
                                if self.db_manager:
                                    try:
                                        self.db_manager.store_data(ticker, category, info_type, data, 
                                                                data_timestamp=data_timestamp, source=source)
                                    except Exception as e:
                                        if self.verbose:
                                            print(f"Error storing data in SQLite: {e}")
                                
                                # Store in MongoDB if available
                                if self.mongodb_manager:
                                    try:
                                        self.mongodb_manager.store_data(ticker, category, info_type, data, 
                                                                    data_timestamp=data_timestamp, source=source)
                                    except Exception as e:
                                        if self.verbose:
                                            print(f"Error storing data in MongoDB: {e}")
                                
                                # Export to Excel
                                self._export_to_excel(ticker, category, info_type, data)
                                
                                results["data_points_stored"] += 1
                                results["data_points_exported"] += 1
                                
                                if self.verbose:
                                    print(f"  ✓ {category} - {info_type}")
                        except Exception as e:
                            if self.verbose:
                                print(f"  ✗ Error with {category} - {info_type}: {e}")
            except Exception as e:
                if self.verbose:
                    print(f"Error processing {ticker}: {e}")
                results["failed_tickers"] += 1
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Add processing time to results
        results["processing_time_seconds"] = processing_time
        results["processing_time_formatted"] = str(datetime.timedelta(seconds=int(processing_time)))
        
        if self.verbose:
            print(f"Processing completed in {results['processing_time_formatted']}")
            print(f"Successfully processed {results['successful_tickers']} tickers")
            print(f"Failed to process {results['failed_tickers']} tickers")
            print(f"Stored {results['data_points_stored']} data points")
            print(f"Exported {results['data_points_exported']} data points to Excel")
        
        return results
    
    def _get_data(self, yf_ticker, ticker_symbol, category, info_type):
        """Get data based on category and info type"""
        if category == "General Information":
            return self._get_general_info(yf_ticker, info_type)
        elif category == "Historical Data":
            return self._get_historical_data(yf_ticker, info_type)
        elif category == "Financial Statements":
            return self._get_financial_statements(yf_ticker, info_type)
        elif category == "Analysis & Holdings":
            return self._get_analysis_and_holdings(yf_ticker, info_type)
        else:
            raise ValueError(f"Unknown category: {category}")
    
    def _get_general_info(self, ticker, info_type):
        """Get general information about the stock."""
        if info_type == "Basic Info":
            info = ticker.info
            # Convert to DataFrame for consistent handling
            if info:
                # Create a one-row DataFrame from info dict
                df = pd.DataFrame([info])
                df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                df.attrs['source'] = 'Yahoo Finance'
                return df
            return None
        
        elif info_type == "Fast Info":
            try:
                fast_info = ticker.fast_info
                if fast_info:
                    # Convert fast_info to dict then DataFrame
                    info_dict = {}
                    for attr in dir(fast_info):
                        if not attr.startswith('_') and attr != 'options':
                            try:
                                info_dict[attr] = getattr(fast_info, attr)
                            except:
                                pass
                    
                    df = pd.DataFrame([info_dict])
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['source'] = 'Yahoo Finance (fast_info)'
                    return df
            except:
                pass
            return None
        
        elif info_type == "News":
            news = ticker.news
            if news:
                df = pd.DataFrame(news)
                df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                df.attrs['source'] = 'Yahoo Finance'
                
                # Convert timestamps to readable format
                if 'providerPublishTime' in df.columns:
                    df['published_date'] = pd.to_datetime(df['providerPublishTime'], unit='s')
                    df.attrs['data_timestamp'] = df['published_date'].max().isoformat()
                
                return df
            return None
        
        return None
    
    def _get_historical_data(self, ticker, info_type):
        """Get historical data for the stock."""
        # Most historical data was already fetched in batch for efficiency
        try:
            if info_type == "Dividends":
                data = ticker.dividends
                if not data.empty:
                    # Convert Series to DataFrame
                    df = data.to_frame()
                    df.columns = ['Dividend']
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.index.max().isoformat()
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
            
            elif info_type == "Splits":
                data = ticker.splits
                if not data.empty:
                    # Convert Series to DataFrame
                    df = data.to_frame()
                    df.columns = ['Split']
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.index.max().isoformat()
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
            
            elif info_type == "Actions":
                data = ticker.actions
                if not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.index.max().isoformat()
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
            
            elif info_type == "Price History":
                # This should have been fetched in batch already
                # Use a smaller timeframe here just in case
                data = ticker.history(period="1y")
                if not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.index.max().isoformat()
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
        except Exception as e:
            if self.verbose:
                print(f"Error fetching {info_type}: {e}")
            return None
    
    def _get_financial_statements(self, ticker, info_type):
        """Get financial statement data for the stock."""
        try:
            if info_type == "Income Statement":
                data = ticker.income_stmt
                if not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.columns.max().isoformat() if hasattr(df.columns, 'max') else None
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
            
            elif info_type == "Balance Sheet":
                data = ticker.balance_sheet
                if not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.columns.max().isoformat() if hasattr(df.columns, 'max') else None
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
            
            elif info_type == "Cash Flow":
                data = ticker.cashflow
                if not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['data_timestamp'] = df.columns.max().isoformat() if hasattr(df.columns, 'max') else None
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
            
            elif info_type == "Earnings":
                data = ticker.earnings
                if not data.empty:
                    df = data.copy() 
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['source'] = 'Yahoo Finance'
                    return df
                return None
        except Exception as e:
            if self.verbose:
                print(f"Error fetching {info_type}: {e}")
            return None
    
    def _get_analysis_and_holdings(self, ticker, info_type):
        """Get analysis and holdings data for the stock."""
        try:
            if info_type == "Recommendations":
                data = ticker.recommendations
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    
                    # Add source information
                    if 'Firm' in df.columns:
                        unique_firms = df["Firm"].unique().tolist()
                        if len(unique_firms) > 5:
                            df.attrs['source'] = f"{', '.join(unique_firms[:5])} and {len(unique_firms) - 5} others (via Yahoo Finance)"
                        else:
                            df.attrs['source'] = f"{', '.join(unique_firms)} (via Yahoo Finance)"
                    else:
                        df.attrs['source'] = 'Yahoo Finance'
                    
                    # Add data timestamp from most recent date
                    if 'Date' in df.columns or 'date' in df.columns:
                        date_col = 'Date' if 'Date' in df.columns else 'date'
                        df.attrs['data_timestamp'] = df[date_col].max().isoformat() if not df[date_col].empty else None
                    
                    return df
                return None
            
            elif info_type == "Analyst Price Target":
                if hasattr(ticker, 'target_price'):
                    price_target = ticker.target_price
                    if price_target is not None:
                        # Extract price target info
                        target_dict = {
                            'Mean': price_target.mean,
                            'Median': price_target.median,
                            'High': price_target.high,
                            'Low': price_target.low,
                            'Number of Analysts': price_target.number_of_analysts
                        }
                        
                        df = pd.DataFrame([target_dict])
                        df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                        df.attrs['source'] = f"Based on {target_dict['Number of Analysts']} analysts (via Yahoo Finance)"
                        return df
                return None
            
            elif info_type == "Upgrades Downgrades":
                data = ticker.upgrades_downgrades
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    
                    # Add source information
                    if 'Firm' in df.columns:
                        unique_firms = df["Firm"].unique().tolist()
                        if len(unique_firms) > 5:
                            df.attrs['source'] = f"{', '.join(unique_firms[:5])} and {len(unique_firms) - 5} others (via Yahoo Finance)"
                        else:
                            df.attrs['source'] = f"{', '.join(unique_firms)} (via Yahoo Finance)"
                    else:
                        df.attrs['source'] = 'Yahoo Finance'
                    
                    # Add data timestamp from most recent date
                    if 'Date' in df.columns or 'date' in df.columns:
                        date_col = 'Date' if 'Date' in df.columns else 'date'
                        df.attrs['data_timestamp'] = df[date_col].max().isoformat() if not df[date_col].empty else None
                    
                    return df
                return None
            
            elif info_type == "Earnings Estimates":
                data = ticker.earnings_forecasts
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['source'] = 'Analyst forecasts (via Yahoo Finance)'
                    return df
                return None
            
            elif info_type == "Revenue Estimates":
                data = ticker.revenue_forecasts
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['source'] = 'Analyst forecasts (via Yahoo Finance)'
                    return df
                return None
            
            elif info_type == "EPS Trend":
                try:
                    # This data might be in the info dictionary
                    info = ticker.info
                    eps_data = {}
                    
                    for key in info:
                        if 'eps' in key.lower() or 'earnings' in key.lower():
                            eps_data[key] = info[key]
                    
                    if eps_data:
                        df = pd.DataFrame([eps_data])
                        df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                        df.attrs['source'] = 'Yahoo Finance'
                        return df
                except:
                    pass
                return None
            
            elif info_type == "Growth Estimates":
                try:
                    # Extract growth estimates from info if available
                    info = ticker.info
                    growth_data = {}
                    
                    for key in info:
                        if 'growth' in key.lower() or 'estimate' in key.lower():
                            growth_data[key] = info[key]
                    
                    if growth_data:
                        df = pd.DataFrame([growth_data])
                        df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                        df.attrs['source'] = 'Yahoo Finance'
                        return df
                except:
                    pass
                return None
            
            elif info_type == "Major Holders":
                try:
                    data = ticker.major_holders
                    if data is not None and not data.empty:
                        df = data.copy()
                        df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                        df.attrs['source'] = 'Yahoo Finance'
                        return df
                except:
                    pass
                return None
            
            elif info_type == "Institutional Holders":
                data = ticker.institutional_holders
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    
                    # Add source information with top holders
                    if 'Holder' in df.columns:
                        top_holders = df["Holder"].head(3).tolist()
                        holder_count = len(df["Holder"].unique())
                        
                        if top_holders:
                            df.attrs['source'] = f"Top holders include {', '.join(top_holders)}"
                            if holder_count > 3:
                                df.attrs['source'] += f" and {holder_count - 3} others (via Yahoo Finance)"
                            else:
                                df.attrs['source'] += " (via Yahoo Finance)"
                        else:
                            df.attrs['source'] = 'Yahoo Finance'
                    else:
                        df.attrs['source'] = 'Yahoo Finance'
                    
                    # Add data timestamp
                    if 'Date Reported' in df.columns:
                        df.attrs['data_timestamp'] = df['Date Reported'].max().isoformat() if not df['Date Reported'].empty else None
                    
                    return df
                return None
            
            elif info_type == "Mutual Fund Holders":
                data = ticker.mutualfund_holders
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    
                    # Add source information with top holders
                    if 'Holder' in df.columns:
                        top_holders = df["Holder"].head(3).tolist()
                        holder_count = len(df["Holder"].unique())
                        
                        if top_holders:
                            df.attrs['source'] = f"Top funds include {', '.join(top_holders)}"
                            if holder_count > 3:
                                df.attrs['source'] += f" and {holder_count - 3} others (via Yahoo Finance)"
                            else:
                                df.attrs['source'] += " (via Yahoo Finance)"
                        else:
                            df.attrs['source'] = 'Yahoo Finance'
                    else:
                        df.attrs['source'] = 'Yahoo Finance'
                    
                    # Add data timestamp
                    if 'Date Reported' in df.columns:
                        df.attrs['data_timestamp'] = df['Date Reported'].max().isoformat() if not df['Date Reported'].empty else None
                    
                    return df
                return None
            
            elif info_type == "Insider Transactions":
                data = ticker.insider_transactions
                if data is not None and not data.empty:
                    df = data.copy()
                    df.attrs['fetch_timestamp'] = datetime.datetime.now().isoformat()
                    df.attrs['source'] = 'SEC Filings (via Yahoo Finance)'
                    
                    # Add data timestamp
                    if 'Transaction Date' in df.columns:
                        df.attrs['data_timestamp'] = df['Transaction Date'].max().isoformat() if not df['Transaction Date'].empty else None
                    
                    return df
                return None
        
        except Exception as e:
            if self.verbose:
                print(f"Error fetching {info_type}: {e}")
            return None
    
    def _export_to_excel(self, ticker, category, info_type, data):
        """
        Export data to Excel file
        
        Parameters:
        ticker (str): Ticker symbol
        category (str): Data category
        info_type (str): Information type
        data (DataFrame): Data to export
        """
        if not isinstance(data, pd.DataFrame) or data.empty:
            return
        
        try:
            # Create ticker directory if it doesn't exist
            ticker_dir = EXPORTS_DIR / ticker
            ticker_dir.mkdir(exist_ok=True)
            
            # Create category directory if it doesn't exist
            category_dir = ticker_dir / category.replace(" ", "_")
            category_dir.mkdir(exist_ok=True)
            
            # File path for Excel file
            file_name = info_type.replace(" ", "_").replace("/", "_") + ".xlsx"
            file_path = category_dir / file_name
            
            # Save to Excel
            data.to_excel(file_path)
            
            if self.verbose:
                print(f"  ✓ Exported {ticker} - {category} - {info_type} to {file_path}")
        except Exception as e:
            if self.verbose:
                print(f"  ✗ Error exporting {ticker} - {category} - {info_type} to Excel: {e}")

def main():
    """Main function to process a list of tickers"""
    # List of tickers to process
    tickers = [
        "HD", "AMZN", "AAPL", "MSFT", "GOOGL", "NVDA", "IBM", "ZM", "SHOP", "PLTR",
        "FTNT", "AI", "DOCN", "FSLY", "TSLA", "NEE", "BEP", "FSLR", "ENPH", "SEDG",
        "PLUG", "CHPT", "HASI", "AES", "MDT", "SYK", "BSX", "ABT", "ISRG", "EW",
        "BDX", "COST", "WMT", "TGT", "KR", "ACI", "BBY", "BJ", "LOW", "PLD",
        "O", "SPG", "AVB", "PSA", "DLR", "AMT", "WELL", "BXP", "EQR"
    ]
    
    processor = BatchProcessor(mongodb_uri=MONGODB_URI, verbose=True)
    results = processor.process_tickers(tickers, force_refresh=True)
    
    # Save summary results to file
    with open(EXPORTS_DIR / "processing_summary.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"Processing summary saved to {EXPORTS_DIR / 'processing_summary.json'}")

if __name__ == "__main__":
    main()