import yfinance as yf
import pandas as pd
import datetime
import time
import numpy as np
import threading
import requests_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

class StockDataFetcher:
    def __init__(self, db_manager=None):
        """
        Initialize the stock data fetcher with caching.
        
        Parameters:
        db_manager (DatabaseManager, optional): Database manager for checking existing data
        """
        # Create a cached session that will be reused across requests
        # This dramatically improves performance by avoiding redundant API calls
        # Cache expires after 15 minutes (900 seconds) for somewhat fresh data
        self.session = requests_cache.CachedSession(
            cache_name='yfinance_cache',
            backend='sqlite',
            expire_after=900
        )
        
        # Set up headers to mimic a browser request to avoid rate limiting
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        })
        
        # Store reference to database manager if provided
        self.db_manager = db_manager
        
        # Track which data we've already fetched to avoid duplicates in a session
        self._fetched_data_cache = set()

    def data_exists(self, ticker_symbol, category, info_type, max_age_hours=24):
        """
        Check if data already exists in the database and is not too old
        
        Parameters:
        ticker_symbol (str): The stock ticker symbol
        category (str): The category of information to fetch
        info_type (str): The specific type of information within the category
        max_age_hours (int): Maximum age in hours to consider data fresh
        
        Returns:
        bool: True if fresh data exists, False otherwise
        """
        # Check internal cache first for this session
        cache_key = f"{ticker_symbol}_{category}_{info_type}"
        if cache_key in self._fetched_data_cache:
            return True
        
        # If no database manager, assume data doesn't exist
        if self.db_manager is None:
            return False
            
        try:
            # First check if database is fully initialized
            try:
                # Check if the tables exist
                conn = self.db_manager.connect()
                cursor = conn.cursor()
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_data'")
                if not cursor.fetchone():
                    print("The stock_data table does not exist yet - database not fully initialized")
                    conn.close()
                    return False
                    
                # Also check for other needed tables
                for table in ['tickers', 'data_categories', 'data_types']:
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                    if not cursor.fetchone():
                        print(f"The {table} table does not exist yet - database not fully initialized")
                        conn.close()
                        return False
                
                conn.close()
            except Exception as e:
                print(f"Error checking database structure: {e}")
                return False
                
            # Use database manager to check for existing data
            try:
                conn = self.db_manager.connect()
                cursor = conn.cursor()
                
                # Query to check if data exists and is fresh
                cursor.execute('''
                SELECT s.fetch_timestamp
                FROM stock_data s
                JOIN tickers t ON s.ticker_id = t.id
                JOIN data_types dt ON s.data_type_id = dt.id
                JOIN data_categories dc ON dt.category_id = dc.id
                WHERE t.symbol = ? AND dc.name = ? AND dt.name = ?
                ''', (ticker_symbol, category, info_type))
                
                result = cursor.fetchone()
                
                # Close connection
                conn.close()
                
                if result:
                    fetch_timestamp = result[0]
                    try:
                        # Convert to datetime for age check
                        fetch_time = pd.to_datetime(fetch_timestamp)
                        now = pd.Timestamp.now()
                        age_hours = (now - fetch_time).total_seconds() / 3600
                        
                        # If fresh enough, data exists
                        return age_hours <= max_age_hours
                    except:
                        # If can't parse timestamp, assume data exists but is old
                        return False
                else:
                    # No data found
                    return False
            except Exception as e:
                print(f"Error checking for existing data: {e}")
                return False
                
        except Exception as e:
            # On error, assume data doesn't exist
            print(f"Error checking if data exists: {e}")
            return False
    
    def get_data(self, ticker_symbol, category, info_type, force_refresh=False):
        """
        Fetch stock data based on category and info type,
        with option to use existing data from database
        
        Parameters:
        ticker_symbol (str): The stock ticker symbol
        category (str): The category of information to fetch
        info_type (str): The specific type of information within the category
        force_refresh (bool): Force refresh even if recent data exists
        
        Returns:
        DataFrame: The requested stock data with timestamp and source metadata
        """
        # Check if we can use existing data
        cache_key = f"{ticker_symbol}_{category}_{info_type}"
        
        if not force_refresh:
            # Check if data already exists
            # For 'News', always get fresh data
            if info_type != "News" and self.data_exists(ticker_symbol, category, info_type):
                # Get existing data from database
                if self.db_manager:
                    existing_data = self.db_manager.get_stored_data(ticker_symbol, category, info_type)
                    if existing_data is not None and not existing_data.empty:
                        # Add to cache so we know we have it
                        self._fetched_data_cache.add(cache_key)
                        return existing_data
        try:
            # Add fetch timestamp to track when data was retrieved
            fetch_timestamp = pd.Timestamp.now()
            
            # Create a Ticker object for the given symbol with our cached session
            ticker = yf.Ticker(ticker_symbol, session=self.session)
            
            # Fetch data based on category and info type
            data = None
            if category == "General Information":
                data = self._get_general_info(ticker, info_type)
            elif category == "Historical Data":
                data = self._get_historical_data(ticker, info_type)
            elif category == "Financial Statements":
                data = self._get_financial_statements(ticker, info_type)
            elif category == "Analysis & Holdings":
                data = self._get_analysis_and_holdings(ticker, info_type)
            else:
                raise ValueError(f"Unknown category: {category}")
            
            # Add metadata to the dataframe if it's not empty
            if data is not None and not data.empty:
                # Using a clean approach that works with any dataframe structure
                if isinstance(data, pd.DataFrame):
                    # Store the fetch timestamp as a dataframe attribute
                    data.attrs['fetch_timestamp'] = fetch_timestamp.isoformat()
                    data.attrs['ticker'] = ticker_symbol
                    data.attrs['category'] = category
                    data.attrs['info_type'] = info_type
                    
                    # Set default source
                    if 'source' not in data.attrs:
                        data.attrs['source'] = 'Yahoo Finance'
                    
                    # Set data timestamp based on data type
                    if 'data_timestamp' not in data.attrs:
                        # For historical data, use the most recent date in the index
                        if category == "Historical Data" and hasattr(data.index, 'max'):
                            try:
                                most_recent = data.index.max()
                                if isinstance(most_recent, (pd.Timestamp, datetime.datetime, datetime.date)):
                                    data.attrs['data_timestamp'] = most_recent.isoformat()
                            except:
                                pass
                                
                        # For financial statements, try to use the column names as timestamps
                        elif category == "Financial Statements" and hasattr(data, 'columns'):
                            try:
                                if isinstance(data.columns[0], (pd.Timestamp, datetime.datetime, datetime.date)):
                                    most_recent = max(data.columns)
                                    data.attrs['data_timestamp'] = most_recent.isoformat()
                            except:
                                pass
                                
                        # For analysis data with a date column
                        elif category == "Analysis & Holdings":
                            try:
                                # For recommendations, analysts, etc. with date columns
                                date_columns = [col for col in data.columns if col in 
                                              ['Date', 'date', 'Announcement Date', 'Period', 'Quarter', 
                                               'Report Date', 'Last Updated']]
                                if date_columns and len(date_columns) > 0:
                                    col = date_columns[0]
                                    if not data[col].empty:
                                        most_recent = data[col].max()
                                        if isinstance(most_recent, (pd.Timestamp, datetime.datetime, datetime.date, str)):
                                            if isinstance(most_recent, str):
                                                # Try to parse string to datetime
                                                try:
                                                    most_recent = pd.to_datetime(most_recent)
                                                except:
                                                    pass
                                            data.attrs['data_timestamp'] = str(most_recent)
                            except:
                                pass
                    
                    # Enhance source information for specific data types
                    self._enhance_data_metadata(data, category, info_type)
            
            return data
        except Exception as e:
            raise Exception(f"Error fetching {info_type} for {ticker_symbol}: {str(e)}")
            
    def _enhance_data_metadata(self, data, category, info_type):
        """
        Add enhanced metadata to the dataframe for specific data types
        
        Parameters:
        data (DataFrame): The data to enhance
        category (str): The category of information
        info_type (str): The specific type of information within the category
        """
        try:
            # For Analysis & Holdings data, add more specific source information
            if category == "Analysis & Holdings":
                # For recommendations, add firm names as source
                if info_type in ["Recommendations", "Upgrades Downgrades"] and "Firm" in data.columns:
                    # Get unique firms
                    unique_firms = data["Firm"].unique().tolist()
                    if unique_firms:
                        # Use up to 5 firm names as source
                        if len(unique_firms) > 5:
                            data.attrs['source'] = f"{', '.join(unique_firms[:5])} and {len(unique_firms) - 5} others (via Yahoo Finance)"
                        else:
                            data.attrs['source'] = f"{', '.join(unique_firms)} (via Yahoo Finance)"
                
                # For analyst price targets, extract source from data
                if info_type == "Analyst Price Target" and not data.empty:
                    try:
                        firm_col = None
                        for col in ["Firm", "Analyst", "Source"]:
                            if col in data.columns:
                                firm_col = col
                                break
                                
                        if firm_col and not data[firm_col].empty:
                            unique_sources = data[firm_col].unique().tolist()
                            if unique_sources:
                                # Use up to 3 sources
                                if len(unique_sources) > 3:
                                    data.attrs['source'] = f"{', '.join(unique_sources[:3])} and {len(unique_sources) - 3} others (via Yahoo Finance)"
                                else:
                                    data.attrs['source'] = f"{', '.join(unique_sources)} (via Yahoo Finance)"
                    except:
                        pass
                
                # For institutional holders, add institutional names
                if info_type in ["Institutional Holders", "Major Holders", "Mutual Fund Holders"] and "Holder" in data.columns:
                    try:
                        top_holders = data["Holder"].head(3).tolist()
                        if top_holders:
                            holder_count = len(data["Holder"].unique())
                            data.attrs['source'] = f"Top holders include {', '.join(top_holders)}" + \
                                f" and {holder_count - 3} others (via Yahoo Finance)" if holder_count > 3 else " (via Yahoo Finance)"
                    except:
                        pass
                        
                # For insider transactions, add insider names
                if info_type in ["Insider Transactions", "Insider Purchases"] and "Insider" in data.columns:
                    try:
                        insiders = data["Insider"].unique().tolist()
                        if insiders:
                            if len(insiders) > 3:
                                data.attrs['source'] = f"{', '.join(insiders[:3])} and {len(insiders) - 3} others (via Yahoo Finance)"
                            else:
                                data.attrs['source'] = f"{', '.join(insiders)} (via Yahoo Finance)"
                    except:
                        pass
            
            # For financial statements, add the reporting period
            elif category == "Financial Statements":
                try:
                    if hasattr(data, 'columns') and len(data.columns) > 0:
                        periods = [str(col) for col in data.columns]
                        if periods:
                            data.attrs['source'] = f"Financial data for periods: {', '.join(periods)} (via Yahoo Finance)"
                except:
                    pass
                    
            # For news data, add providers as source
            elif category == "General Information" and info_type == "News":
                try:
                    if "provider" in data.columns:
                        providers = data["provider"].unique().tolist()
                        if providers:
                            if len(providers) > 3:
                                data.attrs['source'] = f"{', '.join(providers[:3])} and {len(providers) - 3} other news sources"
                            else:
                                data.attrs['source'] = f"{', '.join(providers)}"
                except:
                    pass
        except Exception as e:
            # If anything fails, just continue without enhanced metadata
            print(f"Error enhancing metadata: {str(e)}")
            pass
            
    def get_multiple_data(self, ticker_symbols, category, info_type):
        """
        Fetch the same data for multiple ticker symbols efficiently
        
        Parameters:
        ticker_symbols (list): List of ticker symbols
        category (str): The category of information to fetch
        info_type (str): The specific type of information within the category
        
        Returns:
        dict: Dictionary mapping ticker symbols to their respective data with enhanced metadata
        """
        results = {}
        
        # For historical price data, use batch download
        if category == "Historical Data" and info_type == "Price History":
            try:
                # Use yf.download for efficient batch history download
                data = yf.download(
                    tickers=ticker_symbols,
                    period="5y",  # Always fetch at least 5 years of data when available
                    group_by='ticker',
                    auto_adjust=True,
                    repair=True,  # Repair common data issues
                    threads=True,  # Use multithreading
                    session=self.session  # Use our cached session
                )
                
                fetch_timestamp = pd.Timestamp.now().isoformat()
                
                # Handle both single ticker and multiple ticker cases
                if len(ticker_symbols) == 1:
                    ticker = ticker_symbols[0]
                    results[ticker] = data
                    
                    # Add metadata
                    results[ticker].attrs['fetch_timestamp'] = fetch_timestamp
                    results[ticker].attrs['ticker'] = ticker
                    results[ticker].attrs['category'] = category
                    results[ticker].attrs['info_type'] = info_type
                    results[ticker].attrs['source'] = 'Yahoo Finance'
                    
                    # Add data timestamp (most recent date)
                    if hasattr(data.index, 'max'):
                        try:
                            most_recent = data.index.max()
                            if isinstance(most_recent, (pd.Timestamp, datetime.datetime, datetime.date)):
                                results[ticker].attrs['data_timestamp'] = most_recent.isoformat()
                        except:
                            pass
                else:
                    for ticker in ticker_symbols:
                        if ticker in data.columns.levels[0]:
                            ticker_data = data[ticker]
                            
                            # Add metadata
                            ticker_data.attrs['fetch_timestamp'] = fetch_timestamp
                            ticker_data.attrs['ticker'] = ticker
                            ticker_data.attrs['category'] = category
                            ticker_data.attrs['info_type'] = info_type
                            ticker_data.attrs['source'] = 'Yahoo Finance'
                            
                            # Add data timestamp (most recent date)
                            if hasattr(ticker_data.index, 'max'):
                                try:
                                    most_recent = ticker_data.index.max()
                                    if isinstance(most_recent, (pd.Timestamp, datetime.datetime, datetime.date)):
                                        ticker_data.attrs['data_timestamp'] = most_recent.isoformat()
                                except:
                                    pass
                            
                            results[ticker] = ticker_data
                
                return results
            except Exception as e:
                print(f"Batch download failed: {str(e)}. Falling back to individual fetching.")
                # If batch download fails, fall back to individual fetching
        
        # For most other data types, use parallel processing for better performance
        with ThreadPoolExecutor(max_workers=min(10, len(ticker_symbols))) as executor:
            # Create a function for thread to execute
            def fetch_ticker_data(ticker):
                try:
                    return ticker, self.get_data(ticker, category, info_type)
                except Exception as e:
                    print(f"Error fetching {info_type} for {ticker}: {str(e)}")
                    return ticker, None
            
            # Submit all tasks and process results as they complete
            future_to_ticker = {executor.submit(fetch_ticker_data, ticker): ticker for ticker in ticker_symbols}
            for future in as_completed(future_to_ticker):
                ticker, data = future.result()
                if data is not None:
                    results[ticker] = data
        
        return results

    def _get_general_info(self, ticker, info_type):
        """Get general information about the stock."""
        if info_type == "Basic Info":
            # Get basic info as a Series and convert to DataFrame
            info = ticker.info
            if info and isinstance(info, dict) and len(info) > 0:
                return pd.DataFrame(list(info.items()), columns=['Attribute', 'Value']).set_index('Attribute')
            return pd.DataFrame()
        
        elif info_type == "Fast Info":
            # Get fast info (quick access to key stats)
            try:
                # Direct access to fast info attributes as a dictionary
                fast_info_dict = {}
                
                # Try to get all fast_info attributes
                try:
                    fast_info_dict['dayHigh'] = ticker.fast_info.get('dayHigh', None)
                    fast_info_dict['dayLow'] = ticker.fast_info.get('dayLow', None)
                    fast_info_dict['lastPrice'] = ticker.fast_info.get('lastPrice', None)
                    fast_info_dict['previousClose'] = ticker.fast_info.get('previousClose', None)
                    fast_info_dict['open'] = ticker.fast_info.get('open', None)
                    fast_info_dict['volume'] = ticker.fast_info.get('volume', None)
                    fast_info_dict['marketCap'] = ticker.fast_info.get('marketCap', None)
                    fast_info_dict['fiftyTwoWeekHigh'] = ticker.fast_info.get('fiftyTwoWeekHigh', None)
                    fast_info_dict['fiftyTwoWeekLow'] = ticker.fast_info.get('fiftyTwoWeekLow', None)
                    fast_info_dict['currency'] = ticker.fast_info.get('currency', None)
                    
                    # Additional attributes if available
                    try:
                        # Additional attributes from 'info' since they might not be in fast_info
                        info = ticker.info
                        if info and isinstance(info, dict):
                            # Add some key financial metrics from info
                            for key in ['pe_ratio', 'forwardPE', 'dividendYield', 'trailingEps', 'forwardEps', 'beta']:
                                if key in info:
                                    fast_info_dict[key] = info[key]
                    except Exception:
                        pass
                except Exception as e:
                    # If direct attribute access fails, try dict method
                    try:
                        fast_info = ticker.fast_info
                        if isinstance(fast_info, dict):
                            fast_info_dict = fast_info
                    except Exception:
                        # Last resort: extract some basic info from ticker.info
                        try:
                            info = ticker.info
                            if info and isinstance(info, dict):
                                # Extract key financial metrics that would typically be in fast_info
                                for key in ['previousClose', 'open', 'dayHigh', 'dayLow', 'volume', 'marketCap']:
                                    if key in info:
                                        fast_info_dict[key] = info[key]
                        except Exception:
                            pass
                
                # Filter out None values
                fast_info_dict = {k: v for k, v in fast_info_dict.items() if v is not None}
                
                if fast_info_dict:
                    return pd.DataFrame(list(fast_info_dict.items()), columns=['Attribute', 'Value']).set_index('Attribute')
            except Exception as e:
                print(f"Error processing fast_info: {str(e)}")
            
            return pd.DataFrame()
        
        elif info_type == "News":
            # Get recent news - updated to handle newer yfinance versions
            try:
                # Get news data
                news = ticker.news
                
                if news is None:
                    return pd.DataFrame()
                
                if not isinstance(news, list):
                    return pd.DataFrame()
                
                if len(news) == 0:
                    return pd.DataFrame()
                
                # The structure has changed - news is now a list of dicts with 'id' and 'content' keys
                formatted_news = []
                for item in news:
                    if not isinstance(item, dict):
                        continue
                        
                    # Check if content exists and is a dictionary
                    if 'content' not in item or not isinstance(item['content'], dict):
                        continue
                        
                    content = item['content']
                    
                    # Get provider info safely
                    provider_name = ''
                    if 'provider' in content and isinstance(content['provider'], dict):
                        provider_name = content['provider'].get('displayName', '')
                    
                    # Get URL safely
                    url = ''
                    if 'clickThroughUrl' in content and isinstance(content['clickThroughUrl'], dict):
                        url = content['clickThroughUrl'].get('url', '')
                    
                    news_item = {
                        'title': content.get('title', ''),
                        'provider': provider_name,
                        'summary': content.get('summary', ''),
                        'published_date': content.get('pubDate', ''),
                        'url': url
                    }
                    formatted_news.append(news_item)
                
                if formatted_news:
                    news_df = pd.DataFrame(formatted_news)
                    # Convert timestamp to readable date if needed
                    if 'published_date' in news_df.columns:
                        news_df['published_date'] = pd.to_datetime(news_df['published_date'])
                    return news_df
            
                # Return empty DataFrame if no news or invalid format
                return pd.DataFrame()
            except Exception as e:
                print(f"Error processing news data: {str(e)}")
                return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown general info type: {info_type}")

    def _get_historical_data(self, ticker, info_type):
        """Get historical data for the stock."""
        if info_type == "Price History":
            # Get price history for at least 5 years
            history = ticker.history(period="5y")
            return history
        
        elif info_type == "Dividends":
            # Get dividend history
            dividends = ticker.dividends
            if dividends is not None:
                if isinstance(dividends, pd.Series) and not dividends.empty:
                    return pd.DataFrame(dividends)
                elif isinstance(dividends, pd.DataFrame) and not dividends.empty:
                    return dividends
                elif isinstance(dividends, dict) and dividends:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(dividends)
            return pd.DataFrame()
        
        elif info_type == "Splits":
            # Get stock split history
            splits = ticker.splits
            if splits is not None:
                if isinstance(splits, pd.Series) and not splits.empty:
                    return pd.DataFrame(splits)
                elif isinstance(splits, pd.DataFrame) and not splits.empty:
                    return splits
                elif isinstance(splits, dict) and splits:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(splits)
            return pd.DataFrame()
        
        elif info_type == "Actions":
            # Get dividend and split history combined
            actions = ticker.actions
            if actions is not None:
                if isinstance(actions, pd.DataFrame) and not actions.empty:
                    return actions
                elif isinstance(actions, dict) and actions:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(actions)
            return pd.DataFrame()
        
        elif info_type == "Capital Gains":
            # Get capital gains (mainly for mutual funds)
            capital_gains = ticker.capital_gains
            if capital_gains is not None:
                if isinstance(capital_gains, pd.Series) and not capital_gains.empty:
                    return pd.DataFrame(capital_gains)
                elif isinstance(capital_gains, pd.DataFrame) and not capital_gains.empty:
                    return capital_gains
                elif isinstance(capital_gains, dict) and capital_gains:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(capital_gains)
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown historical data type: {info_type}")

    def _get_financial_statements(self, ticker, info_type):
        """Get financial statement data for the stock."""
        if info_type == "Income Statement":
            # Get annual income statement
            income_stmt = ticker.income_stmt
            if income_stmt is not None:
                if isinstance(income_stmt, pd.DataFrame) and not income_stmt.empty:
                    return income_stmt
                elif isinstance(income_stmt, dict) and income_stmt:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(income_stmt)
            return pd.DataFrame()
        
        elif info_type == "Balance Sheet":
            # Get annual balance sheet
            balance_sheet = ticker.balance_sheet
            if balance_sheet is not None:
                if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
                    return balance_sheet
                elif isinstance(balance_sheet, dict) and balance_sheet:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(balance_sheet)
            return pd.DataFrame()
        
        elif info_type == "Cash Flow":
            # Get annual cash flow statement
            cashflow = ticker.cashflow
            if cashflow is not None:
                if isinstance(cashflow, pd.DataFrame) and not cashflow.empty:
                    return cashflow
                elif isinstance(cashflow, dict) and cashflow:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(cashflow)
            return pd.DataFrame()
        
        elif info_type == "Earnings":
            # Get earnings data
            earnings = ticker.earnings
            if earnings is not None:
                if isinstance(earnings, pd.DataFrame) and not earnings.empty:
                    return earnings
                elif isinstance(earnings, dict) and earnings:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(earnings)
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown financial statement type: {info_type}")

    def batch_process_tickers(self, ticker_symbols, callback=None, force_refresh=False):
        """
        Process multiple tickers and get all available data for them in parallel.
        
        Parameters:
        ticker_symbols (list): List of ticker symbols to process
        callback (function): Callback function to report progress, takes ticker, category, info_type, success
        force_refresh (bool): Force refresh data even if recent data exists
        
        Returns:
        dict: Nested dictionary mapping tickers -> categories -> info_types -> data
        """
        results = {}
        
        # Categories and info types to process for each ticker
        categories_to_process = {
            "General Information": ["Basic Info", "Fast Info", "News"],
            "Historical Data": ["Price History", "Dividends", "Splits", "Actions"],
            "Financial Statements": ["Income Statement", "Balance Sheet", "Cash Flow", "Earnings"],
            "Analysis & Holdings": [
                "Recommendations", "Analyst Price Targets", "Upgrades Downgrades",
                "Earnings Estimates", "Revenue Estimates", "EPS Trend", "Growth Estimates",
                "Major Holders", "Institutional Holders", "Mutual Fund Holders",
                "Insider Transactions"
            ]
        }
        
        # Process each ticker
        for ticker in ticker_symbols:
            ticker_data = {}
            
            # Process each category
            for category, info_types in categories_to_process.items():
                category_data = {}
                
                # Process each info type for this category
                for info_type in info_types:
                    try:
                        # Check if we have this data already
                        if not force_refresh and self.data_exists(ticker, category, info_type):
                            # Get from database
                            if self.db_manager:
                                data = self.db_manager.get_stored_data(ticker, category, info_type)
                                if data is not None and not data.empty:
                                    category_data[info_type] = data
                                    if callback:
                                        callback(ticker, category, info_type, True, from_cache=True)
                                    continue
                        
                        # Fetch new data
                        data = self.get_data(ticker, category, info_type, force_refresh=force_refresh)
                        
                        if data is not None and not data.empty:
                            category_data[info_type] = data
                            
                            # Store in database if provided
                            if self.db_manager:
                                source = data.attrs.get('source', 'Yahoo Finance') if hasattr(data, 'attrs') else None
                                data_timestamp = data.attrs.get('data_timestamp') if hasattr(data, 'attrs') else None
                                self.db_manager.store_data(ticker, category, info_type, data, 
                                                          data_timestamp=data_timestamp, source=source)
                            
                            # Add to fetched cache
                            cache_key = f"{ticker}_{category}_{info_type}"
                            self._fetched_data_cache.add(cache_key)
                            
                            # Report progress if callback provided
                            if callback:
                                callback(ticker, category, info_type, True)
                    except Exception as e:
                        # Report failure if callback provided
                        if callback:
                            callback(ticker, category, info_type, False, error=str(e))
                
                # Add category data if we got anything
                if category_data:
                    ticker_data[category] = category_data
            
            # Add ticker data to results
            if ticker_data:
                results[ticker] = ticker_data
        
        return results

    def _get_analysis_and_holdings(self, ticker, info_type):
        """Get analysis and holdings data for the stock."""
        if info_type == "Recommendations":
            # Get analyst recommendations (can be DataFrame or dict)
            recommendations = ticker.recommendations
            if recommendations is not None:
                if isinstance(recommendations, pd.DataFrame) and not recommendations.empty:
                    return recommendations
                elif isinstance(recommendations, dict) and recommendations:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(recommendations.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Sustainability":
            # Get ESG sustainability scores
            sustainability = ticker.sustainability
            if sustainability is not None:
                if isinstance(sustainability, pd.DataFrame) and not sustainability.empty:
                    return sustainability
                elif isinstance(sustainability, dict) and sustainability:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(sustainability.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Analyst Price Targets":
            # Get analyst price targets (returns a dict, not DataFrame)
            targets = ticker.analyst_price_targets
            if targets is not None:
                if isinstance(targets, pd.DataFrame) and not targets.empty:
                    return targets
                elif isinstance(targets, dict) and targets:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(targets.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Earnings Estimates":
            # Get earnings estimates
            earnings_est = ticker.earnings_estimate
            if earnings_est is not None:
                if isinstance(earnings_est, pd.DataFrame) and not earnings_est.empty:
                    return earnings_est
                elif isinstance(earnings_est, dict) and earnings_est:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(earnings_est.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Revenue Estimates":
            # Get revenue estimates
            revenue_est = ticker.revenue_estimate
            if revenue_est is not None:
                if isinstance(revenue_est, pd.DataFrame) and not revenue_est.empty:
                    return revenue_est
                elif isinstance(revenue_est, dict) and revenue_est:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(revenue_est.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Major Holders":
            # Get major shareholders
            major_holders = ticker.major_holders
            if major_holders is not None:
                if isinstance(major_holders, pd.DataFrame) and not major_holders.empty:
                    return major_holders
                elif isinstance(major_holders, dict) and major_holders:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(major_holders.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Institutional Holders":
            # Get institutional holders
            institutional_holders = ticker.institutional_holders
            if institutional_holders is not None:
                if isinstance(institutional_holders, pd.DataFrame) and not institutional_holders.empty:
                    return institutional_holders
                elif isinstance(institutional_holders, dict) and institutional_holders:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(institutional_holders.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Mutual Fund Holders":
            # Get mutual fund holders
            mutualfund_holders = ticker.mutualfund_holders
            if mutualfund_holders is not None:
                if isinstance(mutualfund_holders, pd.DataFrame) and not mutualfund_holders.empty:
                    return mutualfund_holders
                elif isinstance(mutualfund_holders, dict) and mutualfund_holders:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(mutualfund_holders.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Insider Transactions":
            # Get insider transactions
            try:
                insider_transactions = ticker.insider_transactions
                if insider_transactions is not None:
                    if isinstance(insider_transactions, pd.DataFrame) and not insider_transactions.empty:
                        return insider_transactions
                    elif isinstance(insider_transactions, dict) and insider_transactions:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(insider_transactions.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving insider transactions: {str(e)}")
            return pd.DataFrame()
        
        elif info_type == "Upgrades Downgrades":
            # Get upgrades and downgrades
            try:
                upgrades_downgrades = ticker.upgrades_downgrades
                if upgrades_downgrades is not None:
                    if isinstance(upgrades_downgrades, pd.DataFrame) and not upgrades_downgrades.empty:
                        return upgrades_downgrades
                    elif isinstance(upgrades_downgrades, dict) and upgrades_downgrades:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(upgrades_downgrades.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving upgrades/downgrades: {str(e)}")
            return pd.DataFrame()
            
        elif info_type == "Earnings History":
            # Get earnings history
            try:
                earnings_history = ticker.earnings_history
                if earnings_history is not None:
                    if isinstance(earnings_history, pd.DataFrame) and not earnings_history.empty:
                        return earnings_history
                    elif isinstance(earnings_history, dict) and earnings_history:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(earnings_history.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving earnings history: {str(e)}")
            return pd.DataFrame()
            
        elif info_type == "EPS Trend":
            # Get EPS trend
            try:
                eps_trend = ticker.eps_trend
                if eps_trend is not None:
                    if isinstance(eps_trend, pd.DataFrame) and not eps_trend.empty:
                        return eps_trend
                    elif isinstance(eps_trend, dict) and eps_trend:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(eps_trend.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving EPS trend: {str(e)}")
            return pd.DataFrame()
            
        elif info_type == "Growth Estimates":
            # Get growth estimates
            try:
                growth_estimates = ticker.growth_estimates
                if growth_estimates is not None:
                    if isinstance(growth_estimates, pd.DataFrame) and not growth_estimates.empty:
                        return growth_estimates
                    elif isinstance(growth_estimates, dict) and growth_estimates:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(growth_estimates.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving growth estimates: {str(e)}")
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown analysis/holdings type: {info_type}")
