#!/usr/bin/env python3
"""
Database Diagnostics Tool for Stock Data Scraper

This script performs a comprehensive check of the local SQLite database and MongoDB 
connection, verifying data integrity, providing statistics, and offering export options.
"""

import os
import sys
import json
import pandas as pd
import sqlite3
from datetime import datetime
import traceback
import argparse

# Define colors for terminal output
class TermColors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    """Print formatted header text"""
    print(f"\n{TermColors.HEADER}{TermColors.BOLD}{'=' * 80}{TermColors.ENDC}")
    print(f"{TermColors.HEADER}{TermColors.BOLD}{text.center(80)}{TermColors.ENDC}")
    print(f"{TermColors.HEADER}{TermColors.BOLD}{'=' * 80}{TermColors.ENDC}\n")

def print_success(text):
    """Print success message"""
    print(f"{TermColors.GREEN}✓ {text}{TermColors.ENDC}")

def print_warning(text):
    """Print warning message"""
    print(f"{TermColors.WARNING}⚠ {text}{TermColors.ENDC}")

def print_error(text):
    """Print error message"""
    print(f"{TermColors.FAIL}✗ {text}{TermColors.ENDC}")

def print_info(text):
    """Print info message"""
    print(f"{TermColors.CYAN}ℹ {text}{TermColors.ENDC}")

class SQLiteChecker:
    """Class to check SQLite database integrity and export data"""
    
    def __init__(self, db_path="stock_data.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            if not os.path.exists(self.db_path):
                print_error(f"Database file {self.db_path} does not exist!")
                return False
                
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print_error(f"Failed to connect to SQLite database: {e}")
            return False
            
    def check_tables(self):
        """Check if all expected tables exist"""
        expected_tables = ['tickers', 'data_types', 'stock_data']
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in self.cursor.fetchall()]
        
        missing_tables = [table for table in expected_tables if table not in tables]
        
        if missing_tables:
            print_error(f"Missing tables: {', '.join(missing_tables)}")
            return False
        else:
            print_success("All expected tables exist")
            return True
    
    def check_data_integrity(self):
        """Check data integrity in the database"""
        try:
            # Check ticker references
            self.cursor.execute("""
                SELECT COUNT(*) FROM stock_data 
                WHERE ticker_id NOT IN (SELECT id FROM tickers)
            """)
            orphaned_ticker_data = self.cursor.fetchone()[0]
            
            if orphaned_ticker_data > 0:
                print_warning(f"Found {orphaned_ticker_data} stock data records with invalid ticker references")
            else:
                print_success("All stock data records have valid ticker references")
                
            # Check data type references
            self.cursor.execute("""
                SELECT COUNT(*) FROM stock_data 
                WHERE data_type_id NOT IN (SELECT id FROM data_types)
            """)
            orphaned_data_type_data = self.cursor.fetchone()[0]
            
            if orphaned_data_type_data > 0:
                print_warning(f"Found {orphaned_data_type_data} stock data records with invalid data type references")
            else:
                print_success("All stock data records have valid data type references")
                
            return orphaned_ticker_data == 0 and orphaned_data_type_data == 0
        except Exception as e:
            print_error(f"Failed to check data integrity: {e}")
            return False
    
    def get_database_stats(self):
        """Get statistics about the database"""
        stats = {}
        
        try:
            # Get database file size
            stats['database_size_mb'] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
            
            # Count records in each table
            self.cursor.execute("SELECT COUNT(*) FROM tickers")
            stats['ticker_count'] = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM data_types")
            stats['data_type_count'] = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM stock_data")
            stats['stock_data_count'] = self.cursor.fetchone()[0]
            
            # Get unique categories and info types
            self.cursor.execute("SELECT DISTINCT category FROM data_types")
            stats['unique_categories'] = [row[0] for row in self.cursor.fetchall()]
            
            self.cursor.execute("SELECT DISTINCT info_type FROM data_types")
            stats['unique_info_types'] = [row[0] for row in self.cursor.fetchall()]
            
            # Get earliest and latest timestamps
            self.cursor.execute("SELECT MIN(fetch_timestamp), MAX(fetch_timestamp) FROM stock_data")
            min_timestamp, max_timestamp = self.cursor.fetchone()
            if min_timestamp and max_timestamp:
                stats['earliest_data'] = min_timestamp
                stats['latest_data'] = max_timestamp
            
            return stats
        except Exception as e:
            print_error(f"Failed to get database statistics: {e}")
            return {}
    
    def list_available_tickers(self):
        """List all available tickers in the database"""
        try:
            self.cursor.execute("""
                SELECT t.symbol, t.name, t.exchange, 
                       t.first_fetched, t.last_fetched,
                       COUNT(sd.id) as data_count
                FROM tickers t
                LEFT JOIN stock_data sd ON t.id = sd.ticker_id
                GROUP BY t.id
                ORDER BY t.symbol
            """)
            
            tickers = []
            for row in self.cursor.fetchall():
                tickers.append({
                    'symbol': row[0],
                    'name': row[1],
                    'exchange': row[2],
                    'first_fetched': row[3],
                    'last_fetched': row[4],
                    'data_count': row[5]
                })
            
            return tickers
        except Exception as e:
            print_error(f"Failed to list available tickers: {e}")
            return []
    
    def list_data_for_ticker(self, ticker_symbol):
        """List all data available for a specific ticker"""
        try:
            self.cursor.execute("""
                SELECT dt.category, dt.info_type, sd.fetch_timestamp, sd.data_timestamp, sd.source
                FROM stock_data sd
                JOIN tickers t ON sd.ticker_id = t.id
                JOIN data_types dt ON sd.data_type_id = dt.id
                WHERE t.symbol = ?
                ORDER BY dt.category, dt.info_type
            """, (ticker_symbol,))
            
            data_items = []
            for row in self.cursor.fetchall():
                data_items.append({
                    'category': row[0],
                    'info_type': row[1],
                    'fetch_timestamp': row[2],
                    'data_timestamp': row[3],
                    'source': row[4]
                })
            
            return data_items
        except Exception as e:
            print_error(f"Failed to list data for ticker {ticker_symbol}: {e}")
            return []
    
    def export_all_data_json(self, output_path="stock_data_export.json"):
        """Export all data to a consolidated JSON file"""
        try:
            data_export = {}
            
            # Get all tickers
            self.cursor.execute("SELECT id, symbol FROM tickers")
            tickers = {row[0]: row[1] for row in self.cursor.fetchall()}
            
            # Get all data types
            self.cursor.execute("SELECT id, category, info_type FROM data_types")
            data_types = {row[0]: {'category': row[1], 'info_type': row[2]} for row in self.cursor.fetchall()}
            
            # Organize by ticker
            for ticker_id, ticker_symbol in tickers.items():
                data_export[ticker_symbol] = {}
                
                # Get all data for this ticker
                self.cursor.execute("""
                    SELECT data_type_id, data, fetch_timestamp, data_timestamp, source
                    FROM stock_data
                    WHERE ticker_id = ?
                """, (ticker_id,))
                
                for row in self.cursor.fetchall():
                    data_type_id, data_json, fetch_ts, data_ts, source = row
                    category = data_types[data_type_id]['category']
                    info_type = data_types[data_type_id]['info_type']
                    
                    # Initialize category if not exists
                    if category not in data_export[ticker_symbol]:
                        data_export[ticker_symbol][category] = {}
                    
                    # Add data with metadata
                    data_export[ticker_symbol][category][info_type] = {
                        'data': json.loads(data_json),
                        'metadata': {
                            'fetch_timestamp': fetch_ts,
                            'data_timestamp': data_ts,
                            'source': source
                        }
                    }
            
            # Write to file
            with open(output_path, 'w') as f:
                json.dump(data_export, f, indent=2)
            
            print_success(f"Exported all data to {output_path}")
            return True
        except Exception as e:
            print_error(f"Failed to export data to JSON: {e}")
            traceback.print_exc()
            return False
    
    def export_ticker_csv(self, ticker_symbol, output_dir="exports"):
        """Export all data for a specific ticker to CSV files"""
        try:
            # Create output directory if it doesn't exist
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Get ticker ID
            self.cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (ticker_symbol,))
            result = self.cursor.fetchone()
            if not result:
                print_error(f"Ticker {ticker_symbol} not found in database")
                return False
                
            ticker_id = result[0]
            
            # Get all data for this ticker
            self.cursor.execute("""
                SELECT dt.category, dt.info_type, sd.data
                FROM stock_data sd
                JOIN data_types dt ON sd.data_type_id = dt.id
                WHERE sd.ticker_id = ?
            """, (ticker_id,))
            
            export_count = 0
            for row in self.cursor.fetchall():
                category, info_type, data_json = row
                
                # Create category directory if it doesn't exist
                category_dir = os.path.join(output_dir, ticker_symbol, category)
                if not os.path.exists(category_dir):
                    os.makedirs(category_dir)
                
                # Convert JSON to DataFrame and save as CSV
                try:
                    df = pd.read_json(data_json)
                    output_file = os.path.join(category_dir, f"{info_type}.csv")
                    df.to_csv(output_file, index=True)
                    export_count += 1
                except Exception as e:
                    print_warning(f"Could not export {category}/{info_type} for {ticker_symbol}: {e}")
            
            print_success(f"Exported {export_count} datasets for {ticker_symbol} to {output_dir}/{ticker_symbol}/")
            return True
        except Exception as e:
            print_error(f"Failed to export {ticker_symbol} data to CSV: {e}")
            return False
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()

class MongoDBChecker:
    """Class to check MongoDB connection and data integrity"""
    
    def __init__(self, connection_string=None):
        self.connection_string = connection_string
        self.client = None
        self.db = None
        
    def connect(self):
        """Connect to MongoDB"""
        try:
            # Try to import pymongo
            import pymongo
            from pymongo.mongo_client import MongoClient
            from pymongo.server_api import ServerApi
            
            if not self.connection_string:
                self.connection_string = os.environ.get('MONGODB_URI')
                
            if not self.connection_string:
                print_error("MongoDB connection string not provided")
                return False
            
            print_info(f"Connecting to MongoDB...")
            self.client = MongoClient(self.connection_string, server_api=ServerApi('1'))
            
            # Test connection with a ping
            self.client.admin.command('ping')
            print_success("Successfully connected to MongoDB!")
            
            self.db = self.client["stock_data"]
            return True
        except ImportError:
            print_error("Required MongoDB packages not installed. Run: pip install pymongo[srv] dnspython")
            return False
        except Exception as e:
            print_error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def check_collections(self):
        """Check if all expected collections exist"""
        expected_collections = ['tickers', 'data_types', 'stock_data']
        available_collections = self.db.list_collection_names()
        
        missing_collections = [coll for coll in expected_collections if coll not in available_collections]
        
        if missing_collections:
            print_error(f"Missing collections: {', '.join(missing_collections)}")
            return False
        else:
            print_success("All expected collections exist")
            return True
    
    def get_database_stats(self):
        """Get statistics about the MongoDB database"""
        stats = {}
        
        try:
            # Count documents in each collection
            stats['ticker_count'] = self.db.tickers.count_documents({})
            stats['data_type_count'] = self.db.data_types.count_documents({})
            stats['stock_data_count'] = self.db.stock_data.count_documents({})
            
            # Get unique categories and info types
            stats['unique_categories'] = self.db.data_types.distinct("category")
            stats['unique_info_types'] = self.db.data_types.distinct("info_type")
            
            # Get earliest and latest timestamps
            pipeline = [
                {"$group": {
                    "_id": None,
                    "earliest": {"$min": "$fetch_timestamp"},
                    "latest": {"$max": "$fetch_timestamp"}
                }}
            ]
            timestamp_result = list(self.db.stock_data.aggregate(pipeline))
            if timestamp_result:
                stats['earliest_data'] = timestamp_result[0].get('earliest')
                stats['latest_data'] = timestamp_result[0].get('latest')
            
            return stats
        except Exception as e:
            print_error(f"Failed to get MongoDB statistics: {e}")
            return {}
    
    def list_available_tickers(self):
        """List all available tickers in MongoDB"""
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "stock_data",
                        "localField": "_id",
                        "foreignField": "ticker_id",
                        "as": "data"
                    }
                },
                {
                    "$project": {
                        "symbol": 1,
                        "name": 1,
                        "exchange": 1,
                        "first_fetched": 1,
                        "last_fetched": 1,
                        "data_count": {"$size": "$data"}
                    }
                },
                {"$sort": {"symbol": 1}}
            ]
            
            tickers = []
            for doc in self.db.tickers.aggregate(pipeline):
                tickers.append({
                    'symbol': doc.get('symbol'),
                    'name': doc.get('name'),
                    'exchange': doc.get('exchange'),
                    'first_fetched': doc.get('first_fetched'),
                    'last_fetched': doc.get('last_fetched'),
                    'data_count': doc.get('data_count')
                })
            
            return tickers
        except Exception as e:
            print_error(f"Failed to list available tickers in MongoDB: {e}")
            return []
    
    def list_data_for_ticker(self, ticker_symbol):
        """List all data available for a specific ticker in MongoDB"""
        try:
            # Get ticker ID
            ticker_doc = self.db.tickers.find_one({"symbol": ticker_symbol})
            if not ticker_doc:
                print_error(f"Ticker {ticker_symbol} not found in MongoDB")
                return []
                
            ticker_id = ticker_doc['_id']
            
            # Get all data for this ticker with data type info
            pipeline = [
                {"$match": {"ticker_id": ticker_id}},
                {
                    "$lookup": {
                        "from": "data_types",
                        "localField": "data_type_id",
                        "foreignField": "_id",
                        "as": "data_type_info"
                    }
                },
                {"$unwind": "$data_type_info"},
                {
                    "$project": {
                        "category": "$data_type_info.category",
                        "info_type": "$data_type_info.info_type",
                        "fetch_timestamp": 1,
                        "data_timestamp": 1,
                        "source": 1
                    }
                },
                {"$sort": {"category": 1, "info_type": 1}}
            ]
            
            data_items = []
            for doc in self.db.stock_data.aggregate(pipeline):
                data_items.append({
                    'category': doc.get('category'),
                    'info_type': doc.get('info_type'),
                    'fetch_timestamp': doc.get('fetch_timestamp'),
                    'data_timestamp': doc.get('data_timestamp'),
                    'source': doc.get('source')
                })
            
            return data_items
        except Exception as e:
            print_error(f"Failed to list data for ticker {ticker_symbol} in MongoDB: {e}")
            return []
    
    def export_all_data_json(self, output_path="mongodb_export.json"):
        """Export all data from MongoDB to a consolidated JSON file"""
        try:
            data_export = {}
            
            # Get all tickers
            tickers = {doc['_id']: doc['symbol'] for doc in self.db.tickers.find({}, {"symbol": 1})}
            
            # Get all data types
            data_types = {doc['_id']: {'category': doc['category'], 'info_type': doc['info_type']} 
                         for doc in self.db.data_types.find({}, {"category": 1, "info_type": 1})}
            
            # Organize by ticker
            for ticker_id, ticker_symbol in tickers.items():
                data_export[ticker_symbol] = {}
                
                # Get all data for this ticker
                stock_data_cursor = self.db.stock_data.find({"ticker_id": ticker_id})
                
                for doc in stock_data_cursor:
                    data_type_id = doc['data_type_id']
                    
                    if data_type_id not in data_types:
                        print_warning(f"Invalid data type ID {data_type_id} for ticker {ticker_symbol}")
                        continue
                        
                    category = data_types[data_type_id]['category']
                    info_type = data_types[data_type_id]['info_type']
                    
                    # Initialize category if not exists
                    if category not in data_export[ticker_symbol]:
                        data_export[ticker_symbol][category] = {}
                    
                    # Add data with metadata
                    try:
                        data_json = json.loads(doc['data'])
                        data_export[ticker_symbol][category][info_type] = {
                            'data': data_json,
                            'metadata': {
                                'fetch_timestamp': doc.get('fetch_timestamp'),
                                'data_timestamp': doc.get('data_timestamp'),
                                'source': doc.get('source')
                            }
                        }
                    except json.JSONDecodeError:
                        print_warning(f"Invalid JSON data for {ticker_symbol}/{category}/{info_type}")
            
            # Write to file
            with open(output_path, 'w') as f:
                json.dump(data_export, f, indent=2)
            
            print_success(f"Exported all MongoDB data to {output_path}")
            return True
        except Exception as e:
            print_error(f"Failed to export MongoDB data to JSON: {e}")
            traceback.print_exc()
            return False
    
    def close(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()

def run_sqlite_diagnostics(auto_export=False):
    """Run diagnostics on the SQLite database"""
    print_header("SQLITE DATABASE DIAGNOSTICS")
    
    checker = SQLiteChecker()
    if not checker.connect():
        return False
    
    print_header("CHECKING DATABASE STRUCTURE")
    tables_ok = checker.check_tables()
    
    print_header("CHECKING DATA INTEGRITY")
    integrity_ok = checker.check_data_integrity()
    
    print_header("DATABASE STATISTICS")
    stats = checker.get_database_stats()
    if stats:
        print_info(f"Database size: {stats.get('database_size_mb', 'N/A')} MB")
        print_info(f"Number of tickers: {stats.get('ticker_count', 'N/A')}")
        print_info(f"Number of data types: {stats.get('data_type_count', 'N/A')}")
        print_info(f"Number of stock data records: {stats.get('stock_data_count', 'N/A')}")
        
        if 'unique_categories' in stats:
            print_info(f"Available categories: {', '.join(stats['unique_categories'])}")
        
        if 'earliest_data' in stats and 'latest_data' in stats:
            print_info(f"Data time range: {stats['earliest_data']} to {stats['latest_data']}")
    
    print_header("AVAILABLE TICKERS")
    tickers = checker.list_available_tickers()
    if tickers:
        print_info(f"Found {len(tickers)} tickers in the database")
        for i, ticker in enumerate(tickers[:10]):  # Show first 10 tickers
            print_info(f"{ticker['symbol']}: {ticker['name'] or 'N/A'} - {ticker['data_count']} data points")
        
        if len(tickers) > 10:
            print_info(f"... and {len(tickers) - 10} more tickers")
    else:
        print_warning("No tickers found in the database")
    
    # Auto export option or prompt the user
    if auto_export:
        print_header("AUTO EXPORTING DATA")
        output_path = "stock_data_export.json"
        print_info(f"Exporting all data to {output_path}")
        checker.export_all_data_json(output_path)
        
        # Also export all tickers to CSV
        if tickers:
            output_dir = "exports"
            for ticker in tickers:
                ticker_symbol = ticker['symbol']
                print_info(f"Exporting {ticker_symbol} to CSV files")
                checker.export_ticker_csv(ticker_symbol, output_dir)
    else:
        # Export options
        print_header("EXPORT OPTIONS")
        print_info("1. Export all data to JSON (stock_data_export.json)")
        print_info("2. Export specific ticker to CSV files (exports/TICKER/)")
        
        try:
            choice = input("Enter your choice (1, 2, or any other key to skip): ")
            
            if choice == '1':
                output_path = input("Enter output file path (default: stock_data_export.json): ") or "stock_data_export.json"
                checker.export_all_data_json(output_path)
            elif choice == '2':
                if tickers:
                    print_info("Available tickers:")
                    for i, ticker in enumerate(tickers):
                        print_info(f"{i+1}. {ticker['symbol']} ({ticker['data_count']} data points)")
                    
                    ticker_idx = input("Enter ticker number or symbol: ")
                    try:
                        # Check if input is a number
                        idx = int(ticker_idx) - 1
                        if 0 <= idx < len(tickers):
                            ticker_symbol = tickers[idx]['symbol']
                        else:
                            print_error("Invalid ticker number")
                            ticker_symbol = None
                    except ValueError:
                        # Input is a symbol
                        ticker_symbol = ticker_idx.upper()
                    
                    if ticker_symbol:
                        output_dir = input("Enter output directory (default: exports): ") or "exports"
                        checker.export_ticker_csv(ticker_symbol, output_dir)
                else:
                    print_error("No tickers available for export")
        except EOFError:
            print_warning("Input stream closed, skipping interactive export")
    
    checker.close()
    return tables_ok and integrity_ok

def run_mongodb_diagnostics(connection_string=None, auto_export=False):
    """Run diagnostics on MongoDB"""
    print_header("MONGODB DIAGNOSTICS")
    
    # Initialize the checker
    checker = MongoDBChecker(connection_string)
    if not checker.connect():
        return False
    
    print_header("CHECKING DATABASE STRUCTURE")
    collections_ok = checker.check_collections()
    
    print_header("DATABASE STATISTICS")
    stats = checker.get_database_stats()
    if stats:
        print_info(f"Number of tickers: {stats.get('ticker_count', 'N/A')}")
        print_info(f"Number of data types: {stats.get('data_type_count', 'N/A')}")
        print_info(f"Number of stock data records: {stats.get('stock_data_count', 'N/A')}")
        
        if 'unique_categories' in stats:
            print_info(f"Available categories: {', '.join(stats['unique_categories'])}")
        
        if 'earliest_data' in stats and 'latest_data' in stats:
            print_info(f"Data time range: {stats['earliest_data']} to {stats['latest_data']}")
    
    print_header("AVAILABLE TICKERS")
    tickers = checker.list_available_tickers()
    if tickers:
        print_info(f"Found {len(tickers)} tickers in MongoDB")
        for i, ticker in enumerate(tickers[:10]):  # Show first 10 tickers
            print_info(f"{ticker['symbol']}: {ticker['name'] or 'N/A'} - {ticker['data_count']} data points")
        
        if len(tickers) > 10:
            print_info(f"... and {len(tickers) - 10} more tickers")
    else:
        print_warning("No tickers found in MongoDB")
    
    # Auto export option or prompt the user
    if auto_export:
        print_header("AUTO EXPORTING DATA")
        output_path = "mongodb_export.json"
        print_info(f"Exporting all MongoDB data to {output_path}")
        checker.export_all_data_json(output_path)
        
        # Also list all tickers with their data
        if tickers:
            for ticker in tickers[:5]:  # Limit to first 5 tickers to avoid too much output
                ticker_symbol = ticker['symbol']
                data_items = checker.list_data_for_ticker(ticker_symbol)
                if data_items:
                    print_info(f"Available data for {ticker_symbol}: {len(data_items)} items")
    else:
        try:
            # Export options
            print_header("EXPORT OPTIONS")
            print_info("1. Export all MongoDB data to JSON (mongodb_export.json)")
            print_info("2. View detailed information for a specific ticker")
            
            choice = input("Enter your choice (1, 2, or any other key to skip): ")
            
            if choice == '1':
                output_path = input("Enter output file path (default: mongodb_export.json): ") or "mongodb_export.json"
                checker.export_all_data_json(output_path)
            elif choice == '2':
                if tickers:
                    print_info("Available tickers:")
                    for i, ticker in enumerate(tickers):
                        print_info(f"{i+1}. {ticker['symbol']} ({ticker['data_count']} data points)")
                    
                    ticker_idx = input("Enter ticker number or symbol: ")
                    try:
                        # Check if input is a number
                        idx = int(ticker_idx) - 1
                        if 0 <= idx < len(tickers):
                            ticker_symbol = tickers[idx]['symbol']
                        else:
                            print_error("Invalid ticker number")
                            ticker_symbol = None
                    except ValueError:
                        # Input is a symbol
                        ticker_symbol = ticker_idx.upper()
                    
                    if ticker_symbol:
                        data_items = checker.list_data_for_ticker(ticker_symbol)
                        if data_items:
                            print_info(f"Available data for {ticker_symbol}:")
                            for item in data_items:
                                fetch_time = item.get('fetch_timestamp', 'N/A')
                                print_info(f"{item['category']}/{item['info_type']} - Fetched: {fetch_time}")
                        else:
                            print_warning(f"No data found for ticker {ticker_symbol}")
                else:
                    print_error("No tickers available for inspection")
        except EOFError:
            print_warning("Input stream closed, skipping interactive export")
    
    checker.close()
    return collections_ok

def validate_sqlite_to_mongodb(sqlite_db_path="stock_data.db", mongodb_uri=None):
    """Compare data between SQLite and MongoDB to validate migration"""
    print_header("VALIDATING SQLITE TO MONGODB MIGRATION")
    
    # Connect to SQLite
    sqlite_checker = SQLiteChecker(sqlite_db_path)
    if not sqlite_checker.connect():
        return False
    
    # Connect to MongoDB
    mongodb_checker = MongoDBChecker(mongodb_uri)
    if not mongodb_checker.connect():
        sqlite_checker.close()
        return False
    
    # Compare statistics
    sqlite_stats = sqlite_checker.get_database_stats()
    mongodb_stats = mongodb_checker.get_database_stats()
    
    print_header("DATA COMPARISON")
    
    if not sqlite_stats or not mongodb_stats:
        print_error("Could not retrieve statistics from one or both databases")
        sqlite_checker.close()
        mongodb_checker.close()
        return False
    
    # Compare ticker counts
    sqlite_ticker_count = sqlite_stats.get('ticker_count', 0)
    mongodb_ticker_count = mongodb_stats.get('ticker_count', 0)
    
    if sqlite_ticker_count == mongodb_ticker_count:
        print_success(f"Ticker count matches: {sqlite_ticker_count}")
    else:
        print_warning(f"Ticker count mismatch: SQLite: {sqlite_ticker_count}, MongoDB: {mongodb_ticker_count}")
    
    # Compare data type counts
    sqlite_data_type_count = sqlite_stats.get('data_type_count', 0)
    mongodb_data_type_count = mongodb_stats.get('data_type_count', 0)
    
    if sqlite_data_type_count == mongodb_data_type_count:
        print_success(f"Data type count matches: {sqlite_data_type_count}")
    else:
        print_warning(f"Data type count mismatch: SQLite: {sqlite_data_type_count}, MongoDB: {mongodb_data_type_count}")
    
    # Compare stock data counts
    sqlite_stock_data_count = sqlite_stats.get('stock_data_count', 0)
    mongodb_stock_data_count = mongodb_stats.get('stock_data_count', 0)
    
    if sqlite_stock_data_count == mongodb_stock_data_count:
        print_success(f"Stock data count matches: {sqlite_stock_data_count}")
    else:
        print_warning(f"Stock data count mismatch: SQLite: {sqlite_stock_data_count}, MongoDB: {mongodb_stock_data_count}")
    
    # Compare available tickers
    sqlite_tickers = sqlite_checker.list_available_tickers()
    mongodb_tickers = mongodb_checker.list_available_tickers()
    
    sqlite_ticker_symbols = set(t['symbol'] for t in sqlite_tickers)
    mongodb_ticker_symbols = set(t['symbol'] for t in mongodb_tickers)
    
    tickers_in_sqlite_only = sqlite_ticker_symbols - mongodb_ticker_symbols
    tickers_in_mongodb_only = mongodb_ticker_symbols - sqlite_ticker_symbols
    tickers_in_both = sqlite_ticker_symbols.intersection(mongodb_ticker_symbols)
    
    print_info(f"Tickers in both databases: {len(tickers_in_both)}")
    if tickers_in_sqlite_only:
        print_warning(f"Tickers in SQLite only: {len(tickers_in_sqlite_only)}")
        for ticker in sorted(list(tickers_in_sqlite_only)[:5]):
            print_info(f"  - {ticker}")
        if len(tickers_in_sqlite_only) > 5:
            print_info(f"  - ... and {len(tickers_in_sqlite_only) - 5} more")
    
    if tickers_in_mongodb_only:
        print_warning(f"Tickers in MongoDB only: {len(tickers_in_mongodb_only)}")
        for ticker in sorted(list(tickers_in_mongodb_only)[:5]):
            print_info(f"  - {ticker}")
        if len(tickers_in_mongodb_only) > 5:
            print_info(f"  - ... and {len(tickers_in_mongodb_only) - 5} more")
    
    # Check data for a sample ticker
    if tickers_in_both:
        sample_ticker = next(iter(tickers_in_both))
        print_header(f"SAMPLE DATA COMPARISON FOR {sample_ticker}")
        
        sqlite_data = sqlite_checker.list_data_for_ticker(sample_ticker)
        mongodb_data = mongodb_checker.list_data_for_ticker(sample_ticker)
        
        sqlite_data_types = set((d['category'], d['info_type']) for d in sqlite_data)
        mongodb_data_types = set((d['category'], d['info_type']) for d in mongodb_data)
        
        if sqlite_data_types == mongodb_data_types:
            print_success(f"All data types match for {sample_ticker}")
        else:
            in_sqlite_only = sqlite_data_types - mongodb_data_types
            in_mongodb_only = mongodb_data_types - sqlite_data_types
            
            if in_sqlite_only:
                print_warning(f"Data types in SQLite only: {len(in_sqlite_only)}")
                for cat, info in sorted(list(in_sqlite_only)[:5]):
                    print_info(f"  - {cat}/{info}")
            
            if in_mongodb_only:
                print_warning(f"Data types in MongoDB only: {len(in_mongodb_only)}")
                for cat, info in sorted(list(in_mongodb_only)[:5]):
                    print_info(f"  - {cat}/{info}")
    
    sqlite_checker.close()
    mongodb_checker.close()
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Database Diagnostics Tool for Stock Data Scraper')
    parser.add_argument('--sqlite', action='store_true', help='Run SQLite diagnostics')
    parser.add_argument('--mongodb', action='store_true', help='Run MongoDB diagnostics')
    parser.add_argument('--validate', action='store_true', help='Validate SQLite to MongoDB migration')
    parser.add_argument('--mongodb-uri', help='MongoDB connection string')
    parser.add_argument('--sqlite-db', default='stock_data.db', help='SQLite database file path')
    parser.add_argument('--auto-export', action='store_true', help='Automatically export all data without prompting')
    parser.add_argument('--export-path', help='Path for exported data (default: stock_data_export.json)')
    
    args = parser.parse_args()
    
    # Default to auto-export if running from command line
    auto_export = True
    
    # If no specific action is specified, show menu
    if not (args.sqlite or args.mongodb or args.validate):
        try:
            print_header("DATABASE DIAGNOSTICS TOOL")
            print_info("1. Check SQLite database and export data")
            print_info("2. Check MongoDB database and export data")
            print_info("3. Validate SQLite to MongoDB migration")
            print_info("4. Run all checks")
            
            choice = input("Enter your choice (1-4): ")
            
            if choice == '1':
                run_sqlite_diagnostics(auto_export=auto_export)
            elif choice == '2':
                mongodb_uri = args.mongodb_uri or input("Enter MongoDB connection string: ")
                run_mongodb_diagnostics(mongodb_uri, auto_export=auto_export)
            elif choice == '3':
                mongodb_uri = args.mongodb_uri or input("Enter MongoDB connection string: ")
                validate_sqlite_to_mongodb(args.sqlite_db, mongodb_uri)
            elif choice == '4':
                sqlite_ok = run_sqlite_diagnostics(auto_export=auto_export)
                mongodb_uri = args.mongodb_uri or input("Enter MongoDB connection string: ")
                mongodb_ok = run_mongodb_diagnostics(mongodb_uri, auto_export=auto_export)
                if sqlite_ok and mongodb_ok:
                    validate_sqlite_to_mongodb(args.sqlite_db, mongodb_uri)
            else:
                print_error("Invalid choice")
        except EOFError:
            # If input stream is closed, just run SQLite diagnostics with auto-export
            run_sqlite_diagnostics(auto_export=True)
    else:
        # Run specified diagnostics
        if args.sqlite:
            run_sqlite_diagnostics(auto_export=auto_export)
        
        if args.mongodb:
            # Run MongoDB diagnostics with auto_export enabled
            run_mongodb_diagnostics(args.mongodb_uri, auto_export=auto_export)
        
        if args.validate:
            validate_sqlite_to_mongodb(args.sqlite_db, args.mongodb_uri)

if __name__ == "__main__":
    main()