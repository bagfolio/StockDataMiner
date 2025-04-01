import sqlite3
import pandas as pd
import json
import io
import datetime
import zlib
import pickle

class DatabaseManager:
    def __init__(self, db_name):
        """Initialize the database manager with the given database name."""
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.schema_version = 2  # Increment when schema changes

    def connect(self):
        """Connect to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_name)
            # Enable foreign keys
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.cursor = self.conn.cursor()
            return self.conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            self.conn = None
            self.cursor = None
            return None

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def initialize_database(self):
        """Initialize the database schema if it doesn't exist."""
        try:
            conn = self.connect()
            if not conn:
                print("Failed to connect to database during initialization")
                return False
            
            # Check schema version
            try:
                if self.cursor:
                    self.cursor.execute("SELECT value FROM metadata WHERE key = 'schema_version'")
                    result = self.cursor.fetchone()
                    current_version = int(result[0]) if result else 0
                else:
                    current_version = 0
            except:
                current_version = 0
            
            # If database exists but is outdated, drop and recreate
            if current_version > 0 and current_version < self.schema_version:
                print(f"Upgrading database schema from version {current_version} to {self.schema_version}")
                # Drop existing tables in proper order (respecting foreign keys)
                for table in ['stock_data', 'metadata', 'tickers', 'data_categories', 'data_types']:
                    try:
                        if self.cursor:
                            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    except Exception as e:
                        print(f"Error dropping table {table}: {e}")
                
                if conn:
                    conn.commit()
                current_version = 0
            
            # If no schema yet, create all tables from scratch
            if current_version == 0 and self.cursor:
                try:
                    # Create metadata table for tracking schema version and other info
                    self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                    ''')
                
                    # Create reference tables for normalization
                    self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tickers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL UNIQUE,
                        name TEXT,
                        exchange TEXT,
                        first_fetched DATETIME DEFAULT CURRENT_TIMESTAMP,
                        last_fetched DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    ''')
                
                    self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS data_categories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE
                    )
                    ''')
                
                    self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS data_types (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category_id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        UNIQUE(category_id, name),
                        FOREIGN KEY(category_id) REFERENCES data_categories(id)
                    )
                    ''')
                
                    # Create main data table with foreign key references
                    self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker_id INTEGER NOT NULL,
                        data_type_id INTEGER NOT NULL,
                        data BLOB NOT NULL,
                        fetch_timestamp DATETIME NOT NULL,
                        data_timestamp DATETIME,  -- When the data itself was produced/reported
                        source TEXT,  -- Who produced the data (firm name, analyst, etc.)
                        compressed INTEGER DEFAULT 1,  -- Flag for compression
                        is_pickled INTEGER DEFAULT 1,  -- Flag indicating storage format
                        UNIQUE(ticker_id, data_type_id) ON CONFLICT REPLACE,
                        FOREIGN KEY(ticker_id) REFERENCES tickers(id),
                        FOREIGN KEY(data_type_id) REFERENCES data_types(id)
                    )
                    ''')
                
                    # Create indexes for performance
                    self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_data_ticker ON stock_data(ticker_id)')
                    self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_data_fetch_time ON stock_data(fetch_timestamp)')
                    self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_data_data_time ON stock_data(data_timestamp)')
                    
                    # Insert initial reference data
                    # Categories
                    categories = [
                        "General Information",
                        "Historical Data",
                        "Financial Statements",
                        "Analysis & Holdings"
                    ]
                    
                    for category in categories:
                        self.cursor.execute(
                            "INSERT OR IGNORE INTO data_categories (name) VALUES (?)",
                            (category,)
                        )
                    
                    # Data types per category
                    data_types = {
                        "General Information": ["Basic Info", "Fast Info", "News"],
                        "Historical Data": ["Price History", "Dividends", "Splits", "Actions", "Capital Gains"],
                        "Financial Statements": ["Income Statement", "Balance Sheet", "Cash Flow", "Earnings"],
                        "Analysis & Holdings": [
                            "Recommendations", "Sustainability", "Analyst Price Targets", 
                            "Earnings Estimates", "Revenue Estimates", "Major Holders",
                            "Institutional Holders", "Mutual Fund Holders", "Earnings History",
                            "EPS Trend", "Growth Estimates", "Insider Transactions", "Upgrades/Downgrades"
                        ]
                    }
                    
                    for category, types in data_types.items():
                        # Get category ID
                        self.cursor.execute("SELECT id FROM data_categories WHERE name = ?", (category,))
                        category_id = self.cursor.fetchone()[0]
                        
                        # Insert data types
                        for data_type in types:
                            self.cursor.execute(
                                "INSERT OR IGNORE INTO data_types (category_id, name) VALUES (?, ?)",
                                (category_id, data_type)
                            )
                    
                    # Set schema version
                    self.cursor.execute(
                        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                        ("schema_version", str(self.schema_version))
                    )
                    
                    conn.commit()
                except Exception as e:
                    print(f"Error creating schema: {e}")
                    if conn:
                        conn.rollback()
            
        except Exception as e:
            print(f"Error initializing database: {e}")
            if conn:
                conn.rollback()
        finally:
            self.close()

    def get_or_create_ticker_id(self, ticker_symbol, name=None, exchange=None):
        """Get ticker ID or create if not exists, updating last_fetched."""
        try:
            conn = self.connect()
            if not conn or not self.cursor:
                print(f"Failed to connect to database while getting ticker ID for {ticker_symbol}")
                return None
            
            # Check if ticker exists
            self.cursor.execute(
                "SELECT id FROM tickers WHERE symbol = ?", 
                (ticker_symbol,)
            )
            result = self.cursor.fetchone()
            
            if result:
                ticker_id = result[0]
                # Update last_fetched timestamp
                self.cursor.execute(
                    "UPDATE tickers SET last_fetched = CURRENT_TIMESTAMP WHERE id = ?",
                    (ticker_id,)
                )
            else:
                # Insert new ticker
                self.cursor.execute(
                    "INSERT INTO tickers (symbol, name, exchange) VALUES (?, ?, ?)",
                    (ticker_symbol, name, exchange)
                )
                ticker_id = self.cursor.lastrowid
            
            conn.commit()
            return ticker_id
        except Exception as e:
            print(f"Error getting/creating ticker ID: {e}")
            conn.rollback()
            return None
        finally:
            self.close()

    def get_data_type_id(self, category, info_type):
        """Get data_type_id from category and info_type names."""
        try:
            conn = self.connect()
            
            self.cursor.execute('''
            SELECT dt.id 
            FROM data_types dt
            JOIN data_categories dc ON dt.category_id = dc.id
            WHERE dc.name = ? AND dt.name = ?
            ''', (category, info_type))
            
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            else:
                # If data type doesn't exist yet, create it
                self.cursor.execute("SELECT id FROM data_categories WHERE name = ?", (category,))
                result = self.cursor.fetchone()
                
                if not result:
                    # Create category if it doesn't exist
                    self.cursor.execute("INSERT INTO data_categories (name) VALUES (?)", (category,))
                    category_id = self.cursor.lastrowid
                else:
                    category_id = result[0]
                
                # Create data type
                self.cursor.execute(
                    "INSERT INTO data_types (category_id, name) VALUES (?, ?)",
                    (category_id, info_type)
                )
                conn.commit()
                
                return self.cursor.lastrowid
                
        except Exception as e:
            print(f"Error getting data type ID: {e}")
            return None
        finally:
            self.close()

    def store_data(self, ticker, category, info_type, data, data_timestamp=None, source=None):
        """Store data in the database with source and timestamp information."""
        try:
            conn = self.connect()
            
            # Get or create ticker_id
            ticker_id = self.get_or_create_ticker_id(ticker)
            
            # Get data_type_id
            data_type_id = self.get_data_type_id(category, info_type)
            
            if not ticker_id or not data_type_id:
                raise ValueError("Failed to get reference IDs")
            
            # Serialize and compress data for efficient storage
            # Using pickle for DataFrames with custom index types
            # Compression improves storage efficiency a lot for large datasets
            pickled_data = pickle.dumps(data)
            compressed_data = zlib.compress(pickled_data)
            
            fetch_timestamp = datetime.datetime.now().isoformat()
            
            # Extract timestamp from data.attrs if available
            if data_timestamp is None and hasattr(data, 'attrs') and 'fetch_timestamp' in data.attrs:
                data_timestamp = data.attrs['fetch_timestamp']
            
            # Insert or replace data in the database
            self.cursor.execute('''
            INSERT OR REPLACE INTO stock_data 
            (ticker_id, data_type_id, data, fetch_timestamp, data_timestamp, source, compressed, is_pickled)
            VALUES (?, ?, ?, ?, ?, ?, 1, 1)
            ''', (ticker_id, data_type_id, compressed_data, fetch_timestamp, data_timestamp, source))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error storing data: {e}")
            conn.rollback()
            return False
        finally:
            self.close()

    def get_stored_data(self, ticker, category, info_type):
        """Retrieve stored data from the database."""
        try:
            conn = self.connect()
            
            # Query the database for the requested data
            self.cursor.execute('''
            SELECT s.data, s.compressed, s.is_pickled, s.fetch_timestamp, s.data_timestamp, s.source
            FROM stock_data s
            JOIN tickers t ON s.ticker_id = t.id
            JOIN data_types dt ON s.data_type_id = dt.id
            JOIN data_categories dc ON dt.category_id = dc.id
            WHERE t.symbol = ? AND dc.name = ? AND dt.name = ?
            ''', (ticker, category, info_type))
            
            result = self.cursor.fetchone()
            
            if result:
                data_blob, compressed, is_pickled, fetch_timestamp, data_timestamp, source = result
                
                # Decompress if necessary
                if compressed:
                    data_blob = zlib.decompress(data_blob)
                
                # Unpickle if necessary
                if is_pickled:
                    data = pickle.loads(data_blob)
                else:
                    # For backward compatibility with old CSV storage
                    data_csv = data_blob.decode('utf-8')
                    data = pd.read_csv(io.StringIO(data_csv), index_col=0)
                
                # Add metadata to the dataframe if it's a DataFrame
                if isinstance(data, pd.DataFrame):
                    # Add timestamps and source info as DataFrame attributes
                    data.attrs['fetch_timestamp'] = fetch_timestamp
                    if data_timestamp:
                        data.attrs['data_timestamp'] = data_timestamp
                    if source:
                        data.attrs['source'] = source
                    data.attrs['ticker'] = ticker
                    data.attrs['category'] = category
                    data.attrs['info_type'] = info_type
                
                return data
            else:
                return None
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None
        finally:
            self.close()

    def get_available_data(self):
        """Get a list of all available data in the database with timestamps."""
        try:
            # Ensure database is initialized
            self.initialize_database()
            
            conn = self.connect()
            
            # First check if the needed tables exist
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_data'")
            if not self.cursor.fetchone():
                print("The stock_data table does not exist yet")
                return []
                
            try:
                self.cursor.execute('''
                SELECT 
                    t.symbol, 
                    dc.name AS category, 
                    dt.name AS info_type, 
                    s.fetch_timestamp,
                    s.data_timestamp,
                    s.source
                FROM stock_data s
                JOIN tickers t ON s.ticker_id = t.id
                JOIN data_types dt ON s.data_type_id = dt.id
                JOIN data_categories dc ON dt.category_id = dc.id
                ORDER BY t.symbol, dc.name, dt.name
                ''')
                
                results = self.cursor.fetchall()
                return results
            except Exception as e:
                print(f"Error executing query: {e}")
                return []
                
        except Exception as e:
            print(f"Error retrieving available data: {e}")
            return []
        finally:
            self.close()

    def delete_data(self, ticker, category, info_type):
        """Delete specific data from the database."""
        try:
            conn = self.connect()
            
            self.cursor.execute('''
            DELETE FROM stock_data
            WHERE ticker_id = (SELECT id FROM tickers WHERE symbol = ?)
            AND data_type_id = (
                SELECT dt.id
                FROM data_types dt
                JOIN data_categories dc ON dt.category_id = dc.id
                WHERE dc.name = ? AND dt.name = ?
            )
            ''', (ticker, category, info_type))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting data: {e}")
            conn.rollback()
            return False
        finally:
            self.close()
            
    def clear_database(self):
        """Delete all data from the database but keep the schema."""
        try:
            # Make sure database is initialized first
            self.initialize_database()
            
            conn = self.connect()
            
            # Delete all data in proper order (respecting foreign keys)
            try:
                self.cursor.execute("DELETE FROM stock_data")
            except Exception as e:
                print(f"Warning while clearing stock_data: {e}")
                
            try:
                self.cursor.execute("DELETE FROM tickers")
            except Exception as e:
                print(f"Warning while clearing tickers: {e}")
            
            # Reset auto-increment counters
            try:
                self.cursor.execute("DELETE FROM sqlite_sequence WHERE name='stock_data'")
                self.cursor.execute("DELETE FROM sqlite_sequence WHERE name='tickers'") 
            except Exception as e:
                print(f"Warning while resetting sequences: {e}")
            
            conn.commit()
            
            return True
        except Exception as e:
            print(f"Error clearing database: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
            return False
        finally:
            self.close()
            
    def get_stock_metadata(self, ticker_symbol):
        """Get metadata about a stock including when it was first and last fetched."""
        try:
            conn = self.connect()
            
            self.cursor.execute('''
            SELECT 
                t.symbol, 
                t.name,
                t.exchange,
                t.first_fetched,
                t.last_fetched,
                COUNT(s.id) AS data_count
            FROM tickers t
            LEFT JOIN stock_data s ON t.id = s.ticker_id
            WHERE t.symbol = ?
            GROUP BY t.id
            ''', (ticker_symbol,))
            
            result = self.cursor.fetchone()
            
            if result:
                return {
                    'symbol': result[0],
                    'name': result[1],
                    'exchange': result[2],
                    'first_fetched': result[3],
                    'last_fetched': result[4],
                    'data_count': result[5]
                }
            else:
                return None
        except Exception as e:
            print(f"Error retrieving stock metadata: {e}")
            return None
        finally:
            self.close()
