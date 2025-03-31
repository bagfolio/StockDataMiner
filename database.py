import sqlite3
import pandas as pd
import json
import io

class DatabaseManager:
    def __init__(self, db_name):
        """Initialize the database manager with the given database name."""
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        """Connect to the SQLite database."""
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        return self.conn

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
            
            # Create table to store stock data
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                category TEXT NOT NULL,
                info_type TEXT NOT NULL,
                data BLOB NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, category, info_type) ON CONFLICT REPLACE
            )
            ''')
            
            conn.commit()
        except Exception as e:
            print(f"Error initializing database: {e}")
        finally:
            self.close()

    def store_data(self, ticker, category, info_type, data):
        """Store data in the database."""
        try:
            conn = self.connect()
            
            # Convert DataFrame to CSV for storage
            data_csv = data.to_csv()
            
            # Insert or replace data in the database
            self.cursor.execute('''
            INSERT OR REPLACE INTO stock_data (ticker, category, info_type, data)
            VALUES (?, ?, ?, ?)
            ''', (ticker, category, info_type, data_csv))
            
            conn.commit()
        except Exception as e:
            print(f"Error storing data: {e}")
        finally:
            self.close()

    def get_stored_data(self, ticker, category, info_type):
        """Retrieve stored data from the database."""
        try:
            conn = self.connect()
            
            # Query the database for the requested data
            self.cursor.execute('''
            SELECT data FROM stock_data
            WHERE ticker = ? AND category = ? AND info_type = ?
            ''', (ticker, category, info_type))
            
            result = self.cursor.fetchone()
            
            if result:
                # Convert CSV string back to DataFrame
                data_csv = result[0]
                return pd.read_csv(io.StringIO(data_csv), index_col=0)
            else:
                return None
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None
        finally:
            self.close()

    def get_available_data(self):
        """Get a list of all available data in the database."""
        try:
            conn = self.connect()
            
            self.cursor.execute('''
            SELECT ticker, category, info_type FROM stock_data
            ORDER BY ticker, category, info_type
            ''')
            
            results = self.cursor.fetchall()
            return results
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
            WHERE ticker = ? AND category = ? AND info_type = ?
            ''', (ticker, category, info_type))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting data: {e}")
            return False
        finally:
            self.close()
