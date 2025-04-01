import pandas as pd
import json
from datetime import datetime
import os

# Define a simple ObjectId replacement
class ObjectId:
    def __init__(self, id_str=None):
        self.id_str = id_str if id_str else str(datetime.now().timestamp())

    def __str__(self):
        return self.id_str

# Try importing pymongo with a fallback
try:
    import pymongo
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    import dnspython
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    # Skip the rest of the file if pymongo isn't available
    # Define a bare minimum class to avoid import errors
    __all__ = ['MongoDBManager', 'ObjectId', 'HAS_PYMONGO']

    # Create a dummy MongoDBManager for imports to work
    class MongoDBManager:
        def __init__(self, *args, **kwargs):
            raise ImportError("MongoDB (pymongo[srv]) package is not properly installed")

# Only define the real MongoDBManager if pymongo is available
if HAS_PYMONGO:
    class MongoDBManager:
        def __init__(self, connection_string=None):
            """
            Initialize connection to MongoDB

            Parameters:
            connection_string (str, optional): MongoDB connection string
                If None, will try to use MONGODB_URI environment variable
            """
            # Use provided connection string or try environment variable
            if connection_string is None:
                connection_string = os.environ.get('MONGODB_URI')

            if not connection_string:
                raise ValueError("MongoDB connection string is required. Set MONGODB_URI environment variable.")

            # Add connection options for better reliability
            if '?' in connection_string:
                connection_string += '&'
            else:
                connection_string += '?'
            connection_string += 'retryWrites=true&w=majority&maxPoolSize=50&waitQueueTimeoutMS=2500'

            # Connect to MongoDB
            self.client = MongoClient(connection_string, server_api=ServerApi('1'))

            # Test connection
            try:
                self.client.admin.command('ping')
                print("Successfully connected to MongoDB!")
            except Exception as e:
                print(f"Warning: MongoDB connection issue: {e}")

            self.db = self.client["stock_data"]

            # Create collections (similar to tables)
            self.tickers = self.db["tickers"]
            self.data_types = self.db["data_types"]
            self.stock_data = self.db["stock_data"]

            # Create indexes for faster queries
            self._create_indexes()

        def _create_indexes(self):
            """Create necessary indexes for performance"""
            try:
                self.tickers.create_index("symbol", unique=True)
                self.data_types.create_index([("category", 1), ("info_type", 1)], unique=True)
                self.stock_data.create_index([("ticker_id", 1), ("data_type_id", 1)])
            except Exception as e:
                print(f"Warning: Could not create indexes: {e}")

        def get_or_create_ticker_id(self, ticker_symbol, name=None, exchange=None):
            """
            Get ticker document ID or create if not exists

            Parameters:
            ticker_symbol (str): The stock ticker symbol
            name (str, optional): Company name
            exchange (str, optional): Stock exchange

            Returns:
            ObjectId: MongoDB document ID for the ticker
            """
            ticker_doc = self.tickers.find_one({"symbol": ticker_symbol})

            if ticker_doc:
                # Update last_fetched time
                self.tickers.update_one(
                    {"_id": ticker_doc["_id"]},
                    {"$set": {"last_fetched": datetime.now()}}
                )
                return ticker_doc["_id"]
            else:
                # Insert new ticker
                result = self.tickers.insert_one({
                    "symbol": ticker_symbol,
                    "name": name,
                    "exchange": exchange,
                    "first_fetched": datetime.now(),
                    "last_fetched": datetime.now()
                })
                return result.inserted_id

        def get_data_type_id(self, category, info_type):
            """
            Get data_type document ID from category and info_type names

            Parameters:
            category (str): The category of information
            info_type (str): The specific type of information within the category

            Returns:
            ObjectId: MongoDB document ID for the data type
            """
            data_type = self.data_types.find_one({
                "category": category,
                "info_type": info_type
            })

            if data_type:
                return data_type["_id"]
            else:
                # Create new data type
                result = self.data_types.insert_one({
                    "category": category,
                    "info_type": info_type
                })
                return result.inserted_id

        def store_data(self, ticker, category, info_type, data, data_timestamp=None, source=None):
            """
            Store data in MongoDB with source and timestamp information

            Parameters:
            ticker (str): The stock ticker symbol
            category (str): The category of information
            info_type (str): The specific type of information within the category
            data (DataFrame): The data to store
            data_timestamp (datetime, optional): When the data was reported/created
            source (str, optional): Source of the data

            Returns:
            bool: True if successful, False otherwise
            """
            if not isinstance(data, pd.DataFrame):
                return False

            try:
                # Get IDs
                ticker_id = self.get_or_create_ticker_id(ticker)
                data_type_id = self.get_data_type_id(category, info_type)

                # Convert DataFrame to JSON
                data_json = data.to_json(orient="table", date_format="iso")

                # Create document
                document = {
                    "ticker_id": ticker_id,
                    "data_type_id": data_type_id,
                    "data": data_json,
                    "fetch_timestamp": datetime.now(),
                    "data_timestamp": data_timestamp,
                    "source": source
                }

                # Upsert (update if exists, insert if not)
                self.stock_data.update_one(
                    {"ticker_id": ticker_id, "data_type_id": data_type_id},
                    {"$set": document},
                    upsert=True
                )

                return True
            except Exception as e:
                print(f"Error storing data: {e}")
                return False

        def get_stored_data(self, ticker, category, info_type):
            """
            Retrieve stored data from MongoDB

            Parameters:
            ticker (str): The stock ticker symbol
            category (str): The category of information
            info_type (str): The specific type of information within the category

            Returns:
            DataFrame or None: The retrieved data or None if not found
            """
            try:
                # Get IDs
                ticker_doc = self.tickers.find_one({"symbol": ticker})
                if not ticker_doc:
                    return None

                data_type = self.data_types.find_one({
                    "category": category,
                    "info_type": info_type
                })
                if not data_type:
                    return None

                # Find the data
                result = self.stock_data.find_one({
                    "ticker_id": ticker_doc["_id"],
                    "data_type_id": data_type["_id"]
                })

                if result:
                    # Convert JSON back to DataFrame
                    df = pd.read_json(result["data"], orient="table")

                    # Add metadata as attributes
                    df.attrs["fetch_timestamp"] = result.get("fetch_timestamp")
                    df.attrs["data_timestamp"] = result.get("data_timestamp")
                    df.attrs["source"] = result.get("source")

                    return df
                return None
            except Exception as e:
                print(f"Error retrieving data: {e}")
                return None

        def get_available_data(self):
            """
            Get a list of all available data in MongoDB

            Returns:
            list: List of tuples (ticker, category, info_type, timestamp)
            """
            try:
                results = []

                # Join collections to get readable names
                pipeline = [
                    {
                        "$lookup": {
                            "from": "tickers",
                            "localField": "ticker_id",
                            "foreignField": "_id",
                            "as": "ticker_info"
                        }
                    },
                    {
                        "$lookup": {
                            "from": "data_types",
                            "localField": "data_type_id",
                            "foreignField": "_id",
                            "as": "data_type_info"
                        }
                    },
                    {
                        "$unwind": "$ticker_info"
                    },
                    {
                        "$unwind": "$data_type_info"
                    },
                    {
                        "$project": {
                            "symbol": "$ticker_info.symbol",
                            "category": "$data_type_info.category",
                            "info_type": "$data_type_info.info_type",
                            "fetch_timestamp": 1
                        }
                    }
                ]

                for doc in self.stock_data.aggregate(pipeline):
                    results.append((
                        doc["symbol"],
                        doc["category"],
                        doc["info_type"],
                        doc.get("fetch_timestamp")
                    ))

                return results
            except Exception as e:
                print(f"Error getting available data: {e}")
                return []

        def delete_data(self, ticker, category, info_type):
            """
            Delete specific data from MongoDB

            Parameters:
            ticker (str): The stock ticker symbol
            category (str): The category of information
            info_type (str): The specific type of information within the category

            Returns:
            bool: True if successful, False otherwise
            """
            try:
                # Get IDs
                ticker_doc = self.tickers.find_one({"symbol": ticker})
                if not ticker_doc:
                    return False

                data_type = self.data_types.find_one({
                    "category": category,
                    "info_type": info_type
                })
                if not data_type:
                    return False

                # Delete the data
                result = self.stock_data.delete_one({
                    "ticker_id": ticker_doc["_id"],
                    "data_type_id": data_type["_id"]
                })

                return result.deleted_count > 0
            except Exception as e:
                print(f"Error deleting data: {e}")
                return False

        def clear_database(self):
            """
            Delete all data from the database

            Returns:
            bool: True if successful, False otherwise
            """
            try:
                self.stock_data.delete_many({})
                return True
            except Exception as e:
                print(f"Error clearing database: {e}")
                return False

        def get_stock_metadata(self, ticker_symbol):
            """
            Get metadata about a stock including when it was first and last fetched

            Parameters:
            ticker_symbol (str): The stock ticker symbol

            Returns:
            dict or None: The metadata or None if not found
            """
            try:
                ticker_doc = self.tickers.find_one({"symbol": ticker_symbol})
                if not ticker_doc:
                    return None

                # Get all data for this ticker
                data_count = self.stock_data.count_documents({"ticker_id": ticker_doc["_id"]})

                return {
                    "symbol": ticker_doc["symbol"],
                    "name": ticker_doc.get("name"),
                    "exchange": ticker_doc.get("exchange"),
                    "first_fetched": ticker_doc.get("first_fetched"),
                    "last_fetched": ticker_doc.get("last_fetched"),
                    "data_count": data_count
                }
            except Exception as e:
                print(f"Error getting stock metadata: {e}")
                return None

        def close(self):
            """Close the MongoDB connection"""
            if self.client:
                self.client.close()