import os
from flask import Flask, jsonify, request
from mongodb_manager import MongoDBManager, ObjectId  # Import our custom ObjectId
import pandas as pd
import json
from datetime import datetime

app = Flask(__name__)

# Initialize MongoDB manager with connection pooling
try:
    mongo_uri = os.environ.get('MONGODB_URI')
    if not mongo_uri:
        raise ValueError("MONGODB_URI environment variable not set")
    # Convert URI to use connection pooler
    pooled_uri = mongo_uri.replace('mongodb+srv://', 'mongodb+srv://', 1)
    db_manager = MongoDBManager(pooled_uri)
except Exception as e:
    print(f"Failed to initialize MongoDB connection: {e}")
    db_manager = None

@app.route('/')
def index():
    """API homepage with documentation"""
    return jsonify({
        "name": "Stock Data API",
        "version": "1.0.0",
        "description": "API for accessing stock data from MongoDB",
        "endpoints": {
            "/api/stocks": "Get a list of all available stocks",
            "/api/stock/<ticker>": "Get all available data for a specific stock",
            "/api/stock/<ticker>/<category>/<info_type>": "Get specific data for a stock",
            "/api/available": "Get a list of all available data"
        }
    })

@app.route('/api/stocks')
def get_stocks():
    """Get a list of all available stocks"""
    try:
        # Get unique tickers
        pipeline = [
            {"$lookup": {"from": "tickers", "localField": "ticker_id", "foreignField": "_id", "as": "ticker_info"}},
            {"$unwind": "$ticker_info"},
            {"$group": {"_id": "$ticker_info.symbol", "name": {"$first": "$ticker_info.name"}, "exchange": {"$first": "$ticker_info.exchange"}}},
            {"$project": {"_id": 0, "symbol": "$_id", "name": 1, "exchange": 1}}
        ]
        stocks = list(db_manager.stock_data.aggregate(pipeline))
        return jsonify({"stocks": stocks})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stock/<ticker>')
def get_stock_data(ticker):
    """Get all available data for a specific stock"""
    try:
        # Get all data categories and types for this ticker
        ticker_doc = db_manager.tickers.find_one({"symbol": ticker})
        if not ticker_doc:
            return jsonify({"error": f"Stock {ticker} not found"}), 404
            
        # Get all data for this ticker
        pipeline = [
            {"$match": {"ticker_id": ticker_doc["_id"]}},
            {"$lookup": {"from": "data_types", "localField": "data_type_id", "foreignField": "_id", "as": "data_type_info"}},
            {"$unwind": "$data_type_info"},
            {"$project": {
                "_id": 0,
                "category": "$data_type_info.category",
                "info_type": "$data_type_info.info_type",
                "fetch_timestamp": 1,
                "data_timestamp": 1,
                "source": 1
            }}
        ]
        available_data = list(db_manager.stock_data.aggregate(pipeline))
        
        # Get stock metadata
        metadata = db_manager.get_stock_metadata(ticker)
        
        return jsonify({
            "symbol": ticker,
            "metadata": metadata,
            "available_data": available_data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stock/<ticker>/<category>/<info_type>')
def get_specific_data(ticker, category, info_type):
    """Get specific data for a stock"""
    try:
        # Retrieve the data
        data = db_manager.get_stored_data(ticker, category, info_type)
        
        if data is None:
            return jsonify({
                "error": f"No data found for {ticker} {category} {info_type}"
            }), 404
            
        # Extract metadata
        metadata = {
            "fetch_timestamp": data.attrs.get("fetch_timestamp"),
            "data_timestamp": data.attrs.get("data_timestamp"),
            "source": data.attrs.get("source")
        }
        
        # Convert DataFrame to records format
        records = data.to_dict(orient="records")
        
        return jsonify({
            "symbol": ticker,
            "category": category,
            "info_type": info_type,
            "metadata": metadata,
            "data": records
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/available')
def get_available_data():
    """Get a list of all available data"""
    try:
        available_data = db_manager.get_available_data()
        
        # Format the data
        formatted_data = [
            {
                "symbol": item[0],
                "category": item[1],
                "info_type": item[2],
                "fetch_timestamp": item[3]
            }
            for item in available_data
        ]
        
        return jsonify({"available_data": formatted_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    # Get port from environment or use 5000 as default
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)