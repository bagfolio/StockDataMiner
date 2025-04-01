#!/usr/bin/env python3
"""
Simple Database Export Tool for Stock Data Scraper

This script exports all data from the SQLite database to JSON files.
"""

import os
import json
import sqlite3
import pandas as pd
from datetime import datetime

def export_db_structure(db_path="stock_data.db", output_file="db_structure.json"):
    """Export database structure to JSON"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    structure = {}
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [{"name": row[1], "type": row[2], "notnull": row[3], "pk": row[5]} for row in cursor.fetchall()]
        structure[table] = columns
    
    # Write to file
    with open(output_file, 'w') as f:
        json.dump(structure, f, indent=2)
    
    print(f"Database structure exported to {output_file}")
    conn.close()

def export_tickers(db_path="stock_data.db", output_file="tickers.json"):
    """Export all tickers to JSON"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, symbol, name, exchange, first_fetched, last_fetched
        FROM tickers
    """)
    
    tickers = []
    for row in cursor.fetchall():
        tickers.append({
            "id": row[0],
            "symbol": row[1],
            "name": row[2] or "",
            "exchange": row[3] or "",
            "first_fetched": row[4],
            "last_fetched": row[5]
        })
    
    # Write to file
    with open(output_file, 'w') as f:
        json.dump(tickers, f, indent=2)
    
    print(f"Tickers exported to {output_file}")
    conn.close()

def export_categories_and_types(db_path="stock_data.db", output_file="data_types.json"):
    """Export categories and data types to JSON"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get categories
    cursor.execute("SELECT id, name FROM data_categories")
    categories = {}
    for row in cursor.fetchall():
        categories[row[0]] = {
            "name": row[1],
            "types": []
        }
    
    # Get data types
    cursor.execute("""
        SELECT id, category_id, name
        FROM data_types
    """)
    
    for row in cursor.fetchall():
        type_id, category_id, name = row
        if category_id in categories:
            categories[category_id]["types"].append({
                "id": type_id,
                "name": name
            })
    
    # Write to file
    with open(output_file, 'w') as f:
        json.dump(categories, f, indent=2)
    
    print(f"Categories and data types exported to {output_file}")
    conn.close()

def export_stock_data(db_path="stock_data.db", output_dir="exported_data"):
    """Export all stock data to JSON files organized by ticker"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get tickers
    cursor.execute("SELECT id, symbol FROM tickers")
    tickers = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Get data types with categories
    cursor.execute("""
        SELECT dt.id, dc.name, dt.name
        FROM data_types dt
        JOIN data_categories dc ON dt.category_id = dc.id
    """)
    data_types = {row[0]: {"category": row[1], "type": row[2]} for row in cursor.fetchall()}
    
    # Get stock data
    cursor.execute("""
        SELECT ticker_id, data_type_id, data, fetch_timestamp, data_timestamp, source
        FROM stock_data
    """)
    
    data_by_ticker = {}
    for row in cursor.fetchall():
        ticker_id, data_type_id, data_blob, fetch_ts, data_ts, source = row
        
        if ticker_id not in tickers:
            print(f"Warning: Unknown ticker ID {ticker_id}")
            continue
            
        if data_type_id not in data_types:
            print(f"Warning: Unknown data type ID {data_type_id}")
            continue
            
        ticker_symbol = tickers[ticker_id]
        category = data_types[data_type_id]["category"]
        info_type = data_types[data_type_id]["type"]
        
        # Initialize ticker data structure if needed
        if ticker_symbol not in data_by_ticker:
            data_by_ticker[ticker_symbol] = {}
        
        if category not in data_by_ticker[ticker_symbol]:
            data_by_ticker[ticker_symbol][category] = {}
        
        # Try to parse the data
        try:
            data_json = json.loads(data_blob)
            data_by_ticker[ticker_symbol][category][info_type] = {
                "data": data_json,
                "metadata": {
                    "fetch_timestamp": fetch_ts,
                    "data_timestamp": data_ts,
                    "source": source
                }
            }
        except json.JSONDecodeError:
            print(f"Warning: Invalid JSON data for {ticker_symbol}/{category}/{info_type}")
    
    # Write each ticker's data to a separate file
    for ticker, data in data_by_ticker.items():
        output_file = os.path.join(output_dir, f"{ticker}.json")
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Exported data for {ticker} to {output_file}")
    
    # Write a consolidated file with all data
    if data_by_ticker:
        all_data_file = os.path.join(output_dir, "all_stock_data.json")
        with open(all_data_file, 'w') as f:
            json.dump(data_by_ticker, f, indent=2)
        print(f"Exported all stock data to {all_data_file}")
    else:
        print("No stock data found to export")
    
    conn.close()

def main():
    print("=" * 80)
    print("STOCK DATA EXPORT TOOL".center(80))
    print("=" * 80)
    
    # Create exports directory
    exports_dir = "exports"
    if not os.path.exists(exports_dir):
        os.makedirs(exports_dir)
    
    # Export database structure
    export_db_structure(output_file=os.path.join(exports_dir, "db_structure.json"))
    
    # Export tickers
    export_tickers(output_file=os.path.join(exports_dir, "tickers.json"))
    
    # Export categories and data types
    export_categories_and_types(output_file=os.path.join(exports_dir, "data_types.json"))
    
    # Export stock data
    export_stock_data(output_dir=exports_dir)
    
    print("=" * 80)
    print("EXPORT COMPLETE".center(80))
    print("=" * 80)
    print(f"All data has been exported to the '{exports_dir}' directory")

if __name__ == "__main__":
    main()