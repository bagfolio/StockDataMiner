import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import yfinance as yf
import os
import time
import numpy as np
from database import DatabaseManager
from stock_data import StockDataFetcher

# Helper function to format numbers for better readability
def format_number(val):
    """Format numbers for better readability:
    - Currency/large numbers with commas and 2 decimal places
    - Percentages with 2 decimal places
    - Dates remain unchanged
    - Very small numbers in scientific notation
    """
    if pd.isna(val) or val is None:
        return ''
    
    # Handle different types
    if isinstance(val, (int, float, np.number)):
        # Format percentages
        if isinstance(val, float) and -1 <= val <= 1:
            return f"{val:.2%}"
        
        # Format large numbers (millions+)
        if abs(val) >= 1_000_000:
            return f"${val:,.2f}"
        
        # Format medium numbers with commas
        if abs(val) >= 1000:
            return f"{val:,.2f}"
            
        # Very small numbers in scientific notation
        if abs(val) < 0.001 and val != 0:
            return f"{val:.2e}"
            
        # Other numbers with 2 decimal places
        return f"{val:.2f}"
    
    # Return strings and other types as is
    return val

# Set page title and layout
st.set_page_config(page_title="Stock Data Scraper", layout="wide")

# Initialize database
db_manager = DatabaseManager("stock_data.db")
db_manager.initialize_database()

# Initialize the stock data fetcher
data_fetcher = StockDataFetcher()

# App title and description
st.title("Stock Data Scraper")
st.markdown("""
This application allows you to retrieve, visualize, store, and export stock data from Yahoo Finance.
Enter a ticker symbol and select the information you want to view.
""")

# Sidebar for user inputs
st.sidebar.header("Input Parameters")

# Input for ticker symbol(s)
ticker_input = st.sidebar.text_input("Enter ticker symbol(s) (comma-separated for multiple)", "AAPL")
ticker_symbols = [symbol.strip().upper() for symbol in ticker_input.split(",")]

# Information category selection
st.sidebar.header("Information Categories")

# Organize categories based on the provided yfinance functions
categories = {
    "General Information": ["Basic Info", "Fast Info", "News"],
    "Historical Data": ["Price History", "Dividends", "Splits", "Actions", "Capital Gains"],
    "Financial Statements": ["Income Statement", "Balance Sheet", "Cash Flow", "Earnings"],
    "Analysis & Holdings": ["Recommendations", "Sustainability", "Analyst Price Targets", 
                           "Earnings Estimates", "Revenue Estimates", "Major Holders", 
                           "Institutional Holders", "Mutual Fund Holders"]
}

# Create a dropdown for category selection
selected_category = st.sidebar.selectbox("Select Category", list(categories.keys()))

# Create a dropdown for specific information based on selected category
selected_info = st.sidebar.selectbox("Select Information", categories[selected_category])

# Button to fetch data
fetch_button = st.sidebar.button("Fetch Data")

# Function to display loading animation
def display_loading():
    with st.spinner(f"Fetching {selected_info} for {', '.join(ticker_symbols)}..."):
        time.sleep(0.5)  # Small delay for visual feedback

# Main content area
if fetch_button:
    # Display loading indicator
    display_loading()
    
    # Container for results
    results_container = st.container()
    
    try:
        # Fetch data for each ticker symbol
        all_data = {}
        for symbol in ticker_symbols:
            try:
                # Get data based on selected category and info
                data = data_fetcher.get_data(symbol, selected_category, selected_info)
                
                if data is not None and not data.empty:
                    all_data[symbol] = data
                    
                    # Store data in database
                    db_manager.store_data(symbol, selected_category, selected_info, data)
                else:
                    st.warning(f"No data available for {symbol} - {selected_info}")
            except Exception as e:
                st.error(f"Error fetching data for {symbol}: {str(e)}")
        
        # Display results if data was fetched
        if all_data:
            with results_container:
                st.subheader(f"{selected_info} for {', '.join(ticker_symbols)}")
                
                # Display tabs for each ticker if multiple
                if len(all_data) > 1:
                    tabs = st.tabs(list(all_data.keys()))
                    for i, (symbol, data) in enumerate(all_data.items()):
                        with tabs[i]:
                            # Format numeric columns for better readability
                            formatted_data = data.copy()
                            for col in formatted_data.select_dtypes(include=['number']).columns:
                                formatted_data[col] = formatted_data[col].apply(format_number)
                                
                            st.dataframe(formatted_data)
                            
                            # Create visualization if possible
                            if selected_category == "Historical Data" and selected_info == "Price History":
                                # Plot price history
                                fig = go.Figure()
                                fig.add_trace(go.Scatter(x=data.index, y=data['Close'], name='Close Price'))
                                fig.update_layout(title=f"{symbol} - Close Price", xaxis_title="Date", yaxis_title="Price (USD)")
                                st.plotly_chart(fig)
                            elif data.shape[1] <= 10 and data.shape[0] <= 20 and data.select_dtypes(include=['number']).shape[1] > 0:
                                # Create a simple bar chart for other data types if dimensions are suitable
                                numeric_cols = data.select_dtypes(include=['number']).columns
                                if len(numeric_cols) > 0:
                                    selected_col = numeric_cols[0]
                                    fig = px.bar(data, x=data.index, y=selected_col, title=f"{symbol} - {selected_info} - {selected_col}")
                                    st.plotly_chart(fig)
                else:
                    # Single ticker display
                    symbol = list(all_data.keys())[0]
                    data = all_data[symbol]
                    
                    # Format numeric columns for better readability
                    formatted_data = data.copy()
                    for col in formatted_data.select_dtypes(include=['number']).columns:
                        formatted_data[col] = formatted_data[col].apply(format_number)
                        
                    st.dataframe(formatted_data)
                    
                    # Create visualization if possible
                    if selected_category == "Historical Data" and selected_info == "Price History":
                        # Plot price history
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=data.index, y=data['Close'], name='Close Price'))
                        fig.update_layout(title=f"{symbol} - Close Price", xaxis_title="Date", yaxis_title="Price (USD)")
                        st.plotly_chart(fig)
                    elif data.shape[1] <= 10 and data.shape[0] <= 20 and data.select_dtypes(include=['number']).shape[1] > 0:
                        # Create a simple bar chart for other data types if dimensions are suitable
                        numeric_cols = data.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            selected_col = numeric_cols[0]
                            fig = px.bar(data, x=data.index, y=selected_col, title=f"{symbol} - {selected_info} - {selected_col}")
                            st.plotly_chart(fig)
                
                # Export options
                st.subheader("Export Options")
                
                # Combine all data into a single DataFrame for export
                combined_data = pd.concat(all_data.values(), keys=all_data.keys(), names=['Ticker'])
                
                # Export as CSV
                csv = combined_data.to_csv()
                export_name = f"{'_'.join(ticker_symbols)}_{selected_info.replace(' ', '_')}.csv"
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=export_name,
                    mime="text/csv"
                )
                
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

# Display stored data section
st.sidebar.header("Stored Data")

# Get available data from database
stored_data = db_manager.get_available_data()

if stored_data:
    # Create a selection box for viewing stored data
    st.sidebar.subheader("View Previously Stored Data")
    
    # Get unique tickers from stored data
    unique_tickers = sorted(list(set([entry[0] for entry in stored_data])))
    
    # Create dropdown for ticker selection
    selected_stored_ticker = st.sidebar.selectbox("Select Ticker", unique_tickers)
    
    # Filter data for selected ticker
    ticker_data = [(cat, info) for ticker, cat, info in stored_data if ticker == selected_stored_ticker]
    
    # Get unique categories for the selected ticker
    unique_categories = sorted(list(set([entry[0] for entry in ticker_data])))
    
    if unique_categories:
        # Create dropdown for category selection
        selected_stored_category = st.sidebar.selectbox("Select Category", unique_categories, key="stored_category")
        
        # Filter info for selected category
        category_info = sorted(list(set([entry[1] for entry in ticker_data if entry[0] == selected_stored_category])))
        
        if category_info:
            # Create dropdown for info selection
            selected_stored_info = st.sidebar.selectbox("Select Information", category_info, key="stored_info")
            
            # Button to view stored data
            if st.sidebar.button("View Stored Data"):
                # Retrieve and display the stored data
                stored_df = db_manager.get_stored_data(selected_stored_ticker, selected_stored_category, selected_stored_info)
                
                if stored_df is not None and not stored_df.empty:
                    st.subheader(f"Stored Data: {selected_stored_ticker} - {selected_stored_info}")
                    # Format numeric columns for better readability
                    formatted_df = stored_df.copy()
                    for col in formatted_df.select_dtypes(include=['number']).columns:
                        formatted_df[col] = formatted_df[col].apply(format_number)
                        
                    st.dataframe(formatted_df)
                    
                    # Export stored data as CSV
                    csv = stored_df.to_csv()
                    export_name = f"{selected_stored_ticker}_{selected_stored_info.replace(' ', '_')}_stored.csv"
                    st.download_button(
                        label="Download Stored Data as CSV",
                        data=csv,
                        file_name=export_name,
                        mime="text/csv"
                    )
                else:
                    st.warning("No stored data found for the selected parameters.")
else:
    st.sidebar.write("No data has been stored yet.")

# Footer with information
st.markdown("---")
st.caption("Data provided by Yahoo Finance via yfinance library. Updated as of request time.")
