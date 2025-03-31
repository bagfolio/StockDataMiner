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

# Define variables to avoid potential reference errors
quick_fetch_button = False
custom_fetch_button = False
comprehensive_fetch_button = False
selected_category = ""
selected_info = ""

# App title and description
st.title("Stock Data Scraper")
st.markdown("""
This application allows you to retrieve, visualize, store, and export stock data from Yahoo Finance.
Enter a ticker symbol and select the information you want to view.
""")

# Main user input area
st.sidebar.header("Input Parameters")

# Input for ticker symbol(s)
ticker_input = st.sidebar.text_input("Enter ticker symbol(s) (comma-separated for multiple)", "AAPL")
ticker_symbols = [symbol.strip().upper() for symbol in ticker_input.split(",")]

# Organize categories and data types for internal use and comprehensive data fetching
categories = {
    "General Information": ["Basic Info", "Fast Info", "News"],
    "Historical Data": ["Price History", "Dividends", "Splits", "Actions", "Capital Gains"],
    "Financial Statements": ["Income Statement", "Balance Sheet", "Cash Flow", "Earnings"],
    "Analysis & Holdings": ["Recommendations", "Upgrades Downgrades", "Sustainability", "Analyst Price Targets", 
                           "Earnings Estimates", "Revenue Estimates", "Earnings History", "EPS Trend", 
                           "Growth Estimates", "Major Holders", "Institutional Holders", "Mutual Fund Holders",
                           "Insider Transactions"]
}

# Create tabs for different approaches
tabs = st.tabs(["Quick Analysis", "Custom Query", "Comprehensive Export"])

with tabs[0]:
    st.subheader("Quick Analysis")
    st.write("Get key financial data and visualizations with a single click.")
    
    # Pre-defined common analysis types
    quick_analysis_options = {
        "Price History (5 Years)": ("Historical Data", "Price History"),
        "Key Financial Metrics": ("General Information", "Fast Info"),
        "Analyst Recommendations": ("Analysis & Holdings", "Recommendations"),
        "Upgrades/Downgrades": ("Analysis & Holdings", "Upgrades Downgrades"),
        "Earnings History": ("Analysis & Holdings", "Earnings History"),
        "Growth Estimates": ("Analysis & Holdings", "Growth Estimates"),
        "Insider Transactions": ("Analysis & Holdings", "Insider Transactions"),
        "Latest News": ("General Information", "News")
    }
    
    # Selection and fetch button on the same line
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_quick_analysis = st.selectbox("Select Analysis Type", list(quick_analysis_options.keys()))
    with col2:
        quick_fetch_button = st.button("Analyze", key="quick_fetch")
        
    # Set the category and info type based on selection
    if selected_quick_analysis in quick_analysis_options:
        selected_category, selected_info = quick_analysis_options[selected_quick_analysis]
    
with tabs[1]:
    st.subheader("Custom Query")
    st.write("Select specific data categories and types for detailed analysis.")
    
    # Two-column layout for selections
    col1, col2 = st.columns(2)
    
    with col1:
        # Create a dropdown for category selection
        selected_category = st.selectbox("Select Category", list(categories.keys()))
    
    with col2:
        # Create a dropdown for specific information based on selected category
        selected_info = st.selectbox("Select Information", categories[selected_category])
    
    # Button to fetch data
    custom_fetch_button = st.button("Fetch Data", key="custom_fetch")

with tabs[2]:
    st.subheader("Comprehensive Export")
    st.write("Fetch and download all available data for the specified ticker(s).")
    st.write("This option will fetch ALL available data categories and create a consolidated export file for each ticker.")
    
    # Warning about time
    st.warning("This process may take several minutes for each ticker, especially for companies with extensive data.")
    
    # Button to start comprehensive data collection
    comprehensive_fetch_button = st.button("Fetch All Data", key="comprehensive_fetch")

# Function to display loading animation
def display_loading(message):
    with st.spinner(message):
        time.sleep(0.5)  # Small delay for visual feedback

# Function to fetch data
def fetch_data(symbols, category, info_type):
    all_data = {}
    for symbol in symbols:
        try:
            # Get data based on selected category and info type
            data = data_fetcher.get_data(symbol, category, info_type)
            
            if data is not None and not data.empty:
                all_data[symbol] = data
                
                # Store data in database
                db_manager.store_data(symbol, category, info_type, data)
            else:
                st.warning(f"No data available for {symbol} - {info_type}")
        except Exception as e:
            st.error(f"Error fetching {info_type} for {symbol}: {str(e)}")
    
    return all_data

# Function to display data and visualizations
def display_data(all_data, category, info_type):
    if not all_data:
        return
    
    st.subheader(f"{info_type} for {', '.join(all_data.keys())}")
    
    # Display tabs for each ticker if multiple
    if len(all_data) > 1:
        ticker_tabs = st.tabs(list(all_data.keys()))
        for i, (symbol, data) in enumerate(all_data.items()):
            with ticker_tabs[i]:
                # Format numeric columns for better readability
                formatted_data = data.copy()
                for col in formatted_data.select_dtypes(include=['number']).columns:
                    formatted_data[col] = formatted_data[col].apply(format_number)
                
                st.dataframe(formatted_data)
                
                # Create visualization if possible
                create_visualization(data, symbol, category, info_type)
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
        create_visualization(data, symbol, category, info_type)
    
    # Export options
    st.subheader("Export Options")
    
    # Combine all data into a single DataFrame for export
    combined_data = pd.concat(all_data.values(), keys=all_data.keys(), names=['Ticker'])
    
    # Export as CSV
    csv = combined_data.to_csv()
    export_name = f"{'_'.join(all_data.keys())}_{info_type.replace(' ', '_')}.csv"
    st.download_button(
        label="Download as CSV",
        data=csv,
        file_name=export_name,
        mime="text/csv"
    )

# Function to create appropriate visualizations
def create_visualization(data, symbol, category, info_type):
    if category == "Historical Data" and info_type == "Price History":
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
            fig = px.bar(data, x=data.index, y=selected_col, title=f"{symbol} - {info_type} - {selected_col}")
            st.plotly_chart(fig)
    elif category == "Analysis & Holdings" and info_type in ["Recommendations", "Upgrades Downgrades"]:
        # For recommendations, create a pie chart of the firms if available
        try:
            if "Firm" in data.columns and "To Grade" in data.columns:
                # Group by firm and count occurrences
                firm_counts = data["Firm"].value_counts().reset_index()
                firm_counts.columns = ["Firm", "Count"]
                
                fig = px.pie(firm_counts, values="Count", names="Firm", 
                             title=f"{symbol} - Recommendations by Firm")
                st.plotly_chart(fig)
                
                # Show distribution of grades
                if "To Grade" in data.columns:
                    grade_counts = data["To Grade"].value_counts().reset_index()
                    grade_counts.columns = ["Grade", "Count"]
                    
                    fig = px.bar(grade_counts, x="Grade", y="Count", 
                                title=f"{symbol} - Recommendation Grades Distribution")
                    st.plotly_chart(fig)
        except Exception as e:
            st.info(f"Could not create visualization: {str(e)}")

# Function to fetch and merge all available data for comprehensive export
def fetch_all_data(symbol):
    all_category_data = {}
    
    # Progress bar for tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_categories = sum(len(info_types) for info_types in categories.values())
    processed = 0
    
    # Go through all categories and info types
    for category, info_types in categories.items():
        for info_type in info_types:
            try:
                # Update status
                status_text.text(f"Fetching {info_type} for {symbol}...")
                
                # Get data
                data = data_fetcher.get_data(symbol, category, info_type)
                
                if data is not None and not data.empty:
                    # Store in dictionary with category and info type as keys
                    if category not in all_category_data:
                        all_category_data[category] = {}
                    
                    all_category_data[category][info_type] = data
                    
                    # Store in database
                    db_manager.store_data(symbol, category, info_type, data)
            except Exception as e:
                st.error(f"Error fetching {info_type} for {symbol}: {str(e)}")
            
            # Update progress
            processed += 1
            progress_bar.progress(processed / total_categories)
    
    # Clear progress indicators
    progress_bar.empty()
    status_text.empty()
    
    return all_category_data

# Process Quick Analysis Tab
if 'quick_fetch_button' in locals() and quick_fetch_button:
    # Get the selected category and info type from quick analysis options
    if selected_quick_analysis in quick_analysis_options:
        selected_category, selected_info = quick_analysis_options[selected_quick_analysis]
        
        # Display loading indicator
        display_loading(f"Fetching {selected_info} for {', '.join(ticker_symbols)}...")
        
        # Create a container for results
        with st.container():
            try:
                # Fetch data for each ticker symbol
                all_data = fetch_data(ticker_symbols, selected_category, selected_info)
                
                # Display the data if any was fetched
                if all_data:
                    display_data(all_data, selected_category, selected_info)
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

# Process Custom Query Tab
if 'custom_fetch_button' in locals() and custom_fetch_button:
    # Display loading indicator
    display_loading(f"Fetching {selected_info} for {', '.join(ticker_symbols)}...")
    
    # Create a container for results
    with st.container():
        try:
            # Fetch data for each ticker symbol
            all_data = fetch_data(ticker_symbols, selected_category, selected_info)
            
            # Display the data if any was fetched
            if all_data:
                display_data(all_data, selected_category, selected_info)
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

# Process Comprehensive Export Tab
if 'comprehensive_fetch_button' in locals() and comprehensive_fetch_button:
    # Container for comprehensive export results
    with st.container():
        # Create tabs for each ticker
        if len(ticker_symbols) > 0:
            export_tabs = st.tabs(ticker_symbols)
            
            for i, symbol in enumerate(ticker_symbols):
                with export_tabs[i]:
                    st.subheader(f"Comprehensive Data for {symbol}")
                    
                    # Fetch all available data for this ticker
                    with st.spinner(f"Fetching all available data for {symbol}. This may take several minutes..."):
                        all_category_data = fetch_all_data(symbol)
                    
                    if all_category_data:
                        # Create exportable data for each category
                        for category, info_data in all_category_data.items():
                            # Create an expander for each category
                            with st.expander(f"{category} Data", expanded=False):
                                for info_type, data in info_data.items():
                                    st.subheader(info_type)
                                    
                                    # Format numeric columns for better readability
                                    formatted_data = data.copy()
                                    for col in formatted_data.select_dtypes(include=['number']).columns:
                                        formatted_data[col] = formatted_data[col].apply(format_number)
                                    
                                    # Display a preview of the data
                                    st.dataframe(formatted_data)
                                    
                                    # Individual export options
                                    csv = data.to_csv()
                                    export_name = f"{symbol}_{info_type.replace(' ', '_')}.csv"
                                    st.download_button(
                                        label=f"Download {info_type} as CSV",
                                        data=csv,
                                        file_name=export_name,
                                        mime="text/csv",
                                        key=f"{symbol}_{category}_{info_type}"  # Unique key for each button
                                    )
                        
                        # Create consolidated export file for all data
                        st.subheader("Consolidated Export")
                        st.write("Download all data in a single consolidated file.")
                        
                        # Prepare Excel writer
                        import io
                        buffer = io.BytesIO()
                        
                        # Create a consolidated CSV for all data
                        consolidated_data = []
                        for category, info_data in all_category_data.items():
                            for info_type, data in info_data.items():
                                # Add metadata columns to identify the data
                                if not data.empty:
                                    data_copy = data.copy()
                                    data_copy['__category'] = category
                                    data_copy['__info_type'] = info_type
                                    consolidated_data.append(data_copy)
                        
                        if consolidated_data:
                            # Combine all data frames
                            all_data_df = pd.concat(consolidated_data, ignore_index=True)
                            
                            # Export as CSV
                            csv = all_data_df.to_csv()
                            export_name = f"{symbol}_all_data.csv"
                            st.download_button(
                                label="Download All Data as CSV",
                                data=csv,
                                file_name=export_name,
                                mime="text/csv"
                            )
                        else:
                            st.warning("No data available for consolidated export.")
                    else:
                        st.warning(f"No data was successfully retrieved for {symbol}")
        else:
            st.warning("Please enter at least one ticker symbol to fetch data.")

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

# Export database section
st.markdown("---")
st.subheader("Export Database")
with open("stock_data.db", "rb") as file:
    btn = st.download_button(
        label="Download Database File",
        data=file,
        file_name="stock_data.db",
        mime="application/octet-stream"
    )

# Footer with information
st.markdown("---")
st.caption("Data provided by Yahoo Finance via yfinance library. Updated as of request time.")
