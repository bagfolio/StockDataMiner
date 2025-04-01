import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import yfinance as yf
import os
import time
import numpy as np
import datetime
import json
from database import DatabaseManager
from stock_data import StockDataFetcher

# Import MongoDB manager with a fallback mechanism
try:
    from mongodb_manager import MongoDBManager, HAS_PYMONGO, ObjectId
    import pymongo
    import dnspython
except ImportError:
    HAS_PYMONGO = False
    
    # Fallback ObjectId implementation
    class ObjectId:
        def __init__(self, id_str=None):
            self.id_str = id_str if id_str else str(datetime.datetime.now().timestamp())
            
        def __str__(self):
            return self.id_str

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

# Initialize session state for database settings if not already present
if 'db_type' not in st.session_state:
    st.session_state.db_type = "SQLite (Local)"
if 'mongodb_uri' not in st.session_state:
    st.session_state.mongodb_uri = os.environ.get('MONGODB_URI', '')
if 'db_initialized' not in st.session_state:
    st.session_state.db_initialized = False
if 'db_manager' not in st.session_state:
    st.session_state.db_manager = None
    
# Database selection - check pymongo availability first
if 'pymongo_check_done' not in st.session_state:
    try:
        # Try to import these to see if pymongo is working
        import pymongo
        from bson.objectid import ObjectId
        st.session_state.pymongo_available = True
    except ImportError:
        st.session_state.pymongo_available = False
    st.session_state.pymongo_check_done = True

# Show MongoDB as an option only if available
if st.session_state.pymongo_available:
    db_options = ["SQLite (Local)", "MongoDB (Cloud)"]
    db_help = "Select which database to use for storage"
else:
    db_options = ["SQLite (Local)", "MongoDB (Cloud) - Not Available"]
    db_help = "MongoDB requires the pymongo package to be properly installed"
    # Force SQLite if MongoDB not available
    if st.session_state.db_type == "MongoDB (Cloud)":
        st.session_state.db_type = "SQLite (Local)"

# Display the database selection
db_type = st.sidebar.radio(
    "Database Type", 
    db_options,
    index=0 if st.session_state.db_type == "SQLite (Local)" else 1,
    help=db_help,
    key="db_type_radio"
)

# Disable MongoDB option if not available
if not st.session_state.pymongo_available and db_type == "MongoDB (Cloud) - Not Available":
    st.sidebar.error("MongoDB is not available. Please install PyMongo properly.")
    db_type = "SQLite (Local)"
    st.session_state.db_type = "SQLite (Local)"

# Update session state if selection changed
if db_type != st.session_state.db_type:
    st.session_state.db_type = db_type
    st.session_state.db_initialized = False  # Force re-initialization

# Initialize database based on selection
if db_type == "MongoDB (Cloud)":
    # Get MongoDB connection string from environment variable or user input
    mongodb_uri = st.sidebar.text_input(
        "MongoDB Connection String", 
        value=st.session_state.mongodb_uri,
        type="password", 
        help="Enter your MongoDB connection string (Atlas or local)",
        key="mongodb_uri_input"
    )
    
    # Update session state if connection string changed
    if mongodb_uri != st.session_state.mongodb_uri:
        st.session_state.mongodb_uri = mongodb_uri
        st.session_state.db_initialized = False  # Force re-initialization
    
    # Initialize MongoDB connection if needed
    if not st.session_state.db_initialized and mongodb_uri:
        try:
            # Store in environment variable for future use
            os.environ['MONGODB_URI'] = mongodb_uri
            
            # Use MongoDB Manager
            st.session_state.db_manager = MongoDBManager(mongodb_uri)
            st.sidebar.success("Connected to MongoDB successfully!")
            st.session_state.db_initialized = True
        except Exception as e:
            st.sidebar.error(f"MongoDB connection error: {e}")
            # Fallback to SQLite
            st.sidebar.warning("Falling back to SQLite database...")
            try:
                st.session_state.db_manager = DatabaseManager("stock_data.db")
                st.session_state.db_manager.initialize_database()
                st.session_state.db_initialized = True
            except Exception as e:
                st.sidebar.error(f"Failed to initialize SQLite database: {e}")
    elif not mongodb_uri:
        # No MongoDB URI provided, use SQLite
        if not st.session_state.db_initialized:
            st.sidebar.warning("No MongoDB connection string provided, using SQLite database...")
            try:
                st.session_state.db_manager = DatabaseManager("stock_data.db")
                st.session_state.db_manager.initialize_database()
                st.session_state.db_initialized = True
            except Exception as e:
                st.sidebar.error(f"Failed to initialize SQLite database: {e}")
else:
    # Use SQLite database if not already initialized
    if not st.session_state.db_initialized:
        try:
            # Initialize database
            st.session_state.db_manager = DatabaseManager("stock_data.db")
            st.session_state.db_manager.initialize_database()
            st.session_state.db_initialized = True
        except Exception as e:
            st.sidebar.error(f"Database initialization error: {e}")
            st.sidebar.info("Attempting to reinitialize database...")
            try:
                # Remove existing database file
                if os.path.exists("stock_data.db"):
                    os.remove("stock_data.db")
                st.session_state.db_manager = DatabaseManager("stock_data.db")
                st.session_state.db_manager.initialize_database()
                st.session_state.db_initialized = True
            except Exception as e:
                st.sidebar.error(f"Failed to reinitialize database: {e}")

# Use the database manager from session state
db_manager = st.session_state.db_manager

# Add a documentation link
if db_type == "MongoDB (Cloud)":
    st.sidebar.markdown("""
    📚 [MongoDB Setup Guide](https://github.com/user/stock-data-scraper/blob/main/MONGODB_SETUP.md)
    """)

# A button to view the MongoDB setup guide
if db_type == "MongoDB (Cloud)" and st.sidebar.button("View MongoDB Setup Guide"):
    with open("MONGODB_SETUP.md", "r") as f:
        mongodb_guide = f.read()
    st.markdown(mongodb_guide)

# Initialize the stock data fetcher with database manager
data_fetcher = StockDataFetcher(db_manager=db_manager)

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

# Input for ticker symbol(s) - with two options
st.sidebar.subheader("Enter Stock Symbols")
input_method = st.sidebar.radio(
    "Choose input method:",
    ["Single input field (comma-separated)", "Multiple individual inputs"],
    key="input_method"
)

if input_method == "Single input field (comma-separated)":
    # Original comma-separated input
    ticker_input = st.sidebar.text_input("Enter ticker symbol(s) (comma-separated for multiple)", "AAPL")
    ticker_symbols = [symbol.strip().upper() for symbol in ticker_input.split(",") if symbol.strip()]
else:
    # Individual input fields
    st.sidebar.markdown("Enter up to 10 ticker symbols individually:")
    # Initialize session state for tickers if not already there
    if 'individual_tickers' not in st.session_state:
        st.session_state.individual_tickers = [""] * 10
    
    # Create 10 input fields
    updated_tickers = []
    for i in range(10):
        ticker = st.sidebar.text_input(
            f"Symbol #{i+1}", 
            value=st.session_state.individual_tickers[i],
            key=f"ticker_{i}"
        )
        # Only add non-empty tickers
        if ticker.strip():
            updated_tickers.append(ticker.strip().upper())
    
    # Update session state
    st.session_state.individual_tickers = [
        st.session_state[f"ticker_{i}"] for i in range(10)
    ]
    
    # Set ticker symbols from individual inputs
    ticker_symbols = updated_tickers

# Display the current ticker symbols being used
if ticker_symbols:
    st.sidebar.info(f"Processing symbols: {', '.join(ticker_symbols)}")
else:
    st.sidebar.warning("Please enter at least one ticker symbol")

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
    
    # Options for batch processing
    st.subheader("Batch Processing Options")
    
    # Option to force refresh cached data
    force_refresh = st.checkbox("Force refresh cached data", value=False, 
                               help="When checked, will fetch fresh data even if recent data exists")
    
    # Option to process in parallel
    parallel_processing = st.checkbox("Use parallel processing (faster but more API calls)", value=True,
                                    help="When checked, will process multiple stocks in parallel")
    
    # Progress tracking
    progress_placeholder = st.empty()
    
    # Add status display area
    status_area = st.empty()
    
    # Warning about time
    st.warning("This process may take several minutes, especially for multiple tickers or companies with extensive data.")
    
    # Specify max number of stocks to process at once
    max_tickers_col, _ = st.columns([1, 3])
    with max_tickers_col:
        max_batch_tickers = st.number_input("Max tickers per batch", min_value=1, max_value=20, value=5,
                                         help="Maximum number of tickers to process in a single batch")
    
    # Button to start comprehensive data collection
    comprehensive_fetch_button = st.button("Fetch All Data", key="comprehensive_fetch")

# Function to display loading animation
def display_loading(message):
    with st.spinner(message):
        time.sleep(0.5)  # Small delay for visual feedback

# Function to fetch data with optimized batch processing
def fetch_data(symbols, category, info_type):
    all_data = {}
    
    try:
        # Use batch processing for efficiency
        if len(symbols) > 1 and category == "Historical Data" and info_type == "Price History":
            # Efficient batch download for price history
            with st.spinner(f"Batch downloading price history for {len(symbols)} tickers..."):
                # Get data for all symbols in one API call (much faster)
                batch_data = data_fetcher.get_multiple_data(symbols, category, info_type)
                
                if batch_data:
                    for symbol, data in batch_data.items():
                        if data is not None and not data.empty:
                            all_data[symbol] = data
                            
                            # Extract metadata from dataframe attributes
                            source = None
                            data_timestamp = None
                            
                            if hasattr(data, 'attrs'):
                                fetch_timestamp = data.attrs.get('fetch_timestamp')
                                data_timestamp = data.attrs.get('data_timestamp')
                                source = data.attrs.get('source')
                            
                            # Store in database with metadata
                            db_manager.store_data(symbol, category, info_type, data, 
                                                data_timestamp=data_timestamp, source=source)
                        else:
                            st.warning(f"No price history available for {symbol}")
                            
                return all_data
        
        # For other data types or single symbol requests, process individually
        for symbol in symbols:
            try:
                # Get data based on selected category and info type
                data = data_fetcher.get_data(symbol, category, info_type)
                
                if data is not None and not data.empty:
                    all_data[symbol] = data
                    
                    # Extract metadata from dataframe attributes
                    source = None
                    data_timestamp = None
                    
                    if hasattr(data, 'attrs'):
                        fetch_timestamp = data.attrs.get('fetch_timestamp')
                        data_timestamp = data.attrs.get('data_timestamp')
                        source = data.attrs.get('source')
                    
                    # Store in database with metadata
                    db_manager.store_data(symbol, category, info_type, data, 
                                         data_timestamp=data_timestamp, source=source)
                else:
                    st.warning(f"No data available for {symbol} - {info_type}")
            except Exception as e:
                st.error(f"Error fetching {info_type} for {symbol}: {str(e)}")
    except Exception as e:
        st.error(f"Error in batch processing: {str(e)}")
    
    return all_data

# Function to display data and visualizations with timestamp and source info
def display_data(all_data, category, info_type):
    if not all_data:
        return
    
    st.subheader(f"{info_type} for {', '.join(all_data.keys())}")
    
    # Display tabs for each ticker if multiple
    if len(all_data) > 1:
        ticker_tabs = st.tabs(list(all_data.keys()))
        for i, (symbol, data) in enumerate(all_data.items()):
            with ticker_tabs[i]:
                # Display metadata (timestamp and source information)
                with st.expander("Data Metadata", expanded=True):
                    metadata_cols = st.columns(2)
                    
                    with metadata_cols[0]:
                        # Show fetch timestamp
                        fetch_time = data.attrs.get('fetch_timestamp', 'Unknown') if hasattr(data, 'attrs') else 'Unknown'
                        st.info(f"**Data Fetched:** {fetch_time}")
                        
                        # Show data timestamp if available (when the data was actually reported/created)
                        data_time = data.attrs.get('data_timestamp', '') if hasattr(data, 'attrs') else ''
                        if data_time:
                            st.info(f"**Data Reported/Created:** {data_time}")
                    
                    with metadata_cols[1]:
                        # Show source information if available
                        source = data.attrs.get('source', '') if hasattr(data, 'attrs') else ''
                        if source:
                            st.info(f"**Data Source/Provider:** {source}")
                
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
        
        # Display metadata (timestamp and source information)
        with st.expander("Data Metadata", expanded=True):
            metadata_cols = st.columns(2)
            
            with metadata_cols[0]:
                # Show fetch timestamp
                fetch_time = data.attrs.get('fetch_timestamp', 'Unknown') if hasattr(data, 'attrs') else 'Unknown'
                st.info(f"**Data Fetched:** {fetch_time}")
                
                # Show data timestamp if available (when the data was actually reported/created)
                data_time = data.attrs.get('data_timestamp', '') if hasattr(data, 'attrs') else ''
                if data_time:
                    st.info(f"**Data Reported/Created:** {data_time}")
            
            with metadata_cols[1]:
                # Show source information if available
                source = data.attrs.get('source', '') if hasattr(data, 'attrs') else ''
                if source:
                    st.info(f"**Data Source/Provider:** {source}")
        
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
    
    # Add metadata columns to the exported data
    export_data = combined_data.copy()
    
    # Export as CSV
    csv = export_data.to_csv()
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    export_name = f"{'_'.join(all_data.keys())}_{info_type.replace(' ', '_')}_{current_time}.csv"
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
                    
                    # Extract metadata from dataframe attributes
                    source = None
                    data_timestamp = None
                    
                    if hasattr(data, 'attrs'):
                        fetch_timestamp = data.attrs.get('fetch_timestamp')
                        data_timestamp = data.attrs.get('data_timestamp')
                        source = data.attrs.get('source')
                    
                    # Special processing for analyst data to identify sources
                    if category == "Analysis & Holdings":
                        try:
                            # For recommendations, extract firm names as source
                            if info_type in ["Recommendations", "Upgrades Downgrades"] and "Firm" in data.columns:
                                # Get unique firms
                                unique_firms = data["Firm"].unique().tolist()
                                if unique_firms:
                                    # Use up to 5 firm names as source
                                    if len(unique_firms) > 5:
                                        source = f"{', '.join(unique_firms[:5])} and {len(unique_firms) - 5} others"
                                    else:
                                        source = ', '.join(unique_firms)
                            
                            # For earnings data, try to extract source
                            if "Source" in data.columns:
                                sources = data["Source"].unique().tolist()
                                if sources:
                                    source = ', '.join(sources[:3])
                            
                            # For any dated information, try to use the date from index as data_timestamp
                            if hasattr(data.index, 'name') and data.index.name in ['Date', 'Timestamp', 'Period', 'Quarter', 'Year'] and len(data) > 0:
                                # Find most recent date
                                if hasattr(data.index, 'max'):
                                    most_recent = data.index.max()
                                    if isinstance(most_recent, (pd.Timestamp, datetime.datetime, datetime.date, str)):
                                        data_timestamp = str(most_recent)
                        except Exception as ex:
                            # Just continue if metadata extraction fails
                            pass
                    
                    # Store in database with metadata
                    db_manager.store_data(symbol, category, info_type, data, 
                                         data_timestamp=data_timestamp, source=source)
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

# Create a new function for batch processing 
def batch_process_tickers(ticker_symbols, force_refresh=False, callback=None):
    """
    Process multiple tickers in batch with progress tracking
    
    Parameters:
    ticker_symbols (list): List of ticker symbols to process
    force_refresh (bool): Whether to force refresh data
    callback (function): Optional callback for progress updates
    
    Returns:
    dict: Dictionary mapping tickers to their data
    """
    all_tickers_data = {}
    
    # Define a progress callback
    def progress_callback(ticker, category, info_type, success, from_cache=False, error=None):
        if not success:
            status_msg = f"❌ Error with {ticker} - {category} - {info_type}: {error}"
        elif from_cache:
            status_msg = f"📚 Using cached data for {ticker} - {category} - {info_type}"
        else:
            status_msg = f"✅ Fetched {ticker} - {category} - {info_type}"
            
        if callback:
            callback(status_msg)
    
    # Process each ticker using the batch processing capability
    results = data_fetcher.batch_process_tickers(
        ticker_symbols,
        callback=progress_callback,
        force_refresh=force_refresh
    )
    
    return results

# Process Comprehensive Export Tab
if 'comprehensive_fetch_button' in locals() and comprehensive_fetch_button:
    # Make sure there are ticker symbols to process
    if len(ticker_symbols) > 0:
        # Initialize status display
        status_container = st.empty()
        progress_container = st.empty()
        batch_results_container = st.empty()
        
        # Split tickers into batches for processing
        all_tickers = ticker_symbols.copy()
        
        # Process in batches to avoid overwhelming the API
        total_tickers = len(all_tickers)
        batch_count = (total_tickers + max_batch_tickers - 1) // max_batch_tickers  # Ceiling division
        
        # Initialize progress
        progress_bar = progress_container.progress(0)
        
        all_batch_results = {}
        
        # For storing status messages
        if 'status_messages' not in st.session_state:
            st.session_state.status_messages = []
        
        # Keep track of update count to create unique keys
        if 'update_counter' not in st.session_state:
            st.session_state.update_counter = 0
            
        # Function to update status text
        def update_status(message):
            # Add to the beginning of the list (newest first)
            st.session_state.status_messages.insert(0, message)
            
            # Create a unique key based on the counter
            st.session_state.update_counter += 1
            unique_key = f"status_updates_{st.session_state.update_counter}"
            
            # Join all messages with newlines and display
            status_text = "\n".join(st.session_state.status_messages)
            # Use container.markdown instead of text_area to avoid duplicate key issues
            status_area.empty()  # Clear previous content
            status_area.text_area(
                "Processing Status (newest at top):",
                status_text,
                height=200,
                key=unique_key
            )
        
        # Process each batch
        for i in range(batch_count):
            batch_start = i * max_batch_tickers
            batch_end = min(batch_start + max_batch_tickers, total_tickers)
            current_batch = all_tickers[batch_start:batch_end]
            
            batch_results_container.info(f"Processing batch {i+1}/{batch_count}: {', '.join(current_batch)}")
            
            # Process the batch
            with st.spinner(f"Processing batch {i+1}/{batch_count}..."):
                batch_results = batch_process_tickers(
                    current_batch,
                    force_refresh=force_refresh,
                    callback=update_status
                )
                
                # Store results
                all_batch_results.update(batch_results)
            
            # Update progress
            progress_bar.progress((i + 1) / batch_count)
            
            # Show interim results if there are more batches to process
            if i < batch_count - 1:
                batch_results_container.success(f"Completed batch {i+1}/{batch_count}. Processed {len(batch_results)} tickers.")
        
        # Clear the progress bar and show completion message
        progress_container.empty()
        batch_results_container.success(f"✅ Completed processing {len(all_batch_results)} tickers!")
        
        # Process the collected results for display and export
        if all_batch_results:
            # Create tabs for each ticker
            export_tabs = st.tabs(list(all_batch_results.keys()))
            
            for i, (ticker, ticker_data) in enumerate(all_batch_results.items()):
                with export_tabs[i]:
                    st.subheader(f"Comprehensive Data for {ticker}")
                    
                    if ticker_data:
                        # Create expandable sections for each category
                        for category, info_data in ticker_data.items():
                            # Create an expander for each category
                            with st.expander(f"{category} Data", expanded=False):
                                for info_type, data in info_data.items():
                                    st.subheader(info_type)
                                    
                                    # Display metadata (timestamp and source information)
                                    metadata_cols = st.columns(2)
                                    
                                    with metadata_cols[0]:
                                        # Show fetch timestamp
                                        fetch_time = data.attrs.get('fetch_timestamp', 'Unknown') if hasattr(data, 'attrs') else 'Unknown'
                                        st.info(f"**Data Fetched:** {fetch_time}")
                                        
                                        # Show data timestamp if available
                                        data_time = data.attrs.get('data_timestamp', '') if hasattr(data, 'attrs') else ''
                                        if data_time:
                                            st.info(f"**Data Reported/Created:** {data_time}")
                                    
                                    with metadata_cols[1]:
                                        # Show source information if available
                                        source = data.attrs.get('source', '') if hasattr(data, 'attrs') else ''
                                        if source:
                                            st.info(f"**Data Source/Provider:** {source}")
                                    
                                    # Format numeric columns for better readability
                                    formatted_data = data.copy()
                                    for col in formatted_data.select_dtypes(include=['number']).columns:
                                        formatted_data[col] = formatted_data[col].apply(format_number)
                                    
                                    # Display a preview of the data
                                    st.dataframe(formatted_data)
                                    
                                    # Create visualization if possible
                                    create_visualization(data, ticker, category, info_type)
                                    
                                    # Individual export options
                                    csv = data.to_csv()
                                    export_name = f"{ticker}_{info_type.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                    st.download_button(
                                        label=f"Download {info_type} as CSV",
                                        data=csv,
                                        file_name=export_name,
                                        mime="text/csv",
                                        key=f"{ticker}_{category}_{info_type}"  # Unique key for each button
                                    )
                        
                        # Create consolidated export file for all data
                        st.subheader("Consolidated Export")
                        st.write("Download all data in a single consolidated file.")
                        
                        # Create a consolidated CSV for all data
                        consolidated_data = []
                        for category, info_data in ticker_data.items():
                            for info_type, data in info_data.items():
                                # Add metadata columns to identify the data
                                if not data.empty:
                                    data_copy = data.copy().reset_index()
                                    data_copy['Category'] = category
                                    data_copy['InfoType'] = info_type
                                    data_copy['Ticker'] = ticker
                                    
                                    # Add metadata if available
                                    if hasattr(data, 'attrs'):
                                        data_copy['FetchTimestamp'] = data.attrs.get('fetch_timestamp', '')
                                        data_copy['DataTimestamp'] = data.attrs.get('data_timestamp', '')
                                        data_copy['Source'] = data.attrs.get('source', '')
                                        
                                    consolidated_data.append(data_copy)
                        
                        if consolidated_data:
                            # Combine all data frames
                            all_data_df = pd.concat(consolidated_data, ignore_index=True)
                            
                            # Export as CSV
                            csv = all_data_df.to_csv(index=False)
                            export_name = f"{ticker}_Complete_Dataset_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                            st.download_button(
                                label="Download All Data as CSV",
                                data=csv,
                                file_name=export_name,
                                mime="text/csv"
                            )
                            
                            # Show summary stats
                            st.info(f"""
                            **Data Summary for {ticker}:**
                            - Categories: {len(ticker_data)}
                            - Data Types: {sum(len(info_types) for info_types in ticker_data.values())}
                            - Total Data Points: {sum(len(df) for df in consolidated_data)}
                            """)
                        else:
                            st.warning("No data available for consolidated export.")
                    else:
                        st.warning(f"No data was successfully retrieved for {ticker}")
            
            # Multi-ticker export option
            st.subheader("Multi-ticker Export Options")
            
            # List of tickers to include in export (default all)
            tickers_to_export = st.multiselect(
                "Select tickers to include in the multi-ticker export",
                list(all_batch_results.keys()),
                default=list(all_batch_results.keys())
            )
            
            if tickers_to_export and st.button("Create Multi-ticker Export"):
                # Collect all data for selected tickers
                with st.spinner("Preparing multi-ticker export..."):
                    multi_ticker_dfs = []
                    
                    # Process each selected ticker
                    for ticker in tickers_to_export:
                        ticker_data = all_batch_results.get(ticker, {})
                        for category, info_types in ticker_data.items():
                            for info_type, data in info_types.items():
                                if isinstance(data, pd.DataFrame) and not data.empty:
                                    # Add identifying columns
                                    export_df = data.copy().reset_index()
                                    export_df['Ticker'] = ticker
                                    export_df['Category'] = category
                                    export_df['InfoType'] = info_type
                                    
                                    # Add metadata
                                    if hasattr(data, 'attrs'):
                                        export_df['FetchTimestamp'] = data.attrs.get('fetch_timestamp', '')
                                        export_df['DataTimestamp'] = data.attrs.get('data_timestamp', '')
                                        export_df['Source'] = data.attrs.get('source', '')
                                    
                                    multi_ticker_dfs.append(export_df)
                    
                    if multi_ticker_dfs:
                        # Combine all data
                        all_tickers_csv = pd.concat(multi_ticker_dfs, axis=0)
                        
                        # Create a file name with all tickers (shortened if there are too many)
                        if len(tickers_to_export) <= 5:
                            file_name_prefix = f"{'_'.join(tickers_to_export)}"
                        else:
                            file_name_prefix = f"{tickers_to_export[0]}_and_{len(tickers_to_export)-1}_others"
                        
                        # Offer download
                        st.download_button(
                            label=f"Download Combined Data for {len(tickers_to_export)} Tickers",
                            data=all_tickers_csv.to_csv(index=False),
                            file_name=f"{file_name_prefix}_Combined_Dataset_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                        
                        st.success(f"Multi-ticker export prepared with {len(all_tickers_csv)} rows of data across {len(tickers_to_export)} tickers.")
                    else:
                        st.warning("No data available for the selected tickers.")
        else:
            st.warning("No data was retrieved for any of the tickers.")
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
                    
                    # Display metadata with timestamp and source information
                    with st.expander("Data Metadata", expanded=True):
                        metadata_cols = st.columns(2)
                        
                        with metadata_cols[0]:
                            # Show fetch timestamp
                            fetch_time = stored_df.attrs.get('fetch_timestamp', 'Unknown') if hasattr(stored_df, 'attrs') else 'Unknown'
                            st.info(f"**Data Fetched:** {fetch_time}")
                            
                            # Show data timestamp if available (when the data was actually reported/created)
                            data_time = stored_df.attrs.get('data_timestamp', '') if hasattr(stored_df, 'attrs') else ''
                            if data_time:
                                st.info(f"**Data Reported/Created:** {data_time}")
                        
                        with metadata_cols[1]:
                            # Show source information if available
                            source = stored_df.attrs.get('source', '') if hasattr(stored_df, 'attrs') else ''
                            if source:
                                st.info(f"**Data Source/Provider:** {source}")
                    
                    # Format numeric columns for better readability
                    formatted_df = stored_df.copy()
                    for col in formatted_df.select_dtypes(include=['number']).columns:
                        formatted_df[col] = formatted_df[col].apply(format_number)
                        
                    st.dataframe(formatted_df)
                    
                    # Create visualization if possible for stored data
                    create_visualization(stored_df, selected_stored_ticker, selected_stored_category, selected_stored_info)
                    
                    # Export stored data as CSV
                    csv = stored_df.to_csv()
                    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    export_name = f"{selected_stored_ticker}_{selected_stored_info.replace(' ', '_')}_{current_time}.csv"
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

# Export and manage database section
st.markdown("---")
st.subheader("Database Management")

# Two columns for export and reset
db_cols = st.columns(2)

with db_cols[0]:
    # Export database
    with open("stock_data.db", "rb") as file:
        btn = st.download_button(
            label="Download Database File",
            data=file,
            file_name="stock_data.db",
            mime="application/octet-stream"
        )

with db_cols[1]:
    # Clear database button with confirmation
    clear_db = st.button("Clear Database and Start Fresh", type="secondary")
    if clear_db:
        clear_confirm = st.checkbox("Confirm clearing the entire database? This cannot be undone.")
        if clear_confirm:
            if db_manager.clear_database():
                st.success("Database cleared successfully. All stored data has been removed.")
                # Force a rerun to update the sidebar
                st.experimental_rerun()
            else:
                st.error("Failed to clear database. Please try again.")

# Footer with information
st.markdown("---")
st.caption("Data provided by Yahoo Finance via yfinance library. Updated as of request time.")
