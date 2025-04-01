# MongoDB Setup Guide

This guide explains how to set up MongoDB Atlas (cloud database) to use with the Stock Data Scraper application.

## Why Use MongoDB?

- **Scalability**: Better handles large datasets compared to SQLite
- **Cloud Access**: Access your data from anywhere
- **Performance**: Faster queries for complex data
- **Integration**: Better for connecting with other applications (like SwipeFolio)

## Setting Up MongoDB Atlas (Free Tier)

1. **Create a MongoDB Atlas Account**:
   - Go to [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
   - Sign up for a free account

2. **Create a New Cluster**:
   - Click "Build a Database"
   - Select the "FREE" tier option
   - Choose a cloud provider (AWS, Google Cloud, or Azure)
   - Select a region close to you
   - Click "Create Cluster" (creation takes 1-3 minutes)

3. **Set Up Database Access**:
   - In the sidebar, go to "Database Access"
   - Click "Add New Database User"
   - Create a username and password (remember these!)
   - Set "Database User Privileges" to "Atlas admin"
   - Click "Add User"

4. **Configure Network Access**:
   - In the sidebar, go to "Network Access"
   - Click "Add IP Address"
   - For development, you can click "Allow Access from Anywhere" (not recommended for production)
   - Click "Confirm"

5. **Get Your Connection String**:
   - Return to your cluster overview
   - Click "Connect"
   - Choose "Connect your application"
   - Select "Python" as your driver and the version "3.6 or later"
   - Copy your connection string which looks like:
     ```
     mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/?retryWrites=true&w=majority&appName=YourAppName
     ```
   - Replace `<username>` and `<password>` with your database user credentials
   - Example format:
     ```
     mongodb+srv://thatguy14066:TZsgoq2OzEUbmmlq@swipecloud1.loenl1s.mongodb.net/?retryWrites=true&w=majority&appName=SwipeCloud1
     ```
   - Make sure pymongo[srv] and dnspython packages are installed

6. **Use in the Stock Data Scraper**:
   - Select "MongoDB (Cloud)" in the sidebar
   - Paste your connection string
   - The application will now use MongoDB for data storage

## Database Structure

The application creates the following collections in MongoDB:

- **tickers**: Information about each stock ticker
- **data_types**: Categories and information types
- **stock_data**: The actual stock data with metadata

## Troubleshooting

- **Connection Issues**: Make sure your IP address is allowed in the Network Access list
- **Authentication Failed**: Double-check your username and password in the connection string
- **Timeout Errors**: Check your internet connection
- **ModuleNotFoundError: No module named 'pymongo'**: Run `pip install pymongo[srv] dnspython`
- **ServerSelectionTimeoutError**: This could be due to network issues, firewall blocking MongoDB port, or incorrect connection string
- **Invalid hostname in URI**: Make sure the cluster name in your connection string is correct
- **URI must include username and password**: Ensure you've replaced `<username>` and `<password>` with your actual credentials

If you encounter persistent issues, the application will automatically fall back to SQLite for local storage.

## Data Migration

To migrate data from SQLite to MongoDB:

1. Use the SQLite database first to collect your data
2. Then switch to MongoDB and fetch the same tickers
3. The application will transfer the data to MongoDB

## Free Tier Limitations

MongoDB Atlas free tier provides:
- 512 MB storage
- Shared RAM and vCPU
- No backups
- Suitable for development/testing

For larger datasets or production use, consider upgrading to a paid tier.