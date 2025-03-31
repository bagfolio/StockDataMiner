import yfinance as yf
import pandas as pd
import datetime
import time

class StockDataFetcher:
    def __init__(self):
        """Initialize the stock data fetcher."""
        pass

    def get_data(self, ticker_symbol, category, info_type):
        """
        Fetch stock data based on category and info type
        
        Parameters:
        ticker_symbol (str): The stock ticker symbol
        category (str): The category of information to fetch
        info_type (str): The specific type of information within the category
        
        Returns:
        DataFrame: The requested stock data
        """
        try:
            # Create a Ticker object for the given symbol
            ticker = yf.Ticker(ticker_symbol)
            
            # Fetch data based on category and info type
            if category == "General Information":
                return self._get_general_info(ticker, info_type)
            elif category == "Historical Data":
                return self._get_historical_data(ticker, info_type)
            elif category == "Financial Statements":
                return self._get_financial_statements(ticker, info_type)
            elif category == "Analysis & Holdings":
                return self._get_analysis_and_holdings(ticker, info_type)
            else:
                raise ValueError(f"Unknown category: {category}")
        except Exception as e:
            raise Exception(f"Error fetching {info_type} for {ticker_symbol}: {str(e)}")

    def _get_general_info(self, ticker, info_type):
        """Get general information about the stock."""
        if info_type == "Basic Info":
            # Get basic info as a Series and convert to DataFrame
            info = ticker.info
            if info:
                return pd.DataFrame(list(info.items()), columns=['Attribute', 'Value']).set_index('Attribute')
            return pd.DataFrame()
        
        elif info_type == "Fast Info":
            # Get fast info (quick access to key stats)
            fast_info = ticker.fast_info
            if fast_info:
                return pd.DataFrame(list(fast_info.items()), columns=['Attribute', 'Value']).set_index('Attribute')
            return pd.DataFrame()
        
        elif info_type == "News":
            # Get recent news
            news = ticker.news
            if news:
                news_df = pd.DataFrame(news)
                # Keep only relevant columns
                cols_to_keep = ['title', 'publisher', 'link', 'providerPublishTime']
                news_df = news_df[cols_to_keep].copy()
                # Convert timestamp to readable date
                news_df['providerPublishTime'] = pd.to_datetime(news_df['providerPublishTime'], unit='s')
                news_df.rename(columns={'providerPublishTime': 'Published Date'}, inplace=True)
                return news_df
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown general info type: {info_type}")

    def _get_historical_data(self, ticker, info_type):
        """Get historical data for the stock."""
        if info_type == "Price History":
            # Get price history for the past year
            history = ticker.history(period="1y")
            return history
        
        elif info_type == "Dividends":
            # Get dividend history
            dividends = ticker.dividends
            if not dividends.empty:
                return pd.DataFrame(dividends)
            return pd.DataFrame()
        
        elif info_type == "Splits":
            # Get stock split history
            splits = ticker.splits
            if not splits.empty:
                return pd.DataFrame(splits)
            return pd.DataFrame()
        
        elif info_type == "Actions":
            # Get dividend and split history combined
            actions = ticker.actions
            if not actions.empty:
                return actions
            return pd.DataFrame()
        
        elif info_type == "Capital Gains":
            # Get capital gains (mainly for mutual funds)
            capital_gains = ticker.capital_gains
            if not capital_gains.empty:
                return pd.DataFrame(capital_gains)
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown historical data type: {info_type}")

    def _get_financial_statements(self, ticker, info_type):
        """Get financial statement data for the stock."""
        if info_type == "Income Statement":
            # Get annual income statement
            income_stmt = ticker.income_stmt
            if not income_stmt.empty:
                return income_stmt
            return pd.DataFrame()
        
        elif info_type == "Balance Sheet":
            # Get annual balance sheet
            balance_sheet = ticker.balance_sheet
            if not balance_sheet.empty:
                return balance_sheet
            return pd.DataFrame()
        
        elif info_type == "Cash Flow":
            # Get annual cash flow statement
            cashflow = ticker.cashflow
            if not cashflow.empty:
                return cashflow
            return pd.DataFrame()
        
        elif info_type == "Earnings":
            # Get earnings data
            earnings = ticker.earnings
            if not earnings.empty:
                return earnings
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown financial statement type: {info_type}")

    def _get_analysis_and_holdings(self, ticker, info_type):
        """Get analysis and holdings data for the stock."""
        if info_type == "Recommendations":
            # Get analyst recommendations
            recommendations = ticker.recommendations
            if recommendations is not None and not recommendations.empty:
                return recommendations
            return pd.DataFrame()
        
        elif info_type == "Sustainability":
            # Get ESG sustainability scores
            sustainability = ticker.sustainability
            if sustainability is not None and not sustainability.empty:
                return sustainability
            return pd.DataFrame()
        
        elif info_type == "Analyst Price Targets":
            # Get analyst price targets
            targets = ticker.analyst_price_targets
            if targets is not None and not targets.empty:
                return targets
            return pd.DataFrame()
        
        elif info_type == "Earnings Estimates":
            # Get earnings estimates
            earnings_est = ticker.earnings_estimate
            if earnings_est is not None and not earnings_est.empty:
                return earnings_est
            return pd.DataFrame()
        
        elif info_type == "Revenue Estimates":
            # Get revenue estimates
            revenue_est = ticker.revenue_estimate
            if revenue_est is not None and not revenue_est.empty:
                return revenue_est
            return pd.DataFrame()
        
        elif info_type == "Major Holders":
            # Get major shareholders
            major_holders = ticker.major_holders
            if major_holders is not None and not major_holders.empty:
                return major_holders
            return pd.DataFrame()
        
        elif info_type == "Institutional Holders":
            # Get institutional holders
            institutional_holders = ticker.institutional_holders
            if institutional_holders is not None and not institutional_holders.empty:
                return institutional_holders
            return pd.DataFrame()
        
        elif info_type == "Mutual Fund Holders":
            # Get mutual fund holders
            mutualfund_holders = ticker.mutualfund_holders
            if mutualfund_holders is not None and not mutualfund_holders.empty:
                return mutualfund_holders
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown analysis/holdings type: {info_type}")
