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
            if dividends is not None:
                if isinstance(dividends, pd.Series) and not dividends.empty:
                    return pd.DataFrame(dividends)
                elif isinstance(dividends, pd.DataFrame) and not dividends.empty:
                    return dividends
                elif isinstance(dividends, dict) and dividends:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(dividends)
            return pd.DataFrame()
        
        elif info_type == "Splits":
            # Get stock split history
            splits = ticker.splits
            if splits is not None:
                if isinstance(splits, pd.Series) and not splits.empty:
                    return pd.DataFrame(splits)
                elif isinstance(splits, pd.DataFrame) and not splits.empty:
                    return splits
                elif isinstance(splits, dict) and splits:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(splits)
            return pd.DataFrame()
        
        elif info_type == "Actions":
            # Get dividend and split history combined
            actions = ticker.actions
            if actions is not None:
                if isinstance(actions, pd.DataFrame) and not actions.empty:
                    return actions
                elif isinstance(actions, dict) and actions:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(actions)
            return pd.DataFrame()
        
        elif info_type == "Capital Gains":
            # Get capital gains (mainly for mutual funds)
            capital_gains = ticker.capital_gains
            if capital_gains is not None:
                if isinstance(capital_gains, pd.Series) and not capital_gains.empty:
                    return pd.DataFrame(capital_gains)
                elif isinstance(capital_gains, pd.DataFrame) and not capital_gains.empty:
                    return capital_gains
                elif isinstance(capital_gains, dict) and capital_gains:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(capital_gains)
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown historical data type: {info_type}")

    def _get_financial_statements(self, ticker, info_type):
        """Get financial statement data for the stock."""
        if info_type == "Income Statement":
            # Get annual income statement
            income_stmt = ticker.income_stmt
            if income_stmt is not None:
                if isinstance(income_stmt, pd.DataFrame) and not income_stmt.empty:
                    return income_stmt
                elif isinstance(income_stmt, dict) and income_stmt:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(income_stmt)
            return pd.DataFrame()
        
        elif info_type == "Balance Sheet":
            # Get annual balance sheet
            balance_sheet = ticker.balance_sheet
            if balance_sheet is not None:
                if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
                    return balance_sheet
                elif isinstance(balance_sheet, dict) and balance_sheet:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(balance_sheet)
            return pd.DataFrame()
        
        elif info_type == "Cash Flow":
            # Get annual cash flow statement
            cashflow = ticker.cashflow
            if cashflow is not None:
                if isinstance(cashflow, pd.DataFrame) and not cashflow.empty:
                    return cashflow
                elif isinstance(cashflow, dict) and cashflow:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(cashflow)
            return pd.DataFrame()
        
        elif info_type == "Earnings":
            # Get earnings data
            earnings = ticker.earnings
            if earnings is not None:
                if isinstance(earnings, pd.DataFrame) and not earnings.empty:
                    return earnings
                elif isinstance(earnings, dict) and earnings:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame.from_dict(earnings)
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown financial statement type: {info_type}")

    def _get_analysis_and_holdings(self, ticker, info_type):
        """Get analysis and holdings data for the stock."""
        if info_type == "Recommendations":
            # Get analyst recommendations (can be DataFrame or dict)
            recommendations = ticker.recommendations
            if recommendations is not None:
                if isinstance(recommendations, pd.DataFrame) and not recommendations.empty:
                    return recommendations
                elif isinstance(recommendations, dict) and recommendations:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(recommendations.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Sustainability":
            # Get ESG sustainability scores
            sustainability = ticker.sustainability
            if sustainability is not None:
                if isinstance(sustainability, pd.DataFrame) and not sustainability.empty:
                    return sustainability
                elif isinstance(sustainability, dict) and sustainability:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(sustainability.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Analyst Price Targets":
            # Get analyst price targets (returns a dict, not DataFrame)
            targets = ticker.analyst_price_targets
            if targets is not None:
                if isinstance(targets, pd.DataFrame) and not targets.empty:
                    return targets
                elif isinstance(targets, dict) and targets:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(targets.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Earnings Estimates":
            # Get earnings estimates
            earnings_est = ticker.earnings_estimate
            if earnings_est is not None:
                if isinstance(earnings_est, pd.DataFrame) and not earnings_est.empty:
                    return earnings_est
                elif isinstance(earnings_est, dict) and earnings_est:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(earnings_est.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Revenue Estimates":
            # Get revenue estimates
            revenue_est = ticker.revenue_estimate
            if revenue_est is not None:
                if isinstance(revenue_est, pd.DataFrame) and not revenue_est.empty:
                    return revenue_est
                elif isinstance(revenue_est, dict) and revenue_est:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(revenue_est.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Major Holders":
            # Get major shareholders
            major_holders = ticker.major_holders
            if major_holders is not None:
                if isinstance(major_holders, pd.DataFrame) and not major_holders.empty:
                    return major_holders
                elif isinstance(major_holders, dict) and major_holders:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(major_holders.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Institutional Holders":
            # Get institutional holders
            institutional_holders = ticker.institutional_holders
            if institutional_holders is not None:
                if isinstance(institutional_holders, pd.DataFrame) and not institutional_holders.empty:
                    return institutional_holders
                elif isinstance(institutional_holders, dict) and institutional_holders:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(institutional_holders.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        elif info_type == "Mutual Fund Holders":
            # Get mutual fund holders
            mutualfund_holders = ticker.mutualfund_holders
            if mutualfund_holders is not None:
                if isinstance(mutualfund_holders, pd.DataFrame) and not mutualfund_holders.empty:
                    return mutualfund_holders
                elif isinstance(mutualfund_holders, dict) and mutualfund_holders:
                    # Convert dict to DataFrame for consistent handling
                    return pd.DataFrame(list(mutualfund_holders.items()), columns=['Metric', 'Value']).set_index('Metric')
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown analysis/holdings type: {info_type}")
