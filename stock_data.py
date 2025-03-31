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
            if info and isinstance(info, dict) and len(info) > 0:
                return pd.DataFrame(list(info.items()), columns=['Attribute', 'Value']).set_index('Attribute')
            return pd.DataFrame()
        
        elif info_type == "Fast Info":
            # Get fast info (quick access to key stats)
            try:
                # Direct access to fast info attributes as a dictionary
                fast_info_dict = {}
                
                # Try to get all fast_info attributes
                try:
                    fast_info_dict['dayHigh'] = ticker.fast_info.get('dayHigh', None)
                    fast_info_dict['dayLow'] = ticker.fast_info.get('dayLow', None)
                    fast_info_dict['lastPrice'] = ticker.fast_info.get('lastPrice', None)
                    fast_info_dict['previousClose'] = ticker.fast_info.get('previousClose', None)
                    fast_info_dict['open'] = ticker.fast_info.get('open', None)
                    fast_info_dict['volume'] = ticker.fast_info.get('volume', None)
                    fast_info_dict['marketCap'] = ticker.fast_info.get('marketCap', None)
                    fast_info_dict['fiftyTwoWeekHigh'] = ticker.fast_info.get('fiftyTwoWeekHigh', None)
                    fast_info_dict['fiftyTwoWeekLow'] = ticker.fast_info.get('fiftyTwoWeekLow', None)
                    fast_info_dict['currency'] = ticker.fast_info.get('currency', None)
                    
                    # Additional attributes if available
                    try:
                        # Additional attributes from 'info' since they might not be in fast_info
                        info = ticker.info
                        if info and isinstance(info, dict):
                            # Add some key financial metrics from info
                            for key in ['pe_ratio', 'forwardPE', 'dividendYield', 'trailingEps', 'forwardEps', 'beta']:
                                if key in info:
                                    fast_info_dict[key] = info[key]
                    except Exception:
                        pass
                except Exception as e:
                    # If direct attribute access fails, try dict method
                    try:
                        fast_info = ticker.fast_info
                        if isinstance(fast_info, dict):
                            fast_info_dict = fast_info
                    except Exception:
                        # Last resort: extract some basic info from ticker.info
                        try:
                            info = ticker.info
                            if info and isinstance(info, dict):
                                # Extract key financial metrics that would typically be in fast_info
                                for key in ['previousClose', 'open', 'dayHigh', 'dayLow', 'volume', 'marketCap']:
                                    if key in info:
                                        fast_info_dict[key] = info[key]
                        except Exception:
                            pass
                
                # Filter out None values
                fast_info_dict = {k: v for k, v in fast_info_dict.items() if v is not None}
                
                if fast_info_dict:
                    return pd.DataFrame(list(fast_info_dict.items()), columns=['Attribute', 'Value']).set_index('Attribute')
            except Exception as e:
                print(f"Error processing fast_info: {str(e)}")
            
            return pd.DataFrame()
        
        elif info_type == "News":
            # Get recent news - updated to handle newer yfinance versions
            try:
                # Get news data
                news = ticker.news
                
                if news is None:
                    return pd.DataFrame()
                
                if not isinstance(news, list):
                    return pd.DataFrame()
                
                if len(news) == 0:
                    return pd.DataFrame()
                
                # The structure has changed - news is now a list of dicts with 'id' and 'content' keys
                formatted_news = []
                for item in news:
                    if not isinstance(item, dict):
                        continue
                        
                    # Check if content exists and is a dictionary
                    if 'content' not in item or not isinstance(item['content'], dict):
                        continue
                        
                    content = item['content']
                    
                    # Get provider info safely
                    provider_name = ''
                    if 'provider' in content and isinstance(content['provider'], dict):
                        provider_name = content['provider'].get('displayName', '')
                    
                    # Get URL safely
                    url = ''
                    if 'clickThroughUrl' in content and isinstance(content['clickThroughUrl'], dict):
                        url = content['clickThroughUrl'].get('url', '')
                    
                    news_item = {
                        'title': content.get('title', ''),
                        'provider': provider_name,
                        'summary': content.get('summary', ''),
                        'published_date': content.get('pubDate', ''),
                        'url': url
                    }
                    formatted_news.append(news_item)
                
                if formatted_news:
                    news_df = pd.DataFrame(formatted_news)
                    # Convert timestamp to readable date if needed
                    if 'published_date' in news_df.columns:
                        news_df['published_date'] = pd.to_datetime(news_df['published_date'])
                    return news_df
            
                # Return empty DataFrame if no news or invalid format
                return pd.DataFrame()
            except Exception as e:
                print(f"Error processing news data: {str(e)}")
                return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown general info type: {info_type}")

    def _get_historical_data(self, ticker, info_type):
        """Get historical data for the stock."""
        if info_type == "Price History":
            # Get price history for at least 5 years
            history = ticker.history(period="5y")
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
        
        elif info_type == "Insider Transactions":
            # Get insider transactions
            try:
                insider_transactions = ticker.insider_transactions
                if insider_transactions is not None:
                    if isinstance(insider_transactions, pd.DataFrame) and not insider_transactions.empty:
                        return insider_transactions
                    elif isinstance(insider_transactions, dict) and insider_transactions:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(insider_transactions.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving insider transactions: {str(e)}")
            return pd.DataFrame()
        
        elif info_type == "Upgrades Downgrades":
            # Get upgrades and downgrades
            try:
                upgrades_downgrades = ticker.upgrades_downgrades
                if upgrades_downgrades is not None:
                    if isinstance(upgrades_downgrades, pd.DataFrame) and not upgrades_downgrades.empty:
                        return upgrades_downgrades
                    elif isinstance(upgrades_downgrades, dict) and upgrades_downgrades:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(upgrades_downgrades.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving upgrades/downgrades: {str(e)}")
            return pd.DataFrame()
            
        elif info_type == "Earnings History":
            # Get earnings history
            try:
                earnings_history = ticker.earnings_history
                if earnings_history is not None:
                    if isinstance(earnings_history, pd.DataFrame) and not earnings_history.empty:
                        return earnings_history
                    elif isinstance(earnings_history, dict) and earnings_history:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(earnings_history.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving earnings history: {str(e)}")
            return pd.DataFrame()
            
        elif info_type == "EPS Trend":
            # Get EPS trend
            try:
                eps_trend = ticker.eps_trend
                if eps_trend is not None:
                    if isinstance(eps_trend, pd.DataFrame) and not eps_trend.empty:
                        return eps_trend
                    elif isinstance(eps_trend, dict) and eps_trend:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(eps_trend.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving EPS trend: {str(e)}")
            return pd.DataFrame()
            
        elif info_type == "Growth Estimates":
            # Get growth estimates
            try:
                growth_estimates = ticker.growth_estimates
                if growth_estimates is not None:
                    if isinstance(growth_estimates, pd.DataFrame) and not growth_estimates.empty:
                        return growth_estimates
                    elif isinstance(growth_estimates, dict) and growth_estimates:
                        # Convert dict to DataFrame for consistent handling
                        return pd.DataFrame(list(growth_estimates.items()), columns=['Metric', 'Value']).set_index('Metric')
            except Exception as e:
                print(f"Error retrieving growth estimates: {str(e)}")
            return pd.DataFrame()
        
        else:
            raise ValueError(f"Unknown analysis/holdings type: {info_type}")
