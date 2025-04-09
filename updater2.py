#!/usr/bin/env python3
"""
Continuous All Symbols Updater

This script continuously fetches price data for all symbols from Binance via CCXT every 15 minutes,
with CryptoCompare as a fallback, and updates the TimescaleDB database.
"""

import os
import json
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import asyncio
import aiohttp
from typing import Dict, Optional
from dotenv import load_dotenv
import traceback
import ccxt
import ccxt.async_support as ccxt_async
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Load environment variables
load_dotenv()

# Configuration
CRYPTOCOMPARE_API_KEY = os.environ.get("CRYPTOCOMPARE_API_KEY", "")
if not CRYPTOCOMPARE_API_KEY:
    # Use a default API key if not provided
    CRYPTOCOMPARE_API_KEY = "7d1cd0cc86f2bfe98c6ad54865b2a9b8af1b4bf68c3ecf876fc89f99185f485d"

# Alchemy API configuration
ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY", "QbQkAgj1nrNdLpR09fDOXcYYns8dhiqr")
ALCHEMY_BASE_URL = "https://api.g.alchemy.com/prices/v1"

# Network mapping for tokens
NETWORK_TOKEN_MAP = {
    "ETH_MAINNET": {
        "ETH", "USDC", "USDT", "DAI", "LINK", "UNI", "AAVE", "MKR", "SNX", 
        "COMP", "YFI", "SUSHI", "CRV", "BAL", "GRT", "WBTC", "MATIC"
    },
    "BASE_MAINNET": {
        "USDC", "DAI", "WETH", "cbETH", "USDbC"
    },
    "SOLANA_MAINNET": {
        "SOL", "BONK", "RAY", "SRM", "ORCA", "MNGO", "FIDA", "MSOL"
    }
}

# Binance API configuration
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

# TimescaleDB configuration
DB_HOST = os.environ.get("TIMESCALE_HOST", "localhost")
DB_PORT = os.environ.get("TIMESCALE_PORT", "5432")
DB_NAME = os.environ.get("TIMESCALE_DB", "crypto_data")
DB_USER = os.environ.get("TIMESCALE_USER", "postgres")
DB_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("continuous_all_updater.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("continuous_all_updater")

# Constants
TIMEFRAME = "15m"  # 15-minute intervals
BATCH_SIZE = 3  # Process symbols in batches of 3 to avoid rate limits
RATE_LIMIT_DELAY = 2.0  # Delay between API requests in seconds
BATCH_DELAY = 5.0  # Delay between batches to avoid rate limits
CATALOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "price_collector", "catalog.json")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "crypto_15m")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def load_symbols_from_catalog():
    """Load the list of symbols from the catalog.json file"""
    try:
        with open(CATALOG_PATH, 'r') as f:
            catalog = json.load(f)
            
        # Get the list of Binance symbols
        binance_symbols = catalog.get('data', {}).get('binance', [])
        
        if not binance_symbols:
            logger.error("No Binance symbols found in catalog.json")
            # Fallback to a default list of popular symbols
            return [
                # USDT Pairs - Major
                "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
                "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT",
                "MATIC/USDT", "SHIB/USDT", "LTC/USDT", "UNI/USDT", "ATOM/USDT",
                "ETC/USDT", "XLM/USDT", "BCH/USDT", "NEAR/USDT", "FIL/USDT",
                
                # USDT Pairs - DeFi & New
                "APT/USDT", "OP/USDT", "ARB/USDT", "BERA/USDT", "HBAR/USDT",
                "AAVE/USDT", "GRT/USDT", "ALGO/USDT", "FTM/USDT", "SAND/USDT",
                "MANA/USDT", "GALA/USDT", "THETA/USDT", "EGLD/USDT", "EOS/USDT",
                
                # USDT Pairs - Gaming & NFT
                "AXS/USDT", "IMX/USDT", "BLUR/USDT", "PEPE/USDT", "FLOKI/USDT",
                
                # BTC Cross Pairs
                "ETH/BTC", "BNB/BTC", "SOL/BTC", "XRP/BTC", "ADA/BTC",
                "DOT/BTC", "MATIC/BTC", "AVAX/BTC", "LINK/BTC", "UNI/BTC",
                
                # ETH Cross Pairs
                "BNB/ETH", "SOL/ETH", "LINK/ETH", "MATIC/ETH", "AAVE/ETH",
                "UNI/ETH", "AVAX/ETH", "DOT/ETH", "ADA/ETH", "DOGE/ETH",
                
                # USDC Pairs - Major
                "BTC/USDC", "ETH/USDC", "SOL/USDC", "XRP/USDC", "BNB/USDC",
                "MATIC/USDC", "AVAX/USDC", "LINK/USDC", "DOT/USDC", "ADA/USDC",
                
                # USDC Pairs - DeFi
                "AAVE/USDC", "UNI/USDC", "GRT/USDC", "COMP/USDC", "SNX/USDC",
                "CRV/USDC", "MKR/USDC", "YFI/USDC", "SUSHI/USDC", "BAL/USDC",
                
                # BUSD Pairs - Major
                "BTC/BUSD", "ETH/BUSD", "BNB/BUSD", "SOL/BUSD", "XRP/BUSD",
                "DOGE/BUSD", "ADA/BUSD", "AVAX/BUSD", "LINK/BUSD", "DOT/BUSD",
                
                # New & Trending Pairs
                "BERA/BTC", "BERA/USDC", "BONK/USDT", "JTO/USDT", "SEI/USDT",
                "TIA/USDT", "PYTH/USDT", "WIF/USDT", "JUP/USDT", "ORDI/USDT",
                
                # Stablecoins Cross Pairs
                "USDC/USDT", "BUSD/USDT", "USDT/DAI", "USDC/BUSD", "TUSD/USDT",
                
                # Additional Important Pairs
                "BLUR/ETH", "RNDR/USDT", "INJ/USDT", "SUI/USDT", "FET/USDT",
                "AGIX/USDT", "MINA/USDT", "CFX/USDT", "HOOK/USDT", "MAGIC/USDT"
            ]
        
        logger.info(f"Loaded {len(binance_symbols)} symbols from catalog.json")
        return binance_symbols
    
    except Exception as e:
        logger.error(f"Error loading symbols from catalog.json: {e}")
        # Fallback to a default list of popular symbols
        return ["BTC/USDT", "ETH/USDT", "BNB/USDT"]  # Minimal fallback list

class TimescaleDBManager:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        
    def connect(self):
        """Connect to the TimescaleDB database"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to TimescaleDB at {self.host}:{self.port}/{self.dbname}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to TimescaleDB: {e}")
            return False
    
    def insert_data(self, df: pd.DataFrame):
        """Insert data into the TimescaleDB database"""
        try:
            # Ensure there are no duplicates within the dataframe
            duplicate_count = df.duplicated(subset=['symbol', 'timestamp']).sum()
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate entries in chunk, removing duplicates")
                df = df.drop_duplicates(subset=['symbol', 'timestamp'])
            
            # Convert timestamps to datetime objects
            df['datetime_obj'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
            
            # Track successful inserts
            success_count = 0
            error_count = 0
            
            with self.conn.cursor() as cur:
                # Process in smaller batches to provide progress updates
                batch_size = 100
                total_batches = (len(df) + batch_size - 1) // batch_size
                
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i + batch_size]
                    batch_records = []
                    
                    for _, row in batch.iterrows():
                        batch_records.append((
                            row['datetime_obj'],
                            row['symbol'],
                            row['open'],
                            row['high'],
                            row['low'],
                            row['close'],
                            row['volume']
                        ))
                    
                    try:
                        # Use execute_values for the batch, but with a smaller batch size
                        execute_values(
                            cur,
                            """
                            INSERT INTO all_crypto_prices (time, symbol, open, high, low, close, volume)
                            VALUES %s
                            ON CONFLICT (time, symbol) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                            """,
                            batch_records
                        )
                        success_count += len(batch)
                        
                        # Commit after each batch to avoid large transactions
                        self.conn.commit()
                        
                    except psycopg2.errors.CardinalityViolation as e:
                        # This is the specific error we're trying to handle
                        logger.warning(f"Cardinality violation in batch, falling back to row-by-row insertion")
                        self.conn.rollback()
                        
                        # Fall back to row-by-row insertion for this batch
                        for record in batch_records:
                            try:
                                cur.execute(
                                    """
                                    INSERT INTO all_crypto_prices (time, symbol, open, high, low, close, volume)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (time, symbol) DO UPDATE SET
                                        open = EXCLUDED.open,
                                        high = EXCLUDED.high,
                                        low = EXCLUDED.low,
                                        close = EXCLUDED.close,
                                        volume = EXCLUDED.volume
                                    """,
                                    record
                                )
                                success_count += 1
                            except Exception as row_error:
                                logger.error(f"Error inserting row: {row_error}")
                                error_count += 1
                        
                        # Commit after processing the batch row by row
                        self.conn.commit()
                        
                    except Exception as batch_error:
                        # Handle other errors
                        logger.error(f"Error inserting batch: {batch_error}")
                        self.conn.rollback()
                        error_count += len(batch)
            
            # Log final results
            if error_count > 0:
                logger.warning(f"Inserted {success_count} records with {error_count} errors")
                return success_count > 0  # Return True if at least some records were inserted
            else:
                logger.info(f"Successfully inserted all {success_count} records")
                return True
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            logger.error(traceback.format_exc())
            if self.conn:
                self.conn.rollback()
            return False
    
    def create_table_if_not_exists(self):
        """Create the all_crypto_prices table if it doesn't exist"""
        try:
            with self.conn.cursor() as cur:
                # Create the all_crypto_prices table
                cur.execute("""
                CREATE TABLE IF NOT EXISTS all_crypto_prices (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    PRIMARY KEY (time, symbol)
                );
                """)
                
                # Create a hypertable
                cur.execute("""
                SELECT create_hypertable('all_crypto_prices', 'time', if_not_exists => TRUE);
                """)
                
                # Create an index on the symbol column
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_all_crypto_prices_symbol ON all_crypto_prices (symbol);
                """)
                
                self.conn.commit()
                logger.info("Table created or already exists")
                return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            self.conn.rollback()
            return False
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def get_latest_timestamps(self) -> Dict[str, datetime]:
        """Get the latest timestamp for each symbol"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, MAX(time) as last_update
                    FROM all_crypto_prices
                    GROUP BY symbol
                """)
                return {row[0]: row[1] for row in cur.fetchall()}
        except Exception as e:
            logger.error(f"Error getting latest timestamps: {e}")
            return {}

class BinanceCCXTFetcher:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        self.exchange = None
        self.failed_symbols = []
        self.successful_symbols = []
        self.total_records = 0
    
    async def initialize(self):
        """Initialize the CCXT exchange"""
        try:
            # Create the exchange instance
            if self.api_key and self.api_secret:
                self.exchange = ccxt_async.binance({
                    'apiKey': self.api_key,
                    'secret': self.api_secret,
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': 'spot'
                    }
                })
            else:
                self.exchange = ccxt_async.binance({
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': 'spot'
                    }
                })
            
            # Load markets
            await self.exchange.load_markets()
            logger.info(f"Initialized Binance CCXT with {len(self.exchange.markets)} markets")
            return True
        except Exception as e:
            logger.error(f"Error initializing Binance CCXT: {e}")
            return False
    
    async def fetch_latest_data(self, symbol: str, since: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch latest price data for a symbol using CCXT"""
        if not self.exchange:
            logger.error("Exchange not initialized")
            return None
        
        try:
            # Convert symbol to CCXT format (add /USDT)
            ccxt_symbol = f"{symbol}"
            
            # Check if the symbol exists in the exchange
            if ccxt_symbol not in self.exchange.markets:
                logger.warning(f"Symbol {ccxt_symbol} not found in Binance markets")
                self.failed_symbols.append(symbol)
                return None
            
            # Convert since datetime to timestamp in milliseconds if provided
            since_ts = int(since.timestamp() * 1000) if since else None
            
            # Fetch OHLCV data
            ohlcv = await self.exchange.fetch_ohlcv(
                ccxt_symbol, 
                TIMEFRAME, 
                since=since_ts,
                limit=100
            )
            
            if not ohlcv:
                logger.warning(f"No data returned for {symbol}")
                self.failed_symbols.append(symbol)
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Convert timestamp from milliseconds to seconds
            df['timestamp'] = df['timestamp'] // 1000
            
            # Add symbol column
            df['symbol'] = symbol
            
            # Add datetime column
            df['datetime'] = df['timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
            
            # Ensure proper column order
            columns = [
                'symbol',
                'timestamp',
                'datetime',
                'open',
                'high',
                'low',
                'close',
                'volume'
            ]
            
            # Select only the columns we need
            df = df[columns]
            
            logger.info(f"Successfully fetched {len(df)} records for {symbol} via CCXT")
            self.successful_symbols.append(symbol)
            self.total_records += len(df)
            
            # Save to CSV file
            self.save_to_csv(df, symbol)
            
            return df
        
        except ccxt.NetworkError as e:
            logger.error(f"Network error fetching {symbol}: {e}")
            return None
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error fetching {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {symbol} via CCXT: {e}")
            return None
    
    def save_to_csv(self, df: pd.DataFrame, symbol: str) -> bool:
        """Save data to CSV file"""
        try:
            if df is None or df.empty:
                return False
            
            # Create the filename
            filename = f"{symbol}_15m.csv"
            filepath = os.path.join(DATA_DIR, filename)
            
            # Check if file exists
            file_exists = os.path.isfile(filepath)
            
            if file_exists:
                # Read existing data
                existing_df = pd.read_csv(filepath)
                
                # Convert timestamp to numeric if it's not already
                if existing_df['timestamp'].dtype != 'int64':
                    existing_df['timestamp'] = pd.to_numeric(existing_df['timestamp'])
                
                # Get unique timestamps from both dataframes
                existing_timestamps = set(existing_df['timestamp'])
                new_timestamps = set(df['timestamp'])
                
                # Filter out records that already exist
                df_to_append = df[~df['timestamp'].isin(existing_timestamps)]
                
                if df_to_append.empty:
                    logger.info(f"No new data to append for {symbol}, all records already exist")
                    return True
                
                # Combine the dataframes
                combined_df = pd.concat([existing_df, df_to_append])
                
                # Sort by timestamp
                combined_df = combined_df.sort_values('timestamp')
                
                # Save the combined data
                combined_df.to_csv(filepath, index=False)
                logger.info(f"Merged {len(df_to_append)} new records with existing data for {symbol}")
            else:
                # If file doesn't exist, create new file with header
                df.to_csv(filepath, index=False)
                logger.info(f"Created new file {filepath} with {len(df)} records")
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving data to CSV for {symbol}: {e}")
            return False
    
    async def close(self):
        """Close the exchange connection"""
        if self.exchange:
            await self.exchange.close()
            logger.info("CCXT exchange connection closed")

class CryptoCompareDataFetcher:
    def __init__(self, api_key: str):
        self.base_url = "https://min-api.cryptocompare.com/data/v2/histominute"
        self.api_key = api_key
        
        # Track failed symbols for reporting
        self.failed_symbols = []
        self.successful_symbols = []
        self.total_records = 0
        
    async def fetch_latest_data(self, symbol: str, since: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch latest price data for a symbol from CryptoCompare as fallback"""
        params = {
            "fsym": symbol,
            "tsym": "USD",
            "limit": 100,  # Get last 100 15-minute intervals
            "toTs": -1,    # Current time
            "api_key": self.api_key,
            "aggregate": 15  # 15-minute intervals
        }
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.base_url, params=params) as response:
                        if response.status != 200:
                            logger.error(f"Failed to fetch data for {symbol} from CryptoCompare: HTTP {response.status}")
                            
                            # If we get rate limited, wait and retry
                            if response.status == 429:
                                retry_count += 1
                                wait_time = 2 ** retry_count  # Exponential backoff
                                logger.info(f"Rate limited. Retrying in {wait_time} seconds...")
                                await asyncio.sleep(wait_time)
                                continue
                            
                            self.failed_symbols.append(symbol)
                            return None
                        
                        data = await response.json()
                        if data.get("Response") == "Error":
                            logger.error(f"API error for {symbol} from CryptoCompare: {data.get('Message')}")
                            self.failed_symbols.append(symbol)
                            return None
                        
                        # Convert to DataFrame
                        df = pd.DataFrame(data["Data"]["Data"])
                        
                        if df.empty:
                            logger.warning(f"No data returned for {symbol} from CryptoCompare")
                            self.failed_symbols.append(symbol)
                            return None
                        
                        # Add symbol column
                        df['symbol'] = symbol
                        
                        # Format datetime
                        df['datetime'] = df['time'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
                        df.rename(columns={'time': 'timestamp'}, inplace=True)
                        
                        # Ensure proper column order
                        columns = [
                            'symbol',
                            'timestamp',
                            'datetime',
                            'open',
                            'high',
                            'low',
                            'close',
                            'volumefrom',
                            'volumeto'
                        ]
                        
                        # Select only the columns we need
                        df = df[columns]
                        
                        # Rename volume columns to match format
                        df.rename(columns={'volumefrom': 'volume'}, inplace=True)
                        df = df.drop(columns=['volumeto'])
                        
                        logger.info(f"Successfully fetched {len(df)} records for {symbol} from CryptoCompare")
                        self.successful_symbols.append(symbol)
                        self.total_records += len(df)
                        
                        # Save to CSV file
                        self.save_to_csv(df, symbol)
                        
                        return df
                    
            except Exception as e:
                logger.error(f"Error fetching data for {symbol} from CryptoCompare: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    self.failed_symbols.append(symbol)
                    return None
            
            # Add a small delay to avoid hitting rate limits
            await asyncio.sleep(RATE_LIMIT_DELAY)
        
        return None
    
    def save_to_csv(self, df: pd.DataFrame, symbol: str) -> bool:
        """Save data to CSV file"""
        try:
            if df is None or df.empty:
                return False
            
            # Create the filename
            filename = f"{symbol}_15m.csv"
            filepath = os.path.join(DATA_DIR, filename)
            
            # Check if file exists
            file_exists = os.path.isfile(filepath)
            
            if file_exists:
                # Read existing data
                existing_df = pd.read_csv(filepath)
                
                # Convert timestamp to numeric if it's not already
                if existing_df['timestamp'].dtype != 'int64':
                    existing_df['timestamp'] = pd.to_numeric(existing_df['timestamp'])
                
                # Get unique timestamps from both dataframes
                existing_timestamps = set(existing_df['timestamp'])
                new_timestamps = set(df['timestamp'])
                
                # Filter out records that already exist
                df_to_append = df[~df['timestamp'].isin(existing_timestamps)]
                
                if df_to_append.empty:
                    logger.info(f"No new data to append for {symbol}, all records already exist")
                    return True
                
                # Combine the dataframes
                combined_df = pd.concat([existing_df, df_to_append])
                
                # Sort by timestamp
                combined_df = combined_df.sort_values('timestamp')
                
                # Save the combined data
                combined_df.to_csv(filepath, index=False)
                logger.info(f"Merged {len(df_to_append)} new records with existing data for {symbol}")
            else:
                # If file doesn't exist, create new file with header
                df.to_csv(filepath, index=False)
                logger.info(f"Created new file {filepath} with {len(df)} records")
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving data to CSV for {symbol}: {e}")
            return False

class AlchemyDataFetcher:
    """Fetches price data from Alchemy API"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.successful_symbols = set()
        self.session = None
        
    async def initialize(self):
        """Initialize aiohttp session"""
        self.session = aiohttp.ClientSession()
        return True
        
    async def fetch_latest_data(self, symbol: str, since: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch latest price data for a symbol from Alchemy"""
        try:
            if not self.session:
                logger.error("Session not initialized")
                return None

            # Construct the API URL with the symbol
            url = f"{ALCHEMY_BASE_URL}/tokens/by-symbol?symbols={symbol}&apiKey={self.api_key}"
            
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch data for {symbol} from Alchemy: HTTP {response.status}")
                    return None
                
                data = await response.json()
                
                if not data or 'data' not in data:
                    logger.warning(f"Invalid data format for {symbol} from Alchemy")
                    return None
                
                token_data = data['data']
                if not token_data or not isinstance(token_data, list) or len(token_data) == 0:
                    logger.warning(f"No price data found for {symbol} from Alchemy")
                    return None
                
                # Get current timestamp
                now = int(datetime.now().timestamp())
                
                # Create DataFrame with the current price data
                df = pd.DataFrame([{
                    'timestamp': now,
                    'symbol': symbol,
                    'open': float(token_data[0].get('price', 0)),
                    'high': float(token_data[0].get('price', 0)),
                    'low': float(token_data[0].get('price', 0)),
                    'close': float(token_data[0].get('price', 0)),
                    'volume': 0,  # Alchemy doesn't provide volume data
                    'datetime': datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')
                }])
                
                logger.info(f"Successfully fetched price data for {symbol} via Alchemy")
                self.successful_symbols.add(symbol)
                
                return df
                
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching {symbol} from Alchemy: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {symbol} via Alchemy: {e}")
            return None
    
    async def close(self):
        """Close the API connection"""
        if self.session:
            await self.session.close()
            logger.info("Alchemy API connection closed")

class HyperLiquidFetcher:
    def __init__(self):
        self.base_url = "https://api.hyperliquid.xyz"
        self.failed_symbols = []
        self.successful_symbols = []
        self.total_records = 0
        self.session = None
    
    async def initialize(self):
        """Initialize the API connection"""
        try:
            self.session = aiohttp.ClientSession()
            logger.info("Initialized HyperLiquid API connection")
            return True
        except Exception as e:
            logger.error(f"Error initializing HyperLiquid API: {e}")
            return False
    
    async def fetch_latest_data(self, symbol: str, since: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """Fetch latest price data for a symbol using HyperLiquid API"""
        if not self.session:
            logger.error("Session not initialized")
            return None
        
        try:
            # Convert symbol to HyperLiquid format (remove /USD if present)
            hl_symbol = symbol.replace("/USD", "")
            
            # Endpoint for OHLCV data
            url = f"{self.base_url}/info/candles"
            
            # Calculate the timestamp range
            end_time = int(datetime.now().timestamp())
            start_time = int(since.timestamp()) if since else end_time - 86400  # Default to last 24 hours
            
            payload = {
                "coin": hl_symbol,
                "interval": "15m",
                "startTime": start_time,
                "endTime": end_time
            }
            
            async with self.session.post(url, json=payload) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch data for {symbol} from HyperLiquid: HTTP {response.status}")
                    self.failed_symbols.append(symbol)
                    return None
                
                data = await response.json()
                
                if not data or not isinstance(data, list):
                    logger.warning(f"Invalid data format for {symbol}")
                    self.failed_symbols.append(symbol)
                    return None
                
                # Convert data to DataFrame
                df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                # Convert timestamp to seconds
                df['timestamp'] = pd.to_numeric(df['timestamp']) // 1000
                
                # Convert price columns to float
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Add symbol column
                df['symbol'] = symbol
                
                # Add datetime column
                df['datetime'] = df['timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
                
                # Ensure proper column order
                columns = [
                    'symbol',
                    'timestamp',
                    'datetime',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume'
                ]
                
                # Select only the columns we need
                df = df[columns]
                
                logger.info(f"Successfully fetched {len(df)} records for {symbol} via HyperLiquid")
                self.successful_symbols.append(symbol)
                self.total_records += len(df)
                
                # Save to CSV file
                self.save_to_csv(df, symbol)
                
                return df
            
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching {symbol} from HyperLiquid: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {symbol} via HyperLiquid: {e}")
            return None
    
    def save_to_csv(self, df: pd.DataFrame, symbol: str) -> bool:
        """Save data to CSV file"""
        try:
            if df is None or df.empty:
                return False
            
            # Create the filename
            filename = f"{symbol}_15m.csv"
            filepath = os.path.join(DATA_DIR, filename)
            
            # Check if file exists
            file_exists = os.path.isfile(filepath)
            
            if file_exists:
                # Read existing data
                existing_df = pd.read_csv(filepath)
                
                # Convert timestamp to numeric if it's not already
                if existing_df['timestamp'].dtype != 'int64':
                    existing_df['timestamp'] = pd.to_numeric(existing_df['timestamp'])
                
                # Get unique timestamps from both dataframes
                existing_timestamps = set(existing_df['timestamp'])
                new_timestamps = set(df['timestamp'])
                
                # Filter out records that already exist
                df_to_append = df[~df['timestamp'].isin(existing_timestamps)]
                
                if df_to_append.empty:
                    logger.info(f"No new data to append for {symbol}, all records already exist")
                    return True
                
                # Combine the dataframes
                combined_df = pd.concat([existing_df, df_to_append])
                
                # Sort by timestamp
                combined_df = combined_df.sort_values('timestamp')
                
                # Save the combined data
                combined_df.to_csv(filepath, index=False)
                logger.info(f"Merged {len(df_to_append)} new records with existing data for {symbol}")
            else:
                # If file doesn't exist, create new file with header
                df.to_csv(filepath, index=False)
                logger.info(f"Created new file {filepath} with {len(df)} records")
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving data to CSV for {symbol}: {e}")
            return False
    
    async def close(self):
        """Close the API connection"""
        if self.session:
            await self.session.close()
            logger.info("HyperLiquid API connection closed")

async def process_symbol(
    binance_fetcher: BinanceCCXTFetcher,
    hyperliquid_fetcher: HyperLiquidFetcher,
    alchemy_fetcher: AlchemyDataFetcher,
    symbol: str,
    since: Optional[datetime] = None
) -> Optional[pd.DataFrame]:
    """Process a single symbol"""
    try:
        # Try Binance first
        df = await binance_fetcher.fetch_latest_data(symbol, since)
        if df is not None:
            return df
            
        # If Binance fails, try HyperLiquid
        logger.info(f"Trying HyperLiquid for {symbol}")
        df = await hyperliquid_fetcher.fetch_latest_data(symbol, since)
        if df is not None:
            return df
            
        # If both fail, try Alchemy for supported tokens
        logger.info(f"Trying Alchemy for {symbol}")
        df = await alchemy_fetcher.fetch_latest_data(symbol, since)
        if df is not None:
            return df
            
        return None
    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {e}")
        return None

async def update_prices():
    """Update prices for all symbols"""
    # Initialize database connection
    db_manager = TimescaleDBManager(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    if not db_manager.connect():
        logger.error("Failed to connect to database")
        return
    
    # Create table if it doesn't exist
    if not db_manager.create_table_if_not_exists():
        logger.error("Failed to create table")
        return
    
    # Get latest timestamps for each symbol
    latest_timestamps = db_manager.get_latest_timestamps()
    
    # Initialize fetchers
    binance_fetcher = BinanceCCXTFetcher(BINANCE_API_KEY, BINANCE_API_SECRET)
    hyperliquid_fetcher = HyperLiquidFetcher()
    alchemy_fetcher = AlchemyDataFetcher(ALCHEMY_API_KEY)
    
    if not await binance_fetcher.initialize():
        logger.error("Failed to initialize Binance CCXT")
        return
        
    if not await hyperliquid_fetcher.initialize():
        logger.error("Failed to initialize HyperLiquid API")
        return
        
    if not await alchemy_fetcher.initialize():
        logger.error("Failed to initialize Alchemy")
        return
    
    # Load symbols from catalog
    symbols = load_symbols_from_catalog()
    
    # Ensure symbols is a flat list
    if symbols and isinstance(symbols[0], list):
        symbols = symbols[0]  # Take the first list if we get a list of lists
    
    try:
        # Process symbols in batches
        total_success = 0
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            logger.info(f"Processing batch {i//BATCH_SIZE + 1} of {(len(symbols) + BATCH_SIZE - 1)//BATCH_SIZE}: {batch}")
            
            # Process batch concurrently
            tasks = []
            for symbol in batch:
                since = latest_timestamps.get(symbol)
                task = process_symbol(binance_fetcher, hyperliquid_fetcher, alchemy_fetcher, symbol, since)
                tasks.append(task)
            
            # Wait for all tasks in batch to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for symbol, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing {symbol}: {result}")
                    continue
                    
                if result is not None:
                    # Insert data into database
                    if db_manager.insert_data(result):
                        total_success += 1
            
            # Add delay between batches to avoid rate limits
            if i + BATCH_SIZE < len(symbols):
                await asyncio.sleep(BATCH_DELAY)
        
        # Log results
        binance_success = len(binance_fetcher.successful_symbols)
        hyperliquid_success = len(hyperliquid_fetcher.successful_symbols)
        alchemy_success = len(alchemy_fetcher.successful_symbols)
        
        logger.info("Results:")
        logger.info(f"Binance: {binance_success} symbols successful")
        logger.info(f"HyperLiquid: {hyperliquid_success} symbols successful")
        logger.info(f"Alchemy: {alchemy_success} symbols successful")
        logger.info(f"Summary: {total_success}/{len(symbols)} symbols successful")
        
        # List failed symbols
        failed_symbols = set(symbols) - set(binance_fetcher.successful_symbols) - set(hyperliquid_fetcher.successful_symbols) - set(alchemy_fetcher.successful_symbols)
        if failed_symbols:
            logger.warning(f"Failed to fetch data for {len(failed_symbols)} symbols: {', '.join(failed_symbols)}")
    
    finally:
        # Close connections
        db_manager.close()
        await binance_fetcher.close()
        await hyperliquid_fetcher.close()
        await alchemy_fetcher.close()

def run_update():
    """Run the update process"""
    asyncio.run(update_prices())

def main():
    """Main function to run the continuous updater"""
    logger.info("Starting continuous price updater for all symbols")
    
    # Create a scheduler
    scheduler = BlockingScheduler()
    
    # Add the job to run every hour
    scheduler.add_job(
        run_update,
        trigger=IntervalTrigger(hours=1),
        next_run_time=datetime.now()  # Run first job immediately
    )
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down price updater")
        scheduler.shutdown()

if __name__ == "__main__":
    main() 