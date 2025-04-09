#!/usr/bin/env python3
"""
Continuous Crypto Price Updater for Railway

This script continuously fetches price data for all symbols from Binance via CCXT every hour,
with CryptoCompare as a fallback, and updates the TimescaleDB database.
"""

import os
import json
import time
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import schedule
import traceback
import ccxt
import ccxt.async_support as ccxt_async
import random

# Load environment variables
load_dotenv()

# Configuration
CRYPTOCOMPARE_API_KEY = os.environ.get("CRYPTOCOMPARE_API_KEY", "")
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

# Database configuration (Railway provides DATABASE_URL)
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")

# Constants
TIMEFRAME = "15m"  # 15-minute intervals
BATCH_SIZE = 3  # Process symbols in batches of 3
RATE_LIMIT_DELAY = 2.0  # Delay between API requests
BATCH_DELAY = 5.0  # Delay between batches
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "crypto_15m")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Railway captures stdout/stderr
    ]
)
logger = logging.getLogger("crypto_updater")

# Default symbols if no catalog is available
DEFAULT_SYMBOLS = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "SHIB", "AVAX", "DOT"]

class TimescaleDBManager:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None
        
    def connect(self):
        """Connect to the TimescaleDB database"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            logger.info("Connected to TimescaleDB")
            return True
        except Exception as e:
            logger.error(f"Error connecting to TimescaleDB: {e}")
            return False
    
    def insert_data(self, df: pd.DataFrame):
        """Insert data into the TimescaleDB database"""
        try:
            # Remove duplicates
            df = df.drop_duplicates(subset=['symbol', 'timestamp'])
            
            # Convert timestamps to datetime
            df['datetime_obj'] = pd.to_datetime(df['timestamp'], unit='s')
            
            with self.conn.cursor() as cur:
                # Process in smaller batches
                batch_size = 100
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i + batch_size]
                    records = [(
                        row['datetime_obj'],
                        row['symbol'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['volume']
                    ) for _, row in batch.iterrows()]
                    
                    try:
                        execute_values(
                            cur,
                            """
                            INSERT INTO crypto_prices (time, symbol, open, high, low, close, volume)
                            VALUES %s
                            ON CONFLICT (time, symbol) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                            """,
                            records
                        )
                        self.conn.commit()
                    except Exception as e:
                        logger.error(f"Error inserting batch: {e}")
                        self.conn.rollback()
            
            return True
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def create_table_if_not_exists(self):
        """Create the crypto_prices table if it doesn't exist"""
        try:
            with self.conn.cursor() as cur:
                # Create the table
                cur.execute("""
                CREATE TABLE IF NOT EXISTS crypto_prices (
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
                
                # Create hypertable
                cur.execute("""
                SELECT create_hypertable('crypto_prices', 'time', if_not_exists => TRUE);
                """)
                
                # Create index
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol ON crypto_prices (symbol);
                """)
                
                self.conn.commit()
                logger.info("Table and indexes created successfully")
                return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            self.conn.rollback()
            return False
    
    def get_latest_timestamps(self) -> Dict[str, datetime]:
        """Get the latest timestamp for each symbol"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, MAX(time) as last_update
                    FROM crypto_prices
                    GROUP BY symbol
                """)
                return {row[0]: row[1] for row in cur.fetchall()}
        except Exception as e:
            logger.error(f"Error getting latest timestamps: {e}")
            return {}
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main function to run the continuous updater"""
    logger.info("Starting continuous price updater for Railway")
    
    # Schedule updates every hour
    schedule.every(1).hour.do(run_update)
    
    # Run the first update immediately
    run_update()
    
    # Keep running forever
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main() 