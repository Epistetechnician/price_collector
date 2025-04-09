# Price Collector

A service that collects cryptocurrency price data from multiple sources (Binance, CryptoCompare, and Alchemy) and stores it in a TimescaleDB database.

## Features

- Collects price data every hour
- Supports multiple data sources:
  - Binance (via CCXT)
  - CryptoCompare
  - Alchemy (for blockchain data)
- Stores data in TimescaleDB
- Automated deployment on Railway

## Environment Variables

Make sure to set the following environment variables in your Railway project:

```env
TIMESCALEDB_HOST=your_timescaledb_host
TIMESCALEDB_PORT=your_timescaledb_port
TIMESCALEDB_NAME=your_database_name
TIMESCALEDB_USER=your_database_user
TIMESCALEDB_PASSWORD=your_database_password

BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret

CRYPTOCOMPARE_API_KEY=your_cryptocompare_api_key

ALCHEMY_API_KEY=your_alchemy_api_key
```

## Deployment on Railway

1. Create a new project on Railway
2. Connect your GitHub repository
3. Add a TimescaleDB database to your project
4. Set up the environment variables listed above
5. Deploy! The service will automatically start collecting price data every hour

## Local Development

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Unix/MacOS: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Create a `.env` file with the required environment variables
6. Run the service: `python updater2.py`

## License

MIT 