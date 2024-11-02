# data_manager_yfinance.py
import asyncio
import yfinance as yf
import sqlite3
from market_open import market_is_open

class YFinanceDataManager:
    def __init__(self, symbols, interval="1m"):
        self.symbols = set(symbols)  # Use a set for efficient symbol management
        self.interval = interval
        self.stop_event = asyncio.Event()
        self.tasks = {}  # Track tasks for each symbol

    def download_last_candle(self, ticker):
        print(f"Downloading last candle for {ticker}...")
        data = yf.download(tickers=ticker, interval=self.interval, progress=False)
        if len(data) > 0:
            last_candle = data.iloc[-1]
            print(f"Downloaded last candle for {ticker}: {last_candle}")
            return last_candle
        print(f"No data available for {ticker}.")
        return None

    def save_to_database(self, last_candle, db_file):
        print(f"Saving data to {db_file}...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS candles
                          (timestamp TEXT, open FLOAT, high FLOAT,
                           low FLOAT, close FLOAT, volume FLOAT)''')
        cursor.execute('''INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?)''',
                       (last_candle.name.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S"),
                        float(last_candle['Open']), float(last_candle['High']),
                        float(last_candle['Low']), float(last_candle['Close']),
                        int(last_candle['Volume'])))
        conn.commit()
        conn.close()
        print(f"Data saved for timestamp {last_candle.name}.")

    async def fetch_and_store(self, ticker):
        db_file = f'database/{ticker}_{self.interval}.db'
        while not self.stop_event.is_set():
            if market_is_open():
                last_candle = self.download_last_candle(ticker)
                if last_candle is not None:
                    self.save_to_database(last_candle, db_file=db_file)
            else:
                print(f"Market closed, skipping {ticker} data download.")
            await asyncio.sleep(60)

    async def run(self):
        for ticker in self.symbols:
            self.tasks[ticker] = asyncio.create_task(self.fetch_and_store(ticker))
        await asyncio.gather(*self.tasks.values())

    async def add_symbol(self, ticker):
        """
        Add a new ticker to be tracked if it's not already being tracked.
        """
        if ticker not in self.symbols:
            self.symbols.add(ticker)
            self.tasks[ticker] = asyncio.create_task(self.fetch_and_store(ticker))
            print(f"Started tracking new ticker: {ticker}")

    async def stop(self):
        self.stop_event.set()
        print("Yahoo Finance data manager stopped.")
