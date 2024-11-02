# data_manager_binance.py
import asyncio
from datetime import datetime
from binance import AsyncClient, BinanceSocketManager
import sqlite3

class BinanceDataManager:
    def __init__(self, symbols, interval="1m"):
        self.symbols = set(symbols)  # Use a set for efficient symbol management
        self.interval = interval
        self.client = None
        self.stop_event = asyncio.Event()
        self.tasks = {}  # Keep track of tasks for each symbol

    def timestamp_to_date(self, time):
        return datetime.fromtimestamp(int(str(time)[:-3])).strftime('%d-%m-%Y %H:%M:%S')

    async def download_last_candle(self, symbol):
        bm = BinanceSocketManager(self.client)
        print(f"Starting download for {symbol}...")
        async with bm.kline_socket(symbol=symbol) as stream:
            res = await stream.recv()
            last_candle = (
                self.timestamp_to_date(res['k']['T']), res['k']['o'], res['k']['h'],
                res['k']['l'], res['k']['c'], res['k']['V']
            )
            print(f"Downloaded last candle for {symbol}: {last_candle}")
            return last_candle

    def save_to_database(self, last_candle, db_file):
        print(f"Saving data to {db_file}...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS candles
                          (timestamp TEXT, open FLOAT, high FLOAT,
                           low FLOAT, close FLOAT, volume FLOAT)''')
        cursor.execute('''INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?)''',
                       (last_candle[0], float(last_candle[1]), float(last_candle[2]),
                        float(last_candle[3]), float(last_candle[4]), float(last_candle[5])))
        conn.commit()
        conn.close()
        print(f"Data saved for timestamp {last_candle[0]}.")

    async def fetch_and_store(self, symbol):
        db_file = f'database/{symbol}_{self.interval}.db'
        while not self.stop_event.is_set():
            last_candle = await self.download_last_candle(symbol)
            self.save_to_database(last_candle, db_file=db_file)
            await asyncio.sleep(60)

    async def run(self):
        self.client = await AsyncClient.create()
        # Initialize tasks for each symbol
        for symbol in self.symbols:
            self.tasks[symbol] = asyncio.create_task(self.fetch_and_store(symbol))
        await asyncio.gather(*self.tasks.values())

    async def add_symbol(self, symbol):
        """
        Add a new symbol to be tracked if it's not already being tracked.
        """
        if symbol not in self.symbols:
            self.symbols.add(symbol)
            self.tasks[symbol] = asyncio.create_task(self.fetch_and_store(symbol))
            print(f"Started tracking new symbol: {symbol}")

    async def stop(self):
        self.stop_event.set()
        await self.client.close_connection()
        print("Binance data manager stopped.")
