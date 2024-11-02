#!/usr/bin/env python3
# coding: utf-8
import asyncio
import signal
from data_manager_binance import BinanceDataManager
from data_manager_yfinance import YFinanceDataManager

def read_symbols(file_path):
    with open(file_path) as f:
        return set(line.strip() for line in f)

async def monitor_symbols(binance_manager, yfinance_manager, interval=30):
    """
    Periodically check for new symbols in the symbol files and add them if needed.
    """
    while True:
        new_yfinance_symbols = read_symbols('financial_symbols.txt')
        new_binance_symbols = read_symbols('crypto_symbols.txt')

        # Add new Yahoo Finance symbols
        for symbol in new_yfinance_symbols - yfinance_manager.symbols:
            await yfinance_manager.add_symbol(symbol)

        # Add new Binance symbols
        for symbol in new_binance_symbols - binance_manager.symbols:
            await binance_manager.add_symbol(symbol)

        await asyncio.sleep(interval)

async def main():
    interval = "1m"

    # Initial symbols
    yfinance_symbols = read_symbols('financial_symbols.txt')
    binance_symbols = read_symbols('crypto_symbols.txt')

    # Create data managers
    binance_manager = BinanceDataManager(symbols=binance_symbols, interval=interval)
    yfinance_manager = YFinanceDataManager(symbols=yfinance_symbols, interval=interval)

    # Run both managers concurrently
    binance_task = asyncio.create_task(binance_manager.run())
    yfinance_task = asyncio.create_task(yfinance_manager.run())
    monitor_task = asyncio.create_task(monitor_symbols(binance_manager, yfinance_manager))

    # Handle shutdown signal
    async def shutdown():
        print("Shutting down...")
        await binance_manager.stop()
        await yfinance_manager.stop()
        binance_task.cancel()
        yfinance_task.cancel()
        monitor_task.cancel()

    # Register signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

    # Wait for tasks to finish
    await asyncio.gather(binance_task, yfinance_task, monitor_task, return_exceptions=True)
    print("All tasks completed.")

if __name__ == "__main__":
    asyncio.run(main())
