import binance
from config import BINANCE_API_KEY, BINANCE_API_SECRET


async def connect():
    binance_client = binance.Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    await binance_client.load()
    return binance_client

