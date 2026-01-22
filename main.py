import asyncio
import uvicorn
from fastapi import FastAPI
from src.core.raw_feed_consolidator import run_consolidator
from src.data.sources import tiingo_ws as tiingo
from src.utils import load_tickers

app = FastAPI()

@app.on_event('startup')
async def startup():
    tickers = load_tickers()
    asyncio.create_task(run_consolidator())

    asyncio.create_task(tiingo.iex_stocks_feed(tickers))
    asyncio.create_task(tiingo.crypto_feed(tickers))
    asyncio.create_task(tiingo.fx_feed(tickers))

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
