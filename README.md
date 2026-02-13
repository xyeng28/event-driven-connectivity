# Event Driven Connectivity

## Description
This project collects market data (stocks, ETFs, FX, crypto) from several vendors such as Tiingo, via websockets, webhooks (to be implemented) and async polling.\
It performs simple data normalisation and writes consolidated event-driven feeds by event type (trade, quote, reference price) to HDF5 files for downstream analytics.

⚠️ Designed for demonstration purposes only.\
The main goal is to **showcase connectivity and consolidation of event-driven feeds** from multiple data sources, not to provide a production-ready system or perform data analytics.

## Features
- Async streaming of multiple asset types (stocks, ETFs, FX, crypto)
- Buffering of incoming feeds to reduce memory overhead
- Multi-queue design for different event types from multiple data feeds
- Simple normalisation of feed data
- HDF5 storage with daily partitioning by event type

## Design Highlights
- Async architecture demonstrates concurrent ingestion of event-driven feeds with multiple asset types
- Python asyncio queues decouples feeds. Can be replaced with Kafka, RabbitMQ, Redis etc.
- Daily HDF5 partitioning ensures manageable file sizes
- Deduplication ignores created_at to preserve system ingestion timestamps

## Run / Usage

Starting the main application:
```bash
uvicorn src.main:app --port 8000 --reload (for development)
uvicorn src.main:app --port 8000
```
Stopping and Restarting the Process (Eg. Port in use 8000):
Ctrl + C
- On Unix systems
```bash
lsof -i :8000
kill -9 <pid>
```

- On Windows systems
```bash
netstat -ano | findstr :8000
taskkill /PID <pid> /F
```

## How it Works
- Each feed (stocks, FX, crypto) has its own async queue based on the event type (trade, quote, reference price).
- Data is normalized before pushing to the respective queue.
- Once the buffer reaches a threshold or on shutdown, data is flushed to HDF5 files.
- Daily HDF5 files are named like: `consol_feeds_quote_2026_01_22.h5`.

## Notes
- New York timezone (America/New York) is used for timestamps for consistency
- Deduplication ignores the created_at column

## Future Enhancements
- Add latency metrics for each feed to monitor real-time performance
- Implement graceful shutdown handling for all async queues
- Replace asyncio queues with Kafka/ RabbitMQ/ Redis for production scalability
- Support additional vendors and data sources

## Sample Screenshots
### Sample HDF5 Data
*Example HDF5 file and data shown here is **synthetic** and does not contain any real vendor data.*\
Consolidated Quotes Feeds:
![Consolidated Quotes Feeds Sample](assets/consol_feeds_quote.png)\
Consolidated Trades Feeds:
![Consolidated Trades Feeds Sample](assets/consol_feeds_trade.png)\
Consolidated Reference Prices Feeds:
![Consolidated Reference Prices Feeds Sample](assets/consol_feeds_ref_px.png)

## Disclaimer & Usage Policy
This repository contains code developed for **educational and demonstration purposes only**.
- **"As-Is":** Provided without any express or implied warranties of merchantability, fitness for a particular purpose, or non-infringement.
- **No Liability:** All liability is disclaimed for damages, data loss, or issues arising from use or misuse.  
- **No Downloads/Distribution:** Do **not** download or deploy this code for actual applications without review and modification.
- **Data:** Any sample data shown is placeholder or dummy data, not real or redistributed information.


