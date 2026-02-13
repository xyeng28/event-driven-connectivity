"""
Raw Feed Consolidator Module

This module provides functions to consolidate market feed queues (trade, quote, reference price)
into Pqrquet files. Uses asyncio for asynchronous processing.
"""
import asyncio
import os
from collections import defaultdict
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime as dtt

from src import constants
from src.constants import EVENT_TYPE_TRADE, EVENT_TYPE_QUOTE, EVENT_TYPE_REF_PX
from src.core.queue_manager import trade_queue, quote_queue, ref_px_queue
from src.logger import get_logger

logger = get_logger(__name__)

def save_to_parquet(buffer: dict, pq_dir: str, event_type: str):
    """
    Save buffered events to Parquet file
    """
    if not buffer:
        logger.info("Buffer is empty, skipping save")
        return
    table = pa.Table.from_pylist(list(buffer.values()))
    os.makedirs(pq_dir, exist_ok=True)

    ny_time = dtt.now(constants.NY_TZ)
    timestamp = ny_time.strftime("%Y%m%d_%H%M%S_%f")

    pq_fp = os.path.join(pq_dir, f"consol_feeds_{event_type}_{timestamp}.parquet")
    pq.write_table(table, pq_fp, compression='snappy')
    logger.info(f'Successfully saved {len(buffer)} events to consolidated feeds file {pq_fp}')
    buffer.clear()

async def consolidate_queue(queue, event_type, pq_dir:str='src/data/consol_feeds/', buffer_size:int=30):
    """
    Continuously consumes message from a queue and store them into a Parquet file
    Buffers messages until buffer_size is reached, removes duplicates and
    appends data to a file named by event_type and current date
    """
    logger.info(f'Consolidating queue data for {event_type}')
    buffers = defaultdict(dict)

    while True:
        try:
            data = (await queue.get())['market_feed']
            logger.debug(f'data:{data}')
            if data is None:
                logger.info('Skipping empty message')
                continue

            key = (data['source'], data['symbol'], data['event_time'])
            buffers[event_type][key] = data
            logger.info(f'len(buffers[event_type]) for {event_type}:{len(buffers[event_type])}')

            if len(buffers[event_type]) >= buffer_size:
                logger.info(f'Buffer size {len(buffers[event_type])} >= threshold {buffer_size}.')
                await asyncio.to_thread(save_to_parquet, buffers[event_type], pq_dir, event_type)
        except Exception as e:
            logger.error(f'Error saving to parquet file {e}')
        finally:
            queue.task_done()

async def run_consolidator():
    """
    Runs all queue consolidators concurrently for trade, quote and reference price events
    """
    logger.info(f'Running consolidator')
    consumers = [
        consolidate_queue(trade_queue, EVENT_TYPE_TRADE),
        consolidate_queue(quote_queue, EVENT_TYPE_QUOTE),
        consolidate_queue(ref_px_queue, EVENT_TYPE_REF_PX),
    ]
    await asyncio.gather(*consumers)
