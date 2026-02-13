"""
Raw Feed Consolidator Module

This module provides functions to consolidate market feed queues (trade, quote, reference price)
into HDF5 files. Uses asyncio for asynchronous processing.
"""

import asyncio
import pandas as pd
from datetime import datetime as dtt
from src.constants import NY_TZ, EVENT_TYPE_TRADE, EVENT_TYPE_QUOTE, EVENT_TYPE_REF_PX
from src.core.queue_manager import trade_queue, quote_queue, ref_px_queue
from src.logger import get_logger

logger = get_logger(__name__)

async def consolidate_queue(queue, event_type, h5_dir:str='src/data/consol_feeds/', buffer_size:int=3):
    """
    Continuously consumes message from a queue and store them into a HDF5 file
    Buffers messages until buffer_size is reached, removes duplicates and
    appends data to a file named by event_type and current date
    """
    logger.info(f'Consolidating queue data for {event_type}')
    buffer = []

    while True:
        try:
            data = (await queue.get())['market_feed']
            logger.debug(f'data:{data}')
            now_ny = dtt.now(NY_TZ)
            date_str = now_ny.strftime('%Y_%m_%d')
            h5_fp = f'{h5_dir}consol_feeds_{event_type}_{date_str}.h5'
            if data is None:
                logger.info('Skipping empty message')
                continue

            buffer.append(data)
            logger.info(f'len(buffer) for {event_type}:{len(buffer)}')

            if len(buffer) >= buffer_size:
                logger.info(f'Buffer size {len(buffer)} >= threshold {buffer_size}. Writing to {h5_fp}...')
                df = pd.DataFrame(buffer)
                cols_to_check = df.columns.difference(['created_at'])
                df_dups = df[df.duplicated(subset=cols_to_check, keep=False)]
                logger.info(f'duplicates:\n{df_dups}')
                df.drop_duplicates(subset=cols_to_check, inplace=True)
                df.to_hdf(h5_fp, key='mkt_feed', mode='a', format='table', append=True)
                buffer.clear()
                logger.info(f'Successfully updated {event_type} consolidated feeds file {h5_fp}')
        except Exception as e:
            logger.error(f'Error saving to h5 file {e}')
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
