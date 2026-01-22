from collections import defaultdict
from datetime import timedelta, datetime as dtt
import asyncio
import pandas as pd
from constants import NY_TZ, EVENT_TYPE_TRADE, EVENT_TYPE_QUOTE, EVENT_TYPE_REF_PX
from src.core.queue_manager import trade_queue, quote_queue, ref_px_queue


async def consolidate_queue(queue, event_type, h5_dir:str='src/data/consol_feeds/', buffer_size:int=3, dedup_window_sec=3):
    print(f'Consolidating queue data for {event_type}')
    buffer = []
    # consol_data = defaultdict(list)
    # dedup_window = timedelta(seconds=dedup_window_sec)

    while True:
        try:
            data = (await queue.get())['market_feed']
            print(f'data:{data}')
            now_ny = dtt.now(NY_TZ)
            date_str = now_ny.strftime('%Y_%m_%d')
            h5_fp = f'{h5_dir}consol_feeds_{event_type}_{date_str}.h5'
            if data is None:
                # if buffer:
                #     df = pd.DataFrame(buffer)
                #     df['created_at'] = now_ny
                #     print(df)
                #     df.to_hdf(h5_fp, key=event_type, mode='a', format='table', append=True)
                #     print(f'Successfully updated {event_type} consolidated feeds file')
                #     buffer.clear()
                print('Skipping empty message')
                continue
            #//TODO: Remove duplicates
            # dt = data['datetime']
            # symbol = data['symbol']
            # price = data['price']
            # source = data['source']

            # consol_data = [(px, t, s) for px, t, s in consol_data[symbol] if dt-t <= dedup_window]
            # if (price, dt, source) not in consol_data[symbol]:
            #     consol_data[symbol].append((price, dt, source))
            buffer.append(data)
            print(f'len(buffer) for {event_type}:{len(buffer)}')
            if len(buffer) >= buffer_size:
                print(f'Buffer size {len(buffer)} >= threshold {buffer_size}. Writing to {h5_fp}...')
                df = pd.DataFrame(buffer)
                cols_to_check = df.columns.difference(['created_at'])
                df_dups = df[df.duplicated(subset=cols_to_check, keep=False)]
                print(f'duplicates:\n{df_dups}')
                df.drop_duplicates(subset=cols_to_check, inplace=True)
                # print(f'df: \n{df}')
                df.to_hdf(h5_fp, key='mkt_feed', mode='a', format='table', append=True)
                buffer.clear()
                print(f'Successfully updated {event_type} consolidated feeds file {h5_fp}')
        except Exception as e:
            print(f'Error saving to h5 file {e}')
        finally:
            queue.task_done()

async def run_consolidator():
    consumers = [
        consolidate_queue(trade_queue, EVENT_TYPE_TRADE),
        consolidate_queue(quote_queue, EVENT_TYPE_QUOTE),
        consolidate_queue(ref_px_queue, EVENT_TYPE_REF_PX),
    ]
    await asyncio.gather(*consumers)
