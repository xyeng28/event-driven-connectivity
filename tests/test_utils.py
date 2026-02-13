import unittest
from unittest.mock import patch
from datetime import datetime as dtt
import pytz
from src.logger import get_logger
from src.utils import load_tickers, convert_dt_to_tz
from src.constants import NY_TZ

logger = get_logger(__name__)

class TestUtils(unittest.TestCase):
    @patch('src.utils.pd.read_csv')
    def test_load_tickers_returns_correct_dict(self, mock_read_csv):
        import pandas as pd
        data = {'symbol': ['AAPL', 'GOOG', 'BTCUSD'], 'asset_type': ['STK', 'STK', 'CRYPTO']}
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df
        tickers = load_tickers(fp='test/fake_path.csv')
        mock_read_csv.assert_called_once_with('test/fake_path.csv')

        exp = {
            'STK': ['AAPL', 'GOOG'],
            'CRYPTO': ['BTCUSD']
        }
        self.assertEqual(tickers, exp)

    def test_convert_dt_to_tz_naive(self):
        naive_dt = dtt(2026, 2, 13, 10, 0)
        dt_ny = convert_dt_to_tz(naive_dt)
        assert dt_ny.tzinfo is not None
        assert dt_ny.tzinfo.zone == 'America/New_York'
        assert dt_ny.hour == 5

    def test_convert_dt_to_tz_aware(self):
        utc = pytz.UTC
        aware_dt = utc.localize(dtt(2026, 2, 13, 10, 0, 0))
        dt_ny = convert_dt_to_tz(aware_dt)
        self.assertEqual(dt_ny.tzinfo.zone, NY_TZ.zone)
        self.assertEqual(dt_ny.hour, aware_dt.astimezone(NY_TZ).hour)

    def test_convert_dt_to_tz_with_custom_tz(self):
        custom_tz = pytz.timezone('US/Eastern')
        naive_dt = dtt(2026, 2, 13, 10, 0)
        dt_converted = convert_dt_to_tz(naive_dt, tz=custom_tz)
        self.assertEqual(dt_converted.tzinfo.zone, custom_tz.zone)
        self.assertEqual(dt_converted.hour, pytz.UTC.localize(naive_dt).astimezone(custom_tz).hour)
