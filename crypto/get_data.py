import requests
import pandas as pd
from typing import List
import datetime
import logging

from crypto_price import CryptoPrice

logger = logging.getLogger(__name__)

def download_price_history(start: str, end: str, key: str, asset: str) -> List:
    """
    Download the price history of a crypto asset

    The API response is of the form:
        {"data":[{"priceUsd":"104863.9648002069573128","time":1748822400000,"date":"2025-06-02T00:00:00.000Z"},
            {"priceUsd":"105766.4432982652302685","time":1748908800000,"date":"2025-06-03T00:00:00.000Z"}]}
    Args:
        start (str): The timestamp in milliseconds of the start date
        end (str): The timestamp in milliseconds of the end date
        key (str): The API key
        asset (str): The name of the crypto asset

    Returns:
        pd.DataFrame: The price history
    """

    url = f'https://rest.coincap.io/v3/assets/{asset}/history?interval=d1&start={start}&end={end}&apiKey={key}'
    response = requests.get(url)

    data = response.json()

    if 'data' not in data:
        raise Exception('No crypto price history retrieved')

    data = data['data']

    logger.info(f'Retrieved {len(data)} new price history entries for {asset}')

    return data

def get_new_price_table_entries(start: str, end: str, key: str, asset: str='bitcoin') -> List[CryptoPrice]:
    """
        Download the price history of a crypto asset and return a list of new price table entries for them
    Args:
        start (str): The timestamp in milliseconds of the start date
        end (str): The timestamp in milliseconds of the end date
        key (str): The API key
        asset (str): The name of the crypto asset, e.g. 'bitcoin'

    Returns:
        List[CryptoPrice]: A list of new price table entries
    """

    data = download_price_history(start, end, key, asset)
    table_rows = []
    for e in data:
        date = datetime.datetime.strptime(e['date'], '%Y-%m-%dT%H:%M:%S.000Z').date().isoformat()
        price = e['priceUsd']
        id = asset + '_' + date

        row = CryptoPrice(
            id=id,
            currency_name=asset,
            date=date,
            price=price
        )
        table_rows.append(row)

    return table_rows
