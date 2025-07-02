import requests
import pandas as pd
from collections import defaultdict


def get_price_history(start: str, end: str, key: str, asset: str='bitcoin') -> pd.DataFrame:
    """
    Download the price history of a crypto asset

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

    response_data = defaultdict(list)

    for e in data:
        for k, v in e.items():
            response_data[k].append(v)

    result = pd.DataFrame(response_data)
    return result
