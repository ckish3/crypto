import requests
import pandas as pd
from collections import defaultdict


def get_price_history(start: str, end: str, key: str) -> pd.DataFrame:
    url = f'https://rest.coincap.io/v3/assets/bitcoin/history?interval=d1&start={start}&end={end}&apiKey={key}'
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
