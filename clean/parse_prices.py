import json
import pandas as pd
from datetime import datetime

if __name__ == "__main__":
    
    with open('../data/prices_raw.json', 'r') as f:
        prices = json.load(f)

    columns = ['unix_time', 'low', 'high', 'open', 'close', 'volume']

    df = pd.concat(
        [pd.DataFrame(rows, columns=columns).assign(asset=asset) for (asset, rows) in prices.items()]
    )

    df['local_time'] = df['unix_time'].apply(lambda t: datetime.fromtimestamp(t).strftime("%Y-%m-%d %I:%M:%S %p"))   
    df['mid'] = (df['high'] + df['low']) / 2

    df = df[['asset', 'local_time', 'mid'] + columns]

    df = df.sort_values(['asset', 'unix_time'])

    df.to_csv('../data/prices_formatted.csv', index=False)    
