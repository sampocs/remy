import os
import json
import pandas as pd
import hmac
import hashlib
import time
import requests
import base64
from datetime import datetime
from typing import List
from dotenv import load_dotenv
load_dotenv('.env')

COINBASE_API_KEY = os.environ['COINBASE_API_KEY']
COINBASE_API_SECRET = os.environ['COINBASE_API_SECRET']
COINBASE_API_PASS = os.environ['COINBASE_API_PASS']

# fstring to allow passing of asset
COINBASE_URL = 'https://api.pro.coinbase.com/products/{}-USD/candles'

def get_signature(timestamp: str, method: str, url: str, secret_key: str) -> str:
    """
    Each request has to be signed with the sha256 HMAC of 
    timestamp + method type (GET) + url, using the secret key
    """
    message = timestamp + method + url
    hmac_key = base64.b64decode(secret_key)
    signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
    signature_b64 = base64.b64encode(signature.digest()).decode()
    return signature_b64


def get_prices(asset: str, earliest_tweet: int, latest_tweet: int) -> List:
    """
    Pulls all prices for a given between the times of two tweets
    There's a limit per request, so it moves ot earlier windows until the 
    full time range has been covered
    """

    prices = []

    # Each batch takes reads 299 candles (299 is the limit)
    MAX_CANDLES = 299
    batch_end_time = latest_tweet
    batch_start_time = latest_tweet - (MAX_CANDLES * 60) 

    # Continue polling until the batch end window is before the earliest tweet
    while batch_end_time >= earliest_tweet:

        timestamp = str(time.time())

        url = COINBASE_URL.format(asset)

        headers = {
            'CB-ACCESS-SIGN': get_signature(timestamp, 'GET', url, COINBASE_API_SECRET),
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': COINBASE_API_KEY,
            'CB-ACCESS-PASSPHRASE': COINBASE_API_PASS,
            'Content-Type': 'application/json'
        }

        start_time_iso = datetime.utcfromtimestamp(batch_start_time).isoformat()
        end_time_iso = datetime.utcfromtimestamp(batch_end_time).isoformat()

        params = {
            'granularity': '60',
            'start': start_time_iso,
            'end': end_time_iso
        }
        response = requests.get(url, headers=headers, params=params)
        prices += response.json()

        # Update the batch window for the next request
        batch_end_time = batch_start_time - 60
        batch_start_time = batch_end_time - (MAX_CANDLES * 60)

    return prices


if __name__ == "__main__":

    tweets = pd.read_csv('data/tweets_formatted.csv') 

    # tweets_by_asset = tweets.groupby('asset').size().reset_index(name='count')
    # tweets_by_asset = tweets_by_asset.sort_values('count', ascending=False)
    # top5_assets = list(tweets_by_asset.head(5)['asset'])

    earliest_tweet = tweets['unix_time'].min()
    latest_tweet = tweets['unix_time'].max()

    assets = ['BTC', 'ETH', 'USDT']
    prices = {asset: get_prices(asset, earliest_tweet, latest_tweet) for asset in assets}

    with open('data/prices_raw.json', 'w') as f:
        f.write(json.dumps(prices, indent=True))
