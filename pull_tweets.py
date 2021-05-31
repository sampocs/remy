import os
import json
import requests
from typing import List
from dotenv import load_dotenv
load_dotenv('.env')

WHALE_ALERT_USER_ID = '1039833297751302144'
TWITTER_BEARER_TOKEN = os.environ['TWITTER_BEARER_TOKEN']

TWITTER_URL = 'https://api.twitter.com/2'
NUM_PULLS = 35

def pull_whale_tweets() -> List[dict]:
    """
    Pull twitter API {NUM_PULLS} times and concatenate tweets into an array
    Tweets are pulled in batches of 100
    Each ensuing pull uses the tweet ID of the last tweet from the previous batch
    to define the end of the window for the new batch
    """
    tweets = []

    url = f'{TWITTER_URL}/users/{WHALE_ALERT_USER_ID}/tweets'
    params = {'max_results': '100', 'tweet.fields': 'created_at'}
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {TWITTER_BEARER_TOKEN}'}

    for _ in range(NUM_PULLS):

        response = requests.get(url, params=params, headers=headers).json()

        # Break once we're no longer receiving tweets
        if 'data' not in response:
            break

        tweet_batch = response['data']
        tweets += tweet_batch

        # update params to get next page of tweets
        last_id = tweet_batch[-1]['id']
        params['until_id'] = last_id

    return tweets


if __name__ == "__main__":

    tweets = pull_whale_tweets()

    with open('data/tweets_raw.json', 'w') as f:
        f.write(json.dumps({'data': tweets}, indent=True))