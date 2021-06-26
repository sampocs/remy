import pandas as pd
from datetime import datetime
import json
import asyncio
from aiohttp import ClientSession
from asyncio.locks import Semaphore
from typing import List

    
async def transform_tweet(tweet: dict, session: ClientSession) -> dict:
    """
    Scrapes the whale alert site for the transaction time and replaces the "Details"
     word in the tweet text with link - to replicate the response from the API
    """
    
    url = tweet['link']
    text = tweet['text'].replace('Details', f'\n{url}')

    async with session.get(url) as response:
        html = await response.text()

        try:

            # The transaction time is in a table so we'll use pandas to scrape it 
            dfs = pd.read_html(html, index_col=0)

            # The table headers are the first column so we can use it as the index
            # The timestamp is of the form: "2 years 2 months ago (Sat, 06 Apr 2019 13:42:03 UTC)"
            transaction_time = dfs[0].loc['Timestamp'][1]
            transaction_time = transaction_time[transaction_time.find("(")+1:transaction_time.find(")")]

            # Convert from format "Sat, 06 Apr 2019 13:42:03 UTC" to "2019-04-06T13:42:03.000Z"
            transaction_time = datetime.strptime(transaction_time, '%a, %d %b %Y %H:%M:%S UTC')
            transaction_time = transaction_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
            # Return the updated tweet with a created_at field and the link included in the text
            return {'text': text, 'created_at': transaction_time}

        except:
            return None


async def transform_tweet_bounded(sem: Semaphore, tweet: dict, session: ClientSession) -> dict:
    """
    Wrapper function to bound the number of concurrent API requests 
    when scraping the transaction time
    """
    async with sem:
        return await transform_tweet(tweet, session)


async def main(tweets: List[dict]) -> List[dict]:
    """
    Main function for asynchronously calling the transaction URL from each
    tweet to get the transaction time 
    """

    header = {
        "User-Agent": "Chrome/50.0.2661.75",
        "X-Requested-With": "XMLHttpRequest"
    }

    # Cap concurrent requests at 1000
    sem = asyncio.Semaphore(1000)

    tasks = []

    async with ClientSession(headers=header) as session:
        for tweet in tweets:
            task = asyncio.ensure_future(transform_tweet_bounded(sem, tweet, session))
            tasks.append(task)

        return await asyncio.gather(*tasks)


if __name__ == "__main__":

    with open('../data/tweets_scrape_raw.json', 'r') as f:
        tweets = json.load(f)['data']

    loop = asyncio.get_event_loop()

    future = asyncio.ensure_future(main(tweets))
    loop.run_until_complete(future)

    formatted_tweets = future.result()
    formatted_tweets = [tweet for tweet in formatted_tweets if tweet is not None]

    print(f'Total Tweets: {len(formatted_tweets)}')

    with open('../data/tweets_raw.json', 'w') as f:
        f.write(json.dumps({'data': formatted_tweets}, indent=True))