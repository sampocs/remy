from datetime import datetime, timezone
import json
from emoji import UNICODE_EMOJI
from typing import Tuple, List
import pandas as pd
import sys


def get_local_and_unix_time(time_string: str) -> Tuple[str, str]:
    """ 
    Convert from iso format with milliseconds to unix time and local format with AM/PM
    Local Example (CST): 2021-06-01T12:00:00.000Z ->  2021-06-01 07:00 AM
    """
    utc_time = datetime.fromisoformat(time_string[:-5])

    cst_time = utc_time.replace(tzinfo=timezone.utc).astimezone(tz=None)
    cst_time_formatted = cst_time.strftime("%Y-%m-%d %I:%M:%S %p")

    unix_time = str(int(cst_time.timestamp()))
    return (cst_time_formatted, unix_time)


def is_whale_transaction(tweet_dict: dict) -> bool:
    """
    If the usd_value or num_coins is not an integer, or the asset is not uppercase, 
    the tweet was probably something other than a whale transaction
    """
    try:
        int(tweet_dict['usd_value'])
        int(tweet_dict['num_coins'])
        assert tweet_dict['asset'].upper() == tweet_dict['asset']
        return True
    except:
        return False


def get_transaction_info(tweet_description: str) -> Tuple[str, str, str]:
    """
    Determines the transaction type, source and recipient as inferred from the tweet description

    Description Formats:
        - transferred from #Exchange to unknown wallet  
        - transferred from unknown wallet to #Exchange
        - transferred from Treasury to unknown wallet  
        - transferred from unknown wallet to Treasury 
        - transferred from #Exchange to Treasury
        - transferred from Treasury to #Exchange
        - transferred from unknown wallet to unknown wallet
        - transferred from #Exchange to #Exchange
        - minted at Treasury 
        - burned at Treasury

    Returns (TYPE, SOURCE, RECIPIENT)
        - TYPE can be one of 'MINTED', 'BURNED', or 'TRANSFERRED'
        - SOURCE/RECIPIENT can be one of: 'WALLET', 'TREASURY', 'EXCHANGE', or 'UNDETERMINED'

    """
    def _get_source_type(source):
        if source == 'unknown wallet':
            return 'WALLET'
        if 'Treasury' in source:
            return 'TREASURY'
        if source.startswith('#'):
            return 'EXCHANGE'
        return 'UNDETERMINED'
    
    if tweet_description.startswith('minted at'):
        return ('MINTED', None, None)

    if tweet_description.startswith('burned at'):
        return ('BURNED', None, None)

    if tweet_description.startswith('transferred from'):
        try:
            transaction = tweet_description.lstrip('transferred from')
            source, recipient = [_get_source_type(s) for s in transaction.split(' to ')]

            return ('TRANSFER', source, recipient)

        except:
            return (None, None, None)

    return  (None, None, None)
    

def parse_tweets_to_df(tweets: List[dict]) -> pd.DataFrame:
    """
    Parse JSON response into pandas dataframe
    """
    columns = [
        'local_time', 
        'unix_time', 
        'asset', 
        'transaction_type', 
        'transaction_source',
        'transaction_recipient',
        'num_coins', 
        'usd_value', 
        'tweet_id',
        'description'
    ]
    rows = []

    for tweet in tweets:
        formatted_tweet = {}

        local_time, unix_time = get_local_and_unix_time(tweet['created_at'])

        # Example response:
        # "{at least one emoji} 28,999,985 #BTC   (28,999,985 USD) transferred from unknown wallet to #Coinbase  https://t.co/67Sk5Yg62X"
        # Parse:                num_coins  asset  usd_value        description                                   URL
        # Parsed Index:         0          1      2:4              4:-1                                          -1
        text = [i for i in tweet['text'].split() if i not in UNICODE_EMOJI['en']] # drops emojis
        
        formatted_tweet['local_time']  = local_time
        formatted_tweet['unix_time']   = unix_time
        formatted_tweet['asset']       = text[1].replace('#', '')
        formatted_tweet['num_coins']   = text[0].replace(',', '')
        formatted_tweet['usd_value']   = ' '.join(text[2:4]).replace(',', '').split()[0][1:]
        formatted_tweet['description'] = ' '.join(text[4:-1])
        formatted_tweet['url'] 		   = text[-1]

        transaction_type, transaction_source, transaction_recipient = get_transaction_info(formatted_tweet['description'])
        formatted_tweet['transaction_type']      = transaction_type
        formatted_tweet['transaction_source']    = transaction_source
        formatted_tweet['transaction_recipient'] = transaction_recipient

        # Only add tweets that were whale transactions
        if is_whale_transaction(formatted_tweet):
            rows.append(formatted_tweet)
    
    df = pd.DataFrame(rows, columns=columns)
    df = df.drop_duplicates()

    return df


if __name__ == "__main__":

    suffix = sys.argv[1] if len(sys.argv) > 1 else ''

    with open(f'../data/tweets_raw{suffix}.json', 'r') as f:
        tweets = json.load(f)['data']

    tweets_df = parse_tweets_to_df(tweets)

    tweets_df = tweets_df.drop_duplicates()
    tweets_df = tweets_df.sort_values('unix_time')

    tweets_df.to_csv(f'../data/tweets_formatted{suffix}.csv', index=False)