# Project Remy
## Data Collection
* Twitter Api - Sample Collection: 
    * `pull_tweets_api.py` -> `data/tweets_raw_sample.json`
1. Pull Raw Tweets
    * `pull/pull_tweets_scrape.ipynb` -> `data/tweets_scrape_raw.json`
    * Scrapes tweets from telegram
2. Enrich Tweets with Time
    * `clean/get_tweet_times.py` -> `data/tweets_raw.json`
    * Uses the whale-alert.io link in the tweet to get the transaction time
    * Persists the transaction time in the same schema that's returned from the Twitter API
3. Parse Tweets
    * `clean/parse_tweets.py` -> `data/tweets_formatted.csv`
    * Parses the tweets in a table
3. Pull Raw Prices
    * `pull_prices.py` -> `data/prices_raw.json`
    * Must be run after `data/tweets_formatted.csv` has been created 
    * Uses the coinbase API to pull prices that occured in the time window of the tweets
4. Parse Prices
    * `clean/parse_prices.py` -> `prices_formatted.csv`
    * Parses the coinbase JSON response into a CSV

## Analysis
* `plot_whale_tweets.ipynb`
    * Using a sample of tweets, plots the tweet time on top of a price chart
* `plot_whale_alert_average_impact.ipynb`
    * Plots the average price movement surrounding whale tweets of various categories
    