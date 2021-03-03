import io
import json

from data_collection.twitterv2.db_utils import retrieve_tweets


if __name__ == '__main__':
    tweets = retrieve_tweets("ugandan")
    with io.open('data/ugandan_tweets.json', 'w', encoding='utf-8') as f:
        tweets_no_id = []
        for tweet in tweets:
            tweet.pop('_id')
            tweets_no_id.append(tweet)
        f.write(json.dumps({"tweets": tweets_no_id}, ensure_ascii=False, indent=5))
