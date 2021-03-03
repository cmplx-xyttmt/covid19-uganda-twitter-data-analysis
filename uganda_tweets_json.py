import io
import json

from data_collection.twitterv2.db_utils import retrieve_tweets


if __name__ == '__main__':
    tweets = retrieve_tweets("ugandan")
    # Divide into 2 files
    first = len(tweets)//2
    with io.open('data/ugandan_tweets_1.json', 'w', encoding='utf-8') as f:
        tweets_no_id = []
        for tweet in tweets:
            tweet.pop('_id')
            tweets_no_id.append(tweet)
            if len(tweets_no_id) == first:
                break
        f.write(json.dumps({"tweets": tweets_no_id}, ensure_ascii=False, indent=5))

    with io.open('data/ugandan_tweets_2.json', 'w', encoding='utf-8') as f:
        tweets_no_id = []
        for i in range(first, len(tweets)):
            tweet = tweets[i]
            tweet.pop('_id')
            tweets_no_id.append(tweet)
            if len(tweets_no_id) == first:
                break
        f.write(json.dumps({"tweets": tweets_no_id}, ensure_ascii=False, indent=5))
