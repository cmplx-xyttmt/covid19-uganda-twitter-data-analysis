import io
import json

from data_collection.twitterv2.db_utils import retrieve_tweets
from ReadTweetsFromJson import grouper


if __name__ == '__main__':
    tweets_id = retrieve_tweets("ugandan")
    tweets = []
    for tweet in tweets_id:
        tweet.pop('_id')
        tweets.append(tweet)
    for i, group in enumerate(grouper(tweets, 20000)):
        filename = f'data/ug_tweets_{i + 1}.json'
        with io.open(filename, 'w', encoding='utf-8') as f:
            f.write(json.dumps({"tweets": list(group)}, ensure_ascii=False, indent=5))
