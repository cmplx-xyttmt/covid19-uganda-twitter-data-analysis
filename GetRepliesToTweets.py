"""
This script gets replies tweets that are not retweets
"""
from utils import get_mongo_db_collection, setup_twitter_api
import json
from urllib.parse import unquote


def get_replies(tweet):
    user_id = tweet['user']['screen_name']
    tweet_id = tweet['id_str']
    reply_results = twitter_api.search.tweets(q="(to:{})".format(user_id), since_id=tweet_id)
    replies = reply_results['statuses']
    prev_replies = 0
    while True:
        print("Number of replies so far: ", len(replies))
        if len(replies) >= 15:
            replies = filter_replies(replies, tweet_id)
            if len(replies) == prev_replies:
                break
            else:
                prev_replies = len(replies)
        try:
            next_results = reply_results['search_metadata']['next_results']
        except KeyError as e:
            break
        kwargs = dict([kv.split("=") for kv in unquote(next_results[1:]).split("&")])
        try:
            search_results = twitter_api.search.tweets(**kwargs)
            if len(search_results['statuses']) == 0:
                break
            replies += search_results['statuses']
        except KeyError as e:
            print("{}\nRetry: {}".format(e, twitter_api.retry))
            break
        except Exception as e:
            print("{}".format(e))
            break

    replies = filter_replies(replies, tweet_id)
    if len(replies) > 0:
        save_tweets_to_mongodb(replies)
    return len(replies)


def filter_replies(replies, tweet_id):
    """Filters replies to the specific tweet_id"""
    reps = []
    for reply in replies:
        if reply['in_reply_to_status_id_str'] == tweet_id:
            reps.append(reply)
    return reps


def save_tweets_to_mongodb(statuses):
    result = _mask_tweets.insert_many(statuses)
    print("Result of insert: {0}".format(result.inserted_ids))


def get_non_retweet_tweets():
    cursor = _mask_tweets.find()
    relevant_tweets = []
    for tweet in cursor:
        if 'retweeted_status' not in tweet:
            tweet.pop('_id')
            relevant_tweets.append(tweet)
    return relevant_tweets


if __name__ == '__main__':
    _mask_tweets = get_mongo_db_collection('twitter_db', 'mask_tweets')
    print(len(_mask_tweets))

    _tweets = get_non_retweet_tweets()

    twitter_api = setup_twitter_api(retry=True)

    for _tweet in _tweets:
        print("Number of replies: {}\n".format(get_replies(_tweet)))
