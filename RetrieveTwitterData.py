import twitter
import os
from dotenv import load_dotenv
from urllib.parse import unquote
from pymongo import MongoClient


def get_more_results(tweets):
    num_of_requests_made = 0
    statuses = tweets['statuses']
    tweets_retrieved = 0
    while True:
        print("Number of tweets so far: ", len(statuses) + tweets_retrieved)
        if len(statuses) >= 100:
            save_tweets_to_mongodb(statuses)
            tweets_retrieved += len(statuses)
            statuses = []
        try:
            next_results = tweets['search_metadata']['next_results']
        except KeyError as e:
            break
        kwargs = dict([kv.split("=") for kv in unquote(next_results[1:]).split("&")])
        try:
            search_results = twitter_api.search.tweets(**kwargs)
            if len(search_results['statuses']) == 0:
                break
            statuses += search_results['statuses']
            num_of_requests_made += 1
        except KeyError as e:
            print("{}\nRetry: {}".format(e, twitter_api.retry))
            break
        except Exception as e:
            print("{}".format(e))

    print("Number of requests before no results: ", num_of_requests_made + 1)
    print("Number of tweets: ", len(statuses))


def get_tweets(q_string):
    tweets = twitter_api.search.tweets(q=q_string, tweet_mode='extended')

    # Get more results
    get_more_results(tweets)

    print("Finished retrieving '{}' tweets".format(q_string))


def save_tweets_to_mongodb(statuses):
    result = _tweets.insert_many(statuses)
    print("Result of insert: {0}".format(result.inserted_ids))


if __name__ == '__main__':
    # Setup Twitter API

    load_dotenv()

    API_KEY = os.getenv("TWITTER_API_KEY")
    API_SECRET = os.getenv("TWITTER_API_SECRET")

    auth = twitter.oauth.OAuth("", "", API_KEY, API_SECRET)

    twitter_api = twitter.Twitter(auth=auth, retry=False)

    # Setup MongoDB
    client = MongoClient('localhost', 27017)
    db = client['twitter_db']

    _tweets = db.tweets

    # Run query
    query_list = ["place:Uganda", "bio_location:Kampala", "from:MinofHealthUG", "COVID19UG"]
    _q_string = " OR ".join(query_list)
    get_tweets(_q_string)
