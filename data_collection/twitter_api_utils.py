import twitter
import os
from dotenv import load_dotenv
from urllib.parse import unquote
from data_collection.db_utils import save_tweets_to_mongodb


def setup_twitter_api(retry=False):
    """
    Returns an instance of the Twitter object which can be used to make subsequent queries
    :param retry: indicates whether to retry a request when the rate limit is reached
    """
    # Setup Twitter API
    load_dotenv()

    api_key = os.getenv("TWITTER_API_KEY")
    api_secret = os.getenv("TWITTER_API_SECRET")

    auth = twitter.oauth.OAuth("", "", api_key, api_secret)

    return twitter.Twitter(auth=auth, retry=retry)


def get_query_args(users_list, to_or_from="from", since_id=None):
    """
    Get arguments for search query for tweets from users or replies to to users
    :param users_list: List of users from whom to retrieve the tweets.
    :param to_or_from: Whether the query should retrieve tweets to (replies) or from these accounts.
    :param since_id: Id of the tweet whose date is the earliest date from which to retrieve tweets
    :return: a dict object with the arguments
    """

    # This creates a query of the form "to:user1 OR to:user2..." or "from:user1 OR from:user2"
    query_string = " OR ".join(["{}:{}".format(to_or_from, user) for user in users_list])

    kwargs = {'q': query_string}
    if since_id is not None:
        kwargs['since_id'] = since_id

    return kwargs


def process_request(twitter_api, kwargs=None):
    """
    Processes a request based on a query string.
    :param twitter_api: The twitter api object.
    :param kwargs: arguments to pass into the search query
    :return: a dictionary with the tweets returned from the twitter api.
    """
    tweets = {'statuses': [], 'search_metadata': {'next_results': ""}}
    try:
        tweets = twitter_api.search.tweets(**kwargs, tweet_mode='extended')
    except Exception as e:
        print("Exception: {}".format(e))

    return tweets


def get_tweets(initial_kwargs, twitter_api):
    """
    Retrieves tweets from the twitter api based on the query string and saves them to the database.
    :param initial_kwargs: Dictionary of arguments for the search query.
    :param twitter_api: The twitter api object.
    :return: Number of tweets retrieved for these arguments
    """
    tweets = process_request(twitter_api, kwargs=initial_kwargs)
    num_of_requests_made = 0
    statuses = tweets['statuses']
    search_results = tweets
    tweets_retrieved = 0
    while True:
        if len(statuses) >= 100:
            save_tweets_to_mongodb(statuses)
            tweets_retrieved += len(statuses)
            statuses = []
        try:
            next_results = search_results['search_metadata']['next_results']
            kwargs = dict([kv.split("=") for kv in unquote(next_results[1:]).split("&")])
            search_results = process_request(twitter_api, kwargs=kwargs)
            statuses += search_results['statuses']
            num_of_requests_made += 1
        except Exception as e:
            print("No 'next_results', exiting: {}".format(e))
            break

    if len(statuses) > 0:
        save_tweets_to_mongodb(statuses)
        tweets_retrieved += len(statuses)
    print("Number of requests before no results: ", num_of_requests_made + 1)
    return tweets_retrieved
