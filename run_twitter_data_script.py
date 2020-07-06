from data_collection.twitter_api_utils import get_query_args, get_tweets, setup_twitter_api
from data_collection.db_utils import add_to_users_list
import time
import random
import logging


def random_15_users(users_list):
    """
    Selects 25 random users from the users list.
    This is because when querying twitter, the maximum length of the query string is 500.
    The number 15 is chosen under the assumption that each user name plus other query characters (like "from" and
    " OR ") are not more than 30 characters.
    :param users_list: whole list of users
    :return: list of 15 user names (or less if the length of the list is less than 25)
    """
    return random.sample(users_list, min(15, len(users_list)))


def collect_tweets():
    """
    Collects tweets and replies from users in a dynamically generated list (based on an initial list of users).
    """
    to_from = ["from", "to"]
    users_list = ["MinOfHealthUG", "newvisionwire", "nbstv", "KagutaMuseveni", "ntvuganda", "observerug", "MoICT_Ug",
                  "bukeddetv", "DailyMonitor", "CanaryMugume", "fsnakazibwe", "KKariisa", "nilepostnews", "AKasingye",
                  "HEBobiwine", "kizzabesigye1", "cobbo3", "RedPepperUG", "AmamaMbabazi", "UrbanTVUganda"]

    current_iteration = 0
    twitter_api = setup_twitter_api()
    while True:
        kwargs = get_query_args(random_15_users(users_list), to_or_from=to_from[current_iteration % 2])
        logging.info("Retrieving tweets...")
        number_of_tweets = get_tweets(kwargs, twitter_api)

        logging.info("Number of tweets retrieved for arguments {}: {}".format(kwargs, number_of_tweets))

        # Update list of users
        users_list = add_to_users_list(users_list)

        # TODO: Print summary of db data
        logging.info("Number of users so far: {}".format(len(users_list)))

        logging.info("Waiting for 2 minutes...")
        time.sleep(120)

        current_iteration += 1


if __name__ == '__main__':
    logging.basicConfig(filename='twitter_log.log', level=logging.INFO)
    collect_tweets()
