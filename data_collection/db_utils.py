from pymongo import MongoClient

DATABASE_NAME = "twitter_db"
COLLECTION_NAME = "tweets_collection"


def get_mongo_db_collection(db_name, collection_name):
    """
    Returns the specified mongo db collection which can be used for insertion or querying.
    :param db_name: The name of the database from which to get the collection
    :param collection_name: The name of the collection to get.
    """
    client = MongoClient('localhost', 27017)
    db = client[db_name]

    return db[collection_name]


def save_tweets_to_mongodb(statuses):
    """
    Inserts the tweets into the database.
    :param statuses: The list of tweets to save to the database
    """
    tweets_collection = get_mongo_db_collection(DATABASE_NAME, COLLECTION_NAME)
    tweets_collection.insert_many(statuses)


def get_tweets_from_db(_collection):
    """
    Returns a list of the tweets saved in the db
    :param _collection: The database collection from which to get the tweets
    :return: a list of tweets in the database.
    """
    tweets_cursor = _collection.find()
    tweets = []
    for tweet in tweets_cursor:
        tweets.append(tweet)

    return tweets


def get_users_from_db(collection):
    """
    Returns the list of users screen name whose tweets are in the db
    :param collection: The database collection which contains the tweets.
    :return: a list of the users whose tweets are in the database
    """
    tweets = get_tweets_from_db(collection)
    users = set()
    for tweet in tweets:
        users.add(tweet['user']['screen_name'])
    return list(users)


def get_all_users_from_db():
    """
    Retrieves all users from the database
    :return: a list of users from the database
    """
    return get_users_from_db(get_mongo_db_collection(DATABASE_NAME, COLLECTION_NAME))


def add_to_users_list(current_users_list):
    """
    Adds users from the database to the users list
    :param current_users_list: current list of users
    :return: a new list with both the current users and more users from the db.
    """

    db_users = get_all_users_from_db()
    new_users_list = set(current_users_list).union(set(db_users))
    return list(new_users_list)
