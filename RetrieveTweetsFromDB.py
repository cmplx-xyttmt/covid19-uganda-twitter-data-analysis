from pymongo import MongoClient


def get_tweets_from_db():
    tweets_cursor = _tweets_collection.find()
    num_of_records = _tweets_collection.count_documents({})
    tweets_dict = dict()
    for tweet in tweets_cursor:
        tweets_dict[tweet['id_str']] = tweet

    num_of_unique_records = len(tweets_dict)
    print(num_of_records, num_of_unique_records)


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client['twitter_db']
    _tweets_collection = db.tweets
    get_tweets_from_db()
