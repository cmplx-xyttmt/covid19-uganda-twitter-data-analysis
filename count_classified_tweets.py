from data_collection.twitterv2.db_utils import retrieve_tweets


if __name__ == '__main__':
    tweets = retrieve_tweets("ugandan")
    classified_tweets = 0
    for tweet in tweets:
        if 'prediction' in tweet:
            classified_tweets += 1
    print(classified_tweets)
