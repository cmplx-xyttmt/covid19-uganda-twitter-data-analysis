from data_collection.twitterv2.db_utils import retrieve_tweets
import json


if __name__ == '__main__':
    tweets = retrieve_tweets("ugandan")
    print(len(tweets))
    first_tweet = tweets[0]
    first_tweet.pop('_id', first_tweet)
    print(json.dumps(first_tweet, indent=4))
    # for tweet in tweets:
    #     if "in_reply_to_user_id" in tweet:
    #         print("Found reply tweet")
    #         json_tweet = tweet
    #         json_tweet.pop('_id', json_tweet)
    #         print(json.dumps(json_tweet, indent=4))
    #         break

    conversation = []
    for tweet in tweets:
        if tweet["conversation_id"] == "1366124949489016832":
            conv_tweet = tweet
            conv_tweet.pop('_id')
            conversation.append(conv_tweet)

    print("Tweets in the conversation: 1366124949489016832")
    print(json.dumps(conversation, indent=4))
