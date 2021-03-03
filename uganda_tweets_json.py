import io
import json
import os
import shutil
import time

from ReadTweetsFromJson import grouper
from data_collection.twitterv2.db_utils import retrieve_tweets


def delete_files():
    folder = 'data'
    for file_name in os.listdir(folder):
        file_path = os.path.join(folder, file_name)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


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
        if (i + 1) % 10 == 0:
            print("Waiting for 2 minutes, transfer files quickly before they are deleted")
            time.sleep(120)
            delete_files()
