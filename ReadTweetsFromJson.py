import io
import json


def get_tweets_from_json_file(filename):
    with io.open(filename, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        f.close()
    return json_data['tweets']


if __name__ == '__main__':
    # This is the list of tweets
    _tweets = get_tweets_from_json_file('mask_tweets.json')
