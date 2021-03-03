from data_collection.location_premium.premium_api import create_tweets_url, make_data_request
from datetime import datetime


if __name__ == '__main__':
    # For premium and counts api
    # _from_date = datetime(2020, 3, 1).strftime("%Y%m%d%H%M")
    # _to_date = datetime(2021, 3, 2).strftime("%Y%m%d%H%M")

    # For academic twitter
    _from_date = datetime(2020, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ")
    _to_date = datetime(2021, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ")

    # make_counts_request("place_country:UG", _from_date, _to_date, api_type="academic")
    # make_data_request("place_country:UG since", _from_date, _to_date)
    _url = create_tweets_url("place_country:UG", start_time=_from_date,
                             end_time=_to_date, max_results=500, time_type="all")
    make_data_request(_url)
