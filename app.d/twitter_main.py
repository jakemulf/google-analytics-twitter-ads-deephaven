"""
twitter_main.py

Main file to collect data from the Twitter Ads API

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.
"""
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.time import plus_period, to_datetime

from twitter_ads.client import Client
from twitter_ads.analytics import Analytics
from twitter_ads.enum import  METRIC_GROUP

import json
import os
from datetime import datetime
import time

TWITTER_CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
TWITTER_CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
TWITTER_ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")

twitter_client = Client(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)

def analytics_out_of_range(analytics, start_date, end_date):
    """
    Determines if the analyitcs exists within the given date range.

    Parameters:
        analytics (Analytics): The Twitter analytics object
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
    Returns:
        bool: True if the analytics object is out of the given date range, False otherwise
    """
    #If there's no start date, assume the analytics hasn't been activated
    if analytics._start_time is None:
        return True
    #If there's no end time, assume the analytics is still running, meaning
    #it is out of range only if the analytics start time is after the given end time
    elif analytics._end_time is None:
        analytics_start_time = to_datetime(analytics._start_time[0:-1] + " UTC")
        return analytics_start_time > end_date

    #Analytics times are in the yyyy-mm-ddThh:mm:ssZ format
    analytics_start_time = to_datetime(analytics._start_time[0:-1] + " UTC")
    analytics_end_time = to_datetime(analytics._end_time[0:-1] + " UTC")

    #If the given start date is greater than the analytics range, the analytics is out of range
    #Or, if the given end date is less than the analytics range, the analytics is out of range
    #Otherwise the analytics is in range
    return (start_date >= analytics_start_time and start_date >= analytics_end_time) or (end_date <= analytics_start_time and end_date <= analytics_end_time)

def get_analytics_metrics(account, analytics, start_date, end_date, placement, entity):
    """
    Gets the analytics metrics for the given analytics item for the given date range

    Parameters:
        account (Account): The Twitter account object
        analytics (Analytics): The Twitter analytics object
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
        placement (str): The Twitter placement. Should be one of "ALL_ON_TWITTER" or "PUBLISHER_NETWORK"
        entity (str): The entity of the analytics object for the API request. Should be "CAMPAIGN" or "LINE_ITEM"
    Returns:
        str: A JSON string of the analyitcs response
    """
    if analytics_out_of_range(analytics, start_date, end_date):
        return None

    metric_groups = [METRIC_GROUP.ENGAGEMENT]
    kwargs = {
        "start_time": datetime.strptime(start_date.toDateString(), "%Y-%m-%d"),
        "end_time": datetime.strptime(end_date.toDateString(), "%Y-%m-%d"),
        "entity": entity,
        "granularity": "HOUR",
        "placement": placement
    }
    response = Analytics.all_stats(account, [analytics.id], metric_groups, **kwargs)
    time.sleep(4)

    return json.dumps(response)

def get_campaigns(account):
    """
    Retrieves all the campaigns for the Twitter account

    Parameters:
        account (Account): The Twitter account to pull data from

    Returns:
        list<Campaign>: The list of all campaigns across all accounts 
    """
    campaigns = []
    for campaign in account.campaigns():
        campaigns.append(campaign)
    time.sleep(4)
    return campaigns

def get_line_items(account):
    """
    Retrieves all the line items for the Twitter account

    Parameters:
        account (Account): The Twitter account to pull data from

    Returns:
        list<LineItem>: The list of all line items across all accounts 
    """
    line_items = []
    for line_item in account.line_items():
        line_items.append(line_item)
    time.sleep(4)
    return line_items

def get_funding_instruments(account):
    funding_instruments = []
    for funding_instrument in account.funding_instruments():
        funding_instruments.append(funding_instrument)
    time.sleep(4)
    return funding_instruments

def get_promoted_tweets(account):
    promoted_tweets = []
    for promoted_tweet in account.promoted_tweets():
        promoted_tweets.append(promoted_tweet)
    time.sleep(4)
    return promoted_tweets

def get_media_creatives(account):
    media_creatives = []
    for media_creative in account.media_creatives():
        media_creatives.append(media_creative)
    time.sleep(4)
    return media_creatives

def twitter_ads_main(start_date, end_date, date_increment):
    """
    Main method for the twitter ads data collector. Collects data of various types
    and returns a Deephaven Table

    Parameters:
        start_date (DateTime): The start date as a Deephaven DateTime object.
        end_Date (DateTime): The end date as a Deephaven DateTime object.
        date_increment (Period): The time increment for subsequent data retrievals
    Returns:
        Table: The Deephaven table containing the data
    """
    #Create table writer
    dtw_columns = {
        "Date": dht.DateTime,
        "AccountName": dht.string,
        "AnalyticsType": dht.string,
        "Placement": dht.string,
        "JsonString": dht.string,
    }
    table_writer = DynamicTableWriter(dtw_columns)

    #Get metrics on the account. These tuples represent the
    #analytics name for the Twitter API, the name of the row
    #in the AnalyticsType column, and the function that takes an account
    #and returns a list of entities to pull.

    #To collect more data, add entries to this list
    analytics_types = [
        ("CAMPAIGN", "Campaign", get_campaigns),
        ("LINE_ITEM", "AdGroup", get_line_items),
        ("FUNDING_INSTRUMENT", "FundingInstrument", get_funding_instruments),
        #("PROMOTED_TWEET", "PromotedTweet", get_promoted_tweets[0:1]), #Leaving this out for now
        #since the promoted tweets don't have a start/end time, but instead a created time
        ("MEDIA_CREATIVE", "MediaCreative", get_media_creatives)
    ]

    #Loop through dates
    current_date = start_date
    while current_date < end_date:
        print("Twitter")
        print(current_date)
        next_date = plus_period(current_date, date_increment)

        for account in twitter_client.accounts():
            for (api_name, table_name, analytics_list_method) in analytics_types:
                for analytics in analytics_list_method(account):
                    for placement in ["PUBLISHER_NETWORK", "ALL_ON_TWITTER"]:
                        json_str = get_analytics_metrics(account, analytics, current_date, next_date, placement, api_name)
                        if not (json_str is None):
                            table_writer.write_row(current_date, account.name, table_name, placement, json_str)
        current_date = next_date

    return table_writer.table
