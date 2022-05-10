"""
twitter_main.py

Main file to collect data from the Twitter Ads API

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.
"""
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.time import plus_period, to_datetime, to_period

from twitter_ads.client import Client
from twitter_ads.analytics import Analytics
from twitter_ads.enum import  METRIC_GROUP

import json
import os
from datetime import datetime
import time
import copy

TWITTER_CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
TWITTER_CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
TWITTER_ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")

PROMOTED_TWEET_DURATION = to_period("14D")

twitter_client = Client(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)

class TwitterCollector:
    """
    A class to nicely define what data to collect from Twitter's ads API

    Attributes:
        analytics_items (list<tuple>): A list of tuples that contains the following:
            API analytics name, Deephaven table column name, twitter account, twitter analytics,
            and the analytics range method
    """

    def __init__(self, twitter_client, analytics_types):
        """
        Constructor method

        Parameters:
            twitter_client (Client): The Twitter client to make API calls
            analytics_types (list<tuple>): A list of tuples containing the following:
            API analytics name, Deephaven table column name, the twitter analytics method to pull from,
            and the analytics range method. This is used to build the analytics_items attribute
        """
        self.analytics_items = []
        for account in twitter_client.accounts():
            for (api_name, table_name, analytics_list_method, out_of_range) in analytics_types:
                for analytics in analytics_list_method(account):
                    self.analytics_items.append((api_name, table_name, account, analytics, out_of_range))

    def twitter_analytics_data(self, start_date, end_date, date_increment):
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
            "AnalyticsName": dht.string,
            "Placement": dht.string,
            "JsonString": dht.string,
        }
        table_writer = DynamicTableWriter(dtw_columns)

        #Loop through dates
        current_date = start_date
        while current_date < end_date:
            print("Twitter")
            print(current_date)
            next_date = plus_period(current_date, date_increment)

            for (api_name, table_name, account, analytics, out_of_range) in self.analytics_items:
                if not out_of_range(analytics, current_date, next_date):
                    for placement in ["PUBLISHER_NETWORK", "ALL_ON_TWITTER"]:
                        json_str = get_analytics_metrics(account, analytics, current_date, next_date, placement, api_name)
                        name = None
                        if hasattr(analytics, "name"):
                            name = analytics.name
                        table_writer.write_row(current_date, account.name, table_name, name, placement, json_str)

            current_date = next_date

        return table_writer.table

    def twitter_analytics_metadata(self):
        """
        Returns a Deephaven table containing metadata from the analytics items found in the account
        Returns:
            Table: The Deephaven table
        """
        dtw_columns = {
            "JsonString": dht.string,
        }
        table_writer = DynamicTableWriter(dtw_columns)

        for (_, _, _, analytics, _) in self.analytics_items:
            #Workaround to clear sensitive items since del and pop are deleting the objects,
            #not just clearing the key
            analytics_dict = copy.deepcopy(vars(analytics))
            analytics_dict["_account"] = None
            analytics_dict.pop("_account")
            table_writer.write_row([json.dumps(analytics_dict)])

        return table_writer.table

def promoted_tweet_out_of_range(promoted_tweet, start_date, end_date):
    """
    Determines if the promoted tweet exists within the given date range.

    Parameters:
        promoted_tweet (PromotedTweet): The Twitter promoted tweet object
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
    Returns:
        bool: True if the promoted tweet is out of the given date range, False otherwise
    """
    promoted_tweet_start_time = to_datetime(promoted_tweet._created_at[0:-1] + " UTC")
    promoted_tweet_end_time = plus_period(promoted_tweet_start_time, PROMOTED_TWEET_DURATION)

    return (promoted_tweet_start_time >= start_date and promoted_tweet_start_time >= end_date) or (promoted_tweet_end_time <= end_date and promoted_tweet_end_time <= start_date)

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
        list<Campaign>: The list of all campaigns across the account
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
        list<LineItem>: The list of all line items across the account
    """
    line_items = []
    for line_item in account.line_items():
        line_items.append(line_item)
    time.sleep(4)
    return line_items

def get_funding_instruments(account):
    """
    Retrieves all the funding instruments for the Twitter account

    Parameters:
        account (Account): The Twitter account to pull data from

    Returns:
        list<FundingInstrument>: The list of all funding instruments across the account
    """
    funding_instruments = []
    for funding_instrument in account.funding_instruments():
        funding_instruments.append(funding_instrument)
    time.sleep(4)
    return funding_instruments

def get_promoted_tweets(account):
    """
    Retrieves all the promoted tweets for the Twitter account

    Parameters:
        account (Account): The Twitter account to pull data from

    Returns:
        list<PromotedTweet>: The list of all promoted tweets across the account
    """
    promoted_tweets = []
    for promoted_tweet in account.promoted_tweets():
        promoted_tweets.append(promoted_tweet)
    time.sleep(4)
    return promoted_tweets

def get_media_creatives(account):
    """
    Retrieves all the media creatives for the Twitter account

    Parameters:
        account (Account): The Twitter account to pull data from

    Returns:
        list<MediaCreative>: The list of all media creatives across the account
    """
    media_creatives = []
    for media_creative in account.media_creatives():
        media_creatives.append(media_creative)
    time.sleep(4)
    return media_creatives
