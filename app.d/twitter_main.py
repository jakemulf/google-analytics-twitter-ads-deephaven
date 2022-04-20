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
twitter_accounts = twitter_client.accounts()

def campaign_out_of_range(campaign, start_date, end_date):
    """
    Determines if the campaign exists within the given date range.

    Parameters:
        campaign (Campaign): The Twitter campaign
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
    Returns:
        bool: True if the campaign is out of the given date range, False otherwise
    """
    #Campaign times are in the yyyy-mm-ddThh:mm:ssZ format
    campaign_start_time = to_datetime(campaign._start_time[0:-1] + " UTC")
    campaign_end_time = to_datetime(campaign._end_time[0:-1] + " UTC")

    return (start_date >= campaign_start_time and start_date >= campaign_end_time) or (end_date <= campaign_start_time and end_date <= campaign_end_time)

def get_campaign_metrics(account, campaign, start_date, end_date, placement):
    """
    Gets the campaign metrics for the given campaign for the given date range

    Parameters:
        account (Account): The Twitter account object
        campaign (Campaign): The Twitter campaign object
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
        placement (str): The Twitter placement. Should be one of "ALL_ON_TWITTER" or "PUBLISHER_NETWORK"
    Returns:
        (int, int, int): A tuple representing the clicks, engagements, and impressions on the
            campaign for the given time period.
    """
    if campaign_out_of_range(campaign, start_date, end_date):
        return (None, None, None)

    metric_groups = [METRIC_GROUP.ENGAGEMENT]
    kwargs = {
        "start_time": datetime.strptime(start_date.toDateString(), "%Y-%m-%d"),
        "end_time": datetime.strptime(end_date.toDateString(), "%Y-%m-%d"),
        "entity": "CAMPAIGN",
        "granularity": "TOTAL",
        "placement": placement
    }
    response = Analytics.all_stats(account, [campaign.id], metric_groups, **kwargs)
    time.sleep(4)

    response_data = response[0]["id_data"][0]["metrics"]
    clicks = response_data["clicks"]
    engagements = response_data["engagements"]
    impressions = response_data["impressions"]

    if not (clicks is None):
        clicks = clicks[0]
    if not (engagements is None):
        engagements = engagements[0]
    if not (impressions is None):
        impressions = impressions[0]

    return (clicks, engagements, impressions)

def get_campaigns():
    """
    Retrieves all the campaigns for the Twitter account

    Returns:
        list<Campaign>: The list of all campaigns across all accounts 
    """
    campaigns = []
    for account in twitter_accounts:
        for campaign in account.campaigns():
            campaigns.append(campaign)
        time.sleep(4)
    return campaigns

def twitter_ads_main(start_date, end_date, date_increment):
    """
    Main method for the twitter ads data collector. Retrieves all campaigns, then grabs campaign metrics for each
    campaign for the given date range, and returns a Deephaven table.

    Parameters:
        start_date (DateTime): The start date as a Deephaven DateTime object.
        end_Date (DateTime): The end date as a Deephaven DateTime object.
        date_increment (Period): The time increment for subsequent data retrievals
    Returns:
        Table: A Deephaven table containing the campaign information
    """
    #Create table writer
    dtw_columns = {
        "Date": dht.DateTime,
        "CampaignName": dht.string,
        "CampaignId": dht.string,
        "Placement": dht.string,
        "Clicks": dht.int_,
        "Engagements": dht.int_,
        "Impressions": dht.int_
    }
    table_writer = DynamicTableWriter(dtw_columns)

    #Get campaigns on the account
    campaigns = get_campaigns()

    #Loop through dates
    current_date = start_date
    while current_date < end_date:
        print("Twitter")
        print(current_date)
        next_date = plus_period(current_date, date_increment)
        for account in twitter_accounts:
            for campaign in campaigns:
                for placement in ["PUBLISHER_NETWORK", "ALL_ON_TWITTER"]:
                    (clicks, engagements, impressions) = get_campaign_metrics(account, campaign, current_date, next_date, placement)
                    if not (None in [clicks, engagements, impressions]):
                        table_writer.write_row(current_date, campaign.name, campaign.id, placement, clicks, engagements, impressions)
        current_date = next_date

    return table_writer.table
