"""
twitter_main.py

Main file to collect data from the Twitter Ads API

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.
"""
from deephaven import DynamicTableWriter, Types as dht
from deephaven.DateTimeUtils import plus

import json
import os

TWITTER_ACCOUNT_ID = os.environ.get("TWITTER_ACCOUNT_ID")
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN")

def get_campaign_metrics(campaign_id, start_date, end_date):
    """
    Gets the campaign metrics for the given campaign for the given date range

    Parameters:
        campaing_id (str): The Twitter campaign ID
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
    Returns:
        (int, int, int): A tuple representing the clicks, engagements, and impressions on the
            campaign for the given time period
    """
    params = {
        "start_time": start_date.toDateString(),
        "end_time": end_date.toDateString(),
        "entity_ids": campaign_id,
        "entity": "CAMPAIGN",
        "granularity": "TOTAL",
        "metric_groups": "ENGAGEMENT",
        "placement": "ALL_ON_TWITTER"
    }
    #response = requests.get(build_url(), params=params).json()
    response = json.loads('{  "data_type": "stats",  "time_series_length": 1,  "data": [    {      "id": "8u94t",      "id_data": [        {          "segment": null,          "metrics": {            "impressions": [              1233            ],            "tweets_send": null,            "qualified_impressions": null,            "follows": null,            "app_clicks": null,            "retweets": null,            "likes": [              1            ],            "engagements": [              58            ],            "clicks": [              58            ],            "card_engagements": null,            "poll_card_vote": null,            "replies": null,            "carousel_swipes": null          }        }      ]    }  ],  "request": {    "params": {      "start_time": "2017-05-19T07:00:00Z",      "segmentation_type": null,      "entity_ids": [        "8u94t"      ],      "end_time": "2017-05-26T07:00:00Z",      "country": null,      "placement": "ALL_ON_TWITTER",      "granularity": "TOTAL",      "entity": "LINE_ITEM",      "platform": null,      "metric_groups": [        "ENGAGEMENT"      ]    }  }}')
    response_data = response["data"][0]["id_data"][0]["metrics"]
    return (response_data["clicks"][0], response_data["engagements"][0], response_data["impressions"][0])

def get_campaigns():
    """
    Retrieves all the campaigns for the Twitter account

    Returns:
        dict: A key-value dictionary mapping campaign IDs to campaign names
    """
    campaigns = {}
    next_token = None
    while True:
        #response = requests.get(build_url()).json()
        response = json.loads('{  "request": {    "params": {      "campaign_ids": [        "8wku2"      ],      "account_id": "18ce54d4x5t"    }  },  "next_cursor": null,  "data": [    {      "name": "batch campaigns",      "start_time": "2017-06-30T00:00:00Z",      "reasons_not_servable": [        "PAUSED_BY_ADVERTISER",        "INCOMPLETE"      ],      "servable": false,      "daily_budget_amount_local_micro": 140000000,      "end_time": null,      "funding_instrument_id": "lygyi",      "standard_delivery": true,      "total_budget_amount_local_micro": null,      "id": "8wku2",      "entity_status": "PAUSED",      "currency": "USD",      "created_at": "2017-06-30T21:17:16Z",      "updated_at": "2017-06-30T21:17:16Z",      "deleted": false    }  ]}')
        for value in response["data"]:
            campaign_id = value["id"]
            campaign_name = value["name"]
            campaigns[campaign_id] = campaign_name
        next_token = response.get("next_cursor")
        if next_token is None:
            break
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
    column_names = ["Date", "Campaign", "Clicks", "Engagements", "Impressions"]
    column_types = [dht.datetime, dht.string, dht.int_, dht.int_, dht.int_]
    table_writer = DynamicTableWriter(column_names, column_types)

    #Get campaigns on the account
    campaigns = get_campaigns()

    #Loop through dates
    current_date = start_date
    while current_date < end_date:
        next_date = plus(current_date, date_increment)
        for campaign_id in campaigns.keys():
            (clicks, engagements, impressions) = get_campaign_metrics(campaign_id, current_date, next_date)
            table_writer.logRowPermissive(current_date, campaigns[campaign_id], clicks, engagements, impressions)
        current_date = next_date

    return table_writer.getTable()
