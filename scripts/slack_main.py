"""
slack_main.py

Main file to collect data from Slack's API

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.

Take note that these methods do some operations to guarantee unique time stamps (time stamps are unique
in the slack API) due to some weirdness with pagination giving duplicate results.
"""
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

from slack_sdk import WebClient

import os
import time
import json

SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")

slack_client = WebClient(token=SLACK_API_TOKEN)

def get_channel_info(slack_channel):
    return slack_client.conversations_info(channel=slack_channel)

def get_public_channels():
    """
    Returns information for all of the public slack channels.

    Returns:
        list<tuple(str, str, str)>: A list of channel ID, channel name, and JSON response for all channels
    """
    cursor = None
    channels = []
    while True:
        response = slack_client.conversations_list(cursor=cursor)

        for channel in response["channels"]:
            channels.append((channel["id"], channel["name"], json.dumps(channel)))

        cursor = response["response_metadata"]["next_cursor"] #Note that this endpoint does pagination
        #differently than the other one in this file
        if len(cursor) == 0:
            break
        else:
            print("Pagination found, getting next entries")
            print(cursor)
        time.sleep(3)

    return channels

def get_thread_messages(slack_channel, ts):
    """
    Gets the messages in the thread

    Parameters:
        slack_channel (str): The string ID of the slack channel
        ts (str): A string representing seconds since the Epoch for the time stamp of the thread
    Returns:
        set: A set containing pairs of (ts, text, json) for each message
    """
    s = set()
    next_cursor = None

    while True:
        thread_replies = slack_client.conversations_replies(channel=slack_channel, ts=ts, cursor=next_cursor)
        time.sleep(1.2)

        for message in thread_replies["messages"]:
            if (message["type"] == "message"):
                s.add((message["ts"], message["text"], json.dumps(message)))

        if bool(thread_replies["has_more"]):
            next_cursor = thread_replies["response_metadata"]["next_cursor"]
        else:
            next_cursor = None

        if next_cursor is None:
            break
        else:
            print("Pagination found, getting next entries")
            print(next_cursor)
    return s

def get_channel_messages(slack_channels, start_time=None, end_time=None):
    """
    Returns all of the messages in the channel

    Parameters:
        slack_channels (list<str>): A list of string IDs representing the slack channels to pull from
        start_time (DateTime): If given, only retrieve messages after this time stamp
        end_time (DateTime): If given, only retrieves messages before this time stamp
    Returns
        Table: A Deephaven table of all the messages
    """
    start_time_seconds = None
    if not (start_time is None):
        start_time_seconds = str(start_time.getMillis()/1000)
    end_time_seconds = None
    if not (end_time is None):
        end_time_seconds = str(end_time.getMillis()/1000)

    dtw_columns = {
        "ChannelID": dht.string,
        "TS": dht.string,
        "Text": dht.string,
        "JsonString": dht.string,
    }
    table_writer = DynamicTableWriter(dtw_columns)

    for slack_channel in slack_channels:
        next_cursor = None
        while True:
            channel_history = slack_client.conversations_history(channel=slack_channel, cursor=next_cursor, include_all_metadata=True,
                                                                 oldest=start_time_seconds, latest=end_time_seconds)

            for message in channel_history["messages"]:
                if (message["type"] == "message"):
                    #If message is in a thread, get the thread messages. "thread_ts" seems to
                    #be the only identifier for a thread being present. And the threading API
                    #expects the ts of the original message too
                    if ("thread_ts" in message):
                        for (ts, text, json_str) in get_thread_messages(slack_channel, message["ts"]):
                            table_writer.write_row(slack_channel, ts, text, json_str)
                    #Otherwise just add the message
                    else:
                        table_writer.write_row(slack_channel, message["ts"], message["text"], json.dumps(message))

            if bool(channel_history["has_more"]):
                next_cursor = channel_history["response_metadata"]["next_cursor"]
            else:
                next_cursor = None

            if next_cursor is None:
                break

            print("Pagination found, getting next entries")
            print(next_cursor)
            time.sleep(1.2)

    return table_writer.table

def get_all_slack_messages(start_time=None, end_time=None):
    """
    Gets all the messages across all channels.

    Parameters:
        start_time (DateTime): If given, only retrieve messages after this time stamp
        end_time (DateTime): If given, only retrieves messages before this time stamp

    Returns:
        (Table, Table): The table of slack channel information, and the table of slack message information
    """
    public_channels = get_public_channels()
    channel_ids = []

    dtw_columns = {
        "ChannelID": dht.string,
        "ChannelName": dht.string,
        "JsonString": dht.string,
    }
    table_writer = DynamicTableWriter(dtw_columns)

    for (channel_id, channel_name, channel_json) in public_channels:
        channel_ids.append(channel_id)
        table_writer.write_row(channel_id, channel_name, channel_json)

    return (table_writer.table, get_channel_messages(channel_ids, start_time=start_time, end_time=end_time))
