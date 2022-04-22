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

SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")

slack_client = WebClient(token=SLACK_API_TOKEN)

def get_channel_info(slack_channel):
    return slack_client.conversations_info(channel=slack_channel)

def get_thread_messages(slack_channel, ts):
    """
    Gets the messages in the thread

    Parameters:
        slack_channel (str): The string ID of the slack channel
        ts (str): A string representing seconds since the Epoch for the time stamp of the thread
    Returns:
        set: A set containing pairs of (ts, text) for each message
    """
    s = set()
    next_cursor = None

    while True:
        thread_replies = slack_client.conversations_replies(channel=slack_channel, ts=ts, cursor=next_cursor)
        time.sleep(1)

        for message in thread_replies["messages"]:
            if (message["type"] == "message"):
                s.add((message["ts"], message["text"]))

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

def get_channel_messages(slack_channel):
    """
    Returns all of the messages in the channel

    Parameters:
        slack_channel (str): The string ID of the slack channel
    Returns
        Table: A Deephaven table of all the messages
    """
    dtw_columns = {
        "TS": dht.string,
        "Text": dht.string
    }
    table_writer = DynamicTableWriter(dtw_columns)

    next_cursor = None

    while True:
        channel_history = slack_client.conversations_history(channel=slack_channel, cursor=next_cursor)
        time.sleep(1)

        for message in channel_history["messages"]:
            if (message["type"] == "message"):
                #If message is in a thread, get the thread messages. "thread_ts" seems to
                #be the only identifier for a thread being present. And the threading API
                #expects the ts of the original message too
                if ("thread_ts" in message):
                    for (ts, text) in get_thread_messages(slack_channel, message["ts"]):
                        table_writer.write_row(ts, text)
                #Otherwise just add the message
                else:
                    table_writer.write_row(message["ts"], message["text"])

        if bool(channel_history["has_more"]):
            next_cursor = channel_history["response_metadata"]["next_cursor"]
        else:
            next_cursor = None

        if next_cursor is None:
            break
        else:
            print("Pagination found, getting next entries")
            print(next_cursor)

    return table_writer.table
