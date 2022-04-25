# google-analytics-twitter-ads-deephaven

This app allows you to collect data from the Google Analytics and Twitter Ads APIs, and store the data in Deephaven.

## Configuration

### Google Analytics

Follow the steps on https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py#1_enable_the_api to get your account setup for usage with the Google Analytics API.

When downloading the key, save the JSON file locally to `./secrets/google-key.json`.

Take note of the View ID in your Google Analytics account - you will need that to run this code. You can find the View ID by clicking the dropdown menu on the top-left corner on the homepage of http://analytics.google.com/. The dropdown menu should show 3 columns (`Analytics Accounts`, `Properties & Apps`, and `Views`). Each of these columns should have a number below each section, take one of the ones from the `Views` column to use for this application.

You also will want to take note of the URLs in your Google Analytics account. Currently, this app runs 1 URL at a time, but
you can easily re-run the methods with different URLs.

### Twitter Ads

#### Twitter Dev Account

To start off, you need to create a Twitter developer account. You can do that at https://developer.twitter.com/. You need to have both an email address and phone number on your Twitter account to create a developer account.

Next you need to create a project and an app. Guides to do this can be found at https://developer.twitter.com/en/docs/projects/overview and https://developer.twitter.com/en/docs/apps/overview. Once your app is created, you'll need to create two sets of tokens. This can be done on the settings of the app. You'll need a pair of `Consumer Keys` (`API Key and Secret`) and a pair of `Authentication Tokens` (`Access Token and Secret`). Save the values of these four tokens in the `.env` file of this project.

Lastly, you should see a section called `APP ID`. Save this value as well.

#### Twitter Ads Request

Once you have your `APP ID` and your tokens, you can fill out the forum at https://developer.twitter.com/en/docs/twitter-ads-api/apply to request access to the ads API. Use the `APP ID` from the previous section when asked in this forum.

It may take a few days for you to get approved.

### Slack

To collect data from Slack's API, you need to set up a Slack application. To start, you can go to https://api.slack.com/apps/ and click the **Create New App** button. Give your app a name and assign it to your workspace.

On the next page, click the **Permissions** section underneath **Add features and functionality**. Scroll down to the **Scopes** section and click **Add an OAuth Scope**. This should open up a dropdown menu.

Add the following OAuth scopes: `channels:history`, `channels:read`, `groups:history`, `groups:read`, `im:history`, `im:read`, `mpim:history`, `mpim:read`

Now, scroll up to the **OAuth Tokens for Your Workspace** section and click the **Install to Workspace** button. On the next page, press **Allow**.

If this was done correctly, you should see your app in your Slack workspace's **Apps** section. Invite the app to any channels you want to track metrics in: `/invite @<AppName>`.

Back in the **OAuth Tokens for Your Workspace** section, there should be a **Bot User OAuth Token** field. This is the value used in the `SLACK_API_TOKEN` environmental variable for this project.

Lastly, you need your channel IDs. These are simply found in the channel information in your workspace. You can optionally save one of these in the `SLACK_CHANNEL` environmental variable for this project.

## Launch

Run this script to launch the app:

```
sh start.sh
```

### In Deephaven

Once launched, go to the Deephaven UI (this defaults to: localhost:10000). You can now run code to collect and visualize data from the APIs.

This example shows how to use the Google Analytics data collector

```
metrics_collectors = [
    MetricsCollector(expression="ga:pageViews", metric_column_name="PageViews", dh_type=dht.int_, converter=int),
    MetricsCollector(expression="ga:uniquePageViews", metric_column_name="UniqueViews", dh_type=dht.double, converter=float),
    MetricsCollector(expression="ga:bounceRate", metric_column_name="BounceRate", dh_type=dht.double, converter=float)
]
paths = [
    "/company/careers/posts/internship-2022/",
    "/core/docs/how-to-guides/parquet-partitioned/"
]
start_date = to_datetime("2022-04-12T00:00:00 NY")
end_date = to_datetime("2022-04-18T00:00:00 NY")
page_size = 100000
view_id = "181392643"
date_increment = to_period("1D")

ga_collector = GaCollector(start_date=start_date, end_date=end_date, page_size=page_size, view_id=view_id, date_increment=date_increment, paths=paths, metrics_collectors=metrics_collectors)

tables = ga_collector.collect_data()

#To display in the UI
table_0 = tables[0]
table_1 = tables[1]
#...
```

This example collects campaign data from the Twitter Ads API.

```
from deephaven.time import to_datetime, to_period

start_date = to_datetime("2022-03-11T00:00:00 NY")
end_date = to_datetime("2022-03-14T00:00:00 NY")
date_increment = to_period("1D")

twitter_table = twitter_ads_main(start_date, end_date, date_increment)
```

This example collects data from Slack.

```
slack = get_channel_messages(SLACK_CHANNEL)
```

### Parquet reading and writing

There are two helper methods in `./app.d/parquet_writer.py` that can be used to read and write parquet files, `write_tables` and `read_tables`. `write_tables` expects to receive a list of tables.

```
tables = read_tables(path="/data/")
write_tables(tables, path="/data/test-1/")
```
