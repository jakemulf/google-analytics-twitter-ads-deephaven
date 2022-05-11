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

#### Environmental variables

The following environmental variables need to be set to collect data from Twitter

```
TWITTER_CONSUMER_KEY
TWITTER_CONSUMER_SECRET
TWITTER_ACCESS_TOKEN
TWITTER_ACCESS_TOKEN_SECRET
```

### Slack

To collect data from Slack's API, you need to set up a Slack application. To start, you can go to https://api.slack.com/apps/ and click the **Create New App** button. Give your app a name and assign it to your workspace.

On the next page, click the **Permissions** section underneath **Add features and functionality**. Scroll down to the **Scopes** section and click **Add an OAuth Scope**. This should open up a dropdown menu.

Add the following OAuth scopes: `channels:history`, `channels:read`, `groups:history`, `groups:read`, `im:history`, `im:read`, `mpim:history`, `mpim:read`

Now, scroll up to the **OAuth Tokens for Your Workspace** section and click the **Install to Workspace** button. On the next page, press **Allow**.

If this was done correctly, you should see your app in your Slack workspace's **Apps** section. Invite the app to all of the public channels in your workspace: `/invite @<AppName>`.

Back in the **OAuth Tokens for Your Workspace** section, there should be a **Bot User OAuth Token** field. This is the value used in the `SLACK_API_TOKEN` environmental variable for this project.

## Launch

Run this script to launch the app:

```
sh start.sh
```

### In Deephaven

Once launched, go to the Deephaven UI (this defaults to: localhost:10000). You can now run code to collect and visualize data from the APIs.

This example shows how to use the Google Analytics data collector

Notice: For best performance, `date_increment` should be in increments of days (`1D`, `2D`, etc) with a maximum of `7D`, and time zones should be UTC.

```
from deephaven.time import to_datetime, to_period, TimeZone

TimeZone.set_default_timezone(TimeZone.UTC)

dimension_collectors = [
    DimensionCollector(expression="ga:pagePath", metric_column_name="PagePath"),
    DimensionCollector(expression="ga:sourceMedium", metric_column_name="SourceMedium")
]
metrics_collectors = [
    MetricsCollector(expression="ga:pageViews", metric_column_name="PageViews", dh_type=dht.int_, converter=int),
    MetricsCollector(expression="ga:uniquePageViews", metric_column_name="UniqueViews", dh_type=dht.double, converter=float),
    MetricsCollector(expression="ga:bounceRate", metric_column_name="BounceRate", dh_type=dht.double, converter=float)
]
paths = [
    "/",
]
start_date = to_datetime("2022-04-12T00:00:00 UTC")
end_date = to_datetime("2022-04-18T00:00:00 UTC")
page_size = 100000
view_id = "181392643"
date_increment = to_period("1D")

ga_collector = GaCollector(start_date=start_date, end_date=end_date, page_size=page_size, view_id=view_id,
                           date_increment=date_increment, paths=paths, metrics_collectors=metrics_collectors,
                           dimension_collectors=dimension_collectors)

ga_tables = ga_collector.collect_data()

#To display in the UI
for i in range(len(ga_tables)):
    globals()[f"ga_table{i}"] = ga_tables[i]
```

This example collects campaign data from the Twitter Ads API. The JSON body that is written contains hour by hour metrics.
Based on the Twitter Ads API package, the 24 hour time stamps start at 00:00:00 UTC for the given DateTime.

```
from deephaven.time import to_datetime, to_period, TimeZone

TimeZone.set_default_timezone(TimeZone.UTC)

start_date = to_datetime("2022-03-11T00:00:00 UTC")
end_date = to_datetime("2022-03-14T00:00:00 UTC")
date_increment = to_period("1D")

analytics_types = [
    ("CAMPAIGN", "Campaign", get_campaigns, analytics_out_of_range),
    ("LINE_ITEM", "AdGroup", get_line_items, analytics_out_of_range),
    ("FUNDING_INSTRUMENT", "FundingInstrument", get_funding_instruments, analytics_out_of_range),
    ("PROMOTED_TWEET", "PromotedTweet", get_promoted_tweets, promoted_tweet_out_of_range),
    ("MEDIA_CREATIVE", "MediaCreative", get_media_creatives, analytics_out_of_range)
]
twitter_collector = TwitterCollector(twitter_client, analytics_types)

twitter_table = twitter_collector.twitter_analytics_data(start_date, end_date, date_increment)
twitter_metadata = twitter_collector.twitter_analytics_metadata()
```

This example collects data from Slack.

```
from deephaven.time import to_datetime, TimeZone

TimeZone.set_default_timezone(TimeZone.UTC)

start_time = to_datetime("2022-03-11T00:00:00 UTC")
end_time = to_datetime("2022-03-14T00:00:00 UTC")

(slack_channels, slack_messages) = get_all_slack_messages(start_time=start_time, end_time=end_time)
```

### Parquet reading and writing

There are two helper methods in `./app.d/parquet_writer.py` that can be used to read and write Parquet files, `write_tables` and `read_tables`. `write_tables` expects to receive a list of tables.

```
tables = read_tables(path="/data/")
write_tables(tables, path="/data/test-1/")
```

### Scheduler

The `./app.d/scheduler.py` file contains a script that can be run on a scheduled basis. The default configuration pulls from the current time floored to 3 am (EST) to 24 hours before. The `DAYS_OFFSET` environmental variable can be set to an integer to support offsets of multiple days.

The scheduler simply pulls from all of the data sources (Google, Twitter, etc.) and writes them to Parquet files. The files are written to the `/data/<start_date>/` directory.

The environmental variable `SCHEDULED` needs to be set to `true` for the scheduler to run.

## Github Actions configuration

This project has a simple action for PR checks that launches the project and runs the scheduler with a 0 day offset (meaning no data will be collected).

The following repo environmental variables need to be set for the PR check workflow to run:

```
GOOGLE_KEY
TWITTER_CONSUMER_KEY
TWITTER_CONSUMER_SECRET
TWITTER_ACCESS_TOKEN
TWITTER_ACCESS_TOKEN_SECRET
SLACK_API_TOKEN
SCHEDULED
```

`GOOGLE_KEY` should be the file contents of the `./secrets/google-key.json` file, `SCHEDULED` should be set to `true`, and the rest of them should be the same values used to run the project.
