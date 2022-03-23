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

## Launch

Run this script to launch the app:

```
sh start.sh
```


### In Deephaven

Once launched, go to the Deephaven UI (this defaults to: localhost:10000). You can now run code to collect and visualize data from the APIs.

This example collects data from Google Analytics for the `/blog/2022/01/24/displaying-a-quadrillion-rows/`, showing page views from January 1st, 2022 to March 14th, 2022 with a 1 day increment between data.

```
from deephaven.DateTimeUtils import convertDateTime, convertPeriod

start_date = convertDateTime("2022-01-01T00:00:00 NY")
end_date = convertDateTime("2022-03-14T00:00:00 NY")
expressions = ["ga:pageViews"]
paths = ["/blog/2022/01/24/displaying-a-quadrillion-rows/"]
page_size = 100000
view_id = "181392643"
date_increment = convertPeriod("1D")
metric_column_names = ["PageViews"]
google_table = google_analytics_main(start_date, end_date, expressions, paths, page_size, view_id, date_increment, metric_column_names)
```

This example collects campaign data from the Twitter Ads API.

```
from deephaven.DateTimeUtils import convertDateTime, convertPeriod

start_date = convertDateTime("2022-03-11T00:00:00 NY")
end_date = convertDateTime("2022-03-14T00:00:00 NY")
date_increment = convertPeriod("1D")

twitter_table = twitter_ads_main(start_date, end_date, date_increment)
```

Once you have your two tables, you can create plots with them. The helper script `plot.py` contains a method `plot_url_and_campaign` that displays metrics from the Google and Twitter tables side by side

```
plot = plot_url_and_campaign("/blog/2022/01/24/displaying-a-quadrillion-rows/", "123abc", ["MetricCount"],
                             ["Clicks", "Impressions"], google_table, twitter_table, "PUBLISHER_NETWORK")
```
