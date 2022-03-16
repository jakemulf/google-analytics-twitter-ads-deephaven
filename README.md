# google-analytics-twitter-ads-deephaven

This app allows you to collect data from the Google Analytics and Twitter Ads APIs, and store the data in Deephaven

## Configuration

### Google Analytics

Follow the steps on https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py to get your account setup for usage with the Google Analytics API.

When downloading the key, save the JSON file locally to `./secrets/google-key.json`

Take note of the View ID in your Google Analytics account as well. You will need that to run this code.

You also will want to take note of the URLs in your Google Analytics account. Currently this app runs 1 URL at a time, but
you can easily re-run the methods with different URLs

### Twitter Ads

TODO

## Launch

Run

```
sh start.sh
```

to launch the app

### In Deephaven

Once launched, go to the Deephaven UI (defaults to localhost:10000). You can now run code to collect and visualize data from the APIs.

This example collects data from Google Analytics for the `/blog/2022/01/24/displaying-a-quadrillion-rows/` showing page views from January 1st, 2022 to March 14th, 2022 with a 1 day increment between data.

```
from deephaven.DateTimeUtils import convertDateTime, convertPeriod

start_date = convertDateTime("2022-01-01T00:00:00 NY")
end_date = convertDateTime("2022-03-14T00:00:00 NY")
expression = "ga:pageViews"
path = "/blog/2022/01/24/displaying-a-quadrillion-rows/"
page_size = 100000
view_id = "181392643"
date_increment = convertPeriod("1D")
ga_table = google_analytics_main(start_date, end_date, expression, path, page_size, view_id, date_increment)
```
