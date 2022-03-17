# google-analytics-twitter-ads-deephaven

This app allows you to collect data from the Google Analytics and Twitter Ads APIs, and store the data in Deephaven.

## Configuration

### Google Analytics

Follow the steps on https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py to get your account setup for usage with the Google Analytics API.

When downloading the key, save the JSON file locally to `./secrets/google-key.json`.

Take note of the View ID in your Google Analytics account - you will need that to run this code.

You also will want to take note of the URLs in your Google Analytics account. Currently, this app runs 1 URL at a time, but
you can easily re-run the methods with different URLs.

### Twitter Ads

TODO

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
expression = "ga:pageViews"
path = "/blog/2022/01/24/displaying-a-quadrillion-rows/"
page_size = 100000
view_id = "181392643"
date_increment = convertPeriod("1D")
google_table = google_analytics_main(start_date, end_date, expression, path, page_size, view_id, date_increment)
```

You can also use the `google_analytics_main_wrapper` method if you want to look at multiple paths and expressions.
For every path, each expression is evaluated using the `google_analytics_main` method above. In order to avoid column name
conflicts, the `metric_column_names` parameter is used and must map 1 to 1 to the `expressions` parameter. The created
tables are joined and merged together to create a single view of the data.

```
from deephaven.DateTimeUtils import convertDateTime, convertPeriod

start_date = convertDateTime("2022-03-11T00:00:00 NY")
end_date = convertDateTime("2022-03-14T00:00:00 NY")
page_size = 100000
view_id = "181392643"
date_increment = convertPeriod("1D")

expressions = ["ga:users", "ga:pageViews"]
paths = ["/core/", "/blog/"]
metric_column_names = ["Users", "PageViews"]

google_table = google_analytics_main_wrapper(start_date, end_date, expressions, paths, page_size, view_id, date_increment, metric_column_names)
```

This example collects campaign data from the Twitter Ads API.

```
from deephaven.DateTimeUtils import convertDateTime, convertPeriod

start_date = convertDateTime("2022-03-11T00:00:00 NY")
end_date = convertDateTime("2022-03-14T00:00:00 NY")
date_increment = convertPeriod("1D")

twitter_table = twitter_ads_main(start_date, end_date, date_increment)
```

Once you have your two tables, you can create plots with them. This example plots Twitter clicks with Google Analytics users for the `/blog/2022/02/23/csv-reader/` URL.

```
from deephaven import Plot

ga_twitter_plot = Plot.plot("Users", google_table.where("URL = `/blog/2022/02/23/csv-reader/`"), "Date", "Users")\
    .plot("Clicks", twitter_table.where("Campaign = `batch campaigns`"), "Date", "Clicks").show()
```
