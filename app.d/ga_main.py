"""
ga_main.py

Main file to collect data from Google Analytics.

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.
"""
from deephaven import DynamicTableWriter, merge
import deephaven.dtypes as dht
from deephaven.time import plus_period, minus_period, to_period

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import time
import json

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/google-key.json'
ONE_DAY = to_period("1D")


class GaCollector:
    """
    A class to represent the overall collection of metrics from Google Analyitcs

    Attributes:
        start_date (DateTime): The start date as a Deephaven DateTime object
        end_date (DateTime): The end date as a Deephaven DateTime object
        date_increment (Period): The increment of each date
        page_size (int): The number of entries to collect from each API request to Google Analytics
        view_id (str): The view ID for the Google Analytics account
        paths (list<str>): A list of paths to evaluate in Google Analytics
        metrics_collectors (list<MetricsCollector>): A list of MetricsCollector instances used for expression evaluation
        analytics: An authorized Analytics Reporting API V4 service object.
        ignore_query_strings (bool): If set to True, query strings are stripped away and ignored.
            Otherwise, query strings are normalized to a constant value.
        dimension_collectors (list<DimensionCollector>): A list of DimensionCollector instances used for expression evaluation
    """
    def __init__(self, start_date=None, end_date=None, date_increment=None, page_size=None,
                 view_id=None, paths=None, metrics_collectors=None, ignore_query_strings=True,
                 dimension_collectors=None):
        self.start_date = start_date
        self.end_date = end_date
        self.date_increment = date_increment
        self.page_size = page_size
        self.view_id = view_id
        self.paths = paths
        self.metrics_collectors = metrics_collectors
        self.ignore_query_strings = ignore_query_strings
        self.dimension_collectors = dimension_collectors

        #Create analytics class
        self.analytics = initialize_analyticsreporting()
    
    def _get_google_analytics_report(self, path, start_date, end_date, page_token=None):
      """Queries the Analytics Reporting API V4.

      Modified function taken from https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py

      Parameters:
        path (str): The path to evaluate the expression on.
        ga_collector (GaCollector): The GaCollector instance that defines what to pull
        page_token (str): The page token if making a subsequent request.
      Returns:
        dict: The Analytics Reporting API V4 response.
      """
      metrics = []
      for metrics_collector in self.metrics_collectors:
          metrics.append({'expression': metrics_collector.expression})
      dimensions = []
      for dimension_collector in self.dimension_collectors:
          dimensions.append({'name': dimension_collector.expression})
      body = {
        'reportRequests': [
          {
            'viewId': self.view_id,
            'pageSize': self.page_size,
            'dimensions': dimensions,
            'dateRanges': [
              {
                'startDate': start_date,
                'endDate': end_date
              }
            ],
            'metrics': metrics,
            'dimensionFilterClauses': [
              {
                'filters': [
                  {
                    'dimensionName': 'ga:pagePath',
                    'expressions': [path]
                  }
                ]
              }
            ]
          }
        ]
      }

      if page_token is not None:
        body['reportRequests'][0]['pageToken'] = page_token

      return self.analytics.reports().batchGet(body=body).execute()

    def _google_analytics_table_writer(self, path):
        """
        Table writer for the google analytics collector. This pulls day-by-day information from
        the google analytics API for the given path, and returns a Deephaven table of this information

        Parameters:
            path (str): The path to collect data on
        Returns:
            tuple(Table, Table): A Deephaven table containing the day-by-day data, and a Deephaven table of the JSON responses
        """
        #Create the table writer
        metrics_collector_columns = {}
        for metrics_collector in self.metrics_collectors:
            metrics_collector_columns[metrics_collector.metric_column_name] = metrics_collector.dh_type
        dtw_columns = {
            "Date": dht.DateTime,
            "URL": dht.string,
            "Source": dht.string,
        }
        dtw_columns.update(metrics_collector_columns)
        table_writer = DynamicTableWriter(dtw_columns)

        dtw_columns_json = {
            "Date": dht.DateTime,
            "JsonString": dht.string
        }
        table_writer_json = DynamicTableWriter(dtw_columns_json)

        #Loop through the date range
        current_date = self.start_date
        while current_date < self.end_date:
            print("Google")
            print(current_date)
            next_date = plus_period(current_date, self.date_increment)
            next_date = minus_period(next_date, ONE_DAY) #The analytics API is inclusive, so we need to subtract an extra day
            #Convert deephaven datetimes to yyyy-mm-dd format
            current_date_string = current_date.toDateString()
            next_date_string = next_date.toDateString()

            #If pagination is needed, create variable to store pagination results
            next_page_token = None

            while True:
                response = self._get_google_analytics_report(path, current_date_string,
                                                             next_date_string, page_token=next_page_token)
                time.sleep(1) #Sleep to avoid rate limits for subsequent calls
                parsed_counts = parse_ga_response(response, self.metrics_collectors, self.ignore_query_strings)
                next_page_token = response["reports"][0].get("nextPageToken")

                for row_to_write in parsed_counts:
                    table_writer.write_row([current_date] + row_to_write)
                table_writer_json.write_row(current_date, json.dumps(response))

                #If no pagination, break
                if next_page_token is None:
                    break

            current_date = plus_period(current_date, self.date_increment)

        return (table_writer.table, table_writer_json.table)

    def collect_data(self):
        """
        Main method for the google analytics collector. For every path, every expression is evaluated and stored in a Deephaven table,
        and then the tables are joined together.

        Returns:
            list<Table>: A list of Deephaven tables containing all of the metrics
        """
        tables = []
        for path in self.paths:
            (result_table, json_table) = self._google_analytics_table_writer(path)
            tables.append(result_table)
            tables.append(json_table)
        return tables

class MetricsCollector:
    """
    A class to represent a definition of collecting specific metrics from Google Analytics

    Attributes:
        expression (str): The Google Analytics expression to collect
        metric_column_name (str): The column name in the Deephaven table for the collected metric
        dh_type (dht.type): The Deephaven type of the column
        converter (method): A method to convert a String to the Deephaven type
    """
    def __init__(self, expression=None, metric_column_name=None, dh_type=None, converter=None):
        self.expression = expression
        self.metric_column_name = metric_column_name
        self.dh_type = dh_type
        self.converter = converter

class DimensionCollector:
    """
    A class to represent a definition of collecting specific dimensions from Google Analytics

    Attributes:
        expression (str): The Google Analytics expression to collect
        metric_column_name (str): The column name in the Deephaven table for the collected metric
    """
    def __init__(self, expression=None, metric_column_name=None):
        self.expression = expression
        self.metric_column_name = metric_column_name

def path_format(strn, ignore_query_strings):
    """
    Formats the path. For now, just unifies query parameters.

    Parameters:
        strn (str): The path to format.
        ignore_query_strings (bool): If set to True, query strings are stripped away and ignored.
            Otherwise, query strings are left as is.
    Returns:
        str: The formatted path
    """
    result = None
    if "?" in strn:
        [base, query] = strn.split("?")
        if ignore_query_strings:
            result = base
        else:
            result = base + query
    else:
        result = strn
    return result

def parse_ga_response(d, metrics_collectors, ignore_query_strings):
    """
    Custom parser for the GA API response

    Parameters:
        d (dict): The dictionary response from the Google Analytics API
        metrics_collectors (list<MetricsCollector>): A list of metrics collectors that contain the converter methods
        ignore_query_strings (bool): If set to True, query strings are stripped away and ignored.
            Otherwise, query strings are normalized to a constant value.
    Returns:
        list<list>: A list of lists showing each row to write
    """
    values = []
    for report in d["reports"]:
        if "data" in report.keys():
            if "rows" in report["data"].keys():
                for rows in report["data"]["rows"]:
                    metrics_values = []
                    url = path_format(rows["dimensions"][0], ignore_query_strings)
                    source = rows["dimensions"][1]
                    metrics_values.append(url)
                    metrics_values.append(source)
                    for i in range(len(rows["metrics"][0]["values"])):
                        raw_value = rows["metrics"][0]["values"][i]
                        converter = metrics_collectors[i].converter
                        metrics_values.append(converter(raw_value))
                    values.append(metrics_values)
    return values

def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
        An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics
