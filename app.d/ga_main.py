"""
ga_main.py

Main file to collect data from Google Analytics.

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.
"""
from deephaven import DynamicTableWriter, Types as dht
from deephaven.DateTimeUtils import plus, minus, convertPeriod
from deephaven.TableTools import merge

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import time

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/google-key.json'
ONE_DAY = convertPeriod("1D")

def merge_counts(d1, d2):
    """
    Merges the 2 given dictionaries into 1 by summing their key-value pairs

    Parameters:
        d1 (dict): The first dictionary to merge
        d2 (dict): The second dictionary to merge
    Returns:
        dict: The merged and summed dictionaries
    """
    for key in d2.keys():
        if not key in d1.keys():
            d1[key] = 0
        d1[key] += d2[key]

    return d1

def path_format(strn, normalize_query_strings=True):
    """
    Formats the path. For now, just unifies twitter query parameters.

    Parameters:
        strn (str): The path to format.
        normalize_query_strings (bool): If set to True, sets query strings to a default value.
            Otherwise, doesn't modify query strings.
    Returns:
        str: The formatted path
    """
    result = None
    if "?" in strn:
        [base, query] = strn.split("?")
        if 'twclid' in query:
            query = "twitter"
        elif normalize_query_strings:
            query = "some_query_string"
        result = base + query
    else:
        result = strn
    return result

def generate_counts(d):
    """
    Custom counts generator for the given data

    Parameters:
        d (dict): The dictionary response from the Google Analytics API
    Returns:
        dict: Dictionary containing the sum of the URLs and the values
    """
    counts = {}
    for report in d["reports"]:
        if "data" in report.keys():
            if "rows" in report["data"].keys():
                for rows in report["data"]["rows"]:
                    url = path_format(rows["dimensions"][0])
                    count = int(rows["metrics"][0]["values"][0])
                    if not (url in counts):
                        counts[url] = 0
                    counts[url] += count
    return counts

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

def get_google_analytics_report(analytics, view_id, start_date=None, end_date=None, expression=None, path=None, page_token=None, page_size=None):
  """Queries the Analytics Reporting API V4.

  Modified function taken from https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py

  Parameters:
    analytics: An authorized Analytics Reporting API V4 service object.
    view_id (str): The Google Analytics view ID to collect data from.
    start_date (str): The start date in YYYY-MM-DD format or some other API friendly format.
    end_date (str): The end date in YYYY-MM-DD format or some other API friendly format.
    expression (str): The expression to return for the paths.
    path (str): The path to evaluate the expression on.
    page_token (str): The page token if making a subsequent request.
    page_size (int): The number of rows to grab in the API request.
  Returns:
    dict: The Analytics Reporting API V4 response.
  """
  if start_date is None:
      start_date = '7daysAgo'
  if end_date is None:
      end_date = 'today'
  if expression is None:
      expression = 'ga:users'
  if path is None:
      path = '/'
  if page_size is None:
      page_size = 100000
  body = {
    'reportRequests': [
      {
        'viewId': view_id,
        'pageSize': page_size,
        'dimensions': [
          {
            'name': 'ga:pagePath',
          }
        ],
        'dateRanges': [
          {
            'startDate': start_date,
            'endDate': end_date
          }
        ],
        'metrics': [
          {
            'expression': expression
          }
        ],
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

  return analytics.reports().batchGet(body=body).execute()

def google_analytics_main(start_date, end_date, expression, path, page_size, view_id, date_increment, metric_column_name=None):
    """
    Main method for the google analytics collector. This pulls day-by-day information from
    the google analytics API for the given path, and returns a Deephaven table of this information

    Parameters:
        start_date (DateTime): The start date as a Deephaven DateTime object.
        end_date (DateTime): The end date as a Deephaven DateTime object.
        expression (str): The expression to return for the paths.
        path (str): The path to evaluate the expression on.
        page_size (int): The number of rows to grab in the API request.
        view_id (str): The Google Analytics view ID to collect data from.
        date_increment (Period): The amount of time between data collection periods.
        metric_column_name (str): The column name of the metric column in the resulting Deephaven Table.
    Returns:
        Table: The Deephaven table containing the day-by-day data
    """
    #Create the table writer
    if metric_column_name is None:
        metric_column_name = "MetricCount"
    column_names = ["Date", "URL", metric_column_name]
    column_types = [dht.datetime, dht.string, dht.int_]
    table_writer = DynamicTableWriter(column_names, column_types)
    
    #Create analytics class
    analytics = initialize_analyticsreporting()

    #Loop through the date range
    current_date = start_date
    while current_date < end_date:
        next_date = plus(current_date, date_increment)
        next_date = minus(next_date, ONE_DAY) #The analytics API is inclusive, so we need to subtract an extra day
        #Convert deephaven datetimes to yyyy-mm-dd format
        current_date_string = current_date.toDateString()
        next_date_string = next_date.toDateString()

        #If pagination is needed, create variable to store pagination results
        next_page_token = None

        #Dictionary to contain the results
        total_counts = {}
        while True:
            response = get_google_analytics_report(analytics, view_id, start_date=current_date_string,
                                end_date=next_date_string, expression=expression,
                                path=path, page_size=page_size, page_token=next_page_token)
            total_counts = merge_counts(total_counts, generate_counts(response))
            next_page_token = response["reports"][0].get("nextPageToken")
            #If no pagination, break
            if next_page_token is None:
                break
            time.sleep(1) #Sleep to avoid rate limits for subsequent calls

        #Write the results to the Deephaven table
        for url in total_counts.keys():
            table_writer.logRowPermissive(current_date, url, total_counts[url])

        current_date = plus(current_date, date_increment)

    return table_writer.getTable()

def google_analytics_main_wrapper(start_date, end_date, expressions, paths, page_size, view_id, date_increment, metric_column_names):
    """
    A wrapper method for google_analytics_main in case someone wants to use multiple expressions, paths, and column names.
    For every path, every expression is evaluated and stored in a Deephaven table, and then the tables are joined together.

    len(expressions) and len(metric_column_names) must match.

    Parameters:
        start_date (DateTime): The start date as a Deephaven DateTime object.
        end_date (DateTime): The end date as a Deephaven DateTime object.
        expression (list<str>): A list of expressions to return for the paths.
        paths (list<str>): A list of paths to evaluate the expression on.
        page_size (int): The number of rows to grab in the API request.
        view_id (str): The Google Analytics view ID to collect data from.
        date_increment (Period): The amount of time between data collection periods.
        metric_column_names (list<str>): A list of column names of the metric column in the resulting Deephaven Table.
    Returns:
        Table: A single Deephaven table containing all of the data joined together
    """
    if len(expressions) != len(metric_column_names):
        raise TypeError("len(expressions) and len(metric_column_names) must match")

    table = None
    for path in paths:
        path_table = None
        for i in range(len(expressions)):
            expression = expressions[i]
            metric_column_name = metric_column_names[i]
            result = google_analytics_main(start_date, end_date, expression, path, page_size, view_id, date_increment, metric_column_name)
            if path_table is None:
                path_table = result
            else:
                path_table = path_table.join(result, "Date, URL")
        if table is None:
            table = path_table
        else:
            table = merge(table, path_table)
    return table
