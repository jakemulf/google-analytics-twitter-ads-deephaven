"""
ga_main.py

Main file to collect data from Google Analytics.

This file does not create any tables or plots in Deephaven. Instead, it defines functions
to be called in the Deephaven UI.

Example useage of code to run in Deephaven:

from deephaven.DateTimeUtils import convertDateTime, convertPeriod

start_date = convertDateTime("2022-01-01T00:00:00 NY")
end_date = convertDateTime("2022-03-14T00:00:00 NY")
expression = "ga:pageViews"
path = "/blog/2022/01/24/displaying-a-quadrillion-rows/"
page_size = 100000
view_id = "181392643"
date_increment = convertPeriod("1D")
ga_table = google_analytics_main(start_date, end_date, expression, path, page_size, view_id, date_increment)
"""
from deephaven import DynamicTableWriter, Types as dht
from deephaven.DateTimeUtils import plus, minus, convertPeriod

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

def google_analytics_main(start_date, end_date, expression, path, page_size, view_id, date_increment):
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
    
    Returns:
        Table: The Deephaven table containing the day-by-day data
    """
    #Create the table writer
    column_names = ["Date", "URL", "MetricCount"]
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
