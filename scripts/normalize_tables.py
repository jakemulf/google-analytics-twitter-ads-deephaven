"""
This script normalizes the Twitter Placement and Google query params into a single table
"""
from deephaven import Aggregation as agg, as_list

#Google
def clean_query_params(strn):
    if "query_params" in strn:
        return strn[0:-12]
    else:
        return strn

google_table_summed = google_table.update("URL = clean_query_params(URL)")

agg_list = as_list([
    agg.AggSum("PageViews")
])

google_table_summed = google_table_summed.aggBy(agg_list, "Date", "URL")

#Twitter
agg_list = as_list([
    agg.AggSum("Clicks"),
    agg.AggSum("Engagements"),
    agg.AggSum("Impressions")
])

twitter_table_summed = twitter_table.aggBy(agg_list, "Date", "CampaignName", "CampaignId")
