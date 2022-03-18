"""
plot.py

Helper script that contains methods to plot the Google Analytics and Twitter Ads data
"""
from deephaven import Plot

def plot_url_and_campaign(google_analytics_url, twitter_campaign,
                            google_analytics_metrics, twitter_campaign_metrics,
                            google_analytics_table, twitter_campaign_table):
    """
    Plots the google analytics url information against the twitter campaign information.

    Parameters:
        url (str): The URL in the google analytics table to plot.
        twitter_campaign (str): The name of the twitter campaign to plot.
        google_analytics_metrics (list<str>): The columns to plot from the google analytics table.
        twitter_campaign_metrics (list<str>): The columns to plot from the twitter campaign table.
        google_analytics_table (Table): The Deephaven table containing the google analytics data.
        twitter_campaign_table (Table): The Deephaven table containing the twitter campaign data.
    Returns:
        Plot: The Deephaven plot
    """
    ga_where = f"URL = `{google_analytics_url}`"
    twitter_where = f"Campaign = `{twitter_campaign}`"
    plot = None
    for metric in google_analytics_metrics:
        if plot is None:
            plot = Plot.plot(f"GoogleMetrics{metric}", google_analytics_table.where(ga_where), "Date", metric)
        else:
            plot = plot.plot(f"GoogleMetrics{metric}", google_analytics_table.where(ga_where), "Date", metric)
    for metric in twitter_campaign_metrics:
        if plot is None:
            plot = Plot.plot(f"TwitterMetrics{metric}", twitter_campaign_table.where(twitter_where), "Date", metric)
        else:
            plot = plot.plot(f"TwitterMetrics{metric}", twitter_campaign_table.where(twitter_where), "Date", metric)
    return plot.twinX()\
        .plot("InCampaign", twitter_campaign_table.update("InCampaign = 1"), "Date", "InCampaign").plotStyle("stacked_area")\
        .show()
