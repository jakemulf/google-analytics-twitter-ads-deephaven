"""
plot.py

Helper script that contains methods to plot the Google Analytics and Twitter Ads data
"""
from deephaven import Plot

def plot_url_and_campaign(google_analytics_url, twitter_campaign,
                            google_analytics_metrics, twitter_campaign_metrics,
                            google_analytics_table, twitter_campaign_table, twitter_placement=None,
                            plot=None):
    """
    Plots the google analytics url information against the twitter campaign information.

    Parameters:
        url (str): The URL in the google analytics table to plot.
        twitter_campaign (str): The ID of the twitter campaign to plot.
        google_analytics_metrics (list<str>): The columns to plot from the google analytics table.
        twitter_campaign_metrics (list<str>): The columns to plot from the twitter campaign table.
        google_analytics_table (Table): The Deephaven table containing the google analytics data.
        twitter_campaign_table (Table): The Deephaven table containing the twitter campaign data.
        twitter_placement (str): The Twitter placement. Should be one of "PUBLISHER_NETWORK" or "ALL_ON_TWITTER".
        plot (Plot): The Deephaven plot if wanting to append to an existing plot.
    Returns:
        Plot: The Deephaven plot
    """
    ga_where = f"URL = `{google_analytics_url}`"
    twitter_where_campaign = f"CampaignId = `{twitter_campaign}`"
    twitter_where_placement = "1 = 1" #This is a janky workaround to have a default "true"
    if not (twitter_placement is None):
        twitter_where_placement = f"Placement = `{twitter_placement}`"
    for metric in google_analytics_metrics:
        if plot is None:
            plot = Plot.plot(f"GoogleMetrics{metric}", google_analytics_table.where(ga_where), "Date", metric)
        else:
            plot = plot.plot(f"GoogleMetrics{metric}", google_analytics_table.where(ga_where), "Date", metric)
    for metric in twitter_campaign_metrics:
        if plot is None:
            plot = Plot.plot(f"TwitterMetrics{metric}", twitter_campaign_table.where(twitter_where_campaign, twitter_where_placement), "Date", metric)
        else:
            plot = plot.plot(f"TwitterMetrics{metric}", twitter_campaign_table.where(twitter_where_campaign, twitter_where_placement), "Date", metric)
    return plot.twinX()\
        .plot("InCampaign", twitter_campaign_table.where(twitter_where_campaign, twitter_where_placement).update("InCampaign = 1"), "Date", "InCampaign").plotStyle("stacked_area")\
        .show()
