"""
scheduler.py

A python script that runs various collectors on a timed basis. For best performance, this should be run
on a daily basis at 14:00 UTC. This guarantees that the APIs have collected all the data for the previous day,
and avoids weirdness with daylight savings.
"""
from deephaven.time import now, lower_bin, minus_nanos

import os

ONE_DAY_NANOS = 86400000000000
HOURS_NANOS_8 = 28800000000000

DAYS_OFFSET = int(os.environ.get("DAYS_OFFSET", 1))

end_date = lower_bin(now(), ONE_DAY_NANOS, offset=HOURS_NANOS_8)
start_date = minus_nanos(end_date, ONE_DAY_NANOS * DAYS_OFFSET)

metrics_collectors = [
    MetricsCollector(expression="ga:pageViews", metric_column_name="PageViews", dh_type=dht.int_, converter=int),
    MetricsCollector(expression="ga:uniquePageViews", metric_column_name="UniqueViews", dh_type=dht.double, converter=float),
    MetricsCollector(expression="ga:bounceRate", metric_column_name="BounceRate", dh_type=dht.double, converter=float)
]
paths = [
    "/company/careers/posts/internship-2022/",
    "/core/docs/how-to-guides/parquet-partitioned/"
]
page_size = 100000
view_id = "181392643"
date_increment = to_period("1D")

ga_collector = GaCollector(start_date=start_date, end_date=end_date, page_size=page_size, view_id=view_id,
                           date_increment=date_increment, paths=paths, metrics_collectors=metrics_collectors)

ga_tables = ga_collector.collect_data()
for i in range(len(ga_tables)):
    globals()[f"ga_table{i}"] = ga_tables[i]
twitter_table = twitter_ads_main(start_date, end_date, date_increment)
(slack_channels, slack_messages) = get_all_slack_messages(start_time=start_date, end_time=end_date)

write_tables(tables=ga_tables, path=f"/data/{start_date.toDateString()}/google/")
write_tables(table=twitter_table, path=f"/data/{start_date.toDateString()}/twitter/")
write_tables(table=slack_channels, path=f"/data/{start_date.toDateString()}/slack-channels/")
write_tables(table=slack_messages, path=f"/data/{start_date.toDateString()}/slack-messages/")
