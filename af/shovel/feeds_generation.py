#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

"""ooni-pipeline: * -> RSS/Atom feeds

Generate RSS/Atom feeds
"""

# Compatible with Python2.7 and 3.7 - linted with Black
#
# Inputs: Database
# Outputs:
#
# Example:
# python2.7 ./af/shovel/feeds_generation.py --start '2019-06-01T00:00:00'
#  --end '2019-06-02T00:00:00'
#  --postgres "host=hkgmetadb.infra.ooni.io user=amsapi dbname=metadb"

import argparse
from datetime import timedelta

import psycopg2
import feedgenerator

from oonipl.cli import isomidnight
from oonipl.metrics import setup_metrics

metrics = setup_metrics("pipeline.feeds")


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--start",
        metavar="ISOTIME",
        type=isomidnight,
        help="Airflow execution date",
        required=True,
    )
    ap.add_argument(
        "--end",
        metavar="ISOTIME",
        type=isomidnight,
        help="Airflow execution date + schedule interval",
        required=True,
    )
    ap.add_argument("--postgres", metavar="DSN", help="libpq data source name")

    args = ap.parse_args()
    return args


@metrics.timer("fine_grained")
def generate_fine_grained_feed(c, start_time, end_time):
    q = """
    SELECT test_start_time, probe_cc, probe_asn, input, report_id
    FROM rss_scratchpad
    WHERE test_start_time >= %s
    AND test_start_time < %s
    """
    c.execute(q, [start_time, end_time])
    print("Row count", c.rowcount)

    feed = feedgenerator.Rss201rev2Feed(
        title="OONI events",
        link="https://explorer.ooni.torproject.org/feeds/rss/blocked",
        description="Blocked services and websites detected by OONI",
        language="en",
    )
    for test_start_time, probe_cc, probe_asn, input_url, report_id in c:
        feed.add_item(
            title="{} {}".format(test_start_time, probe_cc),
            link="https://explorer.ooni.torproject.org/measurement/{}".format(
                report_id
            ),
            description="{} {} {}".format(test_start_time, probe_cc, input_url),
        )
    with open("fine_grained.xml", "w") as fp:
        feed.write(fp, "utf-8")


@metrics.timer("aggregated")
def generate_aggregated_feed(c, start_time, end_time):
    """Generate feed aggregated by site and country
    """
    q = """
    SELECT COUNT(input), input, probe_cc
    FROM rss_scratchpad
    WHERE test_start_time >= %s
    AND test_start_time < %s
    GROUP BY input, probe_cc
    HAVING COUNT(input) > %s
    """
    thresold = 3
    c.execute(q, [start_time, end_time, thresold])
    print("Row count", c.rowcount)

    feed = feedgenerator.Rss201rev2Feed(
        title="OONI events",
        link="https://explorer.ooni.torproject.org/feeds/rss/blocked",
        description="Blocked services and websites detected by OONI",
        language="en",
    )
    for count, input_url, probe_cc in c:
        feed.add_item(
            title="{} {}".format(probe_cc, input_url),
            link="https://explorer.ooni.torproject.org",
            description="{} blocked in {}: {} tests".format(input_url, probe_cc, count),
        )
    with open("aggregated.xml", "w") as fp:
        feed.write(fp, "utf-8")


def generate_feeds(postgres_dsn, start_time, end_time):
    pgconn = psycopg2.connect(dsn=postgres_dsn)

    with pgconn, pgconn.cursor() as c:
        c.execute("DROP TABLE rss_scratchpad_measurement")
        q = """
            CREATE UNLOGGED TABLE rss_scratchpad_measurement
            AS
            SELECT *
            FROM measurement
            WHERE measurement_start_time >= %s
            AND measurement_start_time < %s
            AND confirmed=true
        """
        c.execute(q, [start_time, end_time + timedelta(days=7)])

        c.execute("DROP TABLE rss_scratchpad")
        q = """
        CREATE UNLOGGED TABLE rss_scratchpad
        AS
        SELECT anomaly, autoclaved_no, badtail, confirmed, exc, frame_off, frame_size, id, input, intra_off, intra_size,
        measurement_start_time, msm_failure, msm_no,  probe_asn, probe_cc, probe_ip, report_id, residual_no, software_no,
        test_name, test_runtime, test_start_time, textname
        FROM rss_scratchpad_measurement
        JOIN input ON input.input_no = rss_scratchpad_measurement.input_no
        JOIN report ON report.report_no = rss_scratchpad_measurement.report_no
        """
        c.execute(q)

        generate_fine_grained_feed(c, start_time, end_time)
        generate_aggregated_feed(c, start_time, end_time)


def main():
    args = parse_args()
    with metrics.timer("run"):
        generate_feeds(args.postgres, args.start, args.end)


if __name__ == "__main__":
    main()
