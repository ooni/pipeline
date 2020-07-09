#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""OONI URL prioritization service

Configured with /etc/ooni/prio.conf

Runs as a system daemon

Inputs:
    Database tables:
        `citizenlab` db table
        country codes are in the `cc` column, lowercase, with the exception of ZZ

Outputs:
    API
    Statsd metrics

Special country code values:
    ZZ: unknown
    XX: all
"""

from collections import namedtuple
from configparser import ConfigParser
from typing import List
import logging
import random
import time

from systemd.journal import JournalHandler  # debdeps: python3-systemd
import statsd  # debdeps: python3-statsd

from bottle import route
import bottle

from psycopg2.extras import RealDictCursor
import psycopg2

conf = None
test_items = {}
next_update_time = None

log = logging.getLogger("prio")
log.addHandler(JournalHandler(SYSLOG_IDENTIFIER="prio"))
log.setLevel(logging.DEBUG)


metrics = statsd.StatsClient("localhost", 8125, prefix="prio")


def connect_db(c):
    log.info("Connecting to %s %d", c.dbhost, c.dbport)
    conn = psycopg2.connect(
        dbname=c.dbname,
        user=c.dbuser,
        host=c.dbhost,
        port=c.dbport,
        password=c.dbpassword,
    )
    return conn


@metrics.timer("update_url_prioritization")
def update_url_prioritization():
    """Fetch URL prioritization from database and update lookup dict
    """
    log.info("Started update_url_prioritization")
    conn = connect_db(conf)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    log.info("Regenerating URL prioritization data")
    sql = """SELECT priority, domain, url, cc, category_code FROM citizenlab"""
    cur.execute(sql)
    entries = list(cur.fetchall())
    conn.rollback()
    conn.close()

    # Create dict: cc -> category_code -> [entry, ... ]
    entries_by_country = {}
    for e in entries:
        country = e["cc"].upper()
        if country not in entries_by_country:
            entries_by_country[country] = {}
        ccode = e["category_code"]
        entries_by_country[country].setdefault(ccode, []).append(e)

    # merge ZZ into each country: in this way, when we look for entries for a
    # given cc we'll find both global and country-specific URLs
    zz = entries_by_country["ZZ"]
    for ccode, country_dict in entries_by_country.items():
        for category_code, test_items in zz.items():
            country_dict.setdefault(category_code, []).extend(test_items)

    log.info(
        "Update done: %d countries, %d global URLs",
        len(entries_by_country) - 1,
        len(entries_by_country["ZZ"]),
    )
    return entries_by_country


def algo_chao(s: List, k: int) -> List:
    """Chao weighted random sampling
    """
    n = len(s)
    assert len(s) >= k
    wsum = 0
    r = s[:k]
    assert len(r) == k
    for i in range(0, n):
        wsum = wsum + s[i]["priority"]
        if i < k:
            continue
        p = s[i]["priority"] / wsum  # probability for this item
        j = random.random()
        if j <= p:
            pos = random.randint(0, k - 1)
            r[pos] = s[i]

    return r


@metrics.timer("generate_test_list")
def generate_test_list(country_code: str, category_codes: str, limit: int):
    global test_items, next_update_time

    if next_update_time < time.time():
        next_update_time = time.time() + float(conf.refresh_interval_s)
        try:
            test_items = update_url_prioritization()
        except Exception as e:
            log.error(e, exc_info=1)

    candidates_d = test_items[country_code]  # category_code -> [test_item, ... ]

    if category_codes:
        category_codes = [c.strip().upper() for c in category_codes.split(",")]
        for cat in category_codes:
            metrics.incr(f"category_code_requested[category={cat}]")
    else:
        category_codes = candidates_d.keys()

    candidates = []
    for ccode in category_codes:
        s = candidates_d.get(ccode, [])
        candidates.extend(s)

    if limit == -1:
        limit = 100
    limit = min(limit, len(candidates))
    selected = algo_chao(candidates, limit)

    out = []
    for entry in selected:
        out.append(
            {
                "category_code": entry["category_code"],
                "url": entry["url"],
                "country_code": "XX" if entry["cc"] == "ZZ" else entry["cc"],
            }
        )
    metrics.incr("total_urls_served", count=len(out))
    metrics.incr(f"country_code_requested[cc={country_code}]")
    log.info("Serving %d URLs", len(out))
    return out


@route("/api/v1/test-list/urls")
def list_urls():
    """
    https://orchestrate.ooni.io/api/v1/test-list/urls?country_code=IT
    """
    try:
        q = bottle.request.query
        # look for country_code or probe_cc or default to "ZZ" which
        # represents the global list
        country_code = q.country_code.upper() or q.probe_cc.upper() or "ZZ"
        category_codes = q.category_codes
        limit = int(q.limit or -1)
        test_items = generate_test_list(country_code, category_codes, limit)
        out = {
            "metadata": {
                "count": len(test_items),
                "current_page": -1,
                "limit": -1,
                "next_url": "",
                "pages": 1,
            },
            "results": test_items,
        }
        return out
    except Exception as e:
        log.error(e, exc_info=1)
        return {}


def main():
    global conf, test_items, next_update_time
    conffile = "/etc/ooni/prio.conf"
    cp = ConfigParser()
    with open(conffile) as f:
        cp.read_file(f)
    d = cp.defaults()  # parsed values from DEFAULT section
    conf = namedtuple("Conf", d.keys())(*d.values())
    log.info("Refresh interval: %s", conf.refresh_interval_s)

    test_items = update_url_prioritization()
    next_update_time = time.time() + float(conf.refresh_interval_s)

    bottle.run(host="localhost", port=conf.apiport)


if __name__ == "__main__":
    main()
