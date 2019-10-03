#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

"""
OONI Event detector

Run sequence:

Fetch already-processed mean values from internal data directory.
This is done to speed up restarts as processing historical data from the database
would take a very long time.

Fetch historical msmt from the fastpath and measurement/report/input tables

Fetch realtime msmt by subscribing to the notifications channel `fastpath`

Analise msmt score with moving average to detect blocking/unblocking

Save outputs to local directories:
    - RSS feed                                      /var/lib/detector/rss/
    - JSON files with block/unblock events          /var/lib/detector/events/
    - JSON files with current blocking status       /var/lib/detector/status/
    - Internal data                                 /var/lib/detector/_internal/

Outputs are "upserted" where possible. New runs overwrite/update old data.

Runs as a service "detector" in a systemd unit and sandbox

Deployed in the same .deb as fastpath

See README.adoc
"""

# Compatible with Python3.6 and 3.7 - linted with Black
# debdeps: python3-setuptools

from argparse import ArgumentParser
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
import atexit
import hashlib
import logging
import os
import pickle
import select
import sys

from systemd.journal import JournalHandler  # debdeps: python3-systemd
import psycopg2  # debdeps: python3-psycopg2
import psycopg2.extensions
import psycopg2.extras
import ujson  # debdeps: python3-ujson
import feedgenerator  # debdeps: python3-feedgenerator

from fastpath.metrics import setup_metrics
import fastpath.scoring as scoring

log = logging.getLogger("detector")
metrics = setup_metrics(name="detector")

DB_HOST = "hkgmetadb.infra.ooni.io"
DB_USER = "shovel"
DB_NAME = "metadb"
DB_PASSWORD = "yEqgNr2eXvgG255iEBxVeP"  # This is already made public

RO_DB_HOST = "amsmetadb.ooni.nu"
RO_DB_USER = "amsapi"
RO_DB_PASSWORD = "b2HUU6gKM19SvXzXJCzpUV"  # This is already made public

DEFAULT_STARTTIME = datetime(2016, 1, 1)

BASEURL = "http://fastpath.ooni.nu:8080"
WEBAPP_URL = BASEURL + "/webapp"


# Speed up psycopg2's JSON load
psycopg2.extras.register_default_jsonb(loads=ujson.loads, globally=True)
psycopg2.extras.register_default_json(loads=ujson.loads, globally=True)


def fetch_past_data_onequery(conn, start_date):
    """Fetch past data in large chunks
    """
    chunk_size = 20000
    q = """
    SELECT
        coalesce(false) as anomaly,
        coalesce(false) as confirmed,
        input,
        measurement_start_time,
        probe_cc,
        scores::text,
        coalesce('') as report_id,
        test_name,
        tid
    FROM fastpath
    WHERE measurement_start_time >= %(start_date)s

    UNION

    SELECT
        anomaly,
        confirmed,
        input,
        measurement_start_time,
        probe_cc,
        coalesce('') as scores,
        report_id,
        test_name,
        coalesce('') as tid

    FROM measurement
    JOIN report ON report.report_no = measurement.report_no
    JOIN input ON input.input_no = measurement.input_no
    WHERE measurement_start_time >= %(start_date)s

    ORDER BY measurement_start_time
    """

    assert start_date
    p = dict(start_date=str(start_date))

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(q, p)
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            log.info("Fetched msmt chunk of size %d", len(rows))
            for r in rows:
                d = dict(r)
                if d["scores"]:
                    d["scores"] = ujson.loads(d["scores"])

                yield d


def fetch_past_data(conn, start_date):
    """Fetch past data in large chunks
    """
    q = """
    SELECT
        coalesce(false) as anomaly,
        coalesce(false) as confirmed,
        input,
        measurement_start_time,
        probe_cc,
        scores::text,
        coalesce('') as report_id,
        test_name,
        tid
    FROM fastpath
    WHERE measurement_start_time >= %(start_date)s
    AND measurement_start_time < %(end_date)s

    UNION

    SELECT
        anomaly,
        confirmed,
        input,
        measurement_start_time,
        probe_cc,
        coalesce('') as scores,
        report_id,
        test_name,
        coalesce('') as tid

    FROM measurement
    JOIN report ON report.report_no = measurement.report_no
    JOIN input ON input.input_no = measurement.input_no
    WHERE measurement_start_time >= %(start_date)s
    AND measurement_start_time < %(end_date)s

    """
    assert start_date

    end_date = start_date + timedelta(weeks=1)

    chunk_size = 20000
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        while True:
            # Iterate across time blocks
            log.info("Query from %s to %s", start_date, end_date)
            p = dict(start_date=str(start_date), end_date=str(end_date))
            cur.execute(q, p)
            while True:
                # Iterate across chunks of rows
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                log.info("Fetched msmt chunk of size %d", len(rows))
                for r in rows:
                    d = dict(r)
                    if d["scores"]:
                        d["scores"] = ujson.loads(d["scores"])

                    yield d

            start_date += timedelta(weeks=1)
            end_date += timedelta(weeks=1)
            if end_date > datetime.utcnow():
                break


def fetch_past_data_selective(conn, start_date, cc, test_name, inp):
    """Fetch past data in large chunks
    """
    chunk_size = 200_000
    q = """
    SELECT
        coalesce(false) as anomaly,
        coalesce(false) as confirmed,
        input,
        measurement_start_time,
        probe_cc,
        scores::text,
        test_name,
        tid
    FROM fastpath
    WHERE measurement_start_time >= %(start_date)s
    AND probe_cc = %(cc)s
    AND test_name = %(test_name)s
    AND input = %(inp)s

    UNION

    SELECT
        anomaly,
        confirmed,
        input,
        measurement_start_time,
        probe_cc,
        coalesce('') as scores,
        test_name,
        coalesce('') as tid

    FROM measurement
    JOIN report ON report.report_no = measurement.report_no
    JOIN input ON input.input_no = measurement.input_no
    WHERE measurement_start_time >= %(start_date)s
    AND probe_cc = %(cc)s
    AND test_name = %(test_name)s
    AND input = %(inp)s

    ORDER BY measurement_start_time
    """
    p = dict(cc=cc, inp=inp, start_date=start_date, test_name=test_name)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(q, p)
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            log.info("Fetched msmt chunk of size %d", len(rows))
            for r in rows:
                d = dict(r)
                if d["scores"]:
                    d["scores"] = ujson.loads(d["scores"])

                yield d


def backfill_scores(d):
    """Generate scores dict for measurements from the traditional pipeline
    """
    if d.get("scores", None):
        return
    b = (
        scoring.anomaly
        if d["anomaly"]
        else 0 + scoring.confirmed
        if d["confirmed"]
        else 0
    )
    d["scores"] = dict(blocking_general=b)


def detect_blocking_changes_1s_g(g, cc, test_name, inp, start_date):
    """Used by webapp
    :returns: (msmts, changes)
    """
    means = {}
    msmts = []
    changes = []

    for msm in g:
        backfill_scores(msm)
        k = (msm["probe_cc"], msm["test_name"], msm["input"])
        assert isinstance(msm["scores"], dict), repr(msm["scores"])
        change = detect_blocking_changes(means, msm, warmup=True)
        date, mean, bblocked = means[k]
        val = msm["scores"]["blocking_general"]
        if change:
            changes.append(change)

        msmts.append((date, val, mean))

    log.debug("%d msmts processed", len(msmts))
    assert isinstance(msmts[0][0], datetime)
    return (msmts, changes)


def detect_blocking_changes_one_stream(conn, cc, test_name, inp, start_date):
    """Used by webapp
    :returns: (msmts, changes)
    """
    # TODO: move into webapp?
    g = fetch_past_data_selective(conn, start_date, cc, test_name, inp)
    return detect_blocking_changes_1s_g(g, cc, test_name, inp, start_date)


Change = namedtuple(
    "Change",
    [
        "probe_cc",
        "test_name",
        "input",
        "blocked",
        "mean",
        "measurement_start_time",
        "tid",
        "report_id",
    ],
)

MeanStatus = namedtuple("MeanStatus", ["measurement_start_time", "val", "blocked"])


def detect_blocking_changes(means: dict, msm: dict, warmup=False):
    """Detect changes in blocking patterns
    :returns: Change or None
    """
    # TODO: move out params
    upper_limit = 0.10
    lower_limit = 0.02
    # P: averaging value
    # p=1: no averaging
    # p=0.000001: very strong averaging
    p = 0.005

    inp = msm["input"]
    if inp is None:
        return

    if not isinstance(inp, str):
        # Some inputs are lists. TODO: handle them?
        log.debug("odd input")
        return

    k = (msm["probe_cc"], msm["test_name"], inp)
    tid = msm.get("tid", None)
    report_id = msm.get("report_id", None) or None

    assert isinstance(msm["scores"], dict), repr(msm["scores"])
    blocking_general = msm["scores"]["blocking_general"]
    measurement_start_time = msm["measurement_start_time"]
    assert isinstance(measurement_start_time, datetime), repr(measurement_start_time)

    if k not in means:
        # cc/test_name/input tuple never seen before
        blocked = blocking_general > upper_limit
        means[k] = MeanStatus(measurement_start_time, blocking_general, blocked)
        if blocked:
            if not warmup:
                log.info("%r new and blocked", k)
                metrics.incr("detected_blocked")

            return Change(
                measurement_start_time=measurement_start_time,
                blocked=blocked,
                mean=blocking_general,
                probe_cc=msm["probe_cc"],
                input=msm["input"],
                test_name=msm["test_name"],
                tid=tid,
                report_id=report_id,
            )

        else:
            return None

    old = means[k]
    assert isinstance(old, MeanStatus)
    # tdelta = measurement_start_time - old.time
    # TODO: average weighting by time delta; add timestamp to status and means
    # TODO: record msm leading to status change
    new_val = (1 - p) * old.val + p * blocking_general
    means[k] = MeanStatus(measurement_start_time, new_val, old.blocked)

    if old.blocked and new_val < lower_limit:
        # blocking cleared
        means[k] = MeanStatus(measurement_start_time, new_val, False)
        if not warmup:
            log.info("%r cleared %.2f", k, new_val)
            metrics.incr("detected_cleared")

        return Change(
            measurement_start_time=measurement_start_time,
            blocked=False,
            mean=new_val,
            probe_cc=msm["probe_cc"],
            input=msm["input"],
            test_name=msm["test_name"],
            tid=tid,
            report_id=report_id,
        )

    if not old.blocked and new_val > upper_limit:
        means[k] = MeanStatus(measurement_start_time, new_val, True)
        if not warmup:
            log.info("%r blocked %.2f", k, new_val)
            metrics.incr("detected_blocked")

        return Change(
            measurement_start_time=measurement_start_time,
            blocked=True,
            mean=new_val,
            probe_cc=msm["probe_cc"],
            input=msm["input"],
            test_name=msm["test_name"],
            tid=tid,
            report_id=report_id,
        )


def parse_date(d):
    return datetime.strptime(d, "%Y-%m-%d").date()


def setup():
    os.environ["TZ"] = "UTC"
    global conf
    ap = ArgumentParser(__doc__)
    ap.add_argument("--devel", action="store_true", help="Devel mode")
    ap.add_argument("--webapp", action="store_true", help="Run webapp")
    ap.add_argument("--start-date", type=lambda d: parse_date(d))
    ap.add_argument("--db-host", default=DB_HOST, help="Database hostname")
    conf = ap.parse_args()
    if conf.devel:
        root = Path(os.getcwd())
        format = "%(relativeCreated)d %(process)d %(levelname)s %(name)s %(message)s"
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=format)
    else:
        root = Path("/")
        log.addHandler(JournalHandler(SYSLOG_IDENTIFIER="detector"))
        log.setLevel(logging.DEBUG)

    conf.conffile = root / "etc/detector.conf"
    log.info("Using conf file %r", conf.conffile)
    conf.vardir = root / "var/lib/detector"
    conf.outdir = conf.vardir / "output"
    conf.rssdir = conf.outdir / "rss"
    conf.eventdir = conf.outdir / "events"
    conf.statusdir = conf.outdir / "status"
    conf.pickledir = conf.outdir / "_internal"
    for p in (
        conf.vardir,
        conf.outdir,
        conf.rssdir,
        conf.eventdir,
        conf.statusdir,
        conf.pickledir,
    ):
        p.mkdir(parents=True, exist_ok=True)

    # TODO: implement a way to force reprocessing old data


@metrics.timer("handle_new_measurement")
def handle_new_msg(msg, means, rw_conn):
    """
    """
    msm = ujson.loads(msg.payload)
    assert isinstance(msm["scores"], dict), type(msm["scores"])
    # '2019-02-05 11:03:22'
    msm["measurement_start_time"] = datetime.strptime(
        msm["measurement_start_time"], "%Y-%m-%d %H:%M:%S"
    )
    change = detect_blocking_changes(means, msm, warmup=False)
    if change is not None:
        upsert_change(change)


def connect_to_db(db_host, db_user, db_name, db_password):
    dsn = f"host={db_host} user={db_user} dbname={db_name} password={db_password}"
    log.info("Connecting to database: %r", dsn)
    conn = psycopg2.connect(dsn)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def process_historical_data(ro_conn, rw_conn, start_date, means):
    """Process past data
    """
    assert start_date
    log.info("Running process_historical_data from %s", start_date)
    t = metrics.timer("process_historical_data").start()
    cnt = 0
    for past_msm in fetch_past_data(ro_conn, start_date):
        backfill_scores(past_msm)
        change = detect_blocking_changes(means, past_msm, warmup=True)
        cnt += 1
        if change is not None:
            upsert_change(change)

    t.stop()
    for m in means.values():
        assert isinstance(m[2], bool), m

    blk_cnt = sum(m[2] for m in means.values())  # count blocked
    p = 100 * blk_cnt / len(means)
    log.info("%d tracked items, %d blocked (%.3f%%)", len(means), blk_cnt, p)
    log.info("Processed %d measurements. Speed: %d K-items per second", cnt, cnt / t.ms)


def create_url(change):
    return f"{WEBAPP_URL}/chart?cc={change.probe_cc}&test_name={change.test_name}&input={change.input}&start_date="


def basefn(cc, test_name, inp):
    d = f"{cc}:{test_name}:{inp}"
    h = hashlib.shake_128(d.encode()).hexdigest(16)
    return h


# TODO rename changes to events?


def update_rss_feeds(events, hashfname):
    # The files are served by Nginx
    feed = feedgenerator.Rss201rev2Feed(
        title="OONI events",
        link="https://explorer.ooni.torproject.org/feeds/rss/blocked",
        description="Blocked services and websites detected by OONI",
        language="en",
    )
    # TODO: render date properly and add blocked/unblocked
    # TODO: put only recent events in the feed (based on the latest event time)
    for e in events:
        feed.add_item(
            title="{} {}".format(e["measurement_start_time"], e["probe_cc"]),
            link="https://explorer.ooni.torproject.org/measurement/{}".format(
                e["report_id"]
            ),
            description="{} {} {}".format(
                e["measurement_start_time"], e["probe_cc"], e["input"]
            ),
        )
    feedfile = conf.rssdir / f"{hashfname}.xml"
    with feedfile.open("w") as f:
        feed.write(f, "utf-8")


def update_status_files(blocking_events):
    # The files are served by Nginx
    return  # FIXME

    # This contains the last status change for every cc/test_name/input
    # that ever had a block/unblock event
    status = {k: v[-1] for k, v in blocking_events.items()}

    statusfile = conf.statusdir / f"status.json"
    d = dict(format=1, status=status)
    with statusfile.open("w") as f:
        ujson.dump(d, f)

    log.debug("Wrote %s", statusfile)


@metrics.timer("upsert_change")
def upsert_change(change):
    """Create / update RSS and JSON files with a new change
    """
    # Create DB tables in future if needed
    debug_url = create_url(change)
    log.info("Change! %r %r", change, debug_url)

    # Append change to a list in a JSON file
    # It contains all the block/unblock events for a given cc/test_name/input
    hashfname = basefn(change.probe_cc, change.test_name, change.input)
    events_f = conf.eventdir / f"{hashfname}.json"
    if events_f.is_file():
        with events_f.open() as f:
            ecache = ujson.load(f)
    else:
        ecache = dict(format=1, blocking_events=[])

    ecache["blocking_events"].append(change._asdict())
    log.info("Saving %s", events_f)
    with events_f.open("w") as f:
        ujson.dump(ecache, f)

    update_rss_feeds(ecache["blocking_events"], hashfname)

    update_status_files(ecache["blocking_events"])


def load_means():
    """Load means from a pkl file
    The file is safely owned by the detector.
    """
    log.info("Loading means")
    pf = conf.pickledir / "means.pkl"
    if pf.is_file():
        perms = pf.stat().st_mode
        assert (perms & 2) == 0, "Insecure pickle permissions %s" % oct(perms)
        with pf.open("rb") as f:
            means = pickle.load(f)

        assert means
        earliest = min(m[0] for m in means.values())
        latest = max(m[0] for m in means.values())
        log.info("Earliest mean: %s", earliest)
        return means, latest

    log.info("Creating new means file")
    return {}, None


def save_means(means):
    """Save means atomically. Protocol 4
    """
    pf = conf.pickledir / "means.pkl"
    pft = conf.pickledir / "means.pkl.tmp"
    log.info("Saving %d means to %s", len(means), pf)
    with pft.open("wb") as f:
        pickle.dump(means, f, protocol=4)
    pft.rename(pf)
    log.info("Saving completed")


def main():
    setup()
    log.info("Starting")

    ro_conn = connect_to_db(RO_DB_HOST, RO_DB_USER, DB_NAME, RO_DB_PASSWORD)

    if conf.webapp:
        import fastpath.detector_webapp as wa

        wa.db_conn = ro_conn
        log.info("Starting webapp")
        wa.bottle.TEMPLATE_PATH.insert(
            0, "/usr/lib/python3.7/dist-packages/fastpath/views"
        )
        wa.bottle.run(port=8880, debug=conf.devel)
        log.info("Exiting webapp")
        return

    means, latest_mean = load_means()
    atexit.register(save_means, means)

    rw_conn = connect_to_db(conf.db_host, DB_USER, DB_NAME, DB_PASSWORD)

    start_date = latest_mean if latest_mean else DEFAULT_STARTTIME
    process_historical_data(ro_conn, rw_conn, start_date, means)
    save_means(means)

    log.info("Starting real-time processing")
    with rw_conn.cursor() as cur:
        cur.execute("LISTEN fastpath;")

    while True:
        if select.select([rw_conn], [], [], 60) == ([], [], []):
            continue  # timeout

        rw_conn.poll()
        while rw_conn.notifies:
            msg = rw_conn.notifies.pop(0)
            log.debug("Notify from PID %d", msg.pid)
            try:
                handle_new_msg(msg, means, rw_conn)
            except Exception as e:
                log.exception(e)


if __name__ == "__main__":
    main()
