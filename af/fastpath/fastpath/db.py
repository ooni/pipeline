"""
OONI Fastpath

Database connector

See ../../oometa/017-fastpath.install.sql for the tables structure

"""

from textwrap import dedent
import http.client
import logging
import time

import psycopg2  # debdeps: python3-psycopg2
from psycopg2.extras import Json

import ujson

from fastpath.metrics import setup_metrics

log = logging.getLogger("fastpath.db")
metrics = setup_metrics(name="fastpath.db")

conn = None
_autocommit_conn = None

# TODO: move to config file
DB_HOST = "hkgmetadb.infra.ooni.io"
DB_USER = "shovel"
DB_NAME = "metadb"
DB_PASSWORD = "yEqgNr2eXvgG255iEBxVeP"  # This is already made public
CH_DB_HOST = "localhost"
CH_DB_PORT = 8123


# # PostgreSQL backend


def _ping():
    q = "SELECT pg_postmaster_start_time();"
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()
        log.info("Database start time: %s", row[0])


def setup(conf) -> None:
    global conn, _autocommit_conn
    if conf.db_uri:
        dsn = conf.db_uri
    else:
        dsn = f"host={DB_HOST} user={DB_USER} dbname={DB_NAME} password={DB_PASSWORD}"
    log.info("Connecting to database: %r", dsn)
    conn = psycopg2.connect(dsn)
    _autocommit_conn = psycopg2.connect(dsn)
    _autocommit_conn.autocommit = True
    _ping()


@metrics.timer("upsert_summary")
def upsert_summary(
    msm,
    scores,
    anomaly: bool,
    confirmed: bool,
    msm_failure: bool,
    tid,
    filename,
    update,
) -> None:
    """Insert a row in the fastpath_scores table. Overwrite an existing one.
    """
    sql_base_tpl = dedent(
        """\
    INSERT INTO fastpath (tid, report_id, input, probe_cc, probe_asn, test_name,
        test_start_time, measurement_start_time, platform, filename, scores,
        anomaly, confirmed, msm_failure)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT fastpath_pkey DO
    """
    )
    sql_update = dedent(
        """\
    UPDATE SET
        report_id = excluded.report_id,
        input = excluded.input,
        probe_cc = excluded.probe_cc,
        probe_asn = excluded.probe_asn,
        test_name = excluded.test_name,
        test_start_time = excluded.test_start_time,
        measurement_start_time = excluded.measurement_start_time,
        platform = excluded.platform,
        filename = excluded.filename,
        scores = excluded.scores,
        anomaly = excluded.anomaly,
        confirmed = excluded.confirmed,
        msm_failure = excluded.msm_failure
    """
    )
    sql_noupdate = " NOTHING"

    tpl = sql_base_tpl + (sql_update if update else sql_noupdate)

    asn = int(msm["probe_asn"][2:])  # AS123
    platform = "unset"
    if "annotations" in msm and isinstance(msm["annotations"], dict):
        platform = msm["annotations"].get("platform", "unset")
    args = (
        tid,
        msm["report_id"],
        msm.get("input", None),
        msm["probe_cc"],
        asn,
        msm["test_name"],
        msm["test_start_time"],
        msm["measurement_start_time"],
        platform,
        filename,
        Json(scores, dumps=ujson.dumps),
        anomaly,
        confirmed,
        msm_failure,
    )

    # Send notification using pg_notify
    # TODO: do not send notifications during manual run or in devel mode
    cols = (
        "report_id",
        "input",
        "probe_cc",
        "probe_asn",
        "test_name",
        "test_start_time",
        "measurement_start_time",
    )

    assert _autocommit_conn
    with _autocommit_conn.cursor() as cur:
        try:
            cur.execute(tpl, args)
            # log.debug(cur.query.decode())
        except psycopg2.ProgrammingError:
            log.error("upsert syntax error in %r", tpl, exc_info=True)
            return

        if cur.rowcount == 0 and not update:
            metrics.incr("report_id_input_db_collision")
            inp = msm.get("input", "<no input>")
            log.info(f"report_id / input collision {msm['report_id']} {inp}")
            return

        notification = {k: msm.get(k, None) for k in cols}
        notification["trivial_id"] = tid
        notification["scores"] = scores
        notification_json = ujson.dumps(notification)
        q = f"SELECT pg_notify('fastpath', '{notification_json}');"
        cur.execute(q)


# # Clickhouse


def _ch_ping():
    result = _ch_send_query("POST", "SELECT version()")
    log.info(result)


def _ch_create_table_fastpath():
    sql = """
    CREATE TABLE IF NOT EXISTS fastpath (
        "tid" String,
        "report_id" String,
        "input" String,
        "probe_cc" String,
        "probe_asn" Int32,
        "test_name" String,
        "test_start_time" DateTime,
        "measurement_start_time" DateTime,
        "platform" String,
        "filename" String,
        "scores" String,
        anomaly Int8,
        confirmed Int8,
        msm_failure Int8
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (measurement_start_time, report_id, input)
    """
    result = _ch_send_query("POST", sql)
    log.info(result)


def ch_setup(conf) -> None:
    global conn, _autocommit_conn
    conn = http.client.HTTPConnection(f"{CH_DB_HOST}:{CH_DB_PORT}")

    # if conf.db_uri:
    #    dsn = conf.db_uri
    # else:
    #    dsn = f"host={DB_HOST} user={DB_USER} dbname={DB_NAME} password={DB_PASSWORD}"
    # log.info("Connecting to database: %r", dsn)
    _ch_ping()
    _ch_create_table_fastpath()


def _ch_send_query(qtype: str, query: str):
    global conn
    assert qtype in ("get", "put", "POST")
    ka = {
        "Connection": " keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    while True:
        try:
            conn.request(qtype, "/", query, headers=ka)
            result = conn.getresponse()
        except http.client.CannotSendRequest:
            log.error("", exc_info=True)
            time.sleep(0.5)
            conn = http.client.HTTPConnection(f"{CH_DB_HOST}:{CH_DB_PORT}")
            continue

        if result.reason != "OK":
            log.error(result.read().decode())
            raise Exception()
        return result.read().decode()


@metrics.timer("ch_upsert_summary")
def ch_upsert_summary(
    msm,
    scores,
    anomaly: bool,
    confirmed: bool,
    msm_failure: bool,
    tid,
    filename,
    update,
) -> None:
    """Insert a row in the fastpath table. Overwrite an existing one.
    """
    sql_base_tpl = dedent(
        """\
    INSERT INTO fastpath (tid, report_id, input, probe_cc, probe_asn, test_name,
        test_start_time, measurement_start_time, platform, filename, scores,
        anomaly, confirmed, msm_failure)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT fastpath_pkey DO
    """
    )
    sql_base_tpl = dedent(
        """\
    INSERT INTO fastpath (tid, report_id, input, probe_cc, probe_asn, test_name,
        test_start_time, measurement_start_time, platform, filename, scores,
        anomaly, confirmed, msm_failure)
    VALUES ('%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d)
    """
    )
    sql_noupdate = " NOTHING"

    tpl = sql_base_tpl + (sql_update if update else sql_noupdate)

    asn = int(msm["probe_asn"][2:])  # AS123
    platform = "unset"
    if "annotations" in msm and isinstance(msm["annotations"], dict):
        platform = msm["annotations"].get("platform", "unset")
    args = (
        tid,
        msm["report_id"],
        msm.get("input", None),
        msm["probe_cc"],
        asn,
        msm["test_name"],
        msm["test_start_time"],
        msm["measurement_start_time"],
        platform,
        filename,
        ujson.dumps(scores),
        anomaly,
        confirmed,
        msm_failure,
    )
    sql = sql_base_tpl % args
    result = _ch_send_query("POST", sql)
