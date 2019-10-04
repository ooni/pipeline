"""
Detector web application

Currently used only for tuning the detector, it runs event detection for one
cc / test_name / input independently from the event detector daemon.
The output are only displayed in charts and not used to generate RSS feeds
or other.

"""

# TODO: cleanup

from datetime import datetime, timedelta
import logging
import json

from bottle import request
import bottle

from fastpath.detector import (
    detect_blocking_changes_asn_one_stream,
)

from fastpath.metrics import setup_metrics

log = logging.getLogger("detector")
metrics = setup_metrics(name="detector")

db_conn = None  # Set by detector.py or during functional testing

asn_db = None  # Set by detector.py

def _datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("unknown type")


bottle.install(
    bottle.JSONPlugin(json_dumps=lambda o: json.dumps(o, default=_datetime_handler))
)


@bottle.view("chart")
def generate_chart(msmts, changes, cc, test_name, inp):
    """Render measurements and changes into a SVG chart
    :returns: dict
    """
    assert isinstance(msmts[0][0], datetime)
    x1 = 100
    x2 = 1100
    y1 = 50
    y2 = 300
    # scale x
    dates = [e[0] for e in msmts]
    assert len(dates) >= 2
    start_d = min(dates)
    end_d = max(dates)
    delta = (end_d - start_d).total_seconds()
    assert delta != 0
    x_scale = (x2 - x1) / delta

    return dict(
        msmts=msmts,
        changes=changes,
        x_scale=x_scale,
        start_d=start_d,
        x1=x1,
        x2=x2,
        y1=y1,
        y2=y2,
        cc=cc,
        test_name=test_name,
        inp=inp,
    )


@bottle.route("/")
@bottle.view("form")
def index():
    log.debug("Serving index")
    return {}


def plot_series(conn, cc, test_name, inp, start_date):
    """Plot time-series for a CC / test_name / input as SVG chart
    :returns: dict
    """
    (msmts, changes, asn_breakdown) = detect_blocking_changes_asn_one_stream(
        conn, cc, test_name, inp, start_date
    )
    assert isinstance(msmts[0][0], datetime)
    country_chart = generate_chart(msmts, changes, cc, test_name, inp)

    # Most popular ASNs
    popular = sorted(
        asn_breakdown, key=lambda asn: len(asn_breakdown[asn]["msmts"]), reverse=True
    )
    popular = popular[:20]
    asn_charts = []
    for asn in popular:
        asn_name = "AS{} {}".format(asn, asn_db.get(asn, ""))
        a = asn_breakdown[asn]
        try:
            c = generate_chart(a["msmts"], a["changes"], asn_name, test_name, inp)
            asn_charts.append(c)
        except:
            log.error(a)

    charts = [country_chart] + asn_charts
    charts = "".join(charts)
    return charts



@bottle.route("/chart")
# @bottle.view("chart")
@metrics.timer("generate_chart")
def genchart():
    params = ("cc", "test_name", "input", "start_date")
    q = {k: (request.query.get(k, None).strip() or None) for k in params}

    cc = q["cc"]
    assert len(cc) == 2, "CC must be 2 letters"

    test_name = q["test_name"] or "web_connectivity"
    inp = q["input"]
    assert cc, "input is required"
    start_date = q["start_date"]
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_date = datetime.now() - timedelta(days=10)

    return plot_series(db_conn, cc, test_name, inp, start_date)


@bottle.error(500)
def error_handler_500(error):
    log.error(error.exception)
    return repr(error.exception)
