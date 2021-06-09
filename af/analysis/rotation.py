#!/usr/bin/env python3
"""
Reads /etc/ooni/rotation.conf

CREATE UNLOGGED TABLE test_helper_instances (
    name text NOT NULL,
    provider text NOT NULL,
    region text,
    ipaddr inet NOT NULL,
    ipv6addr inet
);
ALTER TABLE test_helper_instances OWNER TO shovel;
"""

from configparser import ConfigParser
from datetime import datetime
import logging
import random
import sys
import time

import psycopg2  # debdeps: python3-psycopg2
from psycopg2.extras import execute_values
import statsd  # debdeps: python3-statsd
import digitalocean  # debdeps: python3-digitalocean

metrics = statsd.StatsClient("127.0.0.1", 8125, prefix="rotation")

log = logging.getLogger("reprocessor")
log.addHandler(logging.StreamHandler())  # Writes to console
log.setLevel(logging.DEBUG)

conffile_path = "/etc/ooni/rotation.conf"
TAG = "roaming-th"


def add_droplet_to_db_table(db_conn, dr):
    vals = [dr.name, dr.region["slug"], dr.ip_address, dr.ip_v6_address]
    q = """INSERT INTO test_helper_instances
        (name, provider, region, ipaddr, ipv6addr)
        VALUES (%s, 'Digital Ocean', %s, %s, %s)
    """
    with db_conn.cursor() as cur:
        cur.execute(q, vals)
        db_conn.commit()


def delete_droplet_from_db_table(db_conn, dr):
    q = """DELETE FROM test_helper_instances
    WHERE name = %s AND provider = 'Digital Ocean' AND region = %s
    """
    vals = [dr.name, dr.region["slug"]]
    with db_conn.cursor() as cur:
        cur.execute(q, vals)
        db_conn.commit()


@metrics.timer("spawn_new_droplet")
def spawn_new_droplet(api, dig_oc_token, live_regions, conf):
    regions = set(r.slug for r in api.get_all_regions() if r.available is True)
    preferred_regions = regions - live_regions
    region = random.choice(list(preferred_regions or regions))

    name = datetime.utcnow().strftime("roaming-th-%Y%m%d%H%M%S")
    log.info(f"Spawning {name} in {region}")
    ssh_keys = api.get_all_sshkeys()
    img = conf["image_name"]
    assert img
    size_slug = conf["size_slug"]
    assert size_slug
    droplet = digitalocean.Droplet(
        backups=False,
        image=img,
        name=name,
        region=region,
        size_slug=size_slug,
        ssh_keys=ssh_keys,
        token=dig_oc_token,
        ipv6=True,
        tags=[
            TAG,
        ],
    )
    droplet.create()

    timeout = time.time() + 60 * 10
    while time.time() < timeout:
        time.sleep(5)
        for action in droplet.get_actions():
            action.load()
            if action.status == "completed":
                log.info(f"Droplet ready")
                return api.get_droplet(droplet.id)

        log.debug("Waiting for droplet to start")

    log.error("Timed out waiting for droplet start")
    raise Exception


# sz=api.get_all_sizes()
# sorted((s.price_monthly, s.slug, s.memory) for s in sz)
# imgs = api.get_all_images()
# [(i.name, i.slug) for i in imgs if i.distribution == "Debian"]


def load_conf():
    cp = ConfigParser()
    with open(conffile_path) as f:
        cp.read_file(f)
    return cp["DEFAULT"]


@metrics.timer("run_time")
def main():
    conf = load_conf()

    dig_oc_token = conf["token"]
    assert dig_oc_token
    assert len(dig_oc_token) == 64

    live_droplets_count = int(conf["live_droplets_count"])
    assert 0 <= live_droplets_count < 100

    api = digitalocean.Manager(token=dig_oc_token)
    # Fetch all test-helper droplets
    droplets = api.get_all_droplets(tag_name=TAG)
    for d in droplets:
        assert TAG in d.tags
    live_droplets = [d for d in droplets if d.status == "active"]
    log.info(f"{len(droplets)} droplets")
    log.info(f"{len(live_droplets)} active droplets")

    # Avoid failure modes where we destroy all VMs or create unlimited amounts
    # or churn too quickly
    # TODO: age cehck
    # TODO: deployer script
    if len(live_droplets) <= live_droplets_count:
        if len(droplets) > live_droplets_count + 2:
            log.error("Unexpected amount of running droplets")
            sys.exit(1)
        live_regions = set(d.region["slug"] for d in droplets)
        db_conn = psycopg2.connect(conf["db_uri"])
        droplet = spawn_new_droplet(api, dig_oc_token, live_regions, conf)
        add_droplet_to_db_table(db_conn, droplet)

    if len(live_droplets) > live_droplets_count:
        by_age = sorted(live_droplets, key=lambda d: d.created_at)
        oldest = by_age[0]
        log.info(f"Destroying {oldest.name} droplet")
        db_conn = psycopg2.connect(conf["db_uri"])
        oldest.destroy()
        delete_droplet_from_db_table(db_conn, oldest)


if __name__ == "__main__":
    main()
