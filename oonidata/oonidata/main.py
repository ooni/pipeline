import argparse
import shutil
from collections import namedtuple
from functools import singledispatch
import tempfile
import os
import gzip
import itertools
import logging
import datetime as dt
import pathlib
import sys
import time
from typing import List, Generator, Tuple, List

import ujson

from .s3feeder import create_s3_client, FileEntry, download_measurement_container
from .s3feeder import jsonl_in_range

Config = namedtuple("Config", ["ccs", "testnames", "keep_s3_cache", "s3cachedir"])

log = logging.getLogger("oonidata")
logging.basicConfig(level=logging.INFO)

# Taken from:
# https://github.com/Jigsaw-Code/net-analysis/blob/master/netanalysis/ooni/data/sync_measurements.py#L33
@singledispatch
def trim_measurement(json_obj, max_string_size: int):
    return json_obj


@trim_measurement.register(dict)
def _(json_dict: dict, max_string_size: int):
    keys_to_delete: List[str] = []
    for key, value in json_dict.items():
        if type(value) == str and len(value) > max_string_size:
            keys_to_delete.append(key)
        else:
            trim_measurement(value, max_string_size)
    for key in keys_to_delete:
        del json_dict[key]
    return json_dict


@trim_measurement.register(list)
def _(json_list: list, max_string_size: int):
    for item in json_list:
        trim_measurement(item, max_string_size)
    return json_list


def trim_container(conf, fe: FileEntry, max_string_size: int):
    mc = fe.output_path(conf.s3cachedir)
    temp_path = diskf.with_suffix(".tmp")
    try:
        with gzip.open(
            temp_path, mode="wt", encoding="utf-8", newline="\n"
        ) as out_file:
            for msmt in load_multiple(mc.as_posix()):
                msmt = trim_measurement(msmt, args.max_string_size)
                ujson.dump(msmt, out_file)
                out_file.write("\n")
            temp_path.replace(mc)
    except:
        temp_path.unlink()
        raise


def sync(args):
    testnames = []
    if args.test_names:
        # Replace _ with a -
        testnames = list(map(lambda x: x.replace("_", ""), args.test_names))

    conf = Config(
        ccs=args.country_codes,
        testnames=testnames,
        keep_s3_cache=True,
        s3cachedir=args.output_dir,
    )
    t0 = time.time()
    s3 = create_s3_client()
    for file_entry in jsonl_in_range(s3, conf, args.since, args.until):
        if not file_entry.matches_filter(ccs, testnames):
            continue
        mc = download_measurement_container(s3, conf, file_entry)
        if args.max_string_size:
            trim_container(conf, fe, args.max_string_size)


def _parse_date_flag(date_str: str) -> dt.date:
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser("OONI Data tools")
    parser.set_defaults(func=lambda r: parser.print_usage())

    subparsers = parser.add_subparsers()

    parser_sync = subparsers.add_parser("sync", help="Sync OONI measurements")
    parser_sync.add_argument(
        "--country-codes",
        type=str,
        nargs="*",
        help="List of probe_cc values to filter by",
    )
    parser_sync.add_argument(
        "--since",
        type=_parse_date_flag,
        default=dt.date.today() - dt.timedelta(days=14),
    )
    parser_sync.add_argument("--until", type=_parse_date_flag, default=dt.date.today())
    parser_sync.add_argument(
        "--test-names", nargs="*", help="List of test_name values to filter by"
    )
    parser_sync.add_argument("--max-string-size", type=int)
    parser_sync.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser_sync.add_argument("--debug", action="store_true")
    parser_sync.set_defaults(func=sync)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
