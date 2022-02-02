import argparse
import shutil
from collections import namedtuple
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

from .s3feeder import stream_cans, load_multiple
from .s3feeder import list_cans_on_s3_for_a_day, list_minicans_on_s3_for_a_day, fetch_cans
from .s3feeder import create_s3_client, _calculate_etr

Config = namedtuple("Config", ["ccs", "testnames", "keep_s3_cache", "s3cachedir"])
FileEntry = namedtuple("FileEntry", ["country", "test_name", "date", "basename"])

log = logging.getLogger("oonidata")
logging.basicConfig(level=logging.INFO)

def sync(args):
    s3cachedir = tempfile.TemporaryDirectory()
    conf = Config(
        ccs=args.country,
        testnames=args.test_name,
        keep_s3_cache=False,
        s3cachedir=pathlib.Path(s3cachedir.name)
    )
    t0 = time.time()
    day = args.first_date
    today = dt.date.today()
    stop_day = args.last_date if args.last_date < today else today
    s3 = create_s3_client()
    while day < stop_day:
        cans_fns = list_cans_on_s3_for_a_day(s3, day)
        minicans_fns = list_minicans_on_s3_for_a_day(s3, day, conf.ccs, conf.testnames)
        cans_fns.extend(minicans_fns)

        log.info(f"Downloading {len(cans_fns)} cans")
        test_name = args.test_name.replace("_", "")
        for cn, can_tuple in enumerate(cans_fns):
            s3fname, size = can_tuple
            basename = pathlib.Path(s3fname).name
            if not basename.endswith(".jsonl.gz"):
                basename = basename.rsplit('.', 2)[0] + '.jsonl.gz'
            dst_path = args.output_dir / args.country / test_name / f"{day:%Y-%m-%d}" / basename
            if dst_path.is_file():
                continue
            os.makedirs(dst_path.parent, exist_ok=True)
            temp_path = dst_path.with_name(f"{dst_path.name}.tmp")
            try:
                with gzip.open(temp_path, mode="wt", encoding="utf-8", newline="\n") as out_file:
                    for can_f in fetch_cans(s3, conf, [can_tuple]):
                        try:
                            etr = _calculate_etr(t0, time.time(), args.first_date, day, stop_day, cn, len(cans_fns))
                            log.info(f"Estimated time remaining: {etr}")
                            for msmt_tup in load_multiple(can_f.as_posix()):
                                msmt = msmt_tup[1]
                                if msmt["test_name"].replace("_", "") != test_name:
                                    continue
                                if msmt["probe_cc"] != args.country:
                                    continue
                                ujson.dump(msmt, out_file)
                                out_file.write("\n")
                        except Exception as e:
                            log.error(str(e), exc_info=True)
                        try:
                            can_f.unlink()
                        except FileNotFoundError:
                            pass
                    temp_path.replace(dst_path)
            except:
                temp_path.unlink()
                s3cachedir.cleanup()
                raise

        day += dt.timedelta(days=1)
    s3cachedir.cleanup()

def _parse_date_flag(date_str: str) -> dt.date:
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()

def main():
    parser = argparse.ArgumentParser("OONI Data tools")

    subparsers = parser.add_subparsers()

    parser_sync = subparsers.add_parser("sync", help="Sync OONI measurements")
    parser_sync.add_argument("--country", type=str, required=True)
    parser_sync.add_argument("--first_date", type=_parse_date_flag,
                        default=dt.date.today() - dt.timedelta(days=14))
    parser_sync.add_argument("--last_date", type=_parse_date_flag,
                        default=dt.date.today())
    parser_sync.add_argument("--test_name", type=str, default='webconnectivity')
    parser_sync.add_argument("--max_string_size", type=int, default=1000)
    parser_sync.add_argument("--output_dir", type=pathlib.Path, required=True)
    parser_sync.add_argument("--debug", action="store_true")
    parser_sync.set_defaults(func=sync)

    args = parser.parse_args()
    sys.exit(args.func(args))

if __name__ == "__main__":
    main()
