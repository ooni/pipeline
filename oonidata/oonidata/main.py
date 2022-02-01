import argparse
from collections import namedtuple
import datetime as dt
import pathlib
import sys
from typing import List

from .s3feeder import stream_cans

Config = namedtuple("Config", ["ccs", "testnames", "keep_s3_cache", "s3cachedir"])

def sync(args):
    conf = Config(
        ccs=args.country,
        testnames=args.test_name,
        keep_s3_cache=True,
        s3cachedir=args.output_dir
    )
    for msmt in stream_cans(conf, args.first_date, args.last_date):
        print(msmt)

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