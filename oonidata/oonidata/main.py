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

from .s3feeder import create_s3_client
from .s3feeder import jsonl_in_range, stream_measurements_from_files

Config = namedtuple("Config", ["ccs", "testnames", "keep_s3_cache", "s3cachedir"])
FileEntry = namedtuple("FileEntry", ["country", "test_name", "date", "basename"])

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

def sync(args):
    test_name = args.test_name.replace("_", "")
    s3cachedir = tempfile.TemporaryDirectory()
    conf = Config(
        ccs=[args.country],
        testnames=[test_name],
        keep_s3_cache=False,
        s3cachedir=pathlib.Path(s3cachedir.name)
    )
    t0 = time.time()
    s3 = create_s3_client()
    for file_entry in jsonl_in_range(s3, conf, args.first_date, args.last_date):
        dst_path = args.output_dir / file_entry.test_name /  file_entry.country_code / f"{file_entry.timestamp:%Y-%m-%d}" / file_entry.filename
        if dst_path.is_file():
            continue
        os.makedirs(dst_path.parent, exist_ok=True)
        temp_path = dst_path.with_name(f"{dst_path.name}.tmp")
        try:
            with gzip.open(temp_path, mode="wt", encoding="utf-8", newline="\n") as out_file:
                jsonl_fns = [(file_entry.fullpath, file_entry.size)]
                for msmt_tup in stream_measurements_from_files(s3, conf, jsonl_fns):
                    msmt = msmt_tup[1]
                    if args.max_string_size:
                        msmt = trim_measurement(msmt, args.max_string_size)
                    ujson.dump(msmt, out_file)
                    out_file.write("\n")
                temp_path.replace(dst_path)
        except:
            temp_path.unlink()
            s3cachedir.cleanup()
            raise

    s3cachedir.cleanup()

def _parse_date_flag(date_str: str) -> dt.date:
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()

def main():
    parser = argparse.ArgumentParser("OONI Data tools")
    parser.set_defaults(func=lambda r: parser.print_usage())

    subparsers = parser.add_subparsers()

    parser_sync = subparsers.add_parser("sync", help="Sync OONI measurements")
    parser_sync.add_argument("--country", type=str, required=True)
    parser_sync.add_argument("--first_date", type=_parse_date_flag,
                        default=dt.date.today() - dt.timedelta(days=14))
    parser_sync.add_argument("--last_date", type=_parse_date_flag,
                        default=dt.date.today())
    parser_sync.add_argument("--test_name", type=str, default='webconnectivity')
    parser_sync.add_argument("--max_string_size", type=int)
    parser_sync.add_argument("--output_dir", type=pathlib.Path, required=True)
    parser_sync.add_argument("--debug", action="store_true")
    parser_sync.set_defaults(func=sync)

    args = parser.parse_args()
    sys.exit(args.func(args))

if __name__ == "__main__":
    main()
