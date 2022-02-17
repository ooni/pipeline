import pytest
import time

from pathlib import Path
from datetime import date

from oonidata.s3feeder import iter_file_entries, create_s3_client, get_jsonl_prefixes
from oonidata.s3feeder import iter_cans_on_s3_for_a_day, jsonl_in_range, list_file_entries
from oonidata.s3feeder import stream_measurements


def test_iter_file_entries_new_jsonl():
    fe_list = list(iter_file_entries("jsonl/webconnectivity/IT/20201020/00/"))
    assert len(fe_list) == 19
    for fe in fe_list:
        assert fe.test_name == "webconnectivity"
        assert fe.country_code == "IT"
        assert fe.size > 0
        assert fe.bucket_name == "ooni-data-eu-fra"
        assert fe.day == date(2020, 10, 20)
        assert fe.ext == "jsonl.gz"

def test_iter_file_entries_old_format():
    fe_list = list(iter_file_entries("raw/20211020/00/IT/webconnectivity/"))
    assert len(fe_list) == 6
    for fe in fe_list:
        assert fe.test_name == "webconnectivity"
        assert fe.country_code == "IT"
        assert fe.size > 0
        assert fe.bucket_name == "ooni-data-eu-fra"
        assert fe.day == date(2021, 10, 20)

def test_iter_cans_on_s3_for_a_day():
    fe_list = list(iter_cans_on_s3_for_a_day(date(2020, 1, 1)))
    assert len(fe_list) == 136
    assert all(map(lambda fe: fe.bucket_name == "ooni-data", fe_list))

def test_get_jsonl_prefixes():
    prefixes = list(get_jsonl_prefixes([], [], date(2020, 1, 1), date(2020, 1, 2)))
    assert len(prefixes) == 2516

def test_jsonl_in_range():
    fe_list = list(jsonl_in_range([], [], date(2020, 1, 1), date(2020, 1, 2)))
    assert len(fe_list) == 1125

def test_stream_measurements(tmp_path):
    fe_list = list_file_entries("jsonl/telegram/IT/20201009/00/")
    assert len(fe_list) == 1
    for _, msmt, msmt_uid in stream_measurements(fe_list, tmp_path, False):
        assert msmt["probe_cc"] == "IT"
        assert msmt["test_name"] == "telegram"
