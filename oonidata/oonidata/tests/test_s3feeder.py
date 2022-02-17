import pytest
import time

from datetime import date
from oonidata.s3feeder import iter_file_entries, create_s3_client, get_jsonl_prefixes
from oonidata.s3feeder import iter_cans_on_s3_for_a_day, jsonl_in_range

@pytest.fixture
def s3():
    return create_s3_client()

def test_iter_file_entries_new_jsonl(s3):
    fe_list = list(iter_file_entries(s3, "jsonl/webconnectivity/IT/20201020/00/"))
    assert len(fe_list) == 19
    for fe in fe_list:
        assert fe.test_name == "webconnectivity"
        assert fe.country_code == "IT"
        assert fe.size > 0
        assert fe.bucket_name == "ooni-data-eu-fra"
        assert fe.day == date(2020, 10, 20)
        assert fe.ext == "jsonl.gz"

def test_iter_file_entries_old_format(s3):
    fe_list = list(iter_file_entries(s3, "raw/20211020/00/IT/webconnectivity/"))
    assert len(fe_list) == 6
    for fe in fe_list:
        assert fe.test_name == "webconnectivity"
        assert fe.country_code == "IT"
        assert fe.size > 0
        assert fe.bucket_name == "ooni-data-eu-fra"
        assert fe.day == date(2021, 10, 20)

def test_iter_cans_on_s3_for_a_day(s3):
    fe_list = list(iter_cans_on_s3_for_a_day(s3, date(2020, 1, 1)))
    assert len(fe_list) == 136
    assert all(map(lambda fe: fe.bucket_name == "ooni-data", fe_list))

def test_get_jsonl_prefixes(s3):
    prefixes = list(get_jsonl_prefixes(s3, [], [], date(2020, 1, 1), date(2020, 1, 2)))
    assert len(prefixes) == 2516

def test_jsonl_in_range(s3):
    fe_list = list(jsonl_in_range(s3, [], [], date(2020, 1, 1), date(2020, 1, 2)))
    assert len(fe_list) == 1125
