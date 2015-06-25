from __future__ import absolute_import, print_function, unicode_literals

import os
import shutil
import logging

from dateutil.parser import parse as date_parse

import luigi
import luigi.worker
import luigi.hdfs
from luigi.task import ExternalTask
from luigi.format import GzipFormat
from luigi.s3 import S3Target
from luigi.file import LocalTarget

from invoke.config import Config

from pipeline.helpers.util import list_report_files

config = Config(runtime_path="invoke.yaml")
logger = logging.getLogger('ooni-pipeline')


class ReportSource(ExternalTask):
    src = luigi.Parameter()

    def output(self):
        file_format = None
        if self.src.endswith(".gz"):
            file_format = GzipFormat()
        if self.src.startswith("s3n://"):
            return S3Target(self.src, format=file_format)
        return LocalTarget(self.src, format=file_format)


class S3CopyRawReport(luigi.Task):
    src = luigi.Parameter()
    dst = luigi.Parameter()
    move = luigi.Parameter()

    def requires(self):
        return ReportSource(self.src)

    def output(self):
        try:
            parts = os.path.basename(self.src).split("-")
            date = date_parse('-'.join(parts[:3]))
            # To facilitate sorting and splitting around "-" we convert the
            # date to be something like: 20150101T000015Z
            timestamp = date.strftime("%Y%m%dT%H%M%SZ")
            filename = "{date}-{asn}-{ext}".format(
                date=timestamp,
                asn=parts[3],
                ext=parts[4]
            )
            uri = os.path.join(self.dst, date.strftime("%Y-%m-%d"), filename)
            return S3Target(uri)
        except Exception:
            return S3Target(os.path.join(self.dst, "failed",
                                         os.path.basename(self.src)))

    def run(self):
        input = self.input()
        output = self.output()
        with output.open('w') as out_file:
            with input.open('r') as in_file:
                shutil.copyfileobj(in_file, out_file)
        if self.move:
            input.remove()


def run(src_directory, dst, worker_processes, move=True):
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch,
                            worker_processes=worker_processes)

    uploaded_files = []
    for filename in list_report_files(
        src_directory, aws_access_key_id=config.aws.access_key_id,
            aws_secret_access_key=config.aws.secret_access_key):
        logging.info("uploading %s" % filename)
        task = S3CopyRawReport(src=filename, dst=dst, move=move)
        uploaded_files.append(task.output().path)
        w.add(task)
    w.run()
    w.stop()
    uploaded_dates = []
    for uploaded_file in uploaded_files:
        uploaded_date = os.path.basename(os.path.dirname(uploaded_file))
        if uploaded_date not in uploaded_dates:
            uploaded_dates.append(uploaded_date)
    return uploaded_dates
