import logging

import luigi
import luigi.worker
import luigi.postgres

from invoke.config import Config

from pipeline.helpers.util import json_loads, get_date_interval
from pipeline.helpers.util import get_imported_dates
from pipeline.helpers.report import header_avro

from pipeline.batch.sanitise import AggregateYAMLReports, AggregateJSONReports

config = Config(runtime_path="invoke.yaml")
logger = logging.getLogger('ooni-pipeline')

columns = []
for field in header_avro["fields"]:
    if field["type"] == "string":
        columns.append((field["name"], "TEXT"))
    elif field["type"] == "array":
        columns.append((field["name"], "TEXT"))
    elif field["type"] == "float":
        columns.append((field["name"], "FLOAT"))

class ReportHeadersToDatabase(luigi.postgres.CopyToTable):
    src = luigi.Parameter()
    dst_private = luigi.Parameter()
    dst_public = luigi.Parameter()

    date = luigi.DateParameter()
    software_name = luigi.Parameter()

    host = str(config.postgres.host)
    database = str(config.postgres.database)
    user = str(config.postgres.username)
    password = str(config.postgres.password)
    table = str(config.postgres.table)

    columns = columns

    def requires(self):
        if self.software_name == "ooniprobe":
            return AggregateYAMLReports(dst_private=self.dst_private,
                                        dst_public=self.dst_public,
                                        src=self.src,
                                        date=self.date,
                                        software_name=self.software_name)
        elif self.software_name == "satellite":
            return AggregateJSONReports(dst_private=self.dst_private,
                                        dst_public=self.dst_public,
                                        src=self.src,
                                        date=self.date,
                                        software_name=self.software_name)

    def format_record(self, record):
        fields = []
        for field in header_avro["fields"]:
            if field["type"] == "string":
                fields.append(str(record.get(field["name"], "")))
            elif field["type"] == "array":
                fields.append(str(record.get(field["name"], "")))
            elif field["type"] == "float":
                fields.append(float(record.get(field["name"], 0)))
        return fields

    def rows(self):
        sanitised_streams = self.input()["sanitised_streams"]
        with sanitised_streams.open('r') as in_file:
            for line in in_file:
                record = json_loads(line.strip('\n'))
                logger.info("Looking at %s with id %s" % (record["record_type"], record["report_id"]))
                if record["record_type"] == "header":
                    logger.info("Found header")
                    yield self.format_record(record)

def run(src, dst_private, dst_public, date_interval, worker_processes=16,
        software_name="ooniprobe"):
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch,
                            worker_processes=worker_processes)

    imported_dates = get_imported_dates(src,
                                        aws_access_key_id=config.aws.access_key_id,
                                        aws_secret_access_key=config.aws.secret_access_key)
    interval = get_date_interval(date_interval)
    for date in interval:
        if str(date) not in imported_dates:
            continue
        logging.info("adding headers for date: %s" % date)
        task = ReportHeadersToDatabase(dst_private=dst_private,
                                       dst_public=dst_public,
                                       src=src, date=date,
                                       software_name=software_name)
        w.add(task)
    w.run()
    w.stop()
