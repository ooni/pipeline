import os
from datetime import timedelta, date

import boto3
from botocore import UNSIGNED
from botocore.config import Config

import psycopg2

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

AWS_REGION_NAME = 'us-east-2'
AWS_ACCESS_KEY_ID = os.env['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.env['AWS_SECRET_ACCESS_KEY']
PG_DSN = os.env['PG_DSN']

batch_client = boto3.client(
    'batch',
    region_name='us-east-2',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

s3_client = boto3.client(
    's3',
    region_name='us-east-2',
    config=Config(signature_version=UNSIGNED),
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def is_bucket_empty(bucket):
    resp = s3_client.list_objects(
            Bucket='ooni-data',
            Prefix='autoclaved/jsonl.tar.lz4/{}/'.format(bucket),
            Delimiter='/',
            MaxKeys=3,
    )
    return len(resp['Contents']) == 1


def submit_jobs(bucket_iter):
    for bucket_date in bucket_iter:
        bucket_date = str(bucket_date)
        if is_bucket_empty(bucket_date):
            print("{} is empty bucket, skipping".format(bucket_date))
            continue
        print("Submitting {}".format(bucket_date))
        response = batch_client.submit_job(
            jobName='bucket{}'.format(bucket_date),
            jobQueue='centrifugator-job-queue',
            jobDefinition='centrifugator-job-definition:6',
            containerOverrides={
                'command': [
                    'python',
                    '/usr/local/bin/centrifugation_aws.py',
                    '--bucket',
                    bucket_date
                ],
                # These are set in the job definition
                #'environment': [
                #    {
                #        'POSTGRES_DSN': '',
                #        'AUTOCLAVED_ROOT': ''
                #    },
                #]
            },
        )

def list_jobs():
    response = batch_client.list_jobs(
        jobQueue='centrifugator-job-queue',
        #jobStatus='FAILED',
        jobStatus='SUCCEEDED',
    )
    to_rerun = []
    for x in response['jobSummaryList']:
        exit_code = x.get('container', {}).get('exitCode', None)
        runtime = x['stoppedAt'] - x['startedAt']
        print(x['jobName'], exit_code, str(timedelta(seconds=runtime/1000)))
        #to_rerun.append(x['jobName'].replace('bucket', ''))

def load_processed_buckets():
    processed_buckets = []
    conn = psycopg2.connect(dsn=PG_DSN)
    with conn.cursor() as c:
        c.execute("SELECT DISTINCT bucket_date FROM autoclaved ORDER BY bucket_date;")
        for row in c:
            processed_buckets.append(row[0])
    return set(processed_buckets)

def to_process_buckets():
    missing_buckets = []

    print("loading buckets from autoclaved table")
    processed_buckets = load_processed_buckets()
    print("loaded {}".format(len(processed_buckets)))

    print("loading buckets from s3")
    paginator = s3_client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket='ooni-data', Prefix='autoclaved/jsonl.tar.lz4/', Delimiter='/'):
        for prefix in result.get('CommonPrefixes', []):
            bucket = prefix.get('Prefix').split('/')[-2]
            if bucket not in processed_buckets:
                missing_buckets.append(bucket)
    print("loaded {}".format(len(missing_buckets)))

    return missing_buckets

def main():
    missing_buckets = to_process_buckets()

    print("missing buckets: {}".format(len(missing_buckets)))
    submit_jobs(missing_buckets)

if __name__ == '__main__':
    main()
