import os
import sys
import argparse

import logging
import autoclaving
from centrifugation import meta_pg, open_s3_or_disk

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('nose').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('smart_open').setLevel(logging.WARNING)

def parse_args():
    p = argparse.ArgumentParser(description='ooni-pipeline: public/autoclaved -> *')
    p.add_argument('--bucket', help='Name of bucket to run', required=True)
    opt = p.parse_args()

    return opt

def main():
    opt = parse_args()
    autoclaved_root = os.environ['AUTOCLAVED_ROOT']
    postgres = os.environ['POSTGRES_DSN']

    try:
        autoclaved_index = os.path.join(autoclaved_root, opt.bucket, autoclaving.INDEX_FNAME)
        open_s3_or_disk(autoclaved_index)
    except ValueError:
        print("Did not find {}".format(autoclaved_index))
        sys.exit(44)

    print("Running: {} {}".format(autoclaved_root, opt.bucket))
    #meta_pg(autoclaved_root, opt.bucket, postgres)

if __name__ == '__main__':
    main()
