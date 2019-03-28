import os
import argparse

from centrifugation import meta_pg

def parse_args():
    p = argparse.ArgumentParser(description='ooni-pipeline: public/autoclaved -> *')
    p.add_argument('--bucket', help='Name of bucket to run', required=True)
    opt = p.parse_args()

    return opt

def main():
    opt = parse_args()
    autoclaved_root = os.environ['AUTOCLAVED_ROOT']
    postgres = os.environ['POSTGRES_DSN']
    meta_pg(autoclaved_root, opt.bucket, postgres)

if __name__ == '__main__':
    main()
