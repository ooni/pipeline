#!/usr/bin/env python3
#
# That is one-liner to filter `index.json.gz` for interesting files to do
# partial bucket reprocessing locally.
#
# Usage (assuming that a bucket "working copy" is a PWD):
# $ mv -n index.json.gz index.json.gz.orig
# $ zcat index.json.gz.orig | .../trim_index.py 2019-04-10/web_connectivity.42.tar.lz4 | gzip >index.json.gz
#

import json
import sys

good = set(sys.argv[1:])

show = None
for line in sys.stdin:
    l = json.loads(line)
    if l['type'] == 'file':
        show = l['filename'] in good
    assert show is not None # enforce `type` order
    if show:
        sys.stdout.write(line)
    if l['type'] == '/file':
        show = None
