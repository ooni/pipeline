#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="oonidata",
    python_requires=">=3.7.0",
    packages=["oonidata"],
    entry_points={"console_scripts": [
        "oonidata=oonidata:main",
    ]},
    install_requires=[
	"boto3",
        "pyyaml",
        "ujson",
        "lz4"
    ],
    zip_safe=False,
)
