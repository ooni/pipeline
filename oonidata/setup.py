#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as in_file:
    long_description = in_file.read()

setup(
    name="oonidata",
    version="0.0.1",
    author="Open Observatory of Network Interference (OONI)",
    author_email="contact@openobservatory.org",
    description="Interact with OONI network measurement data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7.0",
    packages=["oonidata"],
    entry_points={"console_scripts": [
        "oonidata=oonidata.main:main",
    ]},
    install_requires=[
	"boto3",
        "pyyaml",
        "ujson",
        "tqdm",
        "lz4"
    ],
    project_urls={
        "Bug Tracker": "https://github.com/ooni/backend/issues"
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    zip_safe=False,
)
