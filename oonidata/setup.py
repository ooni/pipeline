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
	""
    ],
    include_package_data=True,
    zip_safe=False,
    package_data={'fastpath': ['views/*.tpl', 'static/*']},
)
