# OONI Data

**Attention**
This tool is currently in alpha stage. The CLI API is subject to change and you
should be careful to rely on it for production usage.

## What is this?

OONI data is a tool for interacting with raw OONI measurements. It supports
downloading raw network measurement data in batch.

For the specifications of the base data formats see: https://github.com/ooni/spec/tree/master/data-formats

For the specifications of each of the tests see: https://github.com/ooni/spec/tree/master/nettests

## Example usage

To download raw Web Connectivity measurements for a given country and time range, use the following:
```
oonidata sync --since 2022-02-23 --until 2022-03-17 --country-codes IT --test-names web_connectivity --output-dir ./oonidatastore/
```
