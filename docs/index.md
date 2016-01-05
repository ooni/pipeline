# Open Observatory Pipeline

This is the Open Observatory data processing pipeline

![ooni-pipeline architecture diagram](https://raw.githubusercontent.com/TheTorProject/ooni-pipeline/master/docs/ooni-pipeline-architecture.png)

## Outline of the process

### 1) backend collectors -> s3://ooni-incoming
ooni-backend machines upload "raw reports" (yaml files) to the ooni-incoming S3 bucket. The following is run daily by cron:
```
find /data/bouncer/archive -type f -print0 \
    | xargs -0 -I FILE \
      aws s3 mv FILE s3://ooni-incoming/
```

### 2) s3://ooni-incoming -> s3://ooni-private/raw-reports/yaml
those reports get copied to the ooni-private S3 bucket (this 2nd step is for permissions separation). Another daily cron job:
```
date_bin=$(date -I)
if [ -z "$date_bin" ]; then exit -1; fi

# should be able to do this filtering with the aws --exclude and --include,
# but I can't get that to work.

aws s3 ls s3://ooni-incoming \
        | awk '{print $4}' \
        | grep '.yamloo$' \
        | xargs -I FILE \
                aws s3 mv s3://ooni-incoming/FILE \
                          s3://ooni-private/reports-raw/yaml/$date_bin/
```

### 3) yaml raw reports -> sanitised json, aggregated by day
```
invoke bins_to_sanitised_streams \
    --unsanitised-dir "s3n://ooni-private/reports-raw" \
    --sanitised-dir "s3n://ooni-public" \
    --date-interval 2012-12-01-2016-01-01 \
    --workers 32
```
this command does some sanitisation and aggregates the reports by date
(the folders ("bins") correspond to a pipeline date, not the report measurement date) 
into json streams in the ooni-public bucket.

### 4) sanitised json -> postgres DB
```
invoke streams_to_db --streams-dir "s3n://ooni-public/json"
```
this command reads the json streams and puts each report entry (there are many entries in a report) as a row into the postgres db.

We currently run the bins->streams on a c3.8xlarge (32 core) with 32 processes and 80GB EBS.
(the S3 files get cached on-disk on their way in and out, so this can eat a lot of space). It takes about a day to run through the whole dataset.

The streams->db step, we run on a m4.xlarge with 1 process, and it also takes about a day to run. I haven't looked into what the speed bottleneck here is.

## Configuration

Before running the pipeline you should configure it by editing the
`invoke.yaml` file. An example configuration file is provided inside of
`invoke.yaml.example`.

The files you should probably be editing are the following:

### core

* **tmp_dir** What directory should be used to store temporary files.

* **ssh_private_key_file** What ssh private key shall be used by luigi for sshing into ssh:// machines.

* **ooni_pipeline_path** The location on the ec2 instance where to look for the ooni-pipeline repository.

### aws

* **access_key_id** This is your AWS access key ID for spinning up EC2 instances.

* **secret_access_key** This is your AWS secret token.

* **ssh_private_key_file** This is a private key that will be used for sshing into the started machines.

### postgres

* **host** The hostname of your postgres instance.

* **database** The database name.

* **username** The username to use when logging in.

* **password** The password to use when logging in.

* **table** The database table to use for writing report headers to.

### ooni

* **bridge_db_path** A path to where you have a bridge_db.json file that
    contains mappings between bridge IPs, their hashes and the ring they were
    taken for (this is required for the sanitisation of bridge_reachability
    reports).

### spark

* **spark_submit** Path to where the spark-submit command can be found.

* **master** The name of the yarn master node.

### papertrail

* **hostname** The hostname of the papertrail logging backend

* **port** The port of the papertrail logging backend

### kafka

This is currently not used

### spark

This is currently not used

## List of tasks [ed: may be obsolete]

Tasks are run by using [pyinvoke](http://pyinvoke.org/) and are defined inside
of `tasks.py`.

### Generate streams

This task can be run via:

```
invoke generate_streams --date-interval=DATE_INTERVAL [--src=URI]
                        [--workers=NUM --dst-private=URI --dst-public=URI]
                        [--halt]
```

The purpose of this task is to take the YAML reports that are located at the
address specified by the `src` URI and move them over into the private and
public bucket after having operated on them some transformations and
sanitisations.
The transformations are in particular that of partitioning the data by data and
converting them to JSON. This means that all the reports from 2019-10-11 will
end up in a JSON file named 2019-10-11.json.

Each line of the JSON file will contain the full report header and an extra key
used to identify if it's a header or a measurement.

The reason for splitting it into daily buckets is to avoid random seeking as
much as possible.

* **date-interval**

The date range that should be taken into consideration when running the
`generate_streams` task. If no date range is specified it will run against all
the dates.
The format for the date range is that of the [luigi DateInterval
module](http://luigi.readthedocs.org/en/stable/api/luigi.date_interval.html).
For example: `2019-10` will be the full month of October 2019 or
`2019-10-29-2019-10-31` will be the dates of the 29th and 30th of 2019.

* **src** default: `s3n://ooni-private/reports-raw/yaml/`

Where the reports should be read from. This is considered the **master
dataset** of the pipeline.

* **workers** default: 16

The number of CPU workers to use when running the operations.

* *dst_private*: default: `s3n://ooni-private/`

The target location in which to place the processed JSON streams. They will end
up inside of `$URI/reports-raw/streams/`.

* **dst_public**: default: `s3n://ooni-public/"

The target location in which to place the sanitised and processed JSON streams.
They will end up inside of `$URI/reports-sanitised/streams/`.

* **halt**: default: disabled

This is an optional flag that indicates if we should or should not halt the
machine when done.

### Upload reports

This task can be run via:

```
invoke upload_reports --src=URI [--dst=URI --workers=INT --move --halt]
```

This task is responsible for moving or copying the reports from a certain
incoming AWS bucket to the private ooni-pipeline bucket ready for being
processed by the `generate_streams` task.

It will look inside of src for all files ending with `.yaml`.

The files will be renamed when moving them over to the dst directory using the
following format: `{date}-{asn}-{test_name}-{df_version}-{ext}`.

* **src**: From from where the reports should be copied or moved. In the
ooni-pipeline this is set to `s3://ooni-incoming/` that is the incoming
bucket.

* **dst** default: `s3n://ooni-private/reports-raw/yaml/`

Where the reports should be moved or copied to.

* **workers** default: 16

The number of CPU workers to use when running the operations.

* **move**

If the source file should be deleted once it has been successfully copied.

* **halt**: default: disabled

This is an optional flag that indicates if we should or should not halt the
machine when done.

### List reports

This task can be run via:

```
invoke list_reports --path=URI
```

Will list all the files that appear to be OONI reports in a
certain directory.

* **path**: default: `s3n://ooni-private/reports-raw/yaml/`

That path to list reports inside of.

### Clean streams

This task can be run via:

```
invoke clean_streams --dst-private=URI --dst-public=URI
```

This will delete all the files that are generated by the `generate_streams`
task.
In particular these files are:

* `PRIVATE/reports-raw/streams`

* `PRIVATE/reports-sanitised/yaml`

* `PRIVATE/reports-sanitised/streams`

The arguments are:

* **dst_private**: default: `s3n://ooni-private/`

The directory that contains the raw reports.

* **dst-public**: default: `s3n://ooni-public/`

The directory that contains the public reports.

### Add headers to DB

This task can be run via:

```
invoke add_headers_to_db --date-interval=DATE_INTERVAL [--src=URI]
                        [--workers=NUM --dst-private=URI --dst-public=URI]
                        [--halt]
```

When no date is specified it will run `upload_reports` on `s3n://ooni-incoming`
by moving them, then run on these incoming reports the add_headers_to_db batch
operation.
When a date range is specified it will run the batch operation on such date.

The batch operation will sanitise the YAML reports generating their streams and
then add the report headers to the database.
For the schema of the database see the avro specification inside of
`pipeline/helpers/report.py`.

* **date-interval**

The date range that should be taken into consideration when running the
`generate_streams` task. If no date range is specified it will run against all
the dates.
The format for the date range is that of the [luigi DateInterval
module](http://luigi.readthedocs.org/en/stable/api/luigi.date_interval.html).
For example: `2019-10` will be the full month of October 2019 or
`2019-10-29-2019-10-31` will be the dates of the 29th and 30th of 2019.

* **src** default: `s3n://ooni-private/reports-raw/yaml/`

Where the reports should be read from. This is considered the **master
dataset** of the pipeline.

* **workers** default: 16

The number of CPU workers to use when running the operations.

* *dst_private*: default: `s3n://ooni-private/`

The target location in which to place the processed JSON streams. They will end
up inside of `$URI/reports-raw/streams/`.

* **dst_public**: default: `s3n://ooni-public/"

The target location in which to place the sanitised and processed JSON streams.
They will end up inside of `$URI/reports-sanitised/streams/`.

* **halt**: default: disabled

This is an optional flag that indicates if we should or should not halt the
machine when done.


### Sync reports

This task can be run via:

```
invoke sync_reports --srcs=URI [--dst-private=URI --workers=INT --halt]
```

This task is responsible for moving the reports from a certain set of sources
to the incoming bucket.
This task is usually run on collectors to place the data they have gathered
into the incoming AWS bucket.

It will look inside of src for all files ending with `.yaml`.

* **srcs**: default: `ssh://root@bouncer.infra.ooni.nu/data/bouncer/archive`

The source directories from where to look for OONI reports.

* **dst-private** default: `s3n://ooni-incoming/`

Where the reports should be moved to.

* **workers** default: 16

The number of CPU workers to use when running the operations.

* **halt**: default: disabled

This is an optional flag that indicates if we should or should not halt the
machine when done.

### Start computer

This task can be run via:

```
invoke start_computer [--private-key=PATH --instance-type=INSTANCE_TYPE ]
                      [--invoke_command=INVOKE_COMMAND]
```

* **private-key**: default: `private/ooni-pipeline.pem`

The private key to be used to ssh into the machine.

* **instance_type**: default: `c3.8xlarge`

The type of AWS EC2 instance to start. A full list of them can be found here:
[https://aws.amazon.com/ec2/instance-types/](https://aws.amazon.com/ec2/instance-types/)

* **invoke_command**: default: `add_headers_to_db --workers=32 --halt`

The invoke command to be run once the machine is started. Remember that it may
be important to also run the --halt to avoid extra costs.

### Spark apps

This task can be run via:

```
invoke spark_apps [--private-key=PATH instance-type=INSTANCE_TYPE --invoke_command=INVOKE_COMMAND]
```


This task will run the batch spark based apps on a hadoop cluster. The current
batch operations are responsible for inspecting the sanitised streams bucketed
by date located inside of `--src`, generating some database views based upon them
and writing a processed JSON file inside of `--dst` to indicate that the certain
date has been processed.

* **date-interval**

The format for the date range is that of the [luigi DateInterval
module](http://luigi.readthedocs.org/en/stable/api/luigi.date_interval.html).
For example: `2019-10` will be the full month of October 2019 or
`2019-10-29-2019-10-31` will be the dates of the 29th and 30th of 2019.

* **src**: default: `s3n://ooni-public/reports-sanitised/streams/`

From where to read the JSON streams from.

* **dst**: default: `s3n://ooni-public/processed/`

Where to write a file to indicate a certain date has been processed.

* **workers**: default: 3

The number of CPU workers to use when running the operations.

### Spark submit

This task is work in progress and is not throughly tested, it's for running
spark scripts on a hadoop cluster.
