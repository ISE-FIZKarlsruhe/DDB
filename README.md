# DDB

Misc scripts and docs for working with [DDB](https://www.deutsche-digitale-bibliothek.de/) data.

## Downloads

The `download.py` script reads in object IDs to be downloaded from a sqlite database file, and retrieves the data via HTTP from the DDB

To start, you need to "prime" the table with a list of object IDs to download, this is done with the `download_prime.py` script. This script reads in the IDs from standard in, one per line, and stores the list in the `objs` and `srcs` tables with NULL values for the `download_timestamp` and `bufgz` fields.

For example:

```shell
DB_PATH=adb.sqlite python download_prime.py < ~/some_ids.txt
```

NOTE: We configure the scripts by using environment variables.

After priming, you can run the actual downloading process, which starts up multiple workers and downloads the data via a folder (configured as `OUTPUT_PATH`, default is `./out`) For example:

```shell
DB_PATH=adb.sqlite WORKER_COUNT=5 python download.py
```
