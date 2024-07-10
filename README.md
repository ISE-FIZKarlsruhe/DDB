# DDB

Misc scripts and docs for working with [DDB](https://www.deutsche-digitale-bibliothek.de/) data.

## Downloads

The `download.py` script reads in object IDs to be downloaded from a sqlite database file, and retrieves the data via HTTP from the DDB, using the URI: `https://www.deutsche-digitale-bibliothek.de/item/xml/`

The downloaded data is stored in a table that looks like:

```sql
CREATE TABLE IF NOT EXISTS objs (uid TEXT PRIMARY KEY, download_timestamp TEXT, xmlbufgz BLOB)
```

To start, you need to "prime" the table with a list of object IDs to download, this is done with the `download_prime.py` script. This script reads in the IDs from standard in, one per line, and stores the list in the `objs` table with NULL values for the `download_timestamp` and `xmlbufgz` fields.

For example:

```shell
DB_PATH=adb.sqlite python download_prime.py < ~/some_ids.txt
```

NOTE: We configure the scripts by using environment variables.

After priming, you can run the actual downloading process, which starts up multiple workers and downloads the data via a folder (configured as `OUTPUT_PATH`, default is `./out`) For example:

```shell
DB_PATH=adb.sqlite WORKER_COUNT=5 python download.py
```
