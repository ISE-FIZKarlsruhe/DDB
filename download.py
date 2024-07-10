import sqlite3, os, time, gzip, multiprocessing, queue
import httpx
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DB_PATH = os.environ.get("DB_PATH")
DDB_URI = "https://www.deutsche-digitale-bibliothek.de/item/xml/"
BUF_SIZE = int(os.environ.get("BUF_SIZE", 999))
SELECT_SIZE = int(os.environ.get("SELECT_SIZE", 500000))
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 4))
IMPORT_INTERVAL = int(os.environ.get("IMPORT_INTERVAL", 60))  # in seconds

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "./out/")
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript(
        """CREATE TABLE IF NOT EXISTS objs (uid TEXT PRIMARY KEY, download_timestamp TEXT, xmlbufgz BLOB);
        PRAGMA journal_mode=WAL;"""
    )
    return db


def worker(number, Q):
    while True:
        uid = Q.get()
        if uid is None:
            logging.info(f"Worker {number} None, received stopping.")
            break

        output_filpath = os.path.join(OUTPUT_PATH, uid) + ".xml.gz"
        if os.path.exists(output_filpath):
            continue

        with httpx.Client() as client:
            try:
                uri = DDB_URI + uid
                resp = client.get(uri, follow_redirects=True, timeout=30.0)
                if resp.status_code == 200:
                    open(output_filpath, "wb").write(gzip.compress(resp.content))
                else:
                    logging.error(
                        f"Problem retrieving {uid} status code was {resp.status_code}"
                    )
            except:
                logging.error(f"Problem with {uid}")


def importer(Q):
    db = get_db()
    last_import = time.time()
    keep_going = True
    while not keep_going is None:
        try:
            keep_going = Q.get(timeout=IMPORT_INTERVAL)
            logging.info("Importer: None, received stopping.")
        except queue.Empty:
            pass
        if time.time() - last_import > IMPORT_INTERVAL:
            added = []
            for filename in os.listdir(OUTPUT_PATH):
                if not filename.endswith(".xml.gz"):
                    continue
                filepath = os.path.join(OUTPUT_PATH, filename)
                file_contents = open(filepath, "rb").read()
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime())
                db.execute(
                    "UPDATE objs SET download_timestamp = ?, xmlbufgz = ? WHERE uid = ?",
                    (timestamp, file_contents, filename.replace(".xml.gz", "")),
                )
                added.append(filepath)
            logging.info(f"Committing {len(added)} to database")
            db.commit()
            for filepath in added:
                os.remove(filepath)
            last_import = time.time()


def main():
    db = get_db()

    Q = multiprocessing.Queue()
    QI = multiprocessing.Queue()
    workers = []
    for w in range(WORKER_COUNT):
        wp = multiprocessing.Process(target=worker, daemon=True, args=(w, Q))
        wp.start()
        workers.append(wp)
    importer_p = multiprocessing.Process(target=importer, daemon=True, args=(QI,))
    importer_p.start()

    keep_going = True
    while keep_going:
        uids = [
            uid
            for uid, _ in db.execute(
                f"SELECT uid, download_timestamp FROM objs WHERE xmlbufgz IS NULL LIMIT {SELECT_SIZE}"
            )
        ]
        if len(uids) < 1:
            keep_going = False
        for uid in uids:
            Q.put(uid)
        time.sleep(IMPORT_INTERVAL * 2)
        logging.info(f"Batch of {len(uids)} queued, getting new ones")

    logging.info("Done with Q, waiting for workers to finish")
    for w in range(WORKER_COUNT):
        Q.put(None)
    for w in workers:
        w.join()
    QI.put(None)


if __name__ == "__main__":
    main()
