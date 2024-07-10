import sqlite3, os, sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DB_PATH = os.environ.get("DB_PATH")


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript(
        """CREATE TABLE IF NOT EXISTS objs (uid TEXT PRIMARY KEY, download_timestamp TEXT, xmlbufgz BLOB);
        PRAGMA journal_mode=WAL;"""
    )
    return db


def main():
    db = get_db()
    lines = [(line.strip(),) for line in sys.stdin]
    db.executemany("INSERT OR IGNORE INTO objs VALUES (?, null, null)", lines)
    db.commit()
    logging.info(f"Inserted {len(lines)}")


if __name__ == "__main__":
    main()


# Can be run like this, eg:
# DB_PATH=s2.sqlite python download_prime.py < ~/sec_02_ids_20240709.txt
