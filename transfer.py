#!/usr/bin/env python3
"""
Transfer MongoDB data from a local MongoDB instance to MongoDB Atlas (or any
remote MongoDB), collection by collection, in batches.

Runs on YOUR machine because your source data lives at localhost:27017.

Usage examples
--------------
# Transfer the whole `nse_fno` database using env vars / defaults:
    python transfer.py

# Provide the Atlas string on the command line:
    python transfer.py --dest "mongodb+srv://user:pass@cluster.xxxx.mongodb.net"

# Only specific collections:
    python transfer.py --collections stock_futures spread_daily

# Wipe each destination collection before copying (fresh import):
    python transfer.py --drop

# Idempotent re-run using _id upserts instead of plain inserts:
    python transfer.py --upsert

See README.md for full details.
"""

import argparse
import os
import sys
import time

try:
    from pymongo import MongoClient, ReplaceOne
    from pymongo.errors import BulkWriteError, PyMongoError
except ImportError:
    sys.exit(
        "pymongo is not installed.\n"
        "Install it with:  pip install -r requirements.txt   (or: pip install pymongo)"
    )


DEFAULT_SOURCE_URI = os.environ.get("SOURCE_URI", "mongodb://localhost:27017")
DEFAULT_DEST_URI = os.environ.get("DEST_URI", "")
DEFAULT_SOURCE_DB = os.environ.get("SOURCE_DB", "nse_fno")
DEFAULT_DEST_DB = os.environ.get("DEST_DB", "")  # falls back to source db name


def parse_args():
    p = argparse.ArgumentParser(
        description="Copy a MongoDB database from localhost to Atlas (or any remote).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--source", default=DEFAULT_SOURCE_URI,
                   help="Source MongoDB connection string.")
    p.add_argument("--dest", default=DEFAULT_DEST_URI,
                   help="Destination (Atlas) connection string. Or set DEST_URI env var.")
    p.add_argument("--source-db", default=DEFAULT_SOURCE_DB,
                   help="Database name to read from.")
    p.add_argument("--dest-db", default=DEFAULT_DEST_DB,
                   help="Database name to write to (defaults to --source-db).")
    p.add_argument("--collections", nargs="*", default=None,
                   help="Specific collection names to transfer. Default: all collections.")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Number of documents per write batch.")
    p.add_argument("--drop", action="store_true",
                   help="Drop each destination collection before copying.")
    p.add_argument("--upsert", action="store_true",
                   help="Upsert by _id (safe to re-run) instead of plain insert.")
    p.add_argument("--indexes", action="store_true",
                   help="Also copy index definitions from source collections.")
    p.add_argument("--dry-run", action="store_true",
                   help="Connect and count documents but write nothing.")
    return p.parse_args()


def human(n):
    return f"{n:,}"


def copy_indexes(src_coll, dst_coll):
    copied = 0
    for name, spec in src_coll.index_information().items():
        if name == "_id_":
            continue  # created automatically
        keys = spec["key"]  # list of (field, direction)
        options = {k: v for k, v in spec.items()
                   if k not in ("key", "v", "ns")}
        try:
            dst_coll.create_index(keys, name=name, **options)
            copied += 1
        except PyMongoError as e:
            print(f"      ! could not create index '{name}': {e}")
    return copied


def transfer_collection(src_coll, dst_coll, args):
    total = src_coll.estimated_document_count()
    name = src_coll.name
    print(f"\n-> Collection '{name}': {human(total)} document(s)")

    if args.dry_run:
        print("   [dry-run] skipping write")
        return 0

    if args.drop:
        dst_coll.drop()
        print("   dropped destination collection")

    written = 0
    batch = []
    start = time.time()

    cursor = src_coll.find({}, no_cursor_timeout=True)
    try:
        for doc in cursor:
            if args.upsert:
                batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            else:
                batch.append(doc)

            if len(batch) >= args.batch_size:
                written += flush(dst_coll, batch, args.upsert)
                batch = []
                _progress(written, total, start)

        if batch:
            written += flush(dst_coll, batch, args.upsert)
            _progress(written, total, start)
    finally:
        cursor.close()

    if args.indexes:
        n = copy_indexes(src_coll, dst_coll)
        print(f"   copied {n} index(es)")

    print(f"   done: {human(written)} document(s) in {time.time() - start:.1f}s")
    return written


def flush(dst_coll, batch, upsert):
    try:
        if upsert:
            res = dst_coll.bulk_write(batch, ordered=False)
            return res.upserted_count + res.modified_count + res.matched_count
        else:
            res = dst_coll.insert_many(batch, ordered=False)
            return len(res.inserted_ids)
    except BulkWriteError as bwe:
        # Report but keep going; typically duplicate-key errors on re-runs.
        n_ok = bwe.details.get("nInserted", 0) + bwe.details.get("nUpserted", 0)
        n_err = len(bwe.details.get("writeErrors", []))
        print(f"      ! {n_err} write error(s) in batch (inserted {n_ok}). "
              f"Consider --upsert or --drop to re-run cleanly.")
        return n_ok


def _progress(written, total, start):
    pct = (written / total * 100) if total else 0
    rate = written / (time.time() - start + 1e-9)
    sys.stdout.write(
        f"\r   progress: {human(written)}/{human(total)} "
        f"({pct:5.1f}%)  {rate:,.0f} docs/s"
    )
    sys.stdout.flush()
    if written >= total:
        sys.stdout.write("\n")


def main():
    args = parse_args()

    if not args.dest and not args.dry_run:
        sys.exit(
            "No destination connection string provided.\n"
            "Pass it with --dest \"mongodb+srv://...\" or set the DEST_URI env var."
        )

    dest_db_name = args.dest_db or args.source_db

    print("MongoDB transfer")
    print("================")
    print(f"source : {redact(args.source)}  db={args.source_db}")
    print(f"dest   : {redact(args.dest) or '(dry-run, none)'}  db={dest_db_name}")

    # --- connect source ---
    try:
        src_client = MongoClient(args.source, serverSelectionTimeoutMS=8000)
        src_client.admin.command("ping")
    except PyMongoError as e:
        sys.exit(f"\nFailed to connect to SOURCE ({redact(args.source)}): {e}")

    src_db = src_client[args.source_db]

    # --- connect destination (unless dry-run) ---
    dst_db = None
    if not args.dry_run:
        try:
            dst_client = MongoClient(args.dest, serverSelectionTimeoutMS=15000)
            dst_client.admin.command("ping")
        except PyMongoError as e:
            sys.exit(f"\nFailed to connect to DESTINATION: {e}")
        dst_db = dst_client[dest_db_name]

    # --- resolve collections ---
    all_colls = src_db.list_collection_names()
    colls = args.collections if args.collections else all_colls
    missing = [c for c in colls if c not in all_colls]
    if missing:
        sys.exit(f"\nThese collections do not exist in '{args.source_db}': {missing}\n"
                 f"Available: {all_colls}")

    if not colls:
        sys.exit(f"\nNo collections found in database '{args.source_db}'.")

    print(f"\nCollections to transfer ({len(colls)}): {colls}")

    grand_total = 0
    for name in colls:
        src_coll = src_db[name]
        dst_coll = dst_db[name] if dst_db is not None else None
        grand_total += transfer_collection(src_coll, dst_coll, args)

    print(f"\nAll done. Total documents written: {human(grand_total)}")


def redact(uri):
    """Hide credentials when printing a connection string."""
    if not uri or "@" not in uri:
        return uri
    scheme, rest = uri.split("://", 1) if "://" in uri else ("", uri)
    creds, host = rest.split("@", 1)
    return f"{scheme}://***:***@{host}" if scheme else f"***:***@{host}"


if __name__ == "__main__":
    main()
