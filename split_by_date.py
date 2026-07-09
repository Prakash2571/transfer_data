#!/usr/bin/env python3
"""
Split a MongoDB database BY DATE into two destinations.

Reads from a single source (your complete local DB) and writes:
  * URL1  <-  documents dated  BEFORE  the cutoff   (e.g. up to 31 Dec 2025)
  * URL2  <-  documents dated  ON/AFTER the cutoff   (e.g. 1 Jan 2026 -> now)

Why rebuild instead of "delete 2026 docs from URL1"?
  On MongoDB Atlas free tier (M0), deleting documents does NOT return disk
  space (WiredTiger keeps it, and M0 can't run `compact`). DROPPING a
  collection and re-importing only the data you want DOES reclaim space.
  So this script drops each destination collection (with --drop) and loads
  only the matching slice. That is what actually frees your quota.

Typical workflow
----------------
# 1) Inspect: see each collection's date fields + min/max so you pick correctly
python split_by_date.py --inspect --source "mongodb://localhost:27017"

# 2) Dry run: preview how many docs go to each side (writes nothing)
python split_by_date.py \
    --source "mongodb://localhost:27017" \
    --url1 "mongodb+srv://...fnodata" \
    --url2 "mongodb+srv://...fnodata2" \
    --cutoff 2026-01-01 \
    --date-field opened_at \
    --dry-run

# 3) For real: drop destinations, load the slices, copy indexes to both
python split_by_date.py \
    --source "mongodb://localhost:27017" \
    --url1 "mongodb+srv://...fnodata" \
    --url2 "mongodb+srv://...fnodata2" \
    --cutoff 2026-01-01 \
    --date-field opened_at \
    --drop --indexes

Per-collection date fields
--------------------------
If collections use different date fields, pass a mapping:
    --date-fields "stock_futures=opened_at,spread_daily=date"
Anything not in the mapping falls back to --date-field, then to auto-detect.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

try:
    from pymongo import MongoClient, ReplaceOne
    from pymongo.errors import BulkWriteError, PyMongoError
except ImportError:
    sys.exit("pymongo not installed. Run: pip install -r requirements.txt")


def parse_args():
    p = argparse.ArgumentParser(
        description="Split a MongoDB database into two destinations by a date field.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--source", default=os.environ.get("SOURCE_URI", "mongodb://localhost:27017"),
                   help="Source connection string (your complete data).")
    p.add_argument("--source-db", default=os.environ.get("SOURCE_DB", "nse_fno"),
                   help="Source database name.")
    p.add_argument("--url1", default=os.environ.get("URL1", ""),
                   help="Destination 1 URI: gets docs BEFORE the cutoff.")
    p.add_argument("--url2", default=os.environ.get("URL2", ""),
                   help="Destination 2 URI: gets docs ON/AFTER the cutoff.")
    p.add_argument("--url1-db", default="", help="DB name on URL1 (defaults to source db).")
    p.add_argument("--url2-db", default="", help="DB name on URL2 (defaults to source db).")
    p.add_argument("--cutoff", default="2026-01-01",
                   help="ISO date/datetime boundary (UTC). BEFORE -> url1, ON/AFTER -> url2.")
    p.add_argument("--date-field", default=None,
                   help="Default date field to split on for all collections.")
    p.add_argument("--date-fields", default=None,
                   help="Per-collection overrides, e.g. 'stock_futures=opened_at,spread_daily=date'.")
    p.add_argument("--collections", nargs="*", default=None,
                   help="Only process these collections. Default: all.")
    p.add_argument("--undated", choices=["url1", "url2", "both", "skip"], default="url1",
                   help="Where to put docs that MISS the date field.")
    p.add_argument("--no-date-target", choices=["url1", "url2", "both", "skip"], default="url1",
                   help="For collections with NO usable date field at all.")
    p.add_argument("--batch-size", type=int, default=1000, help="Docs per write batch.")
    p.add_argument("--drop", action="store_true",
                   help="Drop each destination collection before writing (reclaims space).")
    p.add_argument("--upsert", action="store_true",
                   help="Upsert by _id instead of insert (safe re-runs).")
    p.add_argument("--indexes", action="store_true",
                   help="Copy source indexes to both destinations.")
    p.add_argument("--inspect", action="store_true",
                   help="Only inspect the source: show fields, detected date fields, min/max.")
    p.add_argument("--dry-run", action="store_true",
                   help="Show split counts without writing.")
    return p.parse_args()


def redact(uri):
    if not uri or "@" not in uri:
        return uri
    scheme, rest = uri.split("://", 1) if "://" in uri else ("", uri)
    creds, host = rest.split("@", 1)
    return f"{scheme}://***:***@{host}" if scheme else f"***:***@{host}"


def parse_cutoff(s):
    # Accept 'YYYY-MM-DD' or full ISO; treat naive as UTC.
    s = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"Could not parse --cutoff '{s}'. Use YYYY-MM-DD or ISO 8601.")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_field_map(s):
    m = {}
    if not s:
        return m
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            sys.exit(f"Bad --date-fields entry '{part}'. Use coll=field,coll2=field2.")
        k, v = part.split("=", 1)
        m[k.strip()] = v.strip()
    return m


def detect_date_fields(sample):
    """Return top-level field names in a sample doc whose value is a datetime."""
    return [k for k, v in sample.items() if isinstance(v, datetime)]


def resolve_date_field(coll_name, field_map, default_field, sample):
    if coll_name in field_map:
        return field_map[coll_name]
    if default_field:
        return default_field
    detected = detect_date_fields(sample or {})
    if len(detected) == 1:
        return detected[0]
    # Prefer common names when several are present.
    for pref in ("opened_at", "closed_at", "date", "timestamp", "created_at", "ts"):
        if pref in detected:
            return pref
    return detected[0] if detected else None


def human(n):
    return f"{n:,}"


def inspect(src_db, colls):
    print("\nSOURCE INSPECTION")
    print("=================")
    for name in colls:
        coll = src_db[name]
        total = coll.estimated_document_count()
        sample = coll.find_one() or {}
        date_fields = detect_date_fields(sample)
        print(f"\n[{name}]  {human(total)} document(s)")
        print(f"  top-level fields : {list(sample.keys())}")
        print(f"  datetime fields  : {date_fields or '(none detected in sample)'}")
        for f in date_fields:
            lo = coll.find({f: {"$type": "date"}}).sort(f, 1).limit(1)
            hi = coll.find({f: {"$type": "date"}}).sort(f, -1).limit(1)
            lo = next(lo, {}).get(f)
            hi = next(hi, {}).get(f)
            missing = coll.count_documents({f: {"$exists": False}})
            print(f"    - {f}: min={lo}  max={hi}  missing_in={human(missing)}")


def copy_indexes(src_coll, dst_coll):
    copied = 0
    for iname, spec in src_coll.index_information().items():
        if iname == "_id_":
            continue
        keys = spec["key"]
        options = {k: v for k, v in spec.items() if k not in ("key", "v", "ns")}
        try:
            dst_coll.create_index(keys, name=iname, **options)
            copied += 1
        except PyMongoError as e:
            print(f"      ! index '{iname}' failed: {e}")
    return copied


def flush(dst_coll, batch, upsert):
    try:
        if upsert:
            res = dst_coll.bulk_write(batch, ordered=False)
            return res.upserted_count + res.modified_count + res.matched_count
        res = dst_coll.insert_many(batch, ordered=False)
        return len(res.inserted_ids)
    except BulkWriteError as bwe:
        n_ok = bwe.details.get("nInserted", 0) + bwe.details.get("nUpserted", 0)
        n_err = len(bwe.details.get("writeErrors", []))
        print(f"      ! {n_err} write error(s) (inserted {n_ok}). Consider --upsert/--drop.")
        return n_ok


def copy_query(src_coll, dst_coll, query, args, label):
    total = src_coll.count_documents(query)
    print(f"    -> {label}: {human(total)} doc(s)", end="", flush=True)
    if args.dry_run or dst_coll is None or total == 0:
        print(" [no write]" if (args.dry_run or dst_coll is None) else "")
        return total
    written, batch = 0, []
    cursor = src_coll.find(query, no_cursor_timeout=True)
    try:
        for doc in cursor:
            batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) if args.upsert else doc)
            if len(batch) >= args.batch_size:
                written += flush(dst_coll, batch, args.upsert)
                batch = []
        if batch:
            written += flush(dst_coll, batch, args.upsert)
    finally:
        cursor.close()
    print(f"  -> wrote {human(written)}")
    return written


def targets_for(choice, d1, d2):
    return {"url1": [d1], "url2": [d2], "both": [d1, d2], "skip": []}[choice]


def main():
    args = parse_args()
    cutoff = parse_cutoff(args.cutoff)
    field_map = parse_field_map(args.date_fields)

    print("Split-by-date")
    print("=============")
    print(f"source : {redact(args.source)}  db={args.source_db}")
    print(f"cutoff : {cutoff.isoformat()}  (before -> URL1, on/after -> URL2)")

    try:
        src_client = MongoClient(args.source, serverSelectionTimeoutMS=8000)
        src_client.admin.command("ping")
    except PyMongoError as e:
        sys.exit(f"\nFailed to connect to SOURCE: {e}")
    src_db = src_client[args.source_db]

    all_colls = src_db.list_collection_names()
    colls = args.collections or all_colls
    missing = [c for c in colls if c not in all_colls]
    if missing:
        sys.exit(f"\nCollections not found: {missing}. Available: {all_colls}")

    if args.inspect:
        inspect(src_db, colls)
        print("\n(inspection only; nothing written)")
        return

    # Connect destinations (unless dry-run)
    d1_db = d2_db = None
    if not args.dry_run:
        if not args.url1 and not args.url2:
            sys.exit("\nProvide --url1 and/or --url2 (or use --dry-run).")
        if args.url1:
            c1 = MongoClient(args.url1, serverSelectionTimeoutMS=15000)
            c1.admin.command("ping")
            d1_db = c1[args.url1_db or args.source_db]
        if args.url2:
            c2 = MongoClient(args.url2, serverSelectionTimeoutMS=15000)
            c2.admin.command("ping")
            d2_db = c2[args.url2_db or args.source_db]
    print(f"URL1   : {redact(args.url1) or '(none)'}")
    print(f"URL2   : {redact(args.url2) or '(none)'}")

    grand1 = grand2 = 0
    for name in colls:
        src_coll = src_db[name]
        sample = src_coll.find_one() or {}
        field = resolve_date_field(name, field_map, args.date_field, sample)
        d1 = d1_db[name] if d1_db is not None else None
        d2 = d2_db[name] if d2_db is not None else None
        print(f"\n[{name}]  date field: {field or '(none)'}")

        if args.drop and not args.dry_run:
            if d1 is not None:
                d1.drop()
            if d2 is not None:
                d2.drop()
            print("    dropped destination collection(s)")

        if not field:
            # No usable date field: route whole collection per --no-date-target
            for dst in targets_for(args.no_date_target, d1, d2):
                grand = copy_query(src_coll, dst, {}, args,
                                   f"whole -> {'URL1' if dst is d1 else 'URL2'}")
                if dst is d1:
                    grand1 += grand
                else:
                    grand2 += grand
            if args.no_date_target == "skip":
                print("    skipped (no date field)")
        else:
            before_q = {field: {"$lt": cutoff}}
            after_q = {field: {"$gte": cutoff}}
            grand1 += copy_query(src_coll, d1, before_q, args, "before cutoff -> URL1")
            grand2 += copy_query(src_coll, d2, after_q, args, "on/after cutoff -> URL2")

            # Docs missing the date field
            missing_q = {field: {"$exists": False}}
            n_missing = src_coll.count_documents(missing_q)
            if n_missing:
                print(f"    note: {human(n_missing)} doc(s) missing '{field}' "
                      f"-> routed to {args.undated}")
                for dst in targets_for(args.undated, d1, d2):
                    g = copy_query(src_coll, dst, missing_q, args,
                                   f"undated -> {'URL1' if dst is d1 else 'URL2'}")
                    if dst is d1:
                        grand1 += g
                    else:
                        grand2 += g

        if args.indexes and not args.dry_run:
            if d1 is not None:
                print(f"    URL1 indexes copied: {copy_indexes(src_coll, d1)}")
            if d2 is not None:
                print(f"    URL2 indexes copied: {copy_indexes(src_coll, d2)}")

    print(f"\nDone. Documents to URL1: {human(grand1)}   URL2: {human(grand2)}")
    if args.dry_run:
        print("(dry-run: nothing was written)")


if __name__ == "__main__":
    main()
