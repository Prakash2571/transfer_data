#!/usr/bin/env python3
"""
Split a MongoDB database across THREE destinations.

Default routing (matches the nse_fno use-case):
  * URL1 <- stock_futures documents dated  BEFORE  the cutoff (e.g. up to 2025)
  * URL2 <- stock_futures documents dated  ON/AFTER the cutoff (e.g. 2026 -> now)
  * URL3 <- spread_daily + spread_summary  (whole collections)

Reads everything from your complete local source and loads only the right slice
into each cluster, so each fits comfortably in an Atlas M0 (512 MB) free tier.

Every action is logged with a timestamp and a clear URL1/URL2/URL3 label. Use
--log-file run.log to also save the full log.

Workflow
--------
# 1) Inspect the source (confirm the date field on stock_futures)
python split_three.py --inspect --source "mongodb://localhost:27017"

# 2) Dry-run (writes nothing, shows the routing + counts)
python split_three.py --source "mongodb://localhost:27017" \
    --url1 "mongodb+srv://...A" --url2 "mongodb+srv://...B" --url3 "mongodb+srv://...C" \
    --cutoff 2026-01-01 --date-field opened_at --dry-run

# 3) For real (drop destinations, load slices, copy indexes)
python split_three.py --source "mongodb://localhost:27017" \
    --url1 "mongodb+srv://...A" --url2 "mongodb+srv://...B" --url3 "mongodb+srv://...C" \
    --cutoff 2026-01-01 --date-field opened_at --drop --indexes --log-file run.log

Customising the routing
-----------------------
--date-collections stock_futures         # collection(s) split by date -> URL1/URL2
--whole-collections spread_daily spread_summary   # collection(s) sent whole -> URL3
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


_LOG_FH = None


def log(msg="", *, same_line=False, indent=0):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] " + ("  " * indent) + msg
    if same_line:
        sys.stdout.write("\r" + line)
        sys.stdout.flush()
    else:
        print(line)
    if _LOG_FH:
        _LOG_FH.write(line + "\n")
        _LOG_FH.flush()


def rule(char="=", width=68):
    log(char * width)


def parse_args():
    p = argparse.ArgumentParser(
        description="Split a MongoDB database across three destinations.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--source", default=os.environ.get("SOURCE_URI", "mongodb://localhost:27017"))
    p.add_argument("--source-db", default=os.environ.get("SOURCE_DB", "nse_fno"))
    p.add_argument("--url1", default=os.environ.get("URL1", ""),
                   help="Destination 1: date-split collection, BEFORE cutoff.")
    p.add_argument("--url2", default=os.environ.get("URL2", ""),
                   help="Destination 2: date-split collection, ON/AFTER cutoff.")
    p.add_argument("--url3", default=os.environ.get("URL3", ""),
                   help="Destination 3: whole collections (spread_daily, spread_summary).")
    p.add_argument("--url1-db", default="")
    p.add_argument("--url2-db", default="")
    p.add_argument("--url3-db", default="")
    p.add_argument("--cutoff", default="2026-01-01",
                   help="ISO date boundary (UTC). BEFORE -> URL1, ON/AFTER -> URL2.")
    p.add_argument("--date-field", default=None,
                   help="Date field for the date-split collection(s).")
    p.add_argument("--date-collections", nargs="*", default=["stock_futures"],
                   help="Collection(s) split by date across URL1/URL2.")
    p.add_argument("--whole-collections", nargs="*", default=["spread_daily", "spread_summary"],
                   help="Collection(s) copied whole to URL3.")
    p.add_argument("--undated", choices=["url1", "url2", "skip"], default="url1",
                   help="Where to put date-split docs that MISS the date field.")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--drop", action="store_true",
                   help="Drop each destination collection before writing (reclaims space).")
    p.add_argument("--upsert", action="store_true", help="Upsert by _id (safe re-runs).")
    p.add_argument("--indexes", action="store_true", help="Copy source indexes to destinations.")
    p.add_argument("--inspect", action="store_true", help="Only inspect the source.")
    p.add_argument("--dry-run", action="store_true", help="Show routing/counts, write nothing.")
    p.add_argument("--log-file", default=None, help="Also write the full run log to this file.")
    return p.parse_args()


def host_of(uri):
    if not uri:
        return "(none)"
    rest = uri.split("://", 1)[-1]
    if "@" in rest:
        rest = rest.split("@", 1)[1]
    return rest.split("/")[0].split("?")[0]


def parse_cutoff(s):
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


def detect_date_fields(sample):
    return [k for k, v in sample.items() if isinstance(v, datetime)]


def resolve_date_field(default_field, sample):
    if default_field:
        return default_field
    detected = detect_date_fields(sample or {})
    if len(detected) == 1:
        return detected[0]
    for pref in ("opened_at", "closed_at", "date", "timestamp", "created_at", "ts"):
        if pref in detected:
            return pref
    return detected[0] if detected else None


def human(n):
    return f"{n:,}"


def inspect(src_db, colls):
    rule()
    log("SOURCE INSPECTION")
    rule()
    for name in colls:
        coll = src_db[name]
        total = coll.estimated_document_count()
        sample = coll.find_one() or {}
        date_fields = detect_date_fields(sample)
        log("")
        log(f"[{name}]  {human(total)} document(s)")
        log(f"top-level fields : {list(sample.keys())}", indent=1)
        log(f"datetime fields  : {date_fields or '(none detected in sample)'}", indent=1)
        for f in date_fields:
            lo = next(coll.find({f: {"$type": "date"}}).sort(f, 1).limit(1), {}).get(f)
            hi = next(coll.find({f: {"$type": "date"}}).sort(f, -1).limit(1), {}).get(f)
            missing = coll.count_documents({f: {"$exists": False}})
            log(f"- {f}: min={lo}  max={hi}  missing_in={human(missing)}", indent=2)
    log("")
    log("(inspection only; nothing written)")


def copy_indexes(src_coll, dst_coll):
    copied, failed = 0, 0
    for iname, spec in src_coll.index_information().items():
        if iname == "_id_":
            continue
        options = {k: v for k, v in spec.items() if k not in ("key", "v", "ns")}
        try:
            dst_coll.create_index(spec["key"], name=iname, **options)
            copied += 1
        except PyMongoError as e:
            failed += 1
            log(f"! index '{iname}' failed: {e}", indent=3)
    return copied, failed


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
        log(f"! {n_err} write error(s) (inserted {n_ok}). Consider --upsert/--drop.", indent=3)
        return n_ok


def copy_query(src_coll, dst_coll, dst_label, query, args):
    total = src_coll.count_documents(query)
    if dst_coll is None or args.dry_run:
        tag = "[dry-run, no write]" if args.dry_run else "[skipped]"
        log(f"{dst_label:<16} {human(total):>10} doc(s)  {tag}", indent=2)
        return total
    if total == 0:
        log(f"{dst_label:<16} {human(total):>10} doc(s)  (nothing to copy)", indent=2)
        return 0
    written, batch, start = 0, [], time.time()
    cursor = src_coll.find(query, no_cursor_timeout=True)
    try:
        for doc in cursor:
            batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) if args.upsert else doc)
            if len(batch) >= args.batch_size:
                written += flush(dst_coll, batch, args.upsert)
                batch = []
                _progress(dst_label, written, total, start)
        if batch:
            written += flush(dst_coll, batch, args.upsert)
        _progress(dst_label, written, total, start, final=True)
    finally:
        cursor.close()
    return written


def _progress(dst_label, written, total, start, final=False):
    pct = (written / total * 100) if total else 100
    rate = written / (time.time() - start + 1e-9)
    log(f"{dst_label:<16} {human(written):>10}/{human(total)} ({pct:5.1f}%)  {rate:>8,.0f} docs/s",
        indent=2, same_line=not final)


def connect(uri, db_name, default_db):
    if not uri:
        return None
    c = MongoClient(uri, serverSelectionTimeoutMS=15000)
    c.admin.command("ping")
    return c[db_name or default_db]


def main():
    global _LOG_FH
    args = parse_args()
    if args.log_file:
        _LOG_FH = open(args.log_file, "w", encoding="utf-8")

    cutoff = parse_cutoff(args.cutoff)

    rule()
    log("SPLIT INTO THREE DESTINATIONS")
    rule()
    log(f"source : {host_of(args.source)}  db={args.source_db}")
    log(f"cutoff : {cutoff.isoformat()}")
    log(f"URL1 <- {args.date_collections} BEFORE cutoff")
    log(f"URL2 <- {args.date_collections} ON/AFTER cutoff")
    log(f"URL3 <- {args.whole_collections} (whole)")

    try:
        src_client = MongoClient(args.source, serverSelectionTimeoutMS=8000)
        src_client.admin.command("ping")
    except PyMongoError as e:
        sys.exit(f"Failed to connect to SOURCE: {e}")
    src_db = src_client[args.source_db]
    available = src_db.list_collection_names()

    if args.inspect:
        inspect(src_db, [c for c in (args.date_collections + args.whole_collections)
                         if c in available] or available)
        return

    d1 = d2 = d3 = None
    if not args.dry_run:
        if not any([args.url1, args.url2, args.url3]):
            sys.exit("Provide at least one of --url1/--url2/--url3 (or use --dry-run).")
        d1 = connect(args.url1, args.url1_db, args.source_db)
        d2 = connect(args.url2, args.url2_db, args.source_db)
        d3 = connect(args.url3, args.url3_db, args.source_db)
    log(f"URL1   : {host_of(args.url1)}  db={args.url1_db or args.source_db}")
    log(f"URL2   : {host_of(args.url2)}  db={args.url2_db or args.source_db}")
    log(f"URL3   : {host_of(args.url3)}  db={args.url3_db or args.source_db}")
    if args.dry_run:
        log("mode   : DRY-RUN (no data will be written)")

    summary = {}  # name -> {"URL1":n,"URL2":n,"URL3":n}
    run_start = time.time()

    # ---- date-split collections -> URL1 / URL2 ----
    for name in args.date_collections:
        summary[name] = {"URL1": 0, "URL2": 0, "URL3": 0}
        if name not in available:
            log("")
            log(f"COLLECTION [{name}]  -- not found in source, skipping")
            continue
        src_coll = src_db[name]
        field = resolve_date_field(args.date_field, src_coll.find_one() or {})
        log("")
        rule("-")
        log(f"COLLECTION [{name}]  (date-split)   field: {field or '(none)'}")
        rule("-")
        if not field:
            log("! no date field found; specify --date-field. Skipping this collection.", indent=1)
            continue
        c1 = d1[name] if d1 is not None else None
        c2 = d2[name] if d2 is not None else None
        if args.drop and not args.dry_run:
            if c1 is not None:
                c1.drop()
            if c2 is not None:
                c2.drop()
            log("dropped destination collection(s) on URL1/URL2", indent=1)
        summary[name]["URL1"] += copy_query(src_coll, c1, "before->URL1", {field: {"$lt": cutoff}}, args)
        summary[name]["URL2"] += copy_query(src_coll, c2, "on/after->URL2", {field: {"$gte": cutoff}}, args)
        missing_q = {field: {"$exists": False}}
        n_missing = src_coll.count_documents(missing_q)
        if n_missing:
            log(f"{human(n_missing)} doc(s) missing '{field}' -> {args.undated}", indent=1)
            if args.undated == "url1":
                summary[name]["URL1"] += copy_query(src_coll, c1, "undated->URL1", missing_q, args)
            elif args.undated == "url2":
                summary[name]["URL2"] += copy_query(src_coll, c2, "undated->URL2", missing_q, args)
        if args.indexes and not args.dry_run:
            if c1 is not None:
                cc, ff = copy_indexes(src_coll, c1)
                log(f"URL1 indexes copied: {cc}" + (f" ({ff} failed)" if ff else ""), indent=1)
            if c2 is not None:
                cc, ff = copy_indexes(src_coll, c2)
                log(f"URL2 indexes copied: {cc}" + (f" ({ff} failed)" if ff else ""), indent=1)

    # ---- whole collections -> URL3 ----
    for name in args.whole_collections:
        summary[name] = {"URL1": 0, "URL2": 0, "URL3": 0}
        if name not in available:
            log("")
            log(f"COLLECTION [{name}]  -- not found in source, skipping")
            continue
        src_coll = src_db[name]
        log("")
        rule("-")
        log(f"COLLECTION [{name}]  (whole -> URL3)")
        rule("-")
        c3 = d3[name] if d3 is not None else None
        if args.drop and not args.dry_run and c3 is not None:
            c3.drop()
            log("dropped destination collection on URL3", indent=1)
        summary[name]["URL3"] += copy_query(src_coll, c3, "whole->URL3", {}, args)
        if args.indexes and not args.dry_run and c3 is not None:
            cc, ff = copy_indexes(src_coll, c3)
            log(f"URL3 indexes copied: {cc}" + (f" ({ff} failed)" if ff else ""), indent=1)

    # ---- summary ----
    log("")
    rule()
    log("SUMMARY  (documents routed per collection)")
    rule()
    log(f"{'collection':<20}{'-> URL1':>12}{'-> URL2':>12}{'-> URL3':>12}{'total':>12}")
    log("-" * 68)
    t1 = t2 = t3 = 0
    for name, s in summary.items():
        t1 += s["URL1"]; t2 += s["URL2"]; t3 += s["URL3"]
        tot = s["URL1"] + s["URL2"] + s["URL3"]
        log(f"{name:<20}{human(s['URL1']):>12}{human(s['URL2']):>12}{human(s['URL3']):>12}{human(tot):>12}")
    log("-" * 68)
    log(f"{'TOTAL':<20}{human(t1):>12}{human(t2):>12}{human(t3):>12}{human(t1 + t2 + t3):>12}")
    log("")
    log(f"URL1 ({host_of(args.url1)}) received {human(t1)} document(s)")
    log(f"URL2 ({host_of(args.url2)}) received {human(t2)} document(s)")
    log(f"URL3 ({host_of(args.url3)}) received {human(t3)} document(s)")
    log(f"elapsed: {time.time() - run_start:.1f}s")
    if args.dry_run:
        log("mode: DRY-RUN -> nothing was actually written")
    if _LOG_FH:
        log(f"full log saved to: {args.log_file}")
        _LOG_FH.close()


if __name__ == "__main__":
    main()
