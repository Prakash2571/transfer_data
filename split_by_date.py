#!/usr/bin/env python3
"""
Split a MongoDB database BY DATE into two destinations, with clear logging.

Reads from a single source (your complete local DB) and writes:
  * URL1  <-  documents dated  BEFORE  the cutoff   (e.g. up to 31 Dec 2025)
  * URL2  <-  documents dated  ON/AFTER the cutoff   (e.g. 1 Jan 2026 -> now)

Why rebuild instead of "delete 2026 docs from URL1"?
  On MongoDB Atlas free tier (M0), deleting documents does NOT return disk
  space (WiredTiger keeps it, and M0 can't run `compact`). DROPPING a
  collection and re-importing only the data you want DOES reclaim space.

Every action is logged with a timestamp and clearly labelled URL1 / URL2 so you
can watch exactly where each document goes. Add --log-file run.log to also save
the full log to a file you can review afterwards.

Typical workflow
----------------
# 1) Inspect: see each collection's date fields + min/max so you pick correctly
python split_by_date.py --inspect --source "mongodb://localhost:27017"

# 2) Dry run: preview how many docs go to each side (writes nothing)
python split_by_date.py --source "mongodb://localhost:27017" \
    --url1 "mongodb+srv://...fnodata" --url2 "mongodb+srv://...fnodata2" \
    --cutoff 2026-01-01 --date-field opened_at --dry-run

# 3) For real: drop destinations, load the slices, copy indexes to both
python split_by_date.py --source "mongodb://localhost:27017" \
    --url1 "mongodb+srv://...fnodata" --url2 "mongodb+srv://...fnodata2" \
    --cutoff 2026-01-01 --date-field opened_at --drop --indexes --log-file run.log
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


# --------------------------------------------------------------------------- #
# Logging: timestamped lines, optionally tee'd to a file.
# --------------------------------------------------------------------------- #
_LOG_FH = None


def log(msg="", *, same_line=False, indent=0):
    """Print a timestamped, indented log line (and mirror it to the log file)."""
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = f"[{ts}] " + ("  " * indent)
    line = prefix + msg
    if same_line:
        sys.stdout.write("\r" + line)
        sys.stdout.flush()
    else:
        print(line)
    if _LOG_FH:
        _LOG_FH.write(line + "\n")
        _LOG_FH.flush()


def rule(char="=", width=64):
    log(char * width)


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
    p.add_argument("--log-file", default=None,
                   help="Also write the full run log to this file.")
    return p.parse_args()


def redact(uri):
    if not uri or "@" not in uri:
        return uri
    scheme, rest = uri.split("://", 1) if "://" in uri else ("", uri)
    creds, host = rest.split("@", 1)
    return f"{scheme}://***:***@{host}" if scheme else f"***:***@{host}"


def host_of(uri):
    """Short host label for logs, e.g. 'cluster0.abcde.mongodb.net'."""
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
    return [k for k, v in sample.items() if isinstance(v, datetime)]


def resolve_date_field(coll_name, field_map, default_field, sample):
    if coll_name in field_map:
        return field_map[coll_name]
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
        keys = spec["key"]
        options = {k: v for k, v in spec.items() if k not in ("key", "v", "ns")}
        try:
            dst_coll.create_index(keys, name=iname, **options)
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
    """Copy docs matching `query` from src to dst, logging live progress."""
    total = src_coll.count_documents(query)

    if dst_coll is None or args.dry_run:
        tag = "[dry-run, no write]" if args.dry_run else "[skipped]"
        log(f"{dst_label:<14} {human(total):>10} doc(s)  {tag}", indent=2)
        return total
    if total == 0:
        log(f"{dst_label:<14} {human(total):>10} doc(s)  (nothing to copy)", indent=2)
        return 0

    written, batch = 0, []
    start = time.time()
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
    msg = (f"{dst_label:<14} {human(written):>10}/{human(total)} "
           f"({pct:5.1f}%)  {rate:>8,.0f} docs/s")
    log(msg, indent=2, same_line=not final)
    if final:
        return
    # keep the carriage-return line clean; newline printed on final call


def targets_for(choice, d1, d2):
    return {"url1": [d1], "url2": [d2], "both": [d1, d2], "skip": []}[choice]


def main():
    global _LOG_FH
    args = parse_args()
    if args.log_file:
        _LOG_FH = open(args.log_file, "w", encoding="utf-8")

    cutoff = parse_cutoff(args.cutoff)
    field_map = parse_field_map(args.date_fields)

    rule()
    log("SPLIT-BY-DATE")
    rule()
    log(f"source : {host_of(args.source)}  db={args.source_db}")
    log(f"cutoff : {cutoff.isoformat()}")
    log("routing: dated BEFORE cutoff -> URL1   |   dated ON/AFTER cutoff -> URL2")

    try:
        src_client = MongoClient(args.source, serverSelectionTimeoutMS=8000)
        src_client.admin.command("ping")
    except PyMongoError as e:
        sys.exit(f"Failed to connect to SOURCE: {e}")
    src_db = src_client[args.source_db]

    all_colls = src_db.list_collection_names()
    colls = args.collections or all_colls
    missing = [c for c in colls if c not in all_colls]
    if missing:
        sys.exit(f"Collections not found: {missing}. Available: {all_colls}")

    if args.inspect:
        inspect(src_db, colls)
        return

    d1_db = d2_db = None
    if not args.dry_run:
        if not args.url1 and not args.url2:
            sys.exit("Provide --url1 and/or --url2 (or use --dry-run).")
        if args.url1:
            c1 = MongoClient(args.url1, serverSelectionTimeoutMS=15000)
            c1.admin.command("ping")
            d1_db = c1[args.url1_db or args.source_db]
        if args.url2:
            c2 = MongoClient(args.url2, serverSelectionTimeoutMS=15000)
            c2.admin.command("ping")
            d2_db = c2[args.url2_db or args.source_db]
    log(f"URL1   : {host_of(args.url1)}  db={args.url1_db or args.source_db}")
    log(f"URL2   : {host_of(args.url2)}  db={args.url2_db or args.source_db}")
    if args.dry_run:
        log("mode   : DRY-RUN (no data will be written)")

    # summary[collection] = {"URL1": n, "URL2": n}
    summary = {}
    run_start = time.time()

    for name in colls:
        src_coll = src_db[name]
        sample = src_coll.find_one() or {}
        field = resolve_date_field(name, field_map, args.date_field, sample)
        d1 = d1_db[name] if d1_db is not None else None
        d2 = d2_db[name] if d2_db is not None else None
        summary[name] = {"URL1": 0, "URL2": 0}

        log("")
        rule("-")
        log(f"COLLECTION [{name}]   split field: {field or '(none)'}")
        rule("-")

        if args.drop and not args.dry_run:
            if d1 is not None:
                d1.drop()
            if d2 is not None:
                d2.drop()
            log("dropped destination collection(s) on URL1/URL2", indent=1)

        if not field:
            log(f"no date field -> whole collection goes to: {args.no_date_target}", indent=1)
            for dst in targets_for(args.no_date_target, d1, d2):
                which = "URL1" if dst is d1 else "URL2"
                n = copy_query(src_coll, dst, f"whole->{which}", {}, args)
                summary[name][which] += n
        else:
            n1 = copy_query(src_coll, d1, "before->URL1", {field: {"$lt": cutoff}}, args)
            n2 = copy_query(src_coll, d2, "on/after->URL2", {field: {"$gte": cutoff}}, args)
            summary[name]["URL1"] += n1
            summary[name]["URL2"] += n2

            missing_q = {field: {"$exists": False}}
            n_missing = src_coll.count_documents(missing_q)
            if n_missing:
                log(f"{human(n_missing)} doc(s) missing '{field}' -> routed to {args.undated}", indent=1)
                for dst in targets_for(args.undated, d1, d2):
                    which = "URL1" if dst is d1 else "URL2"
                    n = copy_query(src_coll, dst, f"undated->{which}", missing_q, args)
                    summary[name][which] += n

        if args.indexes and not args.dry_run:
            if d1 is not None:
                c, f = copy_indexes(src_coll, d1)
                log(f"URL1 indexes copied: {c}" + (f" ({f} failed)" if f else ""), indent=1)
            if d2 is not None:
                c, f = copy_indexes(src_coll, d2)
                log(f"URL2 indexes copied: {c}" + (f" ({f} failed)" if f else ""), indent=1)

    # ---- final summary table ----
    log("")
    rule()
    log("SUMMARY  (documents routed per collection)")
    rule()
    log(f"{'collection':<22}{'-> URL1':>14}{'-> URL2':>14}{'total':>12}")
    log("-" * 64)
    t1 = t2 = 0
    for name, s in summary.items():
        t1 += s["URL1"]
        t2 += s["URL2"]
        log(f"{name:<22}{human(s['URL1']):>14}{human(s['URL2']):>14}{human(s['URL1'] + s['URL2']):>12}")
    log("-" * 64)
    log(f"{'TOTAL':<22}{human(t1):>14}{human(t2):>14}{human(t1 + t2):>12}")
    log("")
    log(f"URL1 ({host_of(args.url1)}) received {human(t1)} document(s)")
    log(f"URL2 ({host_of(args.url2)}) received {human(t2)} document(s)")
    log(f"elapsed: {time.time() - run_start:.1f}s")
    if args.dry_run:
        log("mode: DRY-RUN -> nothing was actually written")
    if _LOG_FH:
        log(f"full log saved to: {args.log_file}")
        _LOG_FH.close()


if __name__ == "__main__":
    main()
