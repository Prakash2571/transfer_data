# transfer_data

Copy a MongoDB database from your **local** MongoDB (`localhost:27017`) to
**MongoDB Atlas** (free tier) or any remote MongoDB.

Built for migrating the `nse_fno` database (collections `stock_futures`,
`spread_daily`, `spread_summary`, ...), but works with any database.

> ⚠️ Run this on **your own machine** — the one where your local MongoDB is
> running. The script connects to `localhost:27017` for the source and to your
> Atlas cluster for the destination.

## 1. Prerequisites

- Python 3.10+ installed
- Your local MongoDB running on `localhost:27017`
- A MongoDB Atlas free-tier cluster and its connection string

## 2. Install

```bash
pip install -r requirements.txt
```

## 3. Get your Atlas connection string

In the Atlas UI: **Database → Connect → Drivers**. It looks like:

```
mongodb+srv://<user>:<password>@<cluster>.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

Also make sure:
- Your Atlas **Database User** exists (Database Access) with a password.
- Your current IP is allowed under **Network Access** (or add `0.0.0.0/0`
  temporarily for a one-off migration, then remove it).

## 4. Run

Simplest — pass the destination string on the command line:

```bash
python transfer.py --dest "mongodb+srv://user:pass@cluster.xxxxx.mongodb.net"
```

Or put your strings in a `.env` (copy from `.env.example`) and export them:

```bash
export $(grep -v '^#' .env | xargs)
python transfer.py
```

## Common options

| Option                 | What it does                                                        |
|------------------------|---------------------------------------------------------------------|
| `--dest "<uri>"`       | Atlas connection string (or set `DEST_URI`).                        |
| `--source-db nse_fno`  | Source database name (default `nse_fno`).                           |
| `--dest-db nse_fno`    | Target db name on Atlas (defaults to source db name).               |
| `--collections a b c`  | Only transfer these collections. Default: all.                     |
| `--drop`               | Empty each destination collection before copying (clean import).    |
| `--upsert`             | Upsert by `_id` — safe to re-run without duplicate errors.          |
| `--indexes`            | Also recreate the source indexes on the destination.                |
| `--batch-size 1000`    | Docs per write batch (tune for speed vs. memory).                   |
| `--dry-run`            | Connect + count only; writes nothing.                               |

### Recommended first run

```bash
# See what would be transferred, no writes:
python transfer.py --dry-run

# Then do a clean import with indexes:
python transfer.py --dest "mongodb+srv://..." --drop --indexes
```

### Re-running safely

If you may run it more than once, use `--upsert` so existing documents are
replaced instead of causing duplicate-key errors:

```bash
python transfer.py --dest "mongodb+srv://..." --upsert
```

## Notes

- ObjectIds, dates, nested objects, and all BSON types are preserved (they are
  copied as-is via the driver — no JSON conversion).
- The script prints progress per collection and redacts credentials in its logs.
- Atlas free tier (M0) has a 512 MB storage limit — check your data size fits.

## Alternative: mongodump / mongorestore

If you have the MongoDB Database Tools installed you can also do:

```bash
mongodump --uri="mongodb://localhost:27017" --db=nse_fno --out=./dump
mongorestore --uri="mongodb+srv://user:pass@cluster.xxxxx.mongodb.net" --db=nse_fno ./dump/nse_fno
```

This Python script is handy when you want per-collection control, upserts, or
don't have the CLI tools installed.
