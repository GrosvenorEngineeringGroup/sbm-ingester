#!/usr/bin/env python3
"""
One-time migration script: Convert monthly archives to weekly archives.
Usage: python scripts/migrate_archives_to_weekly.py [--dry-run] [--workers N]
"""

import argparse
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import boto3

BUCKET_NAME = "sbm-file-ingester"
PREFIXES = ["newP/archived/", "newIrrevFiles/archived/", "newParseErr/archived/"]

# Thread-local S3 clients for better performance
_thread_local = threading.local()


def get_s3_client() -> Any:
    """Get thread-local S3 client."""
    if not hasattr(_thread_local, "s3"):
        _thread_local.s3 = boto3.client("s3")
    return _thread_local.s3


def get_iso_week(dt: datetime) -> str:
    """Return ISO week format: 2026-W03"""
    return f"{dt.isocalendar().year}-W{dt.isocalendar().week:02d}"


def extract_date_from_filename(filename: str) -> datetime | None:
    """
    Extract date from filename timestamp suffix.
    Example: 'file_2025073018315281.csv' -> datetime(2025, 7, 30)
    """
    match = re.search(r"_(\d{4})(\d{2})(\d{2})\d{6,10}\.csv$", filename, re.IGNORECASE)
    if match:
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime(year, month, day)
        except ValueError:
            pass

    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", filename)
    if match:
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime(year, month, day)
        except ValueError:
            pass

    return None


def get_week_from_month_dir(month_dir: str) -> str | None:
    """Fallback: Get a reasonable week from the month directory."""
    match = re.match(r"(\d{4})-(\d{2})", month_dir)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        try:
            dt = datetime(year, month, 15)
            return get_iso_week(dt)
        except ValueError:
            pass
    return None


def format_time(seconds: float) -> str:
    """Format seconds into human readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        mins = seconds // 60
        secs = seconds % 60
        return f"{mins:.0f}m {secs:.0f}s"
    hours = seconds // 3600
    mins = (seconds % 3600) // 60
    return f"{hours:.0f}h {mins:.0f}m"


def get_terminal_width() -> int:
    """Get terminal width, defaulting to 80 if unavailable."""
    try:
        import shutil

        return shutil.get_terminal_size().columns
    except Exception:
        return 80


class ProgressTracker:
    """Thread-safe progress tracker."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.processed = 0
        self.migrated = 0
        self.errors = 0
        self.skipped = 0
        self.current_prefix = ""
        self.start_time = time.time()
        self._lock = threading.Lock()

    def update(self, *, migrated: int = 0, errors: int = 0, skipped: int = 0, prefix: str = "") -> None:
        with self._lock:
            self.processed += 1
            self.migrated += migrated
            self.errors += errors
            self.skipped += skipped
            if prefix:
                self.current_prefix = prefix

    def print_progress(self, bar_width: int = 30) -> None:
        with self._lock:
            if self.total == 0:
                return

            term_width = get_terminal_width()
            percent = self.processed / self.total
            filled = int(bar_width * percent)
            bar = "█" * filled + "░" * (bar_width - filled)

            elapsed = time.time() - self.start_time
            if self.processed > 0:
                eta = (elapsed / self.processed) * (self.total - self.processed)
                eta_str = format_time(eta)
            else:
                eta_str = "..."

            status = f"  [{bar}] {percent * 100:5.1f}% | {self.processed:,}/{self.total:,} | {eta_str}"

            if self.current_prefix:
                status += f" | {self.current_prefix}"

            stats_str = f" | ✓{self.migrated}"
            if self.errors:
                stats_str += f" ✗{self.errors}"
            if self.skipped:
                stats_str += f" ⊘{self.skipped}"
            status += stats_str

            max_len = term_width - 1
            if len(status) > max_len:
                status = status[: max_len - 3] + "..."

            sys.stdout.write(f"\r\033[K{status}")
            sys.stdout.flush()


def migrate_file(key: str, prefix: str, dry_run: bool) -> tuple[str, str, str | None]:
    """
    Migrate a single file. Returns (status, prefix_name, error_msg).
    status: 'migrated', 'skipped', or 'error'
    """
    prefix_name = prefix.split("/")[0]
    parts = key.replace(prefix, "").split("/")
    filename = parts[-1]
    month_dir = parts[0]

    # Determine target week
    file_date = extract_date_from_filename(filename)
    if file_date:
        target_week = get_iso_week(file_date)
    else:
        target_week = get_week_from_month_dir(month_dir)
        if not target_week:
            return ("skipped", prefix_name, f"no date: {key}")

    dest_key = f"{prefix}{target_week}/{filename}"

    if dry_run:
        return ("migrated", prefix_name, None)

    try:
        s3 = get_s3_client()
        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={"Bucket": BUCKET_NAME, "Key": key},
            Key=dest_key,
        )
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        return ("migrated", prefix_name, None)
    except Exception as e:
        return ("error", prefix_name, f"{key}: {e}")


def collect_files_to_migrate() -> list[tuple[str, str]]:
    """Collect all files that need migration. Returns list of (key, prefix) tuples."""
    print("\nScanning for files to migrate...")
    s3 = boto3.client("s3")
    files = []
    per_prefix = {}

    for prefix in PREFIXES:
        prefix_name = prefix.split("/")[0]
        count = 0
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if "-W" in key:
                    continue

                parts = key.replace(prefix, "").split("/")
                if len(parts) < 2:
                    continue

                month_dir = parts[0]
                if len(month_dir) == 7 and month_dir[4] == "-":
                    files.append((key, prefix))
                    count += 1

        per_prefix[prefix_name] = count
        print(f"  {prefix_name}: {count:,} files")

    print(f"  Total: {len(files):,} files")
    return files


def migrate_monthly_to_weekly(dry_run: bool = False, max_workers: int = 50) -> dict:
    """Migrate all monthly directories to weekly directories using parallel processing."""
    files = collect_files_to_migrate()

    if not files:
        print("\nNo files to migrate.")
        return {"total": 0, "migrated": 0, "errors": 0, "skipped": 0}

    tracker = ProgressTracker(len(files))
    errors_log = []

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Starting migration with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(migrate_file, key, prefix, dry_run): (key, prefix) for key, prefix in files}

        for future in as_completed(futures):
            status, prefix_name, error_msg = future.result()

            if status == "migrated":
                tracker.update(migrated=1, prefix=prefix_name)
            elif status == "skipped":
                tracker.update(skipped=1, prefix=prefix_name)
                if error_msg:
                    errors_log.append(f"SKIP: {error_msg}")
            else:
                tracker.update(errors=1, prefix=prefix_name)
                if error_msg:
                    errors_log.append(f"ERROR: {error_msg}")

            tracker.print_progress()

    print()

    elapsed = time.time() - tracker.start_time
    print(f"\n  Completed in {format_time(elapsed)}")

    if errors_log and len(errors_log) <= 20:
        print("\n  Issues:")
        for msg in errors_log:
            print(f"    {msg}")

    return {
        "total": tracker.processed,
        "migrated": tracker.migrated,
        "errors": tracker.errors,
        "skipped": tracker.skipped,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate monthly archives to weekly archives")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    parser.add_argument("--workers", type=int, default=50, help="Number of parallel workers (default: 50)")
    args = parser.parse_args()

    print("=" * 60)
    print("S3 Archive Migration: Monthly -> Weekly")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Dry run: {args.dry_run}")
    print(f"Workers: {args.workers}")
    print("=" * 60)

    stats = migrate_monthly_to_weekly(dry_run=args.dry_run, max_workers=args.workers)

    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"  Total files processed: {stats['total']:,}")
    print(f"  Successfully migrated: {stats['migrated']:,}")
    print(f"  Skipped (no date):     {stats['skipped']:,}")
    print(f"  Errors:                {stats['errors']:,}")
    print("=" * 60)

    if stats["errors"] > 0:
        sys.exit(1)
