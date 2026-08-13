"""
upload_dedup_1b_v1.py - Upload ~1 Billion objects for RGW dedup scale testing.

This script creates a large-scale dedup test dataset by uploading ~1 billion
S3 objects (4KB each) across 1000 buckets using s5cmd for high-throughput
parallel uploads. Each unique source file is copied 127 times (the dedup
shared-manifest limit), spread across different buckets to test cross-bucket
dedup behavior.

What it does:
  1. Generates ~7.87M unique 4KB random files on local disk (/tmp/dedup_sources_4k/)
  2. Creates 1000 S3 buckets (test-dedup-performance-1 .. 1000) via aws CLI
  3. Uploads each unique file + 127 copies across buckets using s5cmd batch mode
  4. Reports progress with obj/sec rate and ETA

Layout:
  - 1000 buckets: test-dedup-performance-1 .. test-dedup-performance-1000
  - ~1,000,000 objects per bucket (1B total)
  - ~7,874,016 unique source files (4KB each), each copied 127 times
  - Unique sources distributed round-robin across buckets
  - Object size: 4KB (4096 bytes)

Capacity requirements (BEFORE running dedup):
  +----------------------------+-------------+-------------+
  | Metric                     | Replicated  | EC 2+2      |
  |                            | (size=3)    | (k=2, m=2)  |
  +----------------------------+-------------+-------------+
  | Data stored                | 3.7 TiB     | 3.7 TiB     |
  | Raw space used             | 11.1 TiB    | 7.4 TiB     |
  | Bucket index overhead      | ~2-5 GB     | ~2-5 GB     |
  | Source staging disk        | ~30 GB      | ~30 GB      |
  | Minimum cluster raw        | ~15 TiB     | ~10 TiB     |
  | Recommended cluster raw    | ~20+ TiB    | ~14+ TiB    |
  +----------------------------+-------------+-------------+

  After dedup (radosgw-admin dedup exec):
  - Unique data: ~30 GB (7.87M unique x 4KB)
  - Shared manifests point to dedup pool chunks
  - Dedup pool (size=2): ~60 GB raw for chunk storage

Bucket index considerations:
  - 1000 buckets x 1M objects = 1B index entries
  - Dynamic resharding (rgw_max_objs_per_shard=100K) creates ~10 shards/bucket
  - Recommend bucket index pool PG count >= 256
  - Monitor omap stats: ceph health detail | grep omap

Prerequisites:
  - s5cmd installed (https://github.com/peak/s5cmd)
  - aws CLI installed (for bucket creation)
  - RGW user with max_buckets >= 1000
  - Sufficient cluster capacity (see table above)

Usage:
  python3 upload_dedup_1b_v1.py                              # full run
  python3 upload_dedup_1b_v1.py --start-bucket 500           # resume from bucket 500
  python3 upload_dedup_1b_v1.py --buckets 10 --sources 1000  # quick test
  python3 upload_dedup_1b_v1.py --generate-only              # just create source files
  python3 upload_dedup_1b_v1.py --skip-generate              # skip if sources exist

  Resume after interrupt:
  python3 upload_dedup_1b_v1.py --start-source 500000 --skip-generate --skip-buckets
"""

import argparse
import os
import subprocess
import sys
import time

ENDPOINT = "http://grim017:5000"
BUCKET_PREFIX = "test-dedup-performance"
NUM_BUCKETS = 1000
OBJ_PER_BUCKET = 1_000_000
COPIES = 127
TOTAL_UNIQUE = 7_874_016  # ceil(1B / (127+1)) — each unique + 127 copies = 128
OBJ_SIZE = 4096  # 4KB
SOURCE_DIR = "/tmp/dedup_sources_4k"
WORKERS = 256
CHUNK_SIZE = 5000  # sources per s5cmd batch

os.environ["AWS_ACCESS_KEY_ID"] = "dedupscale"
os.environ["AWS_SECRET_ACCESS_KEY"] = "dedupscale"
os.environ["AWS_DEFAULT_REGION"] = "default"
os.environ["AWS_REGION"] = "default"


def generate_sources(source_dir, total, obj_size, start_from=0):
    """Generate unique random 4KB files in batched subdirectories."""
    batch_size = 1000
    generated = 0
    total_to_gen = total - start_from

    print(
        f"\n[Source Generation] Creating {total_to_gen:,} unique {obj_size}B files..."
    )
    print(f"  Directory: {source_dir}")
    print(f"  Disk needed: ~{(total_to_gen * obj_size) / (1024**3):.1f} GB")

    t0 = time.time()
    for i in range(start_from, total):
        subdir = i // batch_size
        batch_dir = os.path.join(source_dir, f"batch_{subdir:05d}")
        os.makedirs(batch_dir, exist_ok=True)

        fpath = os.path.join(batch_dir, f"obj_{i:08d}.bin")
        if os.path.exists(fpath) and os.path.getsize(fpath) == obj_size:
            generated += 1
            continue

        with open(fpath, "wb") as f:
            f.write(os.urandom(obj_size))
        generated += 1

        if generated % 100_000 == 0:
            elapsed = time.time() - t0
            rate = generated / elapsed if elapsed > 0 else 0
            remain = (total_to_gen - generated) / rate if rate > 0 else 0
            print(
                f"  [{time.strftime('%H:%M:%S')}] "
                f"{generated:,}/{total_to_gen:,} files "
                f"| {rate:.0f}/sec | ~{remain/60:.0f}m left",
                flush=True,
            )

    elapsed = time.time() - t0
    print(f"  Done: {generated:,} files in {elapsed:.0f}s\n")


def create_buckets(endpoint, prefix, count, start_from=0):
    """Create buckets via aws CLI (avoids s5cmd LocationConstraint issues)."""
    print(f"\n[Bucket Creation] Creating {count - start_from} buckets via aws s3 mb...")
    created = 0
    skipped = 0
    for i in range(start_from + 1, count + 1):
        bucket = f"{prefix}-{i}"
        result = subprocess.run(
            [
                "aws",
                "--endpoint-url",
                endpoint,
                "s3",
                "mb",
                f"s3://{bucket}",
            ],
            capture_output=True,
        )
        out = result.stdout.decode().strip() if result.stdout else ""
        err = result.stderr.decode().strip() if result.stderr else ""
        if result.returncode == 0:
            created += 1
        elif "BucketAlreadyOwnedByYou" in err or "BucketAlreadyExists" in err:
            skipped += 1
        else:
            print(f"  WARNING: Failed to create {bucket}: {err}")

        if (i - start_from) % 100 == 0:
            print(
                f"  [{time.strftime('%H:%M:%S')}] "
                f"Checked {i - start_from}/{count - start_from} buckets "
                f"(created {created}, existed {skipped})",
                flush=True,
            )

    print(f"  Done: {created} new, {skipped} already existed\n")


def build_batch_file(
    src_start, src_end, source_dir, bucket_prefix, num_buckets, copies, batch_path
):
    """Build s5cmd batch: each source goes to a deterministic bucket with 127 copies."""
    with open(batch_path, "w") as f:
        for src_idx in range(src_start, src_end):
            subdir = src_idx // 1000
            src_file = f"{source_dir}/batch_{subdir:05d}/obj_{src_idx:08d}.bin"

            # Distribute sources round-robin across buckets
            base_bucket_idx = src_idx % num_buckets
            bucket_num = base_bucket_idx + 1
            bucket = f"{bucket_prefix}-{bucket_num}"

            # Original + 127 copies across nearby buckets
            f.write(f"cp {src_file} s3://{bucket}/src_{src_idx:08d}/orig.bin\n")
            for c in range(copies):
                # Spread copies across different buckets for cross-bucket dedup
                copy_bucket_idx = (base_bucket_idx + c) % num_buckets
                copy_bucket_num = copy_bucket_idx + 1
                copy_bucket = f"{bucket_prefix}-{copy_bucket_num}"
                f.write(
                    f"cp {src_file} "
                    f"s3://{copy_bucket}/src_{src_idx:08d}/copy_{c:03d}.bin\n"
                )


def run_s5cmd(batch_path, endpoint, workers):
    result = subprocess.run(
        [
            "s5cmd",
            "--endpoint-url",
            endpoint,
            "--numworkers",
            str(workers),
            "run",
            batch_path,
        ],
        capture_output=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    errors = ""
    if result.returncode != 0 and result.stderr:
        errors = result.stderr.decode().strip()
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Upload 1B dedup test objects across 1000 buckets via s5cmd"
    )
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--bucket-prefix", default=BUCKET_PREFIX)
    parser.add_argument("--buckets", type=int, default=NUM_BUCKETS)
    parser.add_argument("--sources", type=int, default=TOTAL_UNIQUE)
    parser.add_argument("--copies", type=int, default=COPIES)
    parser.add_argument("--obj-size", type=int, default=OBJ_SIZE)
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument(
        "--start-bucket",
        type=int,
        default=0,
        help="Skip bucket creation before this number",
    )
    parser.add_argument(
        "--start-source",
        type=int,
        default=0,
        help="Resume uploads from this source index",
    )
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate source files, don't upload",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip source generation (already done)",
    )
    parser.add_argument(
        "--skip-buckets",
        action="store_true",
        help="Skip bucket creation (already done)",
    )
    args = parser.parse_args()

    total_objects = args.sources * (1 + args.copies)  # orig + copies
    total_remaining = (args.sources - args.start_source) * (1 + args.copies)
    data_stored_tb = (total_objects * args.obj_size) / (1024 ** 4)
    raw_tb = data_stored_tb * 2  # EC 2+2 overhead

    print("=" * 65)
    print("  s5cmd Dedup 1-Billion Object Upload v1")
    print("=" * 65)
    print(f"  Endpoint:       {args.endpoint}")
    print(
        f"  Buckets:        {args.bucket_prefix}-1 .. {args.bucket_prefix}-{args.buckets}"
    )
    print(f"  Unique sources: {args.sources:,}")
    print(
        f"  Copies/source:  {args.copies} (+ 1 original = {args.copies + 1} per source)"
    )
    print(f"  Total objects:  {total_objects:,}")
    print(f"  Object size:    {args.obj_size:,} bytes")
    print(f"  Data stored:    {data_stored_tb:.1f} TiB")
    print(f"  Raw (x3 repl):  {raw_tb:.1f} TiB")
    print(f"  Workers:        {args.workers}")
    print(f"  Resume from:    source {args.start_source:,}")
    print(f"  Remaining:      {total_remaining:,} objects")
    print("=" * 65)

    # Phase 1: Generate source files
    if not args.skip_generate:
        generate_sources(SOURCE_DIR, args.sources, args.obj_size)
    else:
        print("\n[Skipped] Source generation")

    if args.generate_only:
        print("Source generation complete. Exiting (--generate-only).")
        return

    # Verify sources exist
    sample = os.path.join(SOURCE_DIR, "batch_00000", "obj_00000000.bin")
    if not os.path.isfile(sample):
        print(f"\nERROR: Source file {sample} not found!")
        print("Run with --generate-only first, or remove --skip-generate.")
        sys.exit(1)
    print(f"[OK] Source files verified in {SOURCE_DIR}")

    # Phase 2: Create buckets
    if not args.skip_buckets:
        create_buckets(
            args.endpoint, args.bucket_prefix, args.buckets, args.start_bucket
        )
    else:
        print("[Skipped] Bucket creation\n")

    # Phase 3: Test upload
    print("[Testing] Single upload to verify connectivity...")
    test_file = "/tmp/_dedup_1b_test.bin"
    with open(test_file, "wb") as f:
        f.write(os.urandom(args.obj_size))
    test_bucket = f"{args.bucket_prefix}-1"
    result = subprocess.run(
        [
            "s5cmd",
            "--endpoint-url",
            args.endpoint,
            "cp",
            test_file,
            f"s3://{test_bucket}/_test.bin",
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        err = result.stderr.decode().strip() if result.stderr else "unknown"
        print(f"\nERROR: Test upload failed: {err}")
        sys.exit(1)
    subprocess.run(
        [
            "s5cmd",
            "--endpoint-url",
            args.endpoint,
            "rm",
            f"s3://{test_bucket}/_test.bin",
        ],
        capture_output=True,
    )
    os.remove(test_file)
    print("[OK] Test upload succeeded\n")

    # Phase 4: Upload
    print("[Uploading...]\n")
    start_time = time.time()
    uploaded = 0
    error_count = 0

    try:
        for chunk_start in range(args.start_source, args.sources, args.chunk_size):
            chunk_end = min(chunk_start + args.chunk_size, args.sources)
            batch_path = f"/tmp/s5cmd_1b_batch_{chunk_start}.txt"

            build_batch_file(
                chunk_start,
                chunk_end,
                SOURCE_DIR,
                args.bucket_prefix,
                args.buckets,
                args.copies,
                batch_path,
            )

            batch_cmds = (chunk_end - chunk_start) * (1 + args.copies)
            now = time.strftime("%H:%M:%S")
            pct = (uploaded / total_remaining * 100) if total_remaining > 0 else 0
            print(
                f"  [{now}] Sources {chunk_start:,}-{chunk_end - 1:,}: "
                f"{batch_cmds:,} uploads ({pct:.2f}%)...",
                flush=True,
            )

            errors = run_s5cmd(batch_path, args.endpoint, args.workers)
            if errors:
                err_lines = errors.count("ERROR")
                error_count += err_lines
                print(f"    WARNING: {err_lines} errors (sample: {errors[:200]})")

            os.remove(batch_path)

            uploaded += batch_cmds
            elapsed = time.time() - start_time
            rate = uploaded / elapsed if elapsed > 0 else 0
            remain = (total_remaining - uploaded) / rate if rate > 0 else 0
            now = time.strftime("%H:%M:%S")
            print(
                f"  [{now}] Progress: {uploaded:,}/{total_remaining:,} "
                f"| {rate:.0f} obj/sec "
                f"| ~{remain / 3600:.1f}h left "
                f"| errors: {error_count}",
                flush=True,
            )

    except KeyboardInterrupt:
        print(
            f"\n\nInterrupted! Resume with: --start-source {chunk_start} --skip-generate --skip-buckets"
        )
        sys.exit(1)

    elapsed = time.time() - start_time
    rate = uploaded / elapsed if elapsed > 0 else 0
    print()
    print("=" * 65)
    print("  DONE")
    print(f"  Uploaded:  {uploaded:,} objects across {args.buckets} buckets")
    print(f"  Errors:    {error_count}")
    print(f"  Time:      {elapsed / 3600:.1f}h ({elapsed:.0f}s)")
    print(f"  Rate:      {rate:.0f} obj/sec")
    print("=" * 65)


if __name__ == "__main__":
    main()
