import os
import boto3
import logging
from config import (
    S3_BUCKET, S3_PREFIX, AWS_REGION, CACHE_DIR,
    SUPPORTED_EXTENSIONS, SKIP_PATTERNS
)
from database import (
    init_db, insert_competition, insert_event, insert_results,
    mark_file_processed, is_file_processed
)
from extractor import extract_from_pdf, extract_from_image
from parser import parse_results, time_to_seconds

logger = logging.getLogger(__name__)


def _get_s3_client():
    """Create and return a boto3 S3 client."""
    return boto3.client("s3", region_name=AWS_REGION)


def list_s3_files():
    """List all supported files under the S3 prefix.

    Returns a list of S3 keys (strings).
    """
    client = _get_s3_client()
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            ext = os.path.splitext(key)[1].lower()
            if ext in SUPPORTED_EXTENSIONS:
                keys.append(key)
    return keys


def download_file(s3_key):
    """Download a file from S3 to the local cache directory.

    Returns the local file path.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    # Preserve subdirectory structure under cache
    relative = s3_key.replace(S3_PREFIX, "", 1)
    local_path = os.path.join(CACHE_DIR, relative)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    client = _get_s3_client()
    client.download_file(S3_BUCKET, s3_key, local_path)
    logger.info("Downloaded %s -> %s", s3_key, local_path)
    return local_path


def should_process(s3_key):
    """Check if a file should be processed.

    Returns False if:
    - Already processed successfully
    - Filename matches a skip pattern (start order, order book)
    """
    if is_file_processed(s3_key):
        return False

    basename = os.path.basename(s3_key)
    for pattern in SKIP_PATTERNS:
        if pattern in basename:
            return False

    return True


def _infer_season(s3_key):
    """Infer the season from the S3 key path.

    Looks for patterns like '25-26雪季' in the path.
    """
    relative = s3_key.replace(S3_PREFIX, "", 1)
    parts = relative.split("/")
    for part in parts:
        if "雪季" in part:
            return part
    return None


def _get_file_type(s3_key):
    """Get the file type from the extension."""
    ext = os.path.splitext(s3_key)[1].lower().lstrip(".")
    return ext


def process_file(s3_key):
    """Process a single file through the full pipeline:
    download -> extract -> parse -> insert into DB.

    Returns True on success, False on failure.
    """
    file_type = _get_file_type(s3_key)

    try:
        # Download
        local_path = download_file(s3_key)

        # Extract text/data from the file
        if file_type == "pdf":
            extracted = extract_from_pdf(local_path)
        else:
            extracted = extract_from_image(local_path)

        # Parse extracted data into structured records
        parsed = parse_results(extracted)

        if not parsed or not parsed.get("results"):
            mark_file_processed(s3_key, file_type, "skipped", "No results found in file")
            logger.warning("No results found in %s, skipping", s3_key)
            return True

        # Infer season from path
        season = _infer_season(s3_key) or parsed.get("season")

        # Insert competition
        comp_id = insert_competition(
            season=season,
            name=parsed.get("competition", ""),
            venue=parsed.get("venue"),
            date=parsed.get("date"),
        )

        # Insert event
        event_id = insert_event(
            competition_id=comp_id,
            discipline=parsed.get("discipline"),
            gender=parsed.get("gender"),
            age_group=parsed.get("age_group"),
            round_type=parsed.get("round_type"),
            source_file=s3_key,
        )

        # Convert times to seconds and insert results
        results_to_insert = []
        for r in parsed["results"]:
            results_to_insert.append({
                "rank": r.get("rank"),
                "bib": r.get("bib"),
                "name": r.get("name"),
                "team": r.get("team"),
                "run1_time": r.get("run1_time"),
                "run2_time": r.get("run2_time"),
                "total_time": r.get("total_time"),
                "run1_seconds": time_to_seconds(r.get("run1_time")),
                "run2_seconds": time_to_seconds(r.get("run2_time")),
                "total_seconds": time_to_seconds(r.get("total_time")),
                "time_diff": r.get("time_diff"),
                "status": r.get("status", "OK"),
            })

        insert_results(event_id, results_to_insert)

        mark_file_processed(s3_key, file_type, "success")
        logger.info("Successfully processed %s", s3_key)
        return True

    except Exception as e:
        logger.error("Failed to process %s: %s", s3_key, e)
        mark_file_processed(s3_key, file_type, "failed", str(e))
        return False


def run_ingestion(max_files=None, progress_callback=None):
    """Run the full ingestion pipeline.

    Args:
        max_files: Maximum number of files to process (None for all).
        progress_callback: Optional callable(current, total, s3_key) for progress updates.

    Returns:
        dict with counts: total, processed, skipped, failed
    """
    init_db()

    all_files = list_s3_files()
    to_process = [k for k in all_files if should_process(k)]

    if max_files is not None:
        to_process = to_process[:max_files]

    total = len(to_process)
    counts = {"total": total, "processed": 0, "skipped": 0, "failed": 0}

    for i, s3_key in enumerate(to_process):
        if progress_callback:
            progress_callback(i, total, s3_key)

        basename = os.path.basename(s3_key)
        # Double-check skip patterns
        skip = False
        for pattern in SKIP_PATTERNS:
            if pattern in basename:
                mark_file_processed(s3_key, _get_file_type(s3_key), "skipped", f"Matches skip pattern: {pattern}")
                counts["skipped"] += 1
                skip = True
                break
        if skip:
            continue

        success = process_file(s3_key)
        if success:
            counts["processed"] += 1
        else:
            counts["failed"] += 1

    if progress_callback:
        progress_callback(total, total, "完成")

    logger.info("Ingestion complete: %s", counts)
    return counts
