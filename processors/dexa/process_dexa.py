"""
CLI entry point for DEXA PDF extraction.

Usage:
  uv run python -m processors.dexa.process_dexa [options]

Options:
  --input-dir DIR      Directory containing DEXA PDF files
                       (default: /mnt/c/obsidian/Notes/Health & Fitness/DEXA/)
  --output-csv FILE    Output CSV file (default: data/dexa.csv)
  --file FILENAME      Process a single file by name (e.g. "DEXA 2024-08-28.pdf")
  --dry-run            List files that would be processed; make no API calls
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from lib.claude_client import extract_from_pdf
from lib.csv_store import append_row, get_existing_scan_dates
from processors.dexa.prompt import build_extraction_prompt
from processors.dexa.schema import FIELDNAMES, parse_and_validate

DEFAULT_INPUT_DIR = "/mnt/c/obsidian/Notes/Health & Fitness/DEXA/"
DEFAULT_OUTPUT_CSV = "data/dexa.csv"
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def filename_date(pdf_path: Path) -> str | None:
    m = DATE_RE.search(pdf_path.name)
    return m.group(1) if m else None


def main():
    load_dotenv()
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not set in environment or .env", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Extract DEXA PDF data to CSV")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--file", help="Process a single file by name")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_csv = Path(args.output_csv)

    # Gather PDFs
    if args.file:
        pdf_files = [input_dir / args.file]
        missing = [p for p in pdf_files if not p.exists()]
        if missing:
            print(f"ERROR: File not found: {missing[0]}", file=sys.stderr)
            sys.exit(1)
    else:
        pdf_files = sorted(input_dir.glob("*.pdf"))

    if not pdf_files:
        print("No PDF files found.")
        return

    existing_dates = get_existing_scan_dates(output_csv)
    prompt = build_extraction_prompt()

    processed = skipped = errors = 0

    for pdf_path in pdf_files:
        fname_date = filename_date(pdf_path)
        print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Processing: {pdf_path.name}", end="")

        if fname_date and fname_date in existing_dates:
            print(f"  → SKIP (already in CSV)")
            skipped += 1
            continue

        if args.dry_run:
            print(f"  → would call API (filename date: {fname_date})")
            processed += 1
            continue

        print()  # newline before progress messages
        try:
            raw_text = extract_from_pdf(pdf_path, prompt)
        except Exception as e:
            print(f"  ERROR calling API: {e}", file=sys.stderr)
            errors += 1
            continue

        # Strip any accidental markdown fences
        text = raw_text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        try:
            raw_json = json.loads(text)
        except json.JSONDecodeError as e:
            print(f"  ERROR parsing JSON: {e}\n  Raw response:\n{text[:500]}", file=sys.stderr)
            errors += 1
            continue

        row = parse_and_validate(raw_json)

        # Warn if extracted scan_date differs significantly from filename date
        extracted_date = row.get("scan_date", "")
        if fname_date and extracted_date and extracted_date != fname_date:
            try:
                d1 = datetime.strptime(fname_date, "%Y-%m-%d")
                d2 = datetime.strptime(extracted_date, "%Y-%m-%d")
                if abs((d2 - d1).days) > 1:
                    print(f"  WARNING: filename date {fname_date} vs extracted date {extracted_date}")
            except ValueError:
                pass

        # Use extracted date for dedup if available, else filename date
        dedup_date = extracted_date or fname_date or ""
        if dedup_date in existing_dates:
            print(f"  → SKIP (extracted scan_date {dedup_date} already in CSV)")
            skipped += 1
            continue

        row["source_file"] = pdf_path.name
        append_row(output_csv, row, FIELDNAMES)
        existing_dates.add(dedup_date)
        processed += 1
        print(f"  → OK (scan_date: {dedup_date})")

    print(f"\nDone. processed={processed}, skipped={skipped}, errors={errors}")


if __name__ == "__main__":
    main()
