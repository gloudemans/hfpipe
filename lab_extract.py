#!/usr/bin/env python3
"""
lab_extract.py — Lab results extraction pipeline.

For each PDF in the three lab sources, extracts actual result values for
tests that have a valid LOINC code in the crosswalk, plus file-scope metadata
(lab name, collection date, report date, ordering physician). Results are
written to a per-PDF cache; already-cached PDFs are skipped on re-run.
"""

import base64
import json
import os
import re
import sys
import time
from pathlib import Path

# Load .env from the script's directory (no python-dotenv needed)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

import anthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODEL = "claude-sonnet-4-6"

_HF = Path("/mnt/c/obsidian/Notes/Health & Fitness")
CROSSWALK_PATH = _HF / "lab_field_crosswalk.json"
EXTRACTION_CACHE_PATH = _HF / "lab_extraction_cache.json"
RESULTS_CACHE_PATH = _HF / "lab_results_cache.json"

SOURCES = [
    {
        "dir": _HF / "Function Health",
        "category": "Function",
        "pattern": re.compile(
            r"^Function (\d{4}-\d{2}-\d{2}) Lab Results of Record\.pdf$"
        ),
    },
    {
        "dir": _HF / "Paper",
        "category": "Paper",
        "pattern": re.compile(r"^Labs (\d{4}-\d{2}-\d{2})\.pdf$"),
    },
    {
        "dir": _HF / "Ulta Labs",
        "category": "UltaLabs",
        "pattern": re.compile(r"^Labs (\d{4}-\d{2}-\d{2})\.pdf$"),
    },
]

EXTRACTION_SYSTEM = """\
You are extracting lab result values from a medical lab report.

You will be given a lookup table mapping test names to canonical fields and LOINC codes.
Extract ONLY tests whose names appear in the lookup table.

Return a JSON object with exactly two keys:

"metadata": {
  "lab_name": "name of the performing laboratory as printed, or null",
  "collection_date": "YYYY-MM-DD specimen collection date, or null",
  "report_date": "YYYY-MM-DD date results were reported or finalized, or null",
  "ordering_physician": "name of ordering physician, or null"
}

"results": array of objects, one per matched test:
{
  "source_name": "exact test name as printed in the document",
  "canonical_field": "canonical_field value from the lookup table",
  "loinc_code": "loinc_code value from the lookup table",
  "value": "result value as a string",
  "unit": "unit as printed, or null",
  "reference_range": "reference range as printed, or null",
  "flag": "H, L, or A if the result is flagged high/low/abnormal, otherwise null"
}

Rules:
- Match test names case-insensitively and tolerate minor punctuation differences
- If a test appears on both a summary page and a detail page, include it only once
- Set value to the actual result, not interpretation text or footnotes
- For qualitative results use the printed text verbatim (e.g. "Negative", "Reactive")
- Do not invent or guess values — if a test is in the lookup but has no result in the document, omit it
- Return JSON only. No markdown fences, no other text.\
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_json_response(text: str) -> dict | list | None:
    """Strip markdown fences and parse JSON from an API response."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Fall back: find first {...} block
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return None


def call_api_with_retry(client, *, system, user_content, label="call") -> str | None:
    """Make an API call with retries. Rate limits get longer back-off delays."""
    rate_limit_delays = [15, 45, 120]
    transient_delays = [5]

    # Rate limit retry loop
    for attempt, delay in enumerate(rate_limit_delays + [None]):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=16384,
                system=system,
                messages=[{"role": "user", "content": user_content}],
            )
            return response.content[0].text
        except anthropic.RateLimitError:
            if delay is None:
                print(f"  ERROR [{label}]: rate limit persists after {len(rate_limit_delays)} retries, giving up")
                return None
            print(f"  WARNING [{label}]: rate limited (attempt {attempt + 1}), retrying in {delay}s...")
            time.sleep(delay)
        except Exception as exc:
            # For non-rate-limit errors, retry once with a short delay
            print(f"  WARNING [{label}]: {exc}, retrying in {transient_delays[0]}s...")
            time.sleep(transient_delays[0])
            try:
                response = client.messages.create(
                    model=MODEL,
                    max_tokens=16384,
                    system=system,
                    messages=[{"role": "user", "content": user_content}],
                )
                return response.content[0].text
            except Exception as exc2:
                print(f"  ERROR [{label}]: {exc2}")
                return None
    return None


# ---------------------------------------------------------------------------
# Step 1: Build lookup table from crosswalk
# ---------------------------------------------------------------------------


def load_lookup() -> dict[str, dict]:
    """Load LOINC-mapped entries from the crosswalk, keyed by source_name.upper()."""
    if not CROSSWALK_PATH.exists():
        print(f"ERROR: crosswalk not found at {CROSSWALK_PATH}")
        print("Run lab_discovery.py first.")
        sys.exit(1)
    try:
        data = json.loads(CROSSWALK_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"ERROR: crosswalk is malformed JSON: {exc}")
        sys.exit(1)

    lookup: dict[str, dict] = {}
    for record in data:
        if not record.get("loinc_code"):
            continue
        key = record["source_name"].strip().upper()
        lookup[key] = {
            "canonical_field": record["canonical_field"],
            "loinc_code": record["loinc_code"],
            "loinc_name": record.get("loinc_name"),
        }
    return lookup


# ---------------------------------------------------------------------------
# Step 2: Discover PDFs
# ---------------------------------------------------------------------------


def discover_pdfs() -> list[dict]:
    pdfs = []
    for source in SOURCES:
        d = source["dir"]
        if not d.exists():
            print(f"  WARNING: directory not found: {d}")
            continue
        for f in sorted(d.iterdir()):
            m = source["pattern"].match(f.name)
            if m:
                pdfs.append(
                    {
                        "path": f,
                        "source_category": source["category"],
                        "source_date": m.group(1),
                    }
                )
    return pdfs


# ---------------------------------------------------------------------------
# Step 3: Load caches
# ---------------------------------------------------------------------------


def load_json_cache(path: Path, label: str) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            print(f"ERROR: {label} at {path} is not a JSON object. Halting.")
            sys.exit(1)
        return data
    except json.JSONDecodeError as exc:
        print(f"ERROR: {label} at {path} is malformed JSON: {exc}")
        print("Halting to avoid overwriting it. Fix or remove the file and retry.")
        sys.exit(1)


def save_results_cache(cache: dict) -> None:
    RESULTS_CACHE_PATH.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Step 4: Extract results from a single PDF
# ---------------------------------------------------------------------------


def build_filtered_lookup(
    pdf_name: str,
    full_lookup: dict[str, dict],
    extraction_cache: dict,
) -> dict[str, dict]:
    """
    Return the subset of full_lookup relevant to this PDF.
    Uses the extraction cache (which lists source_names found per PDF)
    to limit the lookup to only tests present in this document.
    Falls back to the full lookup if the PDF wasn't cached during discovery.
    """
    if pdf_name not in extraction_cache:
        return full_lookup  # full lookup for PDFs not in extraction cache

    names_in_pdf = {
        e["source_name"].strip().upper()
        for e in extraction_cache[pdf_name]
    }
    return {k: v for k, v in full_lookup.items() if k in names_in_pdf}


def extract_results(
    client,
    pdf_info: dict,
    filtered_lookup: dict[str, dict],
) -> dict | None:
    """
    Send a PDF + filtered lookup to Claude and return the parsed response dict,
    or None on failure.
    """
    path = pdf_info["path"]
    b64_pdf = base64.standard_b64encode(path.read_bytes()).decode("utf-8")

    user_content = [
        {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": b64_pdf,
            },
        },
        {
            "type": "text",
            "text": (
                "Lookup table (source_name → canonical mapping):\n"
                + json.dumps(filtered_lookup, indent=2)
                + "\n\nExtract results from this lab report."
            ),
        },
    ]

    raw = call_api_with_retry(
        client,
        system=EXTRACTION_SYSTEM,
        user_content=user_content,
        label=path.name,
    )
    if raw is None:
        return None

    parsed = parse_json_response(raw)
    if not isinstance(parsed, dict):
        print(f"  ERROR: response was not a JSON object for {path.name}")
        return None
    if "results" not in parsed:
        print(f"  ERROR: response missing 'results' key for {path.name}")
        return None

    return parsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # ------------------------------------------------------------------
    # Step 1: Build lookup
    # ------------------------------------------------------------------
    print("=" * 62)
    print("Step 1: Loading crosswalk")
    print("=" * 62)
    full_lookup = load_lookup()
    print(f"LOINC-mapped tests available: {len(full_lookup)}")

    # ------------------------------------------------------------------
    # Step 2: Discover PDFs
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 2: Discovering PDFs")
    print("=" * 62)
    pdfs = discover_pdfs()
    counts: dict[str, int] = {"Function": 0, "Paper": 0, "UltaLabs": 0}
    for pdf in pdfs:
        counts[pdf["source_category"]] += 1
        print(f"  [{pdf['source_category']:10s}] {pdf['source_date']}  {pdf['path'].name}")

    print(f"\nFound {len(pdfs)} PDF(s):")
    print(f"  Function Health: {counts['Function']}")
    print(f"  Paper scans:     {counts['Paper']}")
    print(f"  Ulta Labs:       {counts['UltaLabs']}")

    if not pdfs:
        print("No PDFs found. Exiting.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Step 3: Load caches
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 3: Loading caches")
    print("=" * 62)
    extraction_cache = load_json_cache(EXTRACTION_CACHE_PATH, "extraction cache")
    results_cache = load_json_cache(RESULTS_CACHE_PATH, "results cache")

    already_cached = [p for p in pdfs if p["path"].name in results_cache]
    to_process = [p for p in pdfs if p["path"].name not in results_cache]

    print(f"Already in results cache: {len(already_cached)}")
    print(f"Need processing:          {len(to_process)}")

    if not to_process:
        print("\nAll PDFs already cached. Nothing to do.")
    else:
        answer = input(f"\nProceed with API calls for {len(to_process)} PDF(s)? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            sys.exit(0)

    client = anthropic.Anthropic()

    # ------------------------------------------------------------------
    # Step 4: Extract from each uncached PDF
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 4: Extracting results from PDFs")
    print("=" * 62)

    newly_extracted = 0
    failed_pdfs: list[str] = []

    for i, pdf in enumerate(to_process, 1):
        name = pdf["path"].name
        print(f"\n[{i}/{len(to_process)}] {pdf['source_category']} {pdf['source_date']}  {name}")

        filtered_lookup = build_filtered_lookup(name, full_lookup, extraction_cache)
        print(f"  Lookup size: {len(filtered_lookup)} test(s)")

        if not filtered_lookup:
            print("  SKIP: no LOINC-mapped tests found for this PDF")
            failed_pdfs.append(name)
            continue

        try:
            parsed = extract_results(client, pdf, filtered_lookup)
        except Exception as exc:
            print(f"  ERROR: unexpected exception: {exc}")
            parsed = None

        if parsed is None:
            print(f"  -> Extraction failed")
            failed_pdfs.append(name)
        else:
            result_count = len(parsed.get("results", []))
            print(f"  -> {result_count} result(s) extracted")

            results_cache[name] = {
                "source_category": pdf["source_category"],
                "source_date": pdf["source_date"],
                "metadata": parsed.get("metadata", {}),
                "results": parsed.get("results", []),
            }
            save_results_cache(results_cache)
            newly_extracted += 1

        if i < len(to_process):
            time.sleep(1)

    # ------------------------------------------------------------------
    # Step 5: Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("SUMMARY")
    print("=" * 62)
    print(f"PDFs discovered: {len(pdfs)}")
    print(f"  Already cached (skipped): {len(already_cached)}")
    print(f"  Newly extracted:          {newly_extracted}")
    print(f"  Errors:                   {len(failed_pdfs)}")

    # Tally results across the full cache
    total_results = 0
    results_by_category: dict[str, int] = {"Function": 0, "Paper": 0, "UltaLabs": 0}
    for entry in results_cache.values():
        n = len(entry.get("results", []))
        total_results += n
        cat = entry.get("source_category", "")
        if cat in results_by_category:
            results_by_category[cat] += n

    print(f"\nTotal results in cache: {total_results} across {len(results_cache)} PDF(s)")
    print(f"  Function Health: {results_by_category['Function']}")
    print(f"  Paper scans:     {results_by_category['Paper']}")
    print(f"  Ulta Labs:       {results_by_category['UltaLabs']}")

    print(f"\nResults cache written to:\n  {RESULTS_CACHE_PATH}")

    if failed_pdfs:
        print(f"\nPDFs with extraction errors ({len(failed_pdfs)}):")
        for name in failed_pdfs:
            print(f"  - {name}")


if __name__ == "__main__":
    main()
