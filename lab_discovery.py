#!/usr/bin/env python3
"""
lab_discovery.py — Lab results field discovery pipeline.

Discovers all matching PDFs across three lab sources, extracts every unique
test name via Claude, deduplicates, enriches with canonical field names and
LOINC codes, and writes/merges a persistent crosswalk JSON file.
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

CROSSWALK_PATH = Path(
    "/mnt/c/obsidian/Notes/Health & Fitness/lab_field_crosswalk.json"
)
CACHE_PATH = Path(
    "/mnt/c/obsidian/Notes/Health & Fitness/lab_extraction_cache.json"
)

SOURCES = [
    {
        "dir": Path("/mnt/c/obsidian/Notes/Health & Fitness/Function Health"),
        "category": "Function",
        "pattern": re.compile(
            r"^Function (\d{4}-\d{2}-\d{2}) Lab Results of Record\.pdf$"
        ),
    },
    {
        "dir": Path("/mnt/c/obsidian/Notes/Health & Fitness/Paper"),
        "category": "Paper",
        "pattern": re.compile(r"^Labs (\d{4}-\d{2}-\d{2})\.pdf$"),
    },
    {
        "dir": Path("/mnt/c/obsidian/Notes/Health & Fitness/Ulta Labs"),
        "category": "UltaLabs",
        "pattern": re.compile(r"^Labs (\d{4}-\d{2}-\d{2})\.pdf$"),
    },
]

EXTRACTION_SYSTEM = """\
You are extracting lab test metadata from a medical lab report.
Do NOT extract result values. Only extract test names and metadata.

For each distinct lab test or measurement in this document output a
JSON object:

{
  "source_name": "exact test name as it appears in the document",
  "panel_context": "parent panel/section heading if applicable (e.g. CBC, LIPID PANEL), null if standalone",
  "example_value": "a representative result value as a string",
  "example_unit": "unit of measure as printed, null if absent",
  "example_ref_range": "reference range as printed, null if absent",
  "value_type": "numeric" | "qualitative" | "ratio" | "percent" | "titer"
}

Rules:
- Include sub-components of panels as separate entries, each with
  panel_context set to the parent panel name
- If the same test appears in both a main report and an appendix
  within the same PDF, include it only once
- Ignore boilerplate laboratory commentary, patient information,
  physician information, and specimen metadata
- For qualitative results like NEGATIVE or POSITIVE, set
  value_type to "qualitative"

Return a JSON array only. No other text.\
"""

ENRICHMENT_SYSTEM = """\
You are building a canonical field mapping for a personal health
data pipeline that consolidates lab results from Quest Diagnostics,
Function Health, Ulta Lab Tests, and scanned paper records.

For each lab test in the input array, return an enriched record
adding these fields:

canonical_field: snake_case name following these conventions:
  - Use the analyte name, not the method or instrument
    (testosterone_total not testosterone_total_ms)
  - Include specimen type only when needed to disambiguate
    (zinc_serum vs zinc_urine)
  - Use panel prefix for ambiguous sub-components
    (cbc_wbc, cbc_rbc, lipid_ldl, lipid_hdl)
  - Omit redundant words — glucose not blood_glucose_concentration
  - For ratios use _ratio suffix (lipid_chol_hdl_ratio)
  - For percent-free values use _pct suffix (psa_free_pct)
  - For antibody tests use _ab suffix (thyroid_peroxidase_ab)

loinc_code: the most appropriate LOINC code given the test name,
  unit, and context. Prefer serum/plasma specimen type when
  ambiguous. Return null if genuinely uncertain.

loinc_name: the LOINC long common name for the code, null if
  loinc_code is null.

confidence: your confidence the LOINC code is correct:
  "high"   — well-known test, unambiguous match
  "medium" — reasonable match but method or specimen ambiguity exists
  "low"    — uncertain, manual review recommended

notes: clinically relevant caveats, e.g. method limitations,
  disambiguation notes, or why confidence is not high.
  null if none.

Return a JSON array of the complete enriched records (all original
fields plus the five new fields). No other text.\
"""

BATCH_SIZE = 15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_json_response(text: str) -> list | None:
    """Try to extract a JSON array from an API response string."""
    text = text.strip()
    # Strip markdown code fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass
    # Fall back: find first [...] block
    m = re.search(r"\[[\s\S]*\]", text)
    if m:
        try:
            result = json.loads(m.group(0))
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass
    return None


def call_api_with_retry(client, *, system, user_content, label="call") -> str | None:
    """Make an API call, retrying once on transient errors."""
    delays = [5]
    for attempt, delay in enumerate(delays + [None]):
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
                print(f"  ERROR [{label}]: rate limit, giving up after retry")
                return None
            print(f"  WARNING [{label}]: rate limited, retrying in {delay}s...")
            time.sleep(delay)
        except Exception as exc:
            if delay is None:
                print(f"  ERROR [{label}]: {exc}")
                return None
            print(f"  WARNING [{label}]: {exc}, retrying in {delay}s...")
            time.sleep(delay)
    return None


# ---------------------------------------------------------------------------
# Step 1: Discover PDFs
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
# Step 2: Extract fields from a single PDF
# ---------------------------------------------------------------------------


def extract_from_pdf(client, pdf_info: dict) -> list[dict]:
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
            "text": "Extract all lab test metadata from this document as a JSON array.",
        },
    ]

    raw = call_api_with_retry(
        client,
        system=EXTRACTION_SYSTEM,
        user_content=user_content,
        label=path.name,
    )
    if raw is None:
        return []

    entries = parse_json_response(raw)
    if entries is None:
        print(f"  ERROR: unparseable JSON response for {path.name}")
        return []

    for e in entries:
        e["source_category"] = pdf_info["source_category"]
        e["source_date"] = pdf_info["source_date"]
    return entries


# ---------------------------------------------------------------------------
# Step 3: Deduplicate and consolidate
# ---------------------------------------------------------------------------


def consolidate(all_entries: list[dict]) -> dict[str, dict]:
    """Group by source_name (case-insensitive), produce one record per test."""
    groups: dict[str, list] = {}
    for e in all_entries:
        key = e["source_name"].strip().upper()
        groups.setdefault(key, []).append(e)

    consolidated: dict[str, dict] = {}
    for key, entries in groups.items():
        # Most frequent non-null panel_context
        panel_counts: dict[str, int] = {}
        for e in entries:
            pc = e.get("panel_context")
            if pc:
                panel_counts[pc] = panel_counts.get(pc, 0) + 1
        panel_context = max(panel_counts, key=lambda k: panel_counts[k]) if panel_counts else None

        sources_seen = sorted(set(e["source_category"] for e in entries))

        # Best representative: prefer entry with both value and unit
        rep = entries[0]
        for e in entries:
            if e.get("example_value") and e.get("example_unit"):
                rep = e
                break

        consolidated[key] = {
            "source_name": entries[0]["source_name"],
            "panel_context": panel_context,
            "occurrences": len(entries),
            "sources_seen": sources_seen,
            "example_value": rep.get("example_value"),
            "example_unit": rep.get("example_unit"),
            "example_ref_range": rep.get("example_ref_range"),
            "value_type": rep.get("value_type"),
            "canonical_field": None,
            "loinc_code": None,
            "loinc_name": None,
            "confidence": None,
            "notes": None,
        }
    return consolidated


# ---------------------------------------------------------------------------
# Step 4: Load existing crosswalk
# ---------------------------------------------------------------------------


def load_crosswalk() -> dict[str, dict]:
    if not CROSSWALK_PATH.exists():
        return {}
    try:
        data = json.loads(CROSSWALK_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"ERROR: crosswalk file exists but is malformed JSON: {exc}")
        print("Halting to avoid overwriting it. Fix or remove the file and retry.")
        sys.exit(1)

    if not isinstance(data, list):
        print("ERROR: crosswalk file top level is not a JSON array. Halting.")
        sys.exit(1)

    result: dict[str, dict] = {}
    for record in data:
        if "source_name" not in record:
            print(f"ERROR: crosswalk record missing 'source_name': {record}")
            sys.exit(1)
        key = record["source_name"].strip().upper()
        result[key] = record
    return result


def merge_with_crosswalk(
    new: dict[str, dict], existing: dict[str, dict]
) -> dict[str, dict]:
    """
    Start from existing, then overlay new scan data.
    For records that already exist, preserve enrichment fields;
    update occurrence counts and example fields from the new scan.
    """
    merged = dict(existing)

    for key, record in new.items():
        if key in merged:
            old = merged[key]
            merged[key] = {
                **record,
                # Preserve previously enriched fields
                "canonical_field": old.get("canonical_field"),
                "loinc_code": old.get("loinc_code"),
                "loinc_name": old.get("loinc_name"),
                "confidence": old.get("confidence"),
                "notes": old.get("notes"),
            }
        else:
            merged[key] = record

    return merged


# ---------------------------------------------------------------------------
# Step 5: Enrich a batch
# ---------------------------------------------------------------------------


def enrich_batch(client, batch: list[dict]) -> list[dict] | None:
    user_msg = f"Enrich these lab test records:\n{json.dumps(batch, indent=2)}"
    raw = call_api_with_retry(
        client,
        system=ENRICHMENT_SYSTEM,
        user_content=user_msg,
        label=f"enrichment batch ({len(batch)} records)",
    )
    if raw is None:
        return None

    result = parse_json_response(raw)
    if result is None:
        print("  ERROR: unparseable JSON in enrichment response")
        return None
    return result


# ---------------------------------------------------------------------------
# Step 6: Write crosswalk
# ---------------------------------------------------------------------------


def load_cache() -> dict[str, list]:
    """Load extraction cache keyed by PDF filename."""
    if not CACHE_PATH.exists():
        return {}
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    return {}


def save_cache(cache: dict[str, list]) -> None:
    CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


def write_crosswalk(records: dict[str, dict]) -> None:
    record_list = list(records.values())
    record_list.sort(
        key=lambda r: (
            r.get("canonical_field") is None,  # nulls last
            (r.get("canonical_field") or "").lower(),
            r["source_name"].lower(),
        )
    )
    CROSSWALK_PATH.write_text(
        json.dumps(record_list, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # ------------------------------------------------------------------
    # Step 1: Discover
    # ------------------------------------------------------------------
    print("=" * 62)
    print("Step 1: Discovering PDFs")
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

    answer = input("\nProceed with API calls? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        sys.exit(0)

    client = anthropic.Anthropic()

    # ------------------------------------------------------------------
    # Step 2: Extract
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 2: Extracting lab test fields from PDFs")
    print("=" * 62)

    cache = load_cache()
    all_entries: list[dict] = []
    failed_pdfs: list[str] = []
    cache_hits = 0

    for i, pdf in enumerate(pdfs, 1):
        name = pdf["path"].name
        print(
            f"\n[{i}/{len(pdfs)}] {pdf['source_category']} {pdf['source_date']}"
            f"  {name}"
        )

        if name in cache:
            entries = cache[name]
            print(f"  -> {len(entries)} test(s) loaded from cache")
            cache_hits += 1
        else:
            try:
                entries = extract_from_pdf(client, pdf)
            except Exception as exc:
                print(f"  ERROR: unexpected exception: {exc}")
                entries = []

            if entries:
                print(f"  -> {len(entries)} test(s) extracted")
                cache[name] = entries
                save_cache(cache)
            else:
                print(f"  -> No entries extracted")
                failed_pdfs.append(name)
                if i < len(pdfs):
                    time.sleep(1)
                continue

        all_entries.extend(entries)
        if i < len(pdfs) and name not in cache:
            time.sleep(1)

    if cache_hits:
        print(f"\n{cache_hits} PDF(s) loaded from cache (no API calls made for these)")

    # ------------------------------------------------------------------
    # Step 3: Consolidate
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 3: Deduplicating and consolidating")
    print("=" * 62)

    new_consolidated = consolidate(all_entries)
    print(f"Unique test names found this run: {len(new_consolidated)}")

    # ------------------------------------------------------------------
    # Step 4: Merge with existing crosswalk
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 4: Loading existing crosswalk")
    print("=" * 62)

    existing = load_crosswalk()
    if existing:
        print(f"Loaded {len(existing)} existing record(s)")
    else:
        print("No existing crosswalk — starting fresh")

    merged = merge_with_crosswalk(new_consolidated, existing)

    already_in_crosswalk = sum(1 for k in new_consolidated if k in existing)
    new_records = len(new_consolidated) - already_in_crosswalk
    print(f"  New records added:    {new_records}")
    print(f"  Already in crosswalk: {already_in_crosswalk}")

    to_enrich = [r for r in merged.values() if r.get("canonical_field") is None]
    print(f"\nRecords needing enrichment: {len(to_enrich)}")

    # ------------------------------------------------------------------
    # Step 5: Enrich
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 5: Enriching with canonical names and LOINC codes")
    print("=" * 62)

    enrich_stats = {"success": 0, "error": 0, "high": 0, "medium": 0, "low": 0}
    batches = [
        to_enrich[i : i + BATCH_SIZE] for i in range(0, len(to_enrich), BATCH_SIZE)
    ]

    for bi, batch in enumerate(batches, 1):
        print(f"\nBatch {bi}/{len(batches)} ({len(batch)} records)...")
        try:
            enriched = enrich_batch(client, batch)
        except Exception as exc:
            print(f"  ERROR: unexpected exception: {exc}")
            enriched = None

        if enriched is None:
            print(f"  Marking {len(batch)} record(s) as error")
            for r in batch:
                key = r["source_name"].strip().upper()
                merged[key]["confidence"] = "error"
            enrich_stats["error"] += len(batch)
        else:
            matched = 0
            for er in enriched:
                raw_name = er.get("source_name", "")
                key = raw_name.strip().upper()
                if key not in merged:
                    # Fuzzy fallback: try to match by position
                    print(f"  WARNING: enriched record '{raw_name}' not found in merged set")
                    continue
                merged[key].update(
                    {
                        "canonical_field": er.get("canonical_field"),
                        "loinc_code": er.get("loinc_code"),
                        "loinc_name": er.get("loinc_name"),
                        "confidence": er.get("confidence"),
                        "notes": er.get("notes"),
                    }
                )
                conf = er.get("confidence")
                if conf in enrich_stats:
                    enrich_stats[conf] += 1
                enrich_stats["success"] += 1
                matched += 1
            print(f"  -> {matched} record(s) enriched")

        if bi < len(batches):
            time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 6: Write
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("Step 6: Writing crosswalk file")
    print("=" * 62)
    write_crosswalk(merged)
    print(f"Written: {CROSSWALK_PATH}")
    print(f"Total records: {len(merged)}")

    # ------------------------------------------------------------------
    # Step 7: Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 62)
    print("SUMMARY")
    print("=" * 62)
    print(f"PDFs processed: {len(pdfs)}")
    print(f"  Function Health: {counts['Function']} file(s)")
    print(f"  Paper scans:     {counts['Paper']} file(s)")
    print(f"  Ulta Labs:       {counts['UltaLabs']} file(s)")
    print()
    print(f"Unique test names found this run: {len(new_consolidated)}")
    print(f"  New (added to crosswalk):       {new_records}")
    print(f"  Already in crosswalk:           {already_in_crosswalk}")
    print()
    print("Enrichment:")
    print(f"  Successfully enriched:   {enrich_stats['success']}")
    print(f"  High confidence LOINC:   {enrich_stats['high']}")
    print(f"  Medium confidence LOINC: {enrich_stats['medium']}")
    print(f"  Low confidence LOINC:    {enrich_stats['low']}")
    print(f"  Errors:                  {enrich_stats['error']}")

    needs_review = [
        r for r in merged.values() if r.get("confidence") in ("low", "error")
    ]
    if needs_review:
        print(f"\nRecords still needing review ({len(needs_review)}):")
        for r in sorted(
            needs_review,
            key=lambda x: (x.get("canonical_field") or "\xff", x["source_name"]),
        ):
            cf = r.get("canonical_field") or "(none)"
            print(f"  - {cf} ({r['source_name']})")

    print(f"\nCrosswalk written to:\n  {CROSSWALK_PATH}")

    if failed_pdfs:
        print(f"\nPDFs with extraction errors ({len(failed_pdfs)}):")
        for name in failed_pdfs:
            print(f"  - {name}")


if __name__ == "__main__":
    main()
