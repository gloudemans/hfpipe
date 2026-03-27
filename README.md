# hfpipe

Personal health data extraction pipeline. Processes PDFs from multiple sources
into structured data using the Claude API.

---

## DEXA Body Composition

**Script:** `processors/dexa/process_dexa.py`
**Source:** `/mnt/c/obsidian/Notes/Health & Fitness/DEXA/` (GE Lunar reports)
**Output:** `data/dexa.csv`

5 scans extracted covering 2024-08-28 through 2026-03-10. Each scan produces
62 fields across 7 body regions (left/right arm, left/right leg, trunk, head,
subtotal) plus whole-body summary, densitometry, VAT, and metabolic metrics.

---

## Lab Results

Processing happens in two stages.

### Stage 1 — Field Discovery (`lab_discovery.py`)

Scans all lab PDFs to build a crosswalk of every unique test name, enriched
with a canonical `snake_case` field name and LOINC code via the Claude API.

**Sources:**
- Function Health (`Function Health/`) — 4 PDFs
- Paper scans (`Paper/`) — 20 PDFs
- Ulta Labs (`Ulta Labs/`) — 1 PDF

**Outputs:**
- `lab_field_crosswalk.json` — 195 unique test names; 191 with LOINC codes
- `lab_extraction_cache.json` — raw per-PDF test metadata (used to skip
  reprocessing on subsequent runs)

### Stage 2 — Result Extraction (`lab_extract.py`)

Goes back to each PDF and extracts actual result values for all LOINC-mapped
tests, plus file-scope metadata (lab name, collection date, report date,
ordering physician). Results are cached per PDF.

**Output:** `lab_results_cache.json`
- 25 PDFs processed
- 979 result records spanning 1989-10-31 through 2026-03-10
  - Function Health: 303 results
  - Paper scans: 674 results
  - Ulta Labs: 2 results

### Known issues
- `Function 2026-03-09 Lab Results of Record.pdf` failed field discovery
  (unparseable API response); re-run `lab_discovery.py` to retry.
- 12 LOINC codes map to more than one canonical field name (synonym
  collisions introduced during enrichment batching); needs manual crosswalk
  cleanup before building a consolidated flat table.
