import csv
from pathlib import Path


def get_existing_scan_dates(csv_path: str | Path) -> set[str]:
    path = Path(csv_path)
    if not path.exists():
        return set()
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        return {row["scan_date"] for row in reader if row.get("scan_date")}


def append_row(csv_path: str | Path, row: dict, fieldnames: list[str]) -> None:
    path = Path(csv_path)
    is_new = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            restval="",
        )
        if is_new:
            writer.writeheader()
        writer.writerow(row)
