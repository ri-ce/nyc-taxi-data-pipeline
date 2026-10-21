"""Download a monthly NYC TLC yellow taxi parquet file."""

from __future__ import annotations

import argparse
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def download_month(month: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"yellow_tripdata_{month}.parquet"
    url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
    if target.exists():
        print(f"Already exists: {target}")
        return target
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, target)
    print(f"Saved to {target}")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC taxi data")
    parser.add_argument("--month", default="2025-01")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
    )
    args = parser.parse_args()
    download_month(args.month, args.output_dir)


if __name__ == "__main__":
    main()
