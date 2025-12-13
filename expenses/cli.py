"""Command-line interface for the expense processor."""

from __future__ import annotations

import argparse
from typing import Sequence

from .runner import run_pipeline
from .categories import DEFAULT_DOWNLOAD_FILENAME


def build_parser() -> argparse.ArgumentParser:
    """
    Build command-line argument parser.
    
    Returns:
        ArgumentParser configured for expense processing options
    """
    parser = argparse.ArgumentParser(
        description="Summarize expense CSV files (single file, multiple files, or directories).",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="CSV files or directories to process. Defaults to ~/Downloads/japan_trip.csv when omitted.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        dest="output_dir",
        help="Directory for combined outputs. Defaults to the common parent of the inputs.",
    )
    parser.add_argument(
        "--default-filename",
        default=DEFAULT_DOWNLOAD_FILENAME,
        help=f"Filename to use from ~/Downloads when no paths are provided (default: {DEFAULT_DOWNLOAD_FILENAME}).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """
    CLI entry point for expense processing.
    
    Parses command-line arguments and runs the pipeline. Can be invoked:
    - Directly: python -m expenses
    - As script: python expenses/cli.py file.csv
    - From code: main(["file.csv", "-o", "/output/dir"])
    
    Args:
        argv: Command-line arguments (None = use sys.argv)
    """
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    run_pipeline(args.paths, output_dir=args.output_dir, default_filename=args.default_filename)


if __name__ == "__main__":
    main()
