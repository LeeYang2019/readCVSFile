# Expense CSV Toolkit

This project turns one or more bank/exported CSV files into tidy summaries you can open in Numbers, Excel, or Google Sheets. It handles messy delimiters, mixed credits/debits, and auto-classifies transactions into custom categories.

## Features

- Robust CSV ingestion (auto-detects encoding, delimiter, quote style, CR/LF issues)
- Works with individual files or whole folders (recursive `*.csv` search)
- Normalizes dates to `YYYY-MM-DD` and trims duplicate header rows from Numbers exports
- Auto-detects expenses (negative or positive amounts) per source file
- Keyword-based categorization with debug CSVs showing matches/misses
- Combined roll-up plus per-category and per-source outputs in `expenses_outputs/`

## Requirements

- Python 3.8+
- `pandas`

Install dependencies once:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Process specific files or folders:

```bash
python -m expenses.cli ~/Downloads/checking_export.csv ~/Desktop/Statements
```

If you omit paths, the tool defaults to `~/Downloads/{default-filename}` (configurable via `--default-filename`).

### CLI options

```
usage: python -m expenses.cli [-h] [-o OUTPUT_DIR] [--default-filename DEFAULT_FILENAME] [paths ...]

positional arguments:
  paths                 CSV files or directories to process.

options:
  -o, --output-dir      Override where the combined summary is written. Defaults to the common parent of inputs.
  --default-filename    Name to look for in ~/Downloads when no paths are provided.
```

Each processed directory/file receives its own `expenses_outputs/` folder containing:

- `<name>_summary_expenses.csv`
- `<name>_category_tables/` (detail + summary for every category)
- `per_source_debug.csv`, `category_rule_matches.csv`, `category_rule_misses.csv`, `category_rule_summary.csv`

### Automator integration

Use `scripts/automator_runner.sh` as the body of an Automator “Run Shell Script” action (set shell to `/bin/zsh`). The script:

1. Logs to `~/Downloads/ExpensesRunner.log`.
2. Accepts drag-and-drop files/folders or prompts via a picker when nothing is provided.
3. Invokes `python -m expenses.cli` inside this repository so the package is on `PYTHONPATH`.

Adjust the `PYTHON`, `WORKDIR`, or `MODULE` variables near the top if your installation paths differ.

## Development

The code is organized under the `expenses` package:

- `categories.py` — keyword rules and canonical groups
- `csv_reader.py` — resilient CSV loading utilities
- `normalization.py` — header/date/money cleanup helpers
- `categorizer.py` — rule-based categorization with debug outputs
- `outputs.py` — CSV writers for summaries
- `runner.py` — orchestrates the full pipeline
- `cli.py` — argparse entry point

Run the suite manually with a representative CSV and confirm that new files appear under `expenses_outputs/`. Add/update keyword rules in `expenses/categories.py` as your spending patterns evolve.
