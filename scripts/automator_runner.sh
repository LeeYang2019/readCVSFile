#!/bin/zsh
# Robust Automator runner (zsh) — logs to ~/Downloads/ExpensesRunner.log
set -o pipefail

LOGFILE="$HOME/Downloads/ExpensesRunner.log"
exec > >(tee -a "$LOGFILE") 2>&1
echo "----- $(date) ----- Automator launch -----"

PYTHON=""
WORKDIR=""
MODULE="expenses.cli"

echo "Python path: $PYTHON"
echo "Module:      $MODULE"
echo "Workdir:     $WORKDIR"
echo "Args received initially: $#"

# Try stdin if Automator pipes data
if [[ $# -eq 0 ]] && ! tty -s; then
  echo "Stdin may have data; reading paths from stdin…"
  inputs=()
  while IFS=$'\n' read -r line; do
    [[ -n "$line" ]] && inputs+=("$line")
  done
  if (( ${#inputs[@]} > 0 )); then
    set -- "${inputs[@]}"
    echo "Args after stdin read: $#"
  fi
fi

# Prompt user when nothing was provided
if [[ $# -eq 0 ]]; then
  echo "No input from Automator; opening file/folder picker…"
  PICKED=$(
    /usr/bin/osascript <<'APPLESCRIPT'
      set chosen to choose file or folder ¬
        with prompt "Select CSV files or folders" ¬
        with multiple selections allowed
      set AppleScript's text item delimiters to "\n"
      set posixList to {}
      repeat with f in chosen
        set end of posixList to POSIX path of f
      end repeat
      return posixList as text
APPLESCRIPT
  )
  IFS=$'\n' read -r -d '' -A PICKED_ARR <<< "$PICKED"$'\n'
  if (( ${#PICKED_ARR[@]} == 0 )); then
    echo "No selection. Exiting."
    osascript -e 'display notification "No selection." with title "Expenses Runner"'
    exit 4
  fi
  set -- "${PICKED_ARR[@]}"
fi

echo "Final input count: $#"
for i in "$@"; do echo "  $i"; done

[[ -x "$PYTHON" ]] || { echo "ERROR: Python not executable: $PYTHON"; exit 2; }
[[ -d "$WORKDIR" ]] || { echo "ERROR: Workdir not found: $WORKDIR"; exit 3; }

echo "Processing (python -m $MODULE)…"
(
  cd "$WORKDIR" || exit 5
  "$PYTHON" -m "$MODULE" "$@"
)
rc=$?

echo "Done. Exit code: $rc"
osascript -e 'display notification "Finished processing." with title "Expenses Runner"'
exit $rc
