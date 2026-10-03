#!/usr/bin/env bash
# usage: seen.sh snapshot|verify PATH
# snapshot: copy the seen list before the push.
# verify:   fail if any ref in that copy is missing from the pushed branch.
set -euo pipefail

mode="$1"
path="$2"
copy="/tmp/seen/$(basename "$path")"

case "$mode" in
    snapshot)
        mkdir -p /tmp/seen
        cp "$path" "$copy"
        ;;
    verify)
        git fetch -q origin "$GIT_BRANCH"
        git show "origin/$GIT_BRANCH:$path" > "$copy.pushed"
        python - "$copy" "$copy.pushed" <<'PY'
import json, sys
local = set(json.load(open(sys.argv[1])))
pushed = set(json.load(open(sys.argv[2])))
missing = local - pushed
if missing:
    sys.exit(f"seen list lost {len(missing)} refs: {sorted(missing)[:5]}")
print(f"seen list ok: {len(local)} refs on the branch")
PY
        ;;
    *)
        echo "usage: seen.sh snapshot|verify PATH" >&2
        exit 2
        ;;
esac
