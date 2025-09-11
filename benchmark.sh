#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run_elaborations_progress.sh [ROOT_DIR]
# Env (optional):
#   RARE_FILE=big.rare
#   REPORT=elaboration_report.json
#
# Live progress (NDJSON per file on STDOUT):
#   {"file_name":"path/to/file","time":"12.347s","ok":true,"elaborations":{"success":1,"failed":0,"total":1}}
#
# Rolling report is rewritten after each pair.

ROOT_DIR="${1:-$PWD}"
RARE_FILE="${RARE_FILE:-big.rare}"
REPORT="${REPORT:-$ROOT_DIR/elaboration_report.json}"

json_escape() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\n'/\\n}
  s=${s//$'\r'/\\r}
  s=${s//$'\t'/\\t}
  printf '%s' "$s"
}

# Epoch milliseconds (portable)
now_ms() {
  if date +%s%3N >/dev/null 2>&1; then
    date +%s%3N
  else
    python3 - <<'PY'
import time
print(int(time.time()*1000))
PY
  fi
}

# Humanize milliseconds to one unit: ms / s / m / h
humanize_ms() {
  local ms=$1
  if (( ms < 1000 )); then
    printf '%dms' "$ms"
  elif (( ms < 60000 )); then
    awk -v ms="$ms" 'BEGIN { printf "%.3fs", ms/1000.0 }'
  elif (( ms < 3600000 )); then
    awk -v ms="$ms" 'BEGIN { printf "%dm", int(ms/60000) }'
  else
    awk -v ms="$ms" 'BEGIN { printf "%dh", int(ms/3600000) }'
  fi
}

# Keep entries as: key|elapsed_h|success|failed|ok
entries=()

write_report() {
  local json="{"
  local first=1
  local ent key elapsed_h success failed ok total esk
  for ent in "${entries[@]}"; do
    IFS='|' read -r key elapsed_h success failed ok <<<"$ent"
    total=$(( success + failed ))
    esk="$(json_escape "$key")"
    if (( first )); then first=0; else json+=", "; fi
    json+="\"$esk\": {\"time\": \"${elapsed_h}\", \"ok\": ${ok}, \"elaborations\": {\"success\": $success, \"failed\": $failed, \"total\": $total}}"
  done
  json+="}"
  tmp="${REPORT}.tmp.$$"
  printf '%s\n' "$json" > "$tmp"
  mv "$tmp" "$REPORT"
}

printf '{}\n' > "$REPORT"

while IFS= read -r -d '' smt2; do
  dir="$(dirname "$smt2")"
  base="$(basename "$smt2" .smt2)"

  # Pair file: prefer <base>.alethe, fallback to <base>.smt2.alethe
  alethe=""
  if [[ -f "$dir/$base.alethe" ]]; then
    alethe="$dir/$base.alethe"
  elif [[ -f "$dir/$base.smt2.alethe" ]]; then
    alethe="$dir/$base.smt2.alethe"
  else
    continue
  fi

  # Key for JSON: path relative to ROOT without .smt2 extension
  rel="${smt2#$ROOT_DIR/}"
  [[ "$rel" == "$smt2" ]] && rel="$(basename "$smt2")"
  file_key="${rel%.smt2}"

  start_ms="$(now_ms)"
  tmp_out="$(mktemp)"
  set +e
  carcara elaborate \
    "$alethe" \
    "$smt2" \
    --rare-file "$RARE_FILE" \
    --no-print-with-sharing \
    --hole-solver "rare_rewrite" \
    --parse-hole-args \
    --expand-let-bindings \
    >"$tmp_out" 2>&1
  set -e
  end_ms="$(now_ms)"
  elapsed_ms=$(( end_ms - start_ms ))
  elapsed_h="$(humanize_ms "$elapsed_ms")"

  # Count markers (SUCCESS keeps using "Elaboration successed")
  success_count=$(grep -cF "Elaboration successed" "$tmp_out" || true)

  # **Changed**: failed is now detected by "Check failed:"
  failed_count=$(grep -cF "Check failed:" "$tmp_out" || true)

  # ok = false if panic OR any check failed
  if grep -qF "panicked at" "$tmp_out"; then
    ok=false
  else
    if (( failed_count > 0 )); then ok=false; else ok=true; fi
  fi
  rm -f "$tmp_out"

  entries+=("$file_key|$elapsed_h|$success_count|$failed_count|$ok")

  esk="$(json_escape "$file_key")"
  total=$(( success_count + failed_count ))
  # Stream progress (NDJSON)
  printf '{"file_name":"%s","time":"%s","ok":%s,"elaborations":{"success":%d,"failed":%d,"total":%d}}\n' \
    "$esk" "$elapsed_h" "$ok" "$success_count" "$failed_count" "$total"

  # Update rolling report
  write_report

done < <(find "$ROOT_DIR" -type f -name '*.smt2' -print0)

echo "Report written to: $REPORT" >&2
