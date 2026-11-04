#!/usr/bin/env bash
# Fetch the shared build tooling a repository uses from jmerhar/devtools, and the coverage tooling
# from jmerhar/coverage, into .devtools/.
#
# Each repository commits this one file, unchanged, as bin/devtools-fetch.sh. What it fetches is
# declared in the [fetch] table of devtools.toml at the repository root:
#
#   [fetch]
#   ref = "v1"                                      # tag, branch or commit of jmerhar/devtools
#   files = ["make/devtools.mk", "bin/ci-ruff.sh"]  # paths in jmerhar/devtools
#   coverage = true                                 # also coverage-report.py and run-kcov.sh
#
# A file keeps its path from jmerhar/devtools under .devtools/, so make/devtools.mk lands at
# .devtools/make/devtools.mk. bin/devtools-config.py, which the fetched scripts read devtools.toml
# through, comes along whenever any file is listed. The coverage tooling lands in .devtools/coverage/,
# beside each other, because run-kcov.sh finds coverage-report.py in its own directory.
#
# Each file is downloaded to a temporary name and moved into place, so an interrupted transfer never
# replaces a good copy. When the network fails, the copy already on disk is kept with a warning, which
# keeps a local run working offline. Any other failure fails the run, after the remaining files have
# been tried: a file with no copy on disk, and a file the server does not have, which means a wrong ref
# or path rather than a network problem, so CI never proceeds without its tooling or with a stale copy.
#
# On a failed run .devtools/make/devtools.mk is backdated, so the make bootstrap that fetches it
# tries again on the next make instead of trusting a partial fetch.
#
# Usage: bin/devtools-fetch.sh    (from the repository root)
#   DEVTOOLS_URL             base URL of jmerhar/devtools; the ref and the path are appended
#   COVERAGE_URL             URL of the coverage tooling's directory
#   DEVTOOLS_FETCH_RETRIES   retries per file after a failed attempt (default 2)
#   DEVTOOLS_FETCH_TIMEOUT   seconds one attempt may take (default 60)
#   CURL                     the curl command (default: curl)
set -euo pipefail

: "${DEVTOOLS_URL:=https://raw.githubusercontent.com/jmerhar/devtools}"
: "${COVERAGE_URL:=https://raw.githubusercontent.com/jmerhar/coverage/v1/bin}"
: "${DEVTOOLS_FETCH_RETRIES:=2}"
: "${DEVTOOLS_FETCH_TIMEOUT:=60}"
: "${CURL:=curl}"

config="devtools.toml"
dest=".devtools"

die() {
  echo "devtools-fetch: $*" >&2
  exit 1
}

[ -f "$config" ] || die "no $config in $PWD; run this from the repository root"

# Prints the [fetch] table as lines of `ref VALUE`, `file PATH` and `coverage`, after checking every
# value, so nothing below has to parse TOML. tomllib is in the standard library from Python 3.11.
read_config() {
  # -I so that a module in the repository cannot stand in for one from the standard library.
  python3 -I - "$config" <<'PY'
import re
import sys

try:
    import tomllib
except ImportError:
    sys.exit(f"Python 3.11 or newer is required to read devtools.toml; found {sys.version.split()[0]}")

path = sys.argv[1]
try:
    with open(path, "rb") as f:
        table = tomllib.load(f).get("fetch")
except tomllib.TOMLDecodeError as e:
    sys.exit(f"{path} is not valid TOML: {e}")
if not isinstance(table, dict):
    sys.exit(f"{path} has no [fetch] table")

unknown = sorted(set(table) - {"ref", "files", "coverage"})
if unknown:
    sys.exit(f"[fetch] in {path} has unknown keys: {', '.join(unknown)}")

files = table.get("files", [])
if not isinstance(files, list) or not all(isinstance(f, str) for f in files):
    sys.exit(f"[fetch] files in {path} must be a list of paths")
# A path stays inside .devtools/: every segment starts with a letter, digit or underscore, which rules
# out absolute paths, `.` and `..`, and nothing in it is special to a shell or a URL.
for f in files:
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9._-]*(/[A-Za-z0-9_][A-Za-z0-9._-]*)*", f):
        sys.exit(f"[fetch] files in {path} has an invalid path: {f!r}")

coverage = table.get("coverage", False)
if not isinstance(coverage, bool):
    sys.exit(f"[fetch] coverage in {path} must be true or false")

ref = table.get("ref")
if files and ref is None:
    sys.exit(f"[fetch] in {path} lists files but sets no ref")
if ref is not None:
    if not isinstance(ref, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", ref) or ".." in ref:
        sys.exit(f"[fetch] ref in {path} is not a valid git ref: {ref!r}")
    print("ref", ref)

if files and "bin/devtools-config.py" not in files:
    files.append("bin/devtools-config.py")
for f in files:
    print("file", f)
if coverage:
    print("coverage")
PY
}

# curl's exit statuses for a network that is down or unreliable: DNS, connecting, a timeout, TLS,
# and a connection dropped or reset mid-transfer. Anything else (a 404, a malformed URL, a curl too
# old for an option) is not cured by keeping the copy on disk.
offline_statuses=" 5 6 7 16 28 35 52 55 56 92 "

kept=0

# Downloads one URL to a path under .devtools/. Succeeds when the file was fetched, or when the
# network failed and a copy exists (counted in $kept); fails otherwise.
fetch() {
  local url="$1" target="$2" tmp status=0
  mkdir -p "$(dirname "$target")" || return 1
  # Unique per run, so two fetches at once never move each other's partial file into place.
  tmp=$(mktemp "$target.part.XXXXXX") || return 1
  "$CURL" -fsSL --connect-timeout 10 --max-time "$DEVTOOLS_FETCH_TIMEOUT" \
    --retry "$DEVTOOLS_FETCH_RETRIES" --retry-all-errors --retry-delay 1 \
    -o "$tmp" "$url" || status=$?
  if [ "$status" -eq 0 ] && [ -s "$tmp" ]; then
    mv -f "$tmp" "$target" || { rm -f "$tmp"; return 1; }
    case "$target" in
      *.sh | *.py) chmod +x "$target" || return 1 ;;
    esac
    return 0
  fi
  rm -f "$tmp"
  if [ "$status" -eq 0 ]; then
    echo "devtools-fetch: $url is empty" >&2
  elif [[ "$offline_statuses" != *" $status "* ]]; then
    echo "devtools-fetch: could not fetch $url (curl exit $status)" >&2
  elif [ -f "$target" ]; then
    echo "devtools-fetch: warning: could not reach $url; using the copy in $target" >&2
    kept=$((kept + 1))
    return 0
  else
    echo "devtools-fetch: could not reach $url, and there is no copy in $target" >&2
  fi
  return 1
}

lines=$(read_config) || die "cannot read $config"

mkdir -p "$dest"
# The directory ignores itself, so a repository needs no .gitignore entry for it.
printf '*\n' > "$dest/.gitignore"

# make fetches again whenever devtools.toml is newer than the fetched makefile, so a config dated in
# the future would have it fetch on every run, and make 4 restart without end.
if [ "$config" -nt "$dest/.gitignore" ]; then
  die "$config is dated in the future; touch it to set its time to now"
fi

ref=""
failed=0
count=0
while read -r kind value; do
  case "$kind" in
    ref) ref="$value" ;;
    file)
      count=$((count + 1))
      fetch "$DEVTOOLS_URL/$ref/$value" "$dest/$value" || failed=$((failed + 1))
      ;;
    coverage)
      for f in coverage-report.py run-kcov.sh; do
        count=$((count + 1))
        fetch "$COVERAGE_URL/$f" "$dest/coverage/$f" || failed=$((failed + 1))
      done
      ;;
  esac
done <<< "$lines"

if [ "$failed" -gt 0 ]; then
  [ -f "$dest/make/devtools.mk" ] && touch -t 200001010000 "$dest/make/devtools.mk"
  die "$failed of $count files failed; check the network, the ref and the paths in $config"
fi
if [ "$kept" -gt 0 ]; then
  echo "devtools-fetch: $count files in $dest/, $kept of them kept from disk"
else
  echo "devtools-fetch: $count files current in $dest/"
fi
