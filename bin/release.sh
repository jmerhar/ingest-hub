#!/usr/bin/env bash
# Cut a release: bin/release.sh X.Y.Z
#
# On a clean main, writes X.Y.Z into every manifest, dates CHANGELOG.md's Unreleased section as
# X.Y.Z's, commits `chore(release): X.Y.Z` and tags it vX.Y.Z with that section as the tag's
# message, signed when git has a signing key. The message is kept verbatim, since git would
# otherwise strip its `###` headings as comments. Nothing is pushed: the last line says how. CI
# publishes the images and the GitHub release when the tag arrives.
set -euo pipefail

cd "$(dirname "$0")/.."
version="${1:?usage: bin/release.sh X.Y.Z}"

if [ -n "$(git status --porcelain)" ]; then
  echo "The working tree has changes; commit or stash them first." >&2
  exit 1
fi
if [ "$(git symbolic-ref --short HEAD 2>/dev/null)" != "main" ]; then
  echo "Releases are cut from main." >&2
  exit 1
fi

# `set` changes nothing unless the whole release can be cut, and the notes are in hand before
# anything is committed, so a failure here leaves neither a commit nor a tag.
python3 bin/version.py set "$version"
if ! notes=$(python3 bin/version.py notes "$version"); then
  git checkout -- .
  exit 1
fi
git commit --quiet --all --message "chore(release): $version"

sign=(--annotate)
if [ -n "$(git config --get user.signingkey || true)" ]; then
  sign=(--sign)
fi
printf 'ingest-hub %s\n\n%s\n' "$version" "$notes" \
  | git tag "${sign[@]}" --cleanup=verbatim "v$version" --file=-

echo "Released $version. Publish it with: git push origin main v$version"
# An editable install records the version it was installed at, so a local run reports this one
# only once the virtualenv is reinstalled; the images install afresh.
echo "Run make install for a local make dev to report $version."
