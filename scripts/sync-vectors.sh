#!/usr/bin/env bash
# Copies flags-stack's conformance vectors (protocol/vectors/eval) verbatim into tests/vectors and
# records the commit in .flags-stack-rev.
#   scripts/sync-vectors.sh ~/code/flags-stack [<commit>]   (default: origin/main)
set -euo pipefail
src=${1:?usage: scripts/sync-vectors.sh <flags-stack checkout> [<commit>]}
rev=$(git -C "$src" rev-parse "${2:-origin/main}")
mkdir -p tests/vectors
rm -f tests/vectors/*.json
for f in $(git -C "$src" ls-tree --name-only "$rev" protocol/vectors/eval/ | grep '\.json$'); do
  git -C "$src" show "$rev:$f" > "tests/vectors/$(basename "$f")"
done
echo "ezgamehost/flags-stack protocol/vectors/eval at $rev (verbatim; change them there)" > tests/vectors/SOURCE
echo "$rev" > .flags-stack-rev
