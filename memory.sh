#!/usr/bin/env bash

echo "========================================"
echo "  GIT BRANCH"
echo "========================================"
git rev-parse --abbrev-ref HEAD

echo ""
echo "========================================"
echo "  LAST 5 COMMITS"
echo "========================================"
git log --oneline --format="%ad  %h  %s" --date=short -5

echo ""
echo "========================================"
echo "  WORKING TREE STATUS"
echo "========================================"
git status --short
if [ -z "$(git status --short)" ]; then
  echo "(clean — no modified or untracked files)"
fi

echo ""
echo "========================================"
echo "  primer.md"
echo "========================================"
cat primer.md
