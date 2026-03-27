#!/bin/bash
# Author: Danilo Melo
# Installing multiple packages with pre-installation check and summary

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 pkg1 pkg2 ..."
  echo "  Example: $0 curl wget git"
  exit 1
fi

if [[ $(id -u) -ne 0 ]]; then
  echo "Please run as root or with sudo"
  exit 2
fi

installed=0
skipped=0
failed=0

for each_pkg in "$@"; do
  if command -v "${each_pkg}" &> /dev/null; then
    echo "[SKIP] ${each_pkg} is already installed"
    (( skipped++ )) || true
  else
    echo "[INFO] Installing ${each_pkg} ..."
    if apt-get install "${each_pkg}" -y &> /dev/null; then
      echo "[OK]   ${each_pkg} installed successfully"
      (( installed++ )) || true
    else
      echo "[FAIL] Unable to install ${each_pkg}"
      (( failed++ )) || true
    fi
  fi
done

echo ""
echo "=== Summary: ${installed} installed, ${skipped} skipped, ${failed} failed ==="
