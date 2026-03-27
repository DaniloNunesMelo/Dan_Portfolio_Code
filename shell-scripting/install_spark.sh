#!/bin/bash
# Author: Danilo Nunes Melo
# Creation date: 2021-08-01
# Last updated:  2021-02-22
# Spark Installation Automated
# Version: 2.0

set -euo pipefail

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if [[ "${EUID}" -ne 0 ]]; then
  echo "=== Root permission required to install ==="
  exit 1
fi

if ! command -v java &> /dev/null; then
  echo "=== Java is required but not installed. Install Java 8+ first. ==="
  exit 1
fi

if [[ $# -gt 1 ]]; then
  echo "=== Usage: $0 [spark-version.tgz] ==="
  echo "  Example: $0 spark-3.5.1-bin-hadoop3.tgz"
  echo "  Leave blank to choose interactively."
  exit 1
fi

spk_vrn="${1:-}"

# ---------------------------------------------------------------------------
# Version discovery
# ---------------------------------------------------------------------------

url_spark='https://downloads.apache.org/spark/'
spark_versions=$(curl --silent "${url_spark}" \
  | grep -oP 'href="spark-[^"]+\.tgz"' \
  | grep -oP 'spark-[^"]+\.tgz') || true

if [[ -z "${spk_vrn}" ]]; then
  PS3="Enter the Spark version to install: "
  select spk_vrn in ${spark_versions}; do
    if [[ -n "${spk_vrn}" ]]; then
      echo "${spk_vrn} selected"
      break
    fi
  done
fi

echo "=== Downloading ${spk_vrn} ==="

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

wget "${url_spark}${spk_vrn}" -P /tmp
echo "=== Download complete ==="

wget "${url_spark}${spk_vrn}.sha512" -P /tmp
echo "=== sha512sum verification ==="
(cd /tmp && sha512sum --check "${spk_vrn}.sha512")

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

echo "=== Unpacking ==="

# Strip .tgz suffix using bash parameter expansion
spk_fld="${spk_vrn%.tgz}"

if [[ ! -d "/usr/local/spark" ]]; then
  echo "=== Creating /usr/local/spark ==="
  mkdir /usr/local/spark
  tar -xf "/tmp/${spk_vrn}" -C /tmp
  mv "/tmp/${spk_fld}/"* /usr/local/spark
  echo "=== Files extracted successfully ==="
else
  echo "=== /usr/local/spark already exists, skipping extraction ==="
fi

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------

echo "=== Configuring environment ==="

if ! grep -qF "SPARK_HOME=/usr/local/spark" ~/.profile; then
  echo "export SPARK_HOME=/usr/local/spark" >> ~/.profile
  echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.profile
  echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
fi

if [[ "${spk_vrn}" == *pyspark* ]]; then
  echo "=== Configuring PySpark ==="
  chmod +x /usr/local/spark/bin/*.sh
  export SPARK_HOME=/usr/local/spark
  export PYSPARK_PYTHON=python3
  ZIPS=("${SPARK_HOME}"/python/lib/*.zip)
  export PYTHONPATH="${ZIPS[*]//:/ }:${PYTHONPATH:-}"
  echo "=== PySpark configuration complete ==="

elif [[ "${spk_vrn}" == *SparkR* ]]; then
  echo "=== Configuring SparkR ==="
  chmod +x /usr/local/spark/bin/*.sh
  export SPARK_HOME=/usr/local/spark
  echo "=== SparkR configuration complete ==="
fi

echo "=== Spark installation complete ==="
echo "=== Reload your shell or run: source ~/.profile ==="
