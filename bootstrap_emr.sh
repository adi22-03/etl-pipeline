#!/bin/bash
# bootstrap_emr.sh
# EMR Bootstrap Action — runs on every node before Spark starts.
# Upload this to S3 and reference it in your EMR cluster config.

set -euo pipefail

echo "Installing Python dependencies..."
sudo pip3 install pyspark==3.5.1 --quiet

echo "Bootstrap complete."
