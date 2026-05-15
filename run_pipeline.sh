#!/bin/bash

# run_pipeline.sh - Legacy Helper v0.2.0
# This script is a legacy helper for maintaining compatibility.
# WARNING: The preferred method is to run 'python run_pipeline.py' directly with Prefect.

set -e  # Stop if there is an error.

# Home banner
echo "=== run_pipeline.sh - Legacy Helper v0.2.0 ==="
echo "WARNING: This is a legacy helper. Prefer 'python run_pipeline.py' for the new Python/Prefect standard."
echo ""

# Check if Python is available.
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 not found. Install Python3."
    exit 1
fi

# Check your Python version (3.9+)
py_version=$(python3 --version | grep -oP '^Python \K\d+\.\d+')
major=$(echo "$py_version" | cut -d. -f1)
minor=$(echo "$py_version" | cut -d. -f2)

if [ "$major" -lt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -lt 9 ]; }; then
    echo "❌ Error: Requires Python 3.9 or higher. Found: $(python3 --version)"
    exit 1
fi

echo "✅ Python $py_version detected. OK."

# Check if .env exists.
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found."
    echo " Create the .env file with the necessary environment variables."
    exit 1
fi

echo "✅ .env found. OK."

echo "🚀 Starting pipeline execution..."
echo ""

# Execute the pipeline.
python3 run_pipeline.py

if [ $? -eq 0 ]; then
    echo "✅ Pipeline executed successfully!"
else
    echo "❌ Error executing the pipeline."
    exit 1
fi

echo "🏁 End."