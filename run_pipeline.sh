#!/bin/bash

PROJECT_DIR="/home/edison/data-engineering/inventory-data-platform"

cd "$PROJECT_DIR" || exit 1

source venv/bin/activate

python run_pipeline.py