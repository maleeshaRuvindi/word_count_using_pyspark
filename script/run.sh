#!/bin/bash
set -e  # Stop script if any command fails

echo "Starting PySpark Job"

# Activate Conda environment
source /opt/miniconda/bin/activate pyspark_env

# Run the PySpark script
python /app/src/run.py process_data --cfg /app/config/config.yaml

echo "Job Completed"
