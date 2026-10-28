#!/bin/bash
# Marketing pipeline: runs the existing scripts in order and stops on the first failure.
# The data is already generated and uploaded to HDFS (/MarketingData), so it starts from the quality checks.
# Run with: bash run_pipeline.sh   (from ~/marketing)

set -e
cd "$(dirname "$0")"

SPARK="spark-submit"

step() {
    echo
    echo "======== $1 ========"
}

# HDFS must be up and the staging data must already be there
if ! jps | grep -q NameNode; then
    echo "HDFS is not running. Start it first: start-dfs.sh && start-yarn.sh"
    exit 1
fi
if ! hdfs dfs -test -d /MarketingData; then
    echo "/MarketingData is not on HDFS. Generate and upload the data first."
    exit 1
fi

step "1/4 QUALITY CHECKS (staging)"
$SPARK marketing_quality_checks.py

step "2/4 PROCESSING (staging to processed)"
$SPARK marketing_processing.py

step "3/4 HIVE TABLES"
beeline -u jdbc:hive2:// -n hadoop --silent=true -f marketing_hive_tables.sql

step "4/4 ANALYSIS (HiveQL queries), RESULTS SAVED TO HDFS"
beeline -u jdbc:hive2:// -n hadoop --silent=true -f marketing_hive_analysis.sql
hdfs dfs -ls -R /MarketingResults
for d in $(hdfs dfs -ls /MarketingResults | awk '{print $NF}' | grep '^/'); do
    echo "--- $d"
    hdfs dfs -cat "$d"/*
done

echo
echo "======== PIPELINE FINISHED in ${SECONDS}s ========"
