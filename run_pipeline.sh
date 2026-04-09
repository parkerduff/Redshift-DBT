#!/bin/bash

# DBT Time Pipeline Runner
# This script helps run the vendor data pipeline

set -e

echo "🚀 Starting DBT Time Pipeline..."

# Check if dbt is installed
if ! command -v dbt &> /dev/null; then
    echo "❌ dbt is not installed. Please install dbt first:"
    echo "   pip install dbt-core dbt-snowflake"
    exit 1
fi

echo "✅ DBT is installed"

# Install dependencies
echo "📦 Installing dbt dependencies..."
dbt deps

# Test connection
echo "🔌 Testing database connection..."
dbt debug

# Run the pipeline
echo "🔄 Running the pipeline..."
dbt run

# Run tests
echo "🧪 Running data quality tests..."
dbt test

echo "✅ Pipeline completed successfully!"
echo ""
echo "📊 To view the results, check the fact_vendor table in your prod_poc schema"
echo "🔄 To run incrementally, just run 'dbt run' again"
echo "📝 For full refresh, run 'dbt run --full-refresh'"
