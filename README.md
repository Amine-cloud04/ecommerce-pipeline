# E-commerce Data Pipeline — Scala + Spark

A data pipeline for analyzing e-commerce transactions using Apache Spark and Scala.

## Pipeline stages
1. **Ingest** — Load customers, products and orders from CSV files
2. **Clean** — Remove nulls, duplicates, fix types and dates
3. **Transform** — Join datasets, compute revenue, add month/year columns
4. **Store** — Save results as Parquet files

## KPIs produced
- Total revenue
- Revenue by product category
- Top 5 products by revenue
- Top 10 customers by spending
- Monthly revenue trend (2024)
- Sales by country

## How to run
```bash
# 1. Generate data
python3 data/generate_data.py

# 2. Run the pipeline
sbt run
```

## Tech stack
- Scala 2.12
- Apache Spark 3.5.1
- sbt 1.9.7
- Java 11
- GitHub Codespaces

## Output
Results are saved to `output/` as Parquet files, one folder per KPI.
